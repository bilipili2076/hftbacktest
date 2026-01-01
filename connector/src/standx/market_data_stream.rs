use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use chrono::Utc;
use futures_util::{Sink, SinkExt, StreamExt};
use hftbacktest::{live::ipc::TO_ALL, prelude::*};
use serde::Deserialize;
use tokio::{sync::broadcast::Receiver, time};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{debug, error, info, warn};

use crate::{
    connector::PublishEvent,
    standx::{
        StandxError,
        rest::{DepthBookSnapshot, StandxClient},
    },
    utils::{parse_depth, parse_px_qty_tup},
};

type StandxClientDepth = DepthBookSnapshot;

#[derive(Debug, Deserialize)]
struct Frame {
    channel: String,
    symbol: String,
    data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct DepthBook {
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

impl From<StandxClientDepth> for DepthBook {
    fn from(value: StandxClientDepth) -> Self {
        Self {
            bids: value.bids,
            asks: value.asks,
        }
    }
}

#[derive(Debug, Deserialize)]
struct Price {
    #[serde(default)]
    spread: Option<[String; 2]>,
    #[serde(default)]
    mid_price: Option<String>,
    #[serde(default)]
    last_price: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PublicTrade {
    price: String,
    qty: String,
    #[serde(default)]
    time: Option<String>,
}

pub struct MarketDataStream {
    ws_url: String,
    ev_tx: tokio::sync::mpsc::UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
    client: StandxClient,
    snapshotted: HashSet<String>,
}

impl MarketDataStream {
    pub fn new(
        ws_url: String,
        ev_tx: tokio::sync::mpsc::UnboundedSender<PublishEvent>,
        symbol_rx: Receiver<String>,
        client: StandxClient,
    ) -> Self {
        Self {
            ws_url,
            ev_tx,
            symbol_rx,
            client,
            snapshotted: HashSet::new(),
        }
    }

    async fn subscribe<W>(&mut self, ws: &mut W) -> Result<(), StandxError>
    where
        W: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        while let Ok(symbol) = self.symbol_rx.try_recv() {
            self.subscribe_symbol(ws, symbol).await?;
        }
        Ok(())
    }

    async fn subscribe_symbol<W>(&mut self, ws: &mut W, symbol: String) -> Result<(), StandxError>
    where
        W: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        for channel in ["depth_book", "public_trade", "price"] {
            info!(?symbol, channel, "standx subscribe");

            let msg = serde_json::json!({
                "subscribe": {"channel": channel, "symbol": symbol.clone()},
            });
            ws.send(tokio_tungstenite::tungstenite::Message::Text(
                msg.to_string().into(),
            ))
            .await
            .map_err(StandxError::from)?;
        }

        if !self.snapshotted.contains(&symbol) {
            self.snapshotted.insert(symbol.clone());
            let mut me = self.clone_for_snapshot();
            tokio::spawn(async move {
                if let Err(error) = me.fetch_and_publish_snapshot(&symbol).await {
                    error!(?error, ?symbol, "Couldn't fetch StandX depth snapshot");
                }
            });
        }

        Ok(())
    }

    fn clone_for_snapshot(&self) -> Self {
        Self {
            ws_url: self.ws_url.clone(),
            ev_tx: self.ev_tx.clone(),
            symbol_rx: self.symbol_rx.resubscribe(),
            client: self.client.clone(),
            snapshotted: HashSet::new(),
        }
    }

    async fn fetch_and_publish_snapshot(&mut self, symbol: &str) -> Result<(), StandxError> {
        let depth: DepthBook = self.client.query_depth_book(symbol).await?.into();
        self.process_depth(symbol.to_string(), depth);
        Ok(())
    }

    fn process_depth(&mut self, symbol: String, book: DepthBook) {
        let bids: Vec<(String, String)> = book
            .bids
            .into_iter()
            .map(|entry| (entry[0].clone(), entry[1].clone()))
            .collect();
        let asks: Vec<(String, String)> = book
            .asks
            .into_iter()
            .map(|entry| (entry[0].clone(), entry[1].clone()))
            .collect();

        match parse_depth(bids, asks) {
            Ok((bids, asks)) => {
                let now = Utc::now().timestamp_nanos_opt().unwrap_or_default();
                self.ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap();

                for (px, qty) in bids {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: symbol.clone(),
                            event: Event {
                                ev: LOCAL_BID_DEPTH_EVENT,
                                exch_ts: now,
                                local_ts: now,
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }

                for (px, qty) in asks {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: symbol.clone(),
                            event: Event {
                                ev: LOCAL_ASK_DEPTH_EVENT,
                                exch_ts: now,
                                local_ts: now,
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }

                self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL)).unwrap();
            }
            Err(error) => {
                error!(?error, ?symbol, "Couldn't parse StandX depth book update");
            }
        }
    }

    fn process_public_trade(&mut self, symbol: String, trade: PublicTrade) {
        if let Ok((px, qty)) = parse_px_qty_tup(trade.price, trade.qty) {
            let exch_ts = trade
                .time
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(&t).ok())
                .and_then(|dt| dt.timestamp_nanos_opt())
                .unwrap_or_else(|| Utc::now().timestamp_nanos_opt().unwrap_or_default());

            self.ev_tx
                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                    symbol,
                    event: Event {
                        ev: EXCH_TRADE_EVENT,
                        exch_ts,
                        local_ts: Utc::now().timestamp_nanos_opt().unwrap_or_default(),
                        order_id: 0,
                        px,
                        qty,
                        ival: 0,
                        fval: 0.0,
                    },
                }))
                .unwrap();
        }
    }

    fn process_price(&mut self, symbol: String, price: Price) {
        if let Some(spread) = price.spread {
            if let Ok((mut bids, mut asks)) = parse_depth(
                vec![(spread[0].clone(), "1".to_string())],
                vec![(spread[1].clone(), "1".to_string())],
            ) {
                let now = Utc::now().timestamp_nanos_opt().unwrap_or_default();
                self.ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap();

                if let Some((px, qty)) = bids.pop() {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: symbol.clone(),
                            event: Event {
                                ev: LOCAL_BID_DEPTH_EVENT,
                                exch_ts: now,
                                local_ts: now,
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }

                if let Some((px, qty)) = asks.pop() {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: symbol.clone(),
                            event: Event {
                                ev: LOCAL_ASK_DEPTH_EVENT,
                                exch_ts: now,
                                local_ts: now,
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }

                self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL)).unwrap();
                return;
            }
        }

        if let Some(mid) = price
            .mid_price
            .or(price.last_price)
            .and_then(|v| v.parse::<f64>().ok())
        {
            let now = Utc::now().timestamp_nanos_opt().unwrap_or_default();
            let qty = 0.0;

            self.ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap();

            self.ev_tx
                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                    symbol: symbol.clone(),
                    event: Event {
                        ev: LOCAL_BID_DEPTH_BBO_EVENT,
                        exch_ts: now,
                        local_ts: now,
                        order_id: 0,
                        px: mid,
                        qty,
                        ival: 0,
                        fval: 0.0,
                    },
                }))
                .unwrap();

            self.ev_tx
                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                    symbol,
                    event: Event {
                        ev: LOCAL_ASK_DEPTH_BBO_EVENT,
                        exch_ts: now,
                        local_ts: now,
                        order_id: 0,
                        px: mid,
                        qty,
                        ival: 0,
                        fval: 0.0,
                    },
                }))
                .unwrap();

            self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL)).unwrap();
        }
    }

    pub async fn connect(&mut self) -> Result<(), StandxError> {
        let request = self.ws_url.clone().into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        info!("connected standx public stream");

        self.subscribe(&mut write).await?;

        let mut ping_checker = time::interval(Duration::from_secs(10));
        let mut last_ping = Instant::now();

        loop {
            tokio::select! {
                _ = ping_checker.tick() => {
                    if last_ping.elapsed() > Duration::from_secs(300) {
                        warn!("standx public stream ping timeout");
                        return Err(StandxError::ConnectionInterrupted);
                    }
                    // opportunistically refresh subscriptions
                    let _ = self.subscribe(&mut write).await;
                }
                msg = self.symbol_rx.recv() => {
                    match msg {
                        Ok(symbol) => {
                            if let Err(error) = self.subscribe_symbol(&mut write, symbol.clone()).await {
                                error!(?error, ?symbol, "standx subscribe failed");
                            }
                            // drain any additional queued symbols
                            let _ = self.subscribe(&mut write).await;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(num)) => {
                            error!(num, "standx public stream missed symbol subscription(s)");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
                message = read.next() => match message {
                    Some(Ok(Message::Text(txt))) => {
                        debug!(raw = %txt, "standx ws recv");
                        match serde_json::from_str::<Frame>(&txt) {
                            Ok(frame) => match frame.channel.as_str() {
                                "depth_book" => {
                                    if let Ok(book) = serde_json::from_value::<DepthBook>(frame.data) {
                                        self.process_depth(frame.symbol, book);
                                    }
                                }
                                "public_trade" => {
                                    if let Ok(trade) = serde_json::from_value::<PublicTrade>(frame.data) {
                                        self.process_public_trade(frame.symbol, trade);
                                    }
                                }
                                "price" => {
                                    if let Ok(price) = serde_json::from_value::<Price>(frame.data) {
                                        self.process_price(frame.symbol, price);
                                    }
                                }
                                _ => {}
                            },
                            Err(error) => {
                                error!(?error, "failed to parse standx market data message");
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        debug!("standx ws recv ping");
                        write.send(Message::Pong(data)).await?;
                        last_ping = Instant::now();
                    }
                    Some(Ok(Message::Pong(_))) => {
                        last_ping = Instant::now();
                    }
                    Some(Ok(msg)) => {
                        debug!(?msg, "standx ws recv non-text");
                    }
                    Some(Err(error)) => return Err(StandxError::from(error)),
                    None => return Err(StandxError::ConnectionInterrupted),
                }
            }
        }
    }
}
