use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::{live::ipc::TO_ALL, prelude::*};
use serde::Deserialize;
use tokio::sync::broadcast::Receiver;
use tokio_tungstenite::connect_async;
use tracing::{error, info};

use crate::{
    connector::PublishEvent,
    standx::StandxError,
    utils::{parse_depth, parse_px_qty_tup},
};

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
}

impl MarketDataStream {
    pub fn new(
        ws_url: String,
        ev_tx: tokio::sync::mpsc::UnboundedSender<PublishEvent>,
        symbol_rx: Receiver<String>,
    ) -> Self {
        Self {
            ws_url,
            ev_tx,
            symbol_rx,
        }
    }

    async fn subscribe(
        &mut self,
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Result<(), StandxError> {
        while let Ok(symbol) = self.symbol_rx.try_recv() {
            for channel in ["depth_book", "public_trade", "price"] {
                let msg = serde_json::json!({
                    "subscribe": {"channel": channel, "symbol": symbol.clone()},
                });
                ws.send(tokio_tungstenite::tungstenite::Message::Text(
                    msg.to_string().into(),
                ))
                .await
                .map_err(StandxError::from)?;
            }
        }
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
        let url = self.ws_url.clone();
        let (mut ws, _) = connect_async(url).await?;
        info!("connected standx public stream");

        self.subscribe(&mut ws).await?;

        while let Some(msg) = ws.next().await {
            let msg = msg?;
            if msg.is_text() {
                match serde_json::from_str::<Frame>(&msg.to_text()?) {
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
            // Refresh subscriptions when new symbols arrive.
            let _ = self.subscribe(&mut ws).await;
        }

        Ok(())
    }
}
