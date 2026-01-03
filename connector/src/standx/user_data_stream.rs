use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use std::time::{Duration, Instant};
use tokio::{sync::broadcast::Receiver, time};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{debug, error, info, warn};

use crate::{
    connector::{GetOrders, PublishEvent},
    standx::{
        StandxError,
        msg::stream::{Frame, StreamEvent},
        ordermanager::SharedOrderManager,
        rest::StandxClient,
    },
};

pub struct UserDataStream {
    client: StandxClient,
    ev_tx: tokio::sync::mpsc::UnboundedSender<PublishEvent>,
    order_manager: SharedOrderManager,
    symbol_rx: Receiver<String>,
}

impl UserDataStream {
    pub fn new(
        client: StandxClient,
        ev_tx: tokio::sync::mpsc::UnboundedSender<PublishEvent>,
        order_manager: SharedOrderManager,
        symbol_rx: Receiver<String>,
    ) -> Self {
        Self {
            client,
            ev_tx,
            order_manager,
            symbol_rx,
        }
    }

    pub async fn connect(&mut self) -> Result<(), StandxError> {
        let url = self.client.market_ws_url().to_string();
        let request = url.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        info!("connected standx market stream with auth");

        let auth = serde_json::json!({
            "auth": {
                "token": self.client.jwt_token(),
                "streams": [
                    {"channel": "order"},
                    {"channel": "position"},
                    {"channel": "balance"},
                ],
            },
        });
        info!("standx auth request sent");
        write.send(Message::Text(auth.to_string().into())).await?;

        // bootstrap position snapshot once we are authenticated
        let client = self.client.clone();
        let ev_tx = self.ev_tx.clone();
        tokio::spawn(async move {
            match client.query_positions(None).await {
                Ok(positions) => {
                    for pos in positions {
                        if let Ok(qty) = pos.qty.parse::<f64>() {
                            let exch_ts = pos
                                .updated_at
                                .as_ref()
                                .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
                                .and_then(|dt| dt.timestamp_nanos_opt())
                                .unwrap_or_else(|| {
                                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
                                });

                            ev_tx
                                .send(PublishEvent::LiveEvent(
                                    hftbacktest::types::LiveEvent::Position {
                                        symbol: pos.symbol.clone(),
                                        qty,
                                        exch_ts,
                                    },
                                ))
                                .ok();
                        }
                    }
                }
                Err(error) => error!(?error, "standx position bootstrap failed"),
            }
        });

        let mut ping_checker = time::interval(Duration::from_secs(10));
        let mut last_ping = Instant::now();
        let mut authed = false;
        let mut subscribed = false;

        loop {
            tokio::select! {
                _ = ping_checker.tick() => {
                    if last_ping.elapsed() > Duration::from_secs(300) {
                        warn!("standx private stream ping timeout");
                        return Err(StandxError::ConnectionInterrupted);
                    }
                }
                msg = self.symbol_rx.recv() => {
                    match msg {
                        Ok(_) => debug!("symbol update received on private stream"),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {},
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                message = read.next() => match message {
                    Some(Ok(Message::Text(txt))) => {
                        // debug!(raw = %txt, "standx ws recv");

                        // handle auth ack/err before decoding into channel events
                        if !authed {
                            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&txt) {
                                let channel = val.get("channel").and_then(|c| c.as_str());
                                let code = val
                                    .get("data")
                                    .and_then(|d| d.get("code"))
                                    .or_else(|| val.get("code"))
                                    .and_then(|c| c.as_i64());

                                if channel == Some("auth") {
                                    match code {
                                        Some(0) | Some(200) => {
                                            authed = true;
                                            info!("standx private auth ok");
                                        }
                                        Some(code) => {
                                            error!(code, "standx private auth failed");
                                        }
                                        None => {
                                            error!("standx private auth missing code");
                                        }
                                    }
                                }
                            }

                            if !authed {
                                continue;
                            }
                        }

                        if authed && !subscribed {
                            for channel in ["order", "position", "balance"] {
                                let subscribe = serde_json::json!({
                                    "subscribe": {"channel": channel}
                                });
                                info!(channel, "standx private subscribe");
                                write
                                    .send(Message::Text(subscribe.to_string().into()))
                                    .await?;
                            }
                            subscribed = true;
                        }

                        match serde_json::from_str::<Frame>(&txt).map(|frame| frame.into_event()) {
                            Ok(StreamEvent::Order(update)) => {
                                debug!(symbol=%update.symbol, cl_ord_id=?update.cl_ord_id, status=?update.status, side=?update.side, qty=%update.qty, fill_qty=%update.fill_qty, price=?update.price, fill_avg_price=?update.fill_avg_price, updated_at=?update.updated_at, "standx order update");

                                match self.order_manager.lock().unwrap().update_from_ws(&update) {
                                    Ok(Some(order)) => {
                                        self.ev_tx
                                            .send(PublishEvent::LiveEvent(
                                                hftbacktest::types::LiveEvent::Order {
                                                    symbol: update.symbol.clone(),
                                                    order,
                                                },
                                            ))
                                            .unwrap();
                                    }
                                    Ok(None) => {}
                                    Err(error) => {
                                        error!(?error, symbol=%update.symbol, cl_ord_id=?update.cl_ord_id, status=?update.status, "order update rejected");

                                        self.ev_tx
                                            .send(PublishEvent::LiveEvent(
                                                hftbacktest::types::LiveEvent::Error(
                                                    hftbacktest::types::LiveError::with(
                                                        hftbacktest::types::ErrorKind::OrderError,
                                                        error.into(),
                                                    ),
                                                ),
                                            ))
                                            .unwrap();
                                    }
                                }
                            }
                            Ok(StreamEvent::Position(update)) => {
                                debug!(?update, "standx position update");
                                if let Ok(qty) = update.qty.parse::<f64>() {
                                    let exch_ts = update
                                        .updated_at
                                        .as_ref()
                                        .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
                                        .and_then(|dt| dt.timestamp_nanos_opt())
                                        .unwrap_or_else(|| {
                                            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
                                        });

                                    self.ev_tx
                                        .send(PublishEvent::LiveEvent(
                                            hftbacktest::types::LiveEvent::Position {
                                                symbol: update.symbol.clone(),
                                                qty,
                                                exch_ts,
                                            },
                                        ))
                                        .unwrap();
                                }
                            }
                            Ok(StreamEvent::Balance(update)) => {
                                debug!(?update, "standx balance update");
                            }
                            Ok(StreamEvent::Unknown) => {}
                            Err(error) => {
                                error!(?error, "failed to parse standx private stream message");
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

        Ok(())
    }
}
