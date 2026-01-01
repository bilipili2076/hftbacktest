use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast::Receiver;
use tokio_tungstenite::connect_async;
use tracing::{debug, error, info};

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
        let (mut ws, _) = connect_async(url).await?;
        info!("connected standx market stream with auth");

        let auth = serde_json::json!({
            "auth": {"token": self.client.jwt_token()},
            "streams": [{"channel": "order"}],
        });
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            auth.to_string(),
        ))
        .await?;

        let subscribe = serde_json::json!({
            "subscribe": {"channel": "order"}
        });
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            subscribe.to_string(),
        ))
        .await?;

        while let Some(msg) = ws.next().await {
            let msg = msg?;
            if msg.is_text() {
                match serde_json::from_str::<Frame>(&msg.to_text()?).map(|frame| frame.into_event())
                {
                    Ok(StreamEvent::Order(update)) => {
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
                                error!(?error, "order update rejected");
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
                    Ok(StreamEvent::Unknown) => {}
                    Err(error) => {
                        error!(?error, "failed to parse standx private stream message");
                    }
                }
            }

            // keep heartbeat alive via symbols channel.
            match self.symbol_rx.try_recv() {
                Ok(_) => debug!("symbol update received on private stream"),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {}
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            }
        }

        Ok(())
    }
}
