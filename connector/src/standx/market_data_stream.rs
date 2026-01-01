use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast::Receiver;
use tokio_tungstenite::connect_async;
use tracing::{error, info};

use crate::{
    connector::PublishEvent,
    standx::{StandxError, msg::stream::Frame},
};

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
            let msg = serde_json::json!({
                "method": "subscribe",
                "channels": ["depth", "trade"],
                "symbols": [symbol],
            });
            ws.send(tokio_tungstenite::tungstenite::Message::Text(
                msg.to_string().into(),
            ))
            .await
            .map_err(StandxError::from)?;
        }
        Ok(())
    }

    pub async fn connect(&mut self) -> Result<(), StandxError> {
        let url = self.ws_url.clone();
        let (mut ws, _) = connect_async(url).await?;
        info!("connected standx public stream");

        self.subscribe(&mut ws).await?;

        while let Some(msg) = ws.next().await {
            let msg = msg?;
            if msg.is_text() {
                if serde_json::from_str::<Frame>(&msg.to_text()?).is_err() {
                    error!("failed to parse standx market data message");
                }
            }
            // Refresh subscriptions when new symbols arrive.
            let _ = self.subscribe(&mut ws).await;
        }

        Ok(())
    }
}
