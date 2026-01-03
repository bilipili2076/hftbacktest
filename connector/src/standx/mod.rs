mod market_data_stream;
pub mod msg;
mod ordermanager;
mod rest;
mod user_data_stream;

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use hftbacktest::types::{ErrorKind, LiveError, LiveEvent, Order, Status, Value};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::{broadcast, broadcast::Sender, mpsc::UnboundedSender};
use tokio_tungstenite::tungstenite;
use tracing::{debug, error, info, warn};

use self::{
    ordermanager::{OrderManager, SharedOrderManager},
    rest::StandxClient,
};
use crate::{
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent},
    utils::{ExponentialBackoff, Retry},
};

#[derive(Error, Debug)]
pub enum StandxError {
    #[error("InstrumentNotFound")]
    InstrumentNotFound,
    #[error("InvalidRequest")]
    InvalidRequest,
    #[error("ListenKeyExpired")]
    ListenKeyExpired,
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
    #[error("ConnectionAbort: {0}")]
    ConnectionAbort(String),
    #[error("ReqError: {0:?}")]
    ReqError(#[from] reqwest::Error),
    #[error("OrderError: {0}")]
    OrderError(String),
    #[error("PrefixUnmatched")]
    PrefixUnmatched,
    #[error("OrderNotFound")]
    OrderNotFound,
    #[error("Tunstenite: {0:?}")]
    Tunstenite(#[from] tungstenite::Error),
    #[error("Config: {0:?}")]
    Config(#[from] toml::de::Error),
    #[error("InvalidResponse: {0}")]
    InvalidResponse(String),
    #[error("SerdeError: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("InvalidSigningKey")]
    InvalidSigningKey,
}

impl From<StandxError> for Value {
    fn from(value: StandxError) -> Value {
        Value::String(value.to_string())
    }
}

#[derive(Deserialize, Clone)]
pub struct Config {
    #[serde(default = "default_market_ws_url")]
    pub stream_url: String,
    #[serde(default = "default_order_ws_url")]
    pub order_stream_url: String,
    #[serde(default = "default_api_url")]
    pub api_url: String,
    #[serde(default)]
    pub order_prefix: String,
    #[serde(default)]
    pub jwt_token: String,
    #[serde(default)]
    pub signing_key: String,
    #[serde(default)]
    pub session_id: String,
}

fn default_market_ws_url() -> String {
    "wss://perps.standx.com/ws-stream/v1".to_string()
}

fn default_order_ws_url() -> String {
    "wss://perps.standx.com/ws-api/v1".to_string()
}

fn default_api_url() -> String {
    "https://perps.standx.com".to_string()
}

type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;

pub struct Standx {
    config: Config,
    symbols: SharedSymbolSet,
    order_manager: SharedOrderManager,
    client: StandxClient,
    symbol_tx: Sender<String>,
}

impl Standx {
    pub fn connect_market_data_stream(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        let base_url = self.config.stream_url.clone();
        let symbol_tx = self.symbol_tx.clone();
        let client = self.client.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: StandxError| {
                    error!(
                        ?error,
                        "An error occurred in the StandX market data stream connection."
                    );
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.into(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = market_data_stream::MarketDataStream::new(
                        base_url.clone(),
                        ev_tx.clone(),
                        symbol_tx.subscribe(),
                        client.clone(),
                    );
                    stream.connect().await?;
                    Ok(())
                })
                .await;
        });
    }

    pub fn connect_user_data_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();
        let symbol_tx = self.symbol_tx.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: StandxError| {
                    error!(
                        ?error,
                        "An error occurred in the StandX user data stream connection."
                    );
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.into(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = user_data_stream::UserDataStream::new(
                        client.clone(),
                        ev_tx.clone(),
                        order_manager.clone(),
                        symbol_tx.subscribe(),
                    );
                    stream.connect().await?;
                    Ok(())
                })
                .await;
        });
    }
}

impl ConnectorBuilder for Standx {
    type Error = StandxError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        let config: Config = toml::from_str(config)?;

        let order_manager = Arc::new(Mutex::new(OrderManager::new(&config.order_prefix)));
        let session_id = if config.session_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            config.session_id.clone()
        };
        let client = StandxClient::new(
            &config.api_url,
            &config.stream_url,
            &config.order_stream_url,
            &config.jwt_token,
            &config.signing_key,
            &session_id,
        )?;
        let (symbol_tx, _) = broadcast::channel(500);

        Ok(Standx {
            config,
            symbols: Default::default(),
            order_manager,
            client,
            symbol_tx,
        })
    }
}

impl Connector for Standx {
    fn register(&mut self, symbol: String) {
        let mut symbols = self.symbols.lock().unwrap();
        if !symbols.contains(&symbol) {
            symbols.insert(symbol.clone());
            self.symbol_tx.send(symbol).unwrap();
        }
    }

    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.order_manager.clone()
    }

    fn run(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        self.connect_market_data_stream(ev_tx.clone());
        if !self.config.jwt_token.is_empty() {
            self.connect_user_data_stream(ev_tx.clone());
        }
    }

    fn submit(&self, symbol: String, mut order: Order, tx: UnboundedSender<PublishEvent>) {
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();

        tokio::spawn(async move {
            let client_order_id = order_manager
                .lock()
                .unwrap()
                .prepare_client_order_id(symbol.clone(), order.clone());

            match client_order_id {
                Some(client_order_id) => {
                    let price = if order.order_type == hftbacktest::types::OrdType::Market {
                        None
                    } else {
                        Some(order.price_tick as f64 * order.tick_size)
                    };

                    info!(
                        symbol = %symbol,
                        order_id = order.order_id,
                        client_order_id = %client_order_id,
                        side = ?order.side,
                        order_type = ?order.order_type,
                        tif = ?order.time_in_force,
                        price_tick = order.price_tick,
                        tick_size = order.tick_size,
                        price = ?price,
                        qty = order.qty,
                        "standx submit: prepared client_order_id"
                    );

                    let result = client
                        .new_order(
                            &client_order_id,
                            &symbol,
                            order.side,
                            price,
                            order.qty,
                            order.order_type,
                            order.time_in_force,
                        )
                        .await;

                    match result {
                        Ok(resp) => {
                            info!(
                                symbol = %symbol,
                                order_id = order.order_id,
                                client_order_id = %client_order_id,
                                resp_order_id = ?resp.id,
                                resp_status = ?resp.status,
                                resp_price = ?resp.price,
                                resp_qty = %resp.qty,
                                resp_fill_qty = %resp.fill_qty,
                                resp_updated_at = ?resp.updated_at,
                                "standx submit: REST ok"
                            );

                            if let Some(order) = order_manager
                                .lock()
                                .unwrap()
                                .update_from_rest(&client_order_id, &resp)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                                    .unwrap();
                            } else {
                                debug!(
                                    symbol = %symbol,
                                    order_id = order.order_id,
                                    client_order_id = %client_order_id,
                                    "standx submit: update_from_rest returned None (already removed?)"
                                );
                            }
                        }
                        Err(error) => {
                            error!(
                                symbol = %symbol,
                                order_id = order.order_id,
                                client_order_id = %client_order_id,
                                error = ?error,
                                "standx submit: REST new_order failed"
                            );

                            if let Some(order) = order_manager
                                .lock()
                                .unwrap()
                                .update_submit_fail(&client_order_id)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                                    symbol: symbol.clone(),
                                    order,
                                }))
                                    .unwrap();
                            } else {
                                warn!(
                                    symbol = %symbol,
                                    order_id = order.order_id,
                                    client_order_id = %client_order_id,
                                    "standx submit: update_submit_fail returned None (already removed?)"
                                );
                            }

                            tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                                ErrorKind::OrderError,
                                error.into(),
                            ))))
                                .unwrap();
                        }
                    }
                }
                None => {
                    warn!(
                        symbol = %symbol,
                        order_id = order.order_id,
                        price_tick = order.price_tick,
                        "standx submit: prepare_client_order_id returned None (duplicate order_id_map hit?) -> mark Expired"
                    );
                    order.req = Status::None;
                    order.status = Status::Expired;
                    tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                        .unwrap();
                }
            }
        });
    }

    fn cancel(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>) {
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();

        tokio::spawn(async move {
            info!(
                symbol = %symbol,
                order_id = order.order_id,
                "standx cancel: request received"
            );

            let client_order_id = order_manager
                .lock()
                .unwrap()
                .get_client_order_id(&symbol, order.order_id);

            match client_order_id {
                Some(client_order_id) => {
                    debug!(
                        symbol = %symbol,
                        order_id = order.order_id,
                        client_order_id = %client_order_id,
                        "standx cancel: mapped order_id -> client_order_id"
                    );

                    let result = client
                        .cancel_order(
                            &client_order_id,
                            &symbol,
                            order.side,
                            order.order_type,
                            order.time_in_force,
                            order.status,     // ✅ 取消前状态
                            order.qty,
                            order.exec_qty,
                        )
                        .await;
                    // let result = client.cancel_order(&client_order_id, &symbol).await;
                    match result {
                        Ok(resp) => {
                            info!(
                                symbol = %symbol,
                                order_id = order.order_id,
                                client_order_id = %client_order_id,
                                resp_order_id = ?resp.id,
                                resp_status = ?resp.status,
                                resp_price = ?resp.price,
                                resp_qty = %resp.qty,
                                resp_fill_qty = %resp.fill_qty,
                                resp_updated_at = ?resp.updated_at,
                                "standx cancel: REST ok"
                            );

                            if let Some(order) = order_manager
                                .lock()
                                .unwrap()
                                .update_from_rest(&client_order_id, &resp)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                                    .unwrap();
                            } else {
                                debug!(
                                    symbol = %symbol,
                                    order_id = order.order_id,
                                    client_order_id = %client_order_id,
                                    "standx cancel: update_from_rest returned None (already removed?)"
                                );
                            }
                        }
                        Err(error) => {
                            error!(
                                symbol = %symbol,
                                order_id = order.order_id,
                                client_order_id = %client_order_id,
                                error = ?error,
                                "standx cancel: REST cancel_order failed"
                            );

                            if let Some(order) = order_manager
                                .lock()
                                .unwrap()
                                .update_cancel_fail(&client_order_id)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                                    .unwrap();
                            } else {
                                warn!(
                                    symbol = %symbol,
                                    order_id = order.order_id,
                                    client_order_id = %client_order_id,
                                    "standx cancel: update_cancel_fail returned None (already removed?)"
                                );
                            }

                            tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                                ErrorKind::OrderError,
                                error.into(),
                            ))))
                                .unwrap();
                        }
                    }
                }
                None => {
                    error!(
                        symbol = %symbol,
                        order_id = order.order_id,
                        "standx cancel: client_order_id corresponding to order_id is not found (order_id_map miss)"
                    );
                }
            }
        });
    }

}
