use hftbacktest::types::{OrderType, Side, Status, TimeInForce};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct SubmitOrderRequest {
    pub client_order_id: String,
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    pub qty: f64,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
}

#[derive(Debug, Serialize)]
pub struct CancelOrderRequest {
    pub client_order_id: String,
    pub symbol: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OrderResponse {
    pub order_id: i64,
    pub client_order_id: String,
    pub symbol: String,
    #[serde(default)]
    pub status: Status,
    #[serde(default)]
    pub side: Side,
    #[serde(default)]
    pub order_type: OrderType,
    #[serde(default)]
    pub time_in_force: TimeInForce,
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub qty: f64,
    #[serde(default)]
    pub executed_qty: f64,
    #[serde(default)]
    pub cum_qty: f64,
    #[serde(default)]
    pub update_time: i64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CancelAllResponse {
    pub canceled: Vec<String>,
}
