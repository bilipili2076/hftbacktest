use hftbacktest::types::{OrderType, Side, Status, TimeInForce};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEvent {
    #[serde(rename = "depth")]
    Depth(Depth),
    #[serde(rename = "trade")]
    Trade(Trade),
    #[serde(rename = "order_update")]
    OrderUpdate(OrderUpdate),
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Depth {
    pub symbol: String,
    pub bids: Vec<[f64; 2]>,
    pub asks: Vec<[f64; 2]>,
    #[serde(default)]
    pub ts: i64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Trade {
    pub symbol: String,
    pub price: f64,
    pub qty: f64,
    #[serde(default)]
    pub side: Side,
    #[serde(default)]
    pub ts: i64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OrderUpdate {
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
    pub leaves_qty: f64,
    #[serde(default)]
    pub filled_qty: f64,
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub last_filled_price: f64,
    #[serde(default)]
    pub ts: i64,
}
