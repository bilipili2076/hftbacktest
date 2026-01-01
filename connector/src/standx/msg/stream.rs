use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use serde::Deserialize;

use super::{from_str_to_ord_type, from_str_to_side, from_str_to_status, from_str_to_tif};

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
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(default)]
    pub ts: i64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OrderUpdate {
    pub client_order_id: String,
    pub symbol: String,
    #[serde(deserialize_with = "from_str_to_status")]
    pub status: Status,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(deserialize_with = "from_str_to_ord_type")]
    pub order_type: OrdType,
    #[serde(deserialize_with = "from_str_to_tif")]
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
