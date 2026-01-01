use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use serde::{Deserialize, Serialize};

use super::{
    from_str_to_ord_type, from_str_to_side, from_str_to_status, from_str_to_tif, serialize_ord_type,
    serialize_side, serialize_tif,
};

#[derive(Debug, Serialize)]
pub struct SubmitOrderRequest {
    pub client_order_id: String,
    pub symbol: String,
    #[serde(serialize_with = "serialize_side")]
    pub side: Side,
    pub price: f64,
    pub qty: f64,
    #[serde(serialize_with = "serialize_ord_type")]
    pub order_type: OrdType,
    #[serde(serialize_with = "serialize_tif")]
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
    #[serde(deserialize_with = "from_str_to_status")]
    pub status: Status,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(deserialize_with = "from_str_to_ord_type")]
    pub order_type: OrdType,
    #[serde(deserialize_with = "from_str_to_tif")]
    pub time_in_force: TimeInForce,
    pub price: f64,
    pub qty: f64,
    pub executed_qty: f64,
    pub cum_qty: f64,
    pub update_time: i64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CancelAllResponse {
    pub canceled: Vec<String>,
}
