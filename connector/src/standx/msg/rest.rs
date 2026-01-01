use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use serde::{Deserialize, Serialize};

use super::{from_str_to_ord_type, from_str_to_side, from_str_to_status, from_str_to_tif};

#[derive(Debug, Serialize)]
pub struct NewOrderRequest {
    pub symbol: String,
    #[serde(serialize_with = "super::serialize_side")]
    pub side: Side,
    #[serde(serialize_with = "super::serialize_ord_type")]
    pub order_type: OrdType,
    pub qty: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    #[serde(serialize_with = "super::serialize_tif")]
    pub time_in_force: TimeInForce,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cl_ord_id: Option<String>,
    #[serde(default)]
    pub reduce_only: bool,
}

#[derive(Debug, Serialize)]
pub struct CancelOrderRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cl_ord_id: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BasicResponse {
    pub code: i64,
    pub message: String,
    #[serde(default)]
    pub request_id: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OrderDetail {
    #[serde(default)]
    pub id: Option<i64>,
    #[serde(default)]
    pub cl_ord_id: Option<String>,
    pub symbol: String,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(deserialize_with = "from_str_to_ord_type")]
    pub order_type: OrdType,
    #[serde(deserialize_with = "from_str_to_tif")]
    pub time_in_force: TimeInForce,
    #[serde(deserialize_with = "from_str_to_status")]
    pub status: Status,
    pub qty: String,
    pub fill_qty: String,
    #[serde(default)]
    pub fill_avg_price: Option<String>,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PositionSnapshot {
    pub symbol: String,
    pub qty: String,
    #[serde(default)]
    pub updated_at: Option<String>,
}

impl OrderDetail {
    pub fn updated_at_ns(&self) -> Option<i64> {
        self.updated_at
            .as_ref()
            .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
            .and_then(|dt| dt.timestamp_nanos_opt())
    }
}
