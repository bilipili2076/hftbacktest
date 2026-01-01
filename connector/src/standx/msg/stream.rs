use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use serde::Deserialize;

use super::{from_str_to_ord_type, from_str_to_side, from_str_to_status, from_str_to_tif};

#[derive(Debug, Deserialize)]
pub struct Frame {
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub data: serde_json::Value,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum StreamEvent {
    Order(OrderUpdate),
    Position(PositionUpdate),
    Balance(BalanceUpdate),
    Unknown,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OrderUpdate {
    #[serde(default)]
    pub cl_ord_id: Option<String>,
    pub symbol: String,
    #[serde(deserialize_with = "from_str_to_status")]
    pub status: Status,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(deserialize_with = "from_str_to_ord_type")]
    pub order_type: OrdType,
    #[serde(deserialize_with = "from_str_to_tif")]
    pub time_in_force: TimeInForce,
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
pub struct PositionUpdate {
    pub symbol: String,
    pub qty: String,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BalanceUpdate {
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub total: Option<String>,
    #[serde(default)]
    pub free: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

impl OrderUpdate {
    pub fn updated_at_ns(&self) -> Option<i64> {
        self.updated_at
            .as_ref()
            .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
            .and_then(|dt| dt.timestamp_nanos_opt())
    }
}

impl Frame {
    pub fn into_event(self) -> StreamEvent {
        match self.channel.as_deref() {
            Some("order") => serde_json::from_value::<OrderUpdate>(self.data)
                .map(StreamEvent::Order)
                .unwrap_or(StreamEvent::Unknown),
            Some("position") => serde_json::from_value::<PositionUpdate>(self.data)
                .map(StreamEvent::Position)
                .unwrap_or(StreamEvent::Unknown),
            Some("balance") => serde_json::from_value::<BalanceUpdate>(self.data)
                .map(StreamEvent::Balance)
                .unwrap_or(StreamEvent::Unknown),
            _ => StreamEvent::Unknown,
        }
    }
}
