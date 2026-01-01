use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use serde::{
    Deserialize, Deserializer, Serializer,
    de::{Error, Unexpected},
};

pub mod rest;
pub mod stream;

pub fn serialize_side<S>(side: &Side, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
        _ => "none",
    })
}

pub fn serialize_ord_type<S>(ord_type: &OrdType, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(match ord_type {
        OrdType::Limit => "limit",
        OrdType::Market => "market",
        _ => "unsupported",
    })
}

pub fn serialize_tif<S>(tif: &TimeInForce, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(match tif {
        TimeInForce::GTC => "gtc",
        TimeInForce::IOC => "ioc",
        // StandX uses `alo` for post-only; map to GTX internally
        TimeInForce::GTX => "alo",
        TimeInForce::FOK => "fok",
        _ => "unsupported",
    })
}

pub fn from_str_to_side<'de, D>(deserializer: D) -> Result<Side, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match s.to_ascii_lowercase().as_str() {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        _ => Err(Error::invalid_value(Unexpected::Other(s), &"buy or sell")),
    }
}

pub fn from_str_to_status<'de, D>(deserializer: D) -> Result<Status, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match s.to_ascii_lowercase().as_str() {
        "new" | "open" => Ok(Status::New),
        "partially_filled" | "partial" => Ok(Status::PartiallyFilled),
        "filled" => Ok(Status::Filled),
        "canceled" | "cancelled" => Ok(Status::Canceled),
        "rejected" => Ok(Status::Rejected),
        "untriggered" | "expired" => Ok(Status::Expired),
        other => Err(Error::invalid_value(
            Unexpected::Other(other),
            &"open,filled,canceled,rejected,untriggered",
        )),
    }
}

pub fn from_str_to_ord_type<'de, D>(deserializer: D) -> Result<OrdType, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match s.to_ascii_lowercase().as_str() {
        "limit" => Ok(OrdType::Limit),
        "market" => Ok(OrdType::Market),
        _ => Err(Error::invalid_value(
            Unexpected::Other(s),
            &"limit or market",
        )),
    }
}

pub fn from_str_to_tif<'de, D>(deserializer: D) -> Result<TimeInForce, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match s.to_ascii_lowercase().as_str() {
        "gtc" => Ok(TimeInForce::GTC),
        "alo" => Ok(TimeInForce::GTX),
        "ioc" => Ok(TimeInForce::IOC),
        "fok" => Ok(TimeInForce::FOK),
        other => Err(Error::invalid_value(
            Unexpected::Other(other),
            &"gtc,alo,ioc,fok",
        )),
    }
}
