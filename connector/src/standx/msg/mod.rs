use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use serde::{
    Deserialize,
    Deserializer,
    Serializer,
    de::{Error, Unexpected},
};

pub mod rest;
pub mod stream;

pub fn serialize_side<S>(side: &Side, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
        _ => "NONE",
    })
}

pub fn serialize_ord_type<S>(ord_type: &OrdType, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(match ord_type {
        OrdType::Limit => "LIMIT",
        OrdType::Market => "MARKET",
        _ => "UNSUPPORTED",
    })
}

pub fn serialize_tif<S>(tif: &TimeInForce, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(match tif {
        TimeInForce::GTC => "GTC",
        TimeInForce::GTX => "GTX",
        TimeInForce::FOK => "FOK",
        TimeInForce::IOC => "IOC",
        _ => "UNSUPPORTED",
    })
}

pub fn from_str_to_side<'de, D>(deserializer: D) -> Result<Side, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match s.to_ascii_uppercase().as_str() {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        _ => Err(Error::invalid_value(Unexpected::Other(s), &"BUY or SELL")),
    }
}

pub fn from_str_to_status<'de, D>(deserializer: D) -> Result<Status, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match s.to_ascii_uppercase().as_str() {
        "NEW" => Ok(Status::New),
        "PARTIALLY_FILLED" => Ok(Status::PartiallyFilled),
        "FILLED" => Ok(Status::Filled),
        "CANCELED" | "CANCELLED" => Ok(Status::Canceled),
        "EXPIRED" => Ok(Status::Expired),
        _ => Err(Error::invalid_value(
            Unexpected::Other(s),
            &"NEW,PARTIALLY_FILLED,FILLED,CANCELED,EXPIRED",
        )),
    }
}

pub fn from_str_to_ord_type<'de, D>(deserializer: D) -> Result<OrdType, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match s.to_ascii_uppercase().as_str() {
        "LIMIT" => Ok(OrdType::Limit),
        "MARKET" => Ok(OrdType::Market),
        _ => Err(Error::invalid_value(Unexpected::Other(s), &"LIMIT or MARKET")),
    }
}

pub fn from_str_to_tif<'de, D>(deserializer: D) -> Result<TimeInForce, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match s.to_ascii_uppercase().as_str() {
        "GTC" => Ok(TimeInForce::GTC),
        "GTX" => Ok(TimeInForce::GTX),
        "IOC" => Ok(TimeInForce::IOC),
        "FOK" => Ok(TimeInForce::FOK),
        _ => Err(Error::invalid_value(
            Unexpected::Other(s),
            &"GTC,GTX,IOC,FOK",
        )),
    }
}
