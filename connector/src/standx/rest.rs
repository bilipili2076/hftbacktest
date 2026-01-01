use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use sha2::Sha256;
use tokio::time::Duration;

use crate::standx::{
    StandxError,
    msg::rest::{CancelAllResponse, CancelOrderRequest, OrderResponse, SubmitOrderRequest},
};

#[derive(Clone)]
pub struct StandxClient {
    api_url: String,
    api_key: String,
    api_secret: String,
    http: reqwest::Client,
}

impl StandxClient {
    pub fn new(api_url: &str, api_key: &str, api_secret: &str) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if !api_key.is_empty() {
            headers.insert("X-API-KEY", HeaderValue::from_str(api_key).unwrap());
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to create http client");

        Self {
            api_url: api_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            http: client,
        }
    }

    pub fn api_url(&self) -> &str {
        &self.api_url
    }

    fn sign_payload(&self, payload: &str) -> Result<String, StandxError> {
        if self.api_secret.is_empty() {
            return Ok(String::new());
        }
        let mut mac = Hmac::<Sha256>::new_from_slice(self.api_secret.as_bytes())?;
        mac.update(payload.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }

    pub async fn create_private_token(&self) -> Result<String, StandxError> {
        let expires = Utc::now().timestamp();
        let payload = format!("timestamp={expires}");
        let signature = self.sign_payload(&payload)?;
        let url = format!(
            "{}/perps/ws-token?{}&signature={}",
            self.api_url, payload, signature
        );
        let resp = self.http.post(url).send().await?.error_for_status()?;
        let value: serde_json::Value = resp.json().await?;
        value
            .get("token")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| StandxError::InvalidResponse("token".into()))
    }

    pub async fn submit_order(
        &self,
        client_order_id: &str,
        symbol: &str,
        side: hftbacktest::types::Side,
        price: f64,
        qty: f64,
        order_type: hftbacktest::types::OrdType,
        time_in_force: hftbacktest::types::TimeInForce,
    ) -> Result<OrderResponse, StandxError> {
        let request = SubmitOrderRequest {
            client_order_id: client_order_id.to_string(),
            symbol: symbol.to_string(),
            side,
            price,
            qty,
            order_type,
            time_in_force,
        };
        let payload = serde_json::to_string(&request)?;
        let signature = self.sign_payload(&payload)?;
        let url = format!("{}/perps/orders", self.api_url);
        let resp = self
            .http
            .post(url)
            .query(&[("signature", signature)])
            .body(payload)
            .send()
            .await?
            .error_for_status()?;
        Ok(resp.json::<OrderResponse>().await?)
    }

    pub async fn cancel_order(
        &self,
        client_order_id: &str,
        symbol: &str,
    ) -> Result<OrderResponse, StandxError> {
        let request = CancelOrderRequest {
            client_order_id: client_order_id.to_string(),
            symbol: symbol.to_string(),
        };
        let payload = serde_json::to_string(&request)?;
        let signature = self.sign_payload(&payload)?;
        let url = format!("{}/perps/orders", self.api_url);
        let resp = self
            .http
            .delete(url)
            .query(&[("signature", signature)])
            .body(payload)
            .send()
            .await?
            .error_for_status()?;
        Ok(resp.json::<OrderResponse>().await?)
    }

    pub async fn cancel_all(&self, symbol: &str) -> Result<CancelAllResponse, StandxError> {
        let payload = format!("symbol={symbol}");
        let signature = self.sign_payload(&payload)?;
        let url = format!(
            "{}/perps/orders/all?{}&signature={}",
            self.api_url, payload, signature
        );
        let resp = self.http.delete(url).send().await?.error_for_status()?;
        Ok(resp.json::<CancelAllResponse>().await?)
    }
}
