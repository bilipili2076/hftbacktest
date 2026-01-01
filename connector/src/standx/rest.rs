use std::str::FromStr;

use base64::Engine;
use ed25519_dalek::{SigningKey, pkcs8::DecodePrivateKey};
use hftbacktest::types::{OrdType, Side, TimeInForce};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use tokio::time::Duration;

use crate::standx::{
    StandxError,
    msg::rest::{BasicResponse, CancelOrderRequest, NewOrderRequest, OrderDetail},
};

#[derive(Clone)]
pub struct StandxClient {
    api_url: String,
    market_ws_url: String,
    order_ws_url: String,
    jwt_token: String,
    signing_key: Option<SigningKey>,
    session_id: String,
    http: reqwest::Client,
}

fn parse_signing_key(raw: &str) -> Result<Option<SigningKey>, StandxError> {
    if raw.trim().is_empty() {
        return Ok(None);
    }

    if let Ok(key) = SigningKey::from_pkcs8_pem(raw) {
        return Ok(Some(key));
    }

    let engine = base64::engine::general_purpose::STANDARD;
    if let Ok(decoded) = engine.decode(raw.as_bytes()) {
        if decoded.len() == 32 {
            let mut buf = [0u8; 32];
            buf.copy_from_slice(&decoded);
            return Ok(Some(SigningKey::from_bytes(&buf)));
        }
    }

    if raw.len() == 64 {
        if let Ok(decoded) = hex::decode(raw) {
            if decoded.len() == 32 {
                let mut buf = [0u8; 32];
                buf.copy_from_slice(&decoded);
                return Ok(Some(SigningKey::from_bytes(&buf)));
            }
        }
    }

    Err(StandxError::InvalidSigningKey)
}

impl StandxClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        api_url: &str,
        market_ws_url: &str,
        order_ws_url: &str,
        jwt_token: &str,
        signing_key: &str,
        session_id: &str,
    ) -> Result<Self, StandxError> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to create http client");

        Ok(Self {
            api_url: api_url.trim_end_matches('/').to_string(),
            market_ws_url: market_ws_url.to_string(),
            order_ws_url: order_ws_url.to_string(),
            jwt_token: jwt_token.to_string(),
            signing_key: parse_signing_key(signing_key)?,
            session_id: session_id.to_string(),
            http,
        })
    }

    pub fn market_ws_url(&self) -> &str {
        &self.market_ws_url
    }

    pub fn order_ws_url(&self) -> &str {
        &self.order_ws_url
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn jwt_token(&self) -> &str {
        &self.jwt_token
    }

    fn signing_headers(&self, payload: &str) -> Result<HeaderMap, StandxError> {
        let mut headers = HeaderMap::new();
        if !self.jwt_token.is_empty() {
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", self.jwt_token)).unwrap(),
            );
        }

        let Some(signing_key) = &self.signing_key else {
            return Ok(headers);
        };

        let request_id = uuid::Uuid::new_v4().to_string();
        let timestamp = chrono::Utc::now().timestamp_millis();
        let sign_msg = format!("v1,{request_id},{timestamp},{payload}");
        let signature = signing_key.sign(sign_msg.as_bytes());
        headers.insert("x-request-sign-version", HeaderValue::from_static("v1"));
        headers.insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
        headers.insert(
            "x-request-timestamp",
            HeaderValue::from_str(&timestamp.to_string()).unwrap(),
        );
        headers.insert(
            "x-request-signature",
            HeaderValue::from_str(&base64::engine::general_purpose::STANDARD.encode(signature))
                .unwrap(),
        );
        Ok(headers)
    }

    pub async fn new_order(
        &self,
        client_order_id: &str,
        symbol: &str,
        side: Side,
        price: Option<f64>,
        qty: f64,
        order_type: OrdType,
        time_in_force: TimeInForce,
    ) -> Result<OrderDetail, StandxError> {
        let request = NewOrderRequest {
            symbol: symbol.to_string(),
            side,
            order_type,
            qty: qty.to_string(),
            price: price.map(|p| p.to_string()),
            time_in_force,
            cl_ord_id: Some(client_order_id.to_string()),
            reduce_only: false,
        };

        let payload = serde_json::to_string(&request)?;
        let mut headers = self.signing_headers(&payload)?;
        headers.insert(
            "x-session-id",
            HeaderValue::from_str(self.session_id()).unwrap(),
        );

        let url = format!("{}/api/new_order", self.api_url);
        let resp = self
            .http
            .post(url)
            .headers(headers)
            .body(payload)
            .send()
            .await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(StandxError::InvalidResponse(body));
        }
        let ack: BasicResponse = serde_json::from_str(&body)?;
        if ack.code != 0 {
            return Err(StandxError::OrderError(ack.message));
        }

        self.query_order_by_client(client_order_id).await
    }

    pub async fn cancel_order(
        &self,
        client_order_id: &str,
        _symbol: &str,
    ) -> Result<OrderDetail, StandxError> {
        let request = CancelOrderRequest {
            order_id: None,
            cl_ord_id: Some(client_order_id.to_string()),
        };
        let payload = serde_json::to_string(&request)?;
        let mut headers = self.signing_headers(&payload)?;
        headers.insert(
            "x-session-id",
            HeaderValue::from_str(self.session_id()).unwrap(),
        );

        let url = format!("{}/api/cancel_order", self.api_url);
        let resp = self
            .http
            .post(url)
            .headers(headers)
            .body(payload)
            .send()
            .await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(StandxError::InvalidResponse(body));
        }
        let ack: BasicResponse = serde_json::from_str(&body)?;
        if ack.code != 0 {
            return Err(StandxError::OrderError(ack.message));
        }

        self.query_order_by_client(client_order_id).await
    }

    pub async fn query_order_by_client(
        &self,
        client_order_id: &str,
    ) -> Result<OrderDetail, StandxError> {
        let mut headers = HeaderMap::new();
        if !self.jwt_token.is_empty() {
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", self.jwt_token)).unwrap(),
            );
        }

        let url = format!(
            "{}/api/query_order?cl_ord_id={}",
            self.api_url, client_order_id
        );
        let resp = self.http.get(url).headers(headers).send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(StandxError::InvalidResponse(body));
        }

        let detail: OrderDetail = serde_json::from_str(&body)?;
        Ok(detail)
    }
}
