use std::str::FromStr;

use base64::Engine;
use ed25519_dalek::{Signer, SigningKey, pkcs8::DecodePrivateKey};
use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::Deserialize;
use tokio::time::Duration;
use tracing::{debug, info, warn};

use crate::standx::{
    StandxError,
    msg::rest::{
        BasicResponse, CancelOrderRequest, NewOrderRequest, OrderDetail, PositionSnapshot,
    },
};

#[derive(Debug, Clone, Deserialize)]
pub struct DepthBookSnapshot {
    pub asks: Vec<[String; 2]>,
    pub bids: Vec<[String; 2]>,
}

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

fn parse_signing_key(raw: &str) -> Option<SigningKey> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(key) = SigningKey::from_pkcs8_pem(trimmed) {
        return Some(key);
    }

    let engine = base64::engine::general_purpose::STANDARD;
    if let Ok(decoded) = engine.decode(trimmed.as_bytes()) {
        if decoded.len() == 32 {
            let mut buf = [0u8; 32];
            buf.copy_from_slice(&decoded);
            return Some(SigningKey::from_bytes(&buf));
        }
    }

    if trimmed.len() == 64 {
        if let Ok(decoded) = hex::decode(trimmed) {
            if decoded.len() == 32 {
                let mut buf = [0u8; 32];
                buf.copy_from_slice(&decoded);
                return Some(SigningKey::from_bytes(&buf));
            }
        }
    }

    warn!("StandX signing_key could not be parsed; request signing will be disabled");
    None
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
            signing_key: parse_signing_key(signing_key),
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

    pub async fn query_depth_book(&self, symbol: &str) -> Result<DepthBookSnapshot, StandxError> {
        let url = format!("{}/api/query_depth_book", self.api_url);
        let resp = self
            .http
            .get(url)
            .query(&[("symbol", symbol)])
            .send()
            .await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(StandxError::InvalidResponse(body));
        }
        serde_json::from_str::<DepthBookSnapshot>(&body).map_err(StandxError::from)
    }

    pub async fn query_positions(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<PositionSnapshot>, StandxError> {
        let url = format!("{}/api/query_positions", self.api_url);
        let mut req = self.http.get(url);
        if let Some(sym) = symbol {
            req = req.query(&[("symbol", sym)]);
        }
        if !self.jwt_token.is_empty() {
            req = req.bearer_auth(&self.jwt_token);
        }

        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(StandxError::InvalidResponse(body));
        }
        serde_json::from_str::<Vec<PositionSnapshot>>(&body).map_err(StandxError::from)
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
            HeaderValue::from_str(
                &base64::engine::general_purpose::STANDARD.encode(signature.to_bytes()),
            )
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

        debug!(
            client_order_id = %client_order_id,
            symbol = %symbol,
            side = ?side,
            order_type = ?order_type,
            time_in_force = ?time_in_force,
            price = ?price,
            qty = qty,
            payload = %payload,
            "standx REST new_order -> sending"
        );

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

        debug!(
            client_order_id = %client_order_id,
            status = %status,
            body = %body,
            "standx REST new_order <- ack raw"
        );

        if !status.is_success() {
            return Err(StandxError::InvalidResponse(body));
        }
        let ack: BasicResponse = serde_json::from_str(&body)?;

        debug!(
            client_order_id = %client_order_id,
            code = ack.code,
            msg = %ack.message,
            "standx REST new_order <- ack parsed"
        );

        if ack.code != 0 {
            warn!(
                client_order_id = %client_order_id,
                code = ack.code,
                msg = %ack.message,
                "standx REST new_order rejected"
            );
            return Err(StandxError::OrderError(ack.message));
        }

        debug!(
            client_order_id = %client_order_id,
            "standx REST query_order_by_client start (after new_order ack=0)"
        );
        let detail = OrderDetail {
            // 这些字段名按你日志里出现过的来填：
            id: None,
            symbol: symbol.to_string(),
            side,
            order_type,
            time_in_force,
            status: Status::New,      // ← TODO: 换成你真实存在的“已发送/等待确认”状态
            price: price.map(|p| p.to_string()),
            qty: qty.to_string(),
            fill_qty: "0".to_string(),
            updated_at: None,

            // 如果你的 OrderDetail 里有 cl_ord_id，强烈建议填上
            // （如果没有这个字段，把这一行删掉）
            cl_ord_id: Some(client_order_id.to_string()),
            fill_avg_price: None,
        };
        Ok(detail)
        // match self.query_order_by_client(client_order_id).await {
        //     Ok(detail) => {
        //         info!(
        //             client_order_id = %client_order_id,
        //             symbol = %detail.symbol,
        //             order_id = ?detail.id,
        //             status = ?detail.status,
        //             price = ?detail.price,
        //             qty = %detail.qty,
        //             fill_qty = %detail.fill_qty,
        //             updated_at = ?detail.updated_at,
        //             "standx REST query_order_by_client ok"
        //         );
        //         Ok(detail)
        //     }
        //     Err(e) => {
        //         warn!(
        //             client_order_id = %client_order_id,
        //             error = ?e,
        //             "standx REST query_order_by_client FAILED after new_order ack=0 (this can cause ghost orders if treated as Expired)"
        //         );
        //         Err(e)
        //     }
        // }
    }


    pub async fn cancel_order(
        &self,
        client_order_id: &str,
        symbol: &str,
        // 从调用处传入“取消前快照”，避免伪造错误字段/终态
        side: Side,
        order_type: OrdType,
        time_in_force: TimeInForce,
        cur_status: Status,
        qty: f64,
        fill_qty: f64,
    ) -> Result<OrderDetail, StandxError> {
        let request = CancelOrderRequest {
            order_id: None,
            cl_ord_id: Some(client_order_id.to_string()),
        };
        let payload = serde_json::to_string(&request)?;

        debug!(
            client_order_id = %client_order_id,
            payload = %payload,
            "standx REST cancel_order -> sending"
        );

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

        debug!(
            client_order_id = %client_order_id,
            status = %status,
            body = %body,
            "standx REST cancel_order <- ack raw"
        );

        if !status.is_success() {
            return Err(StandxError::InvalidResponse(body));
        }
        let ack: BasicResponse = serde_json::from_str(&body)?;

        debug!(
            client_order_id = %client_order_id,
            code = ack.code,
            msg = %ack.message,
            "standx REST cancel_order <- ack parsed"
        );

        if ack.code != 0 {
            warn!(
                client_order_id = %client_order_id,
                code = ack.code,
                msg = %ack.message,
                "standx REST cancel_order rejected"
            );
            return Err(StandxError::OrderError(ack.message));
        }

        debug!(
            client_order_id = %client_order_id,
            "standx REST query_order_by_client start (after cancel_order ack=0)"
        );
        // let detail = OrderDetail {
        //     id: None,
        //     symbol: symbol.to_string(),
        //     side: Side::Buy,
        //     order_type: OrdType::Limit,
        //     time_in_force: TimeInForce::GTC,
        //     status: Status::Canceled,   // ← TODO: 换成你真实存在的“取消已发送/等待确认”状态
        //     price: None,
        //     qty: "0".to_string(),
        //     fill_qty: "0".to_string(),
        //     updated_at: None,
        //     cl_ord_id: Some(client_order_id.to_string()),
        //     fill_avg_price: None,
        // };
        Ok(OrderDetail {
            id: None,
            symbol: symbol.to_string(),
            side,
            order_type,
            time_in_force,
            status: Status::Canceled,          // ✅ 不要写 Canceled
            price: None,                 // ✅ 避免覆盖 price_tick
            qty: qty.to_string(),        // ✅ 保持原 qty
            fill_qty: fill_qty.to_string(),
            fill_avg_price: None,
            updated_at: None,
            cl_ord_id: Some(client_order_id.to_string()),
        })
        // Ok(detail)
        // match self.query_order_by_client(client_order_id).await {
        //     Ok(detail) => {
        //         info!(
        //             client_order_id = %client_order_id,
        //             symbol = %detail.symbol,
        //             order_id = ?detail.id,
        //             status = ?detail.status,
        //             price = ?detail.price,
        //             qty = %detail.qty,
        //             fill_qty = %detail.fill_qty,
        //             updated_at = ?detail.updated_at,
        //             "standx REST query_order_by_client ok (after cancel)"
        //         );
        //         Ok(detail)
        //     }
        //     Err(e) => {
        //         warn!(
        //             client_order_id = %client_order_id,
        //             error = ?e,
        //             "standx REST query_order_by_client FAILED after cancel_order ack=0"
        //         );
        //         Err(e)
        //     }
        // }
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

        debug!(
            client_order_id = %client_order_id,
            url = %url,
            "standx REST query_order_by_client -> sending"
        );

        let resp = self.http.get(url).headers(headers).send().await?;
        let status = resp.status();
        let body = resp.text().await?;

        debug!(
            client_order_id = %client_order_id,
            status = %status,
            body = %body,
            "standx REST query_order_by_client <- raw"
        );

        if !status.is_success() {
            return Err(StandxError::InvalidResponse(body));
        }

        let detail: OrderDetail = serde_json::from_str(&body)?;
        Ok(detail)
    }

}
