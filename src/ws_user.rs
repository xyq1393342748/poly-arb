use anyhow::{anyhow, bail, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    auth::ApiAuth,
    config::Config,
    types::{OrderStatus, Side},
};

#[derive(Debug, Clone)]
pub struct UserWsClient {
    url: String,
    auth: ApiAuth,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UserEvent {
    OrderUpdate {
        order_id: String,
        status: OrderStatus,
        size_matched: f64,
        avg_price: f64,
    },
    TradeExecution {
        trade_id: String,
        order_id: String,
        price: f64,
        size: f64,
        side: Side,
        asset_id: String,
    },
}

impl UserWsClient {
    pub fn new(config: &Config, auth: &ApiAuth) -> Self {
        Self {
            url: config.api.clob_user_ws_url.clone(),
            auth: auth.clone(),
        }
    }

    pub async fn run(&self, tx: mpsc::Sender<UserEvent>) -> Result<()> {
        let mut backoff = 1_u64;
        loop {
            match self.run_connection(tx.clone()).await {
                Ok(()) => backoff = 1,
                Err(err) if err.to_string().contains("auth failed") => return Err(err),
                Err(_) => {
                    sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(30);
                }
            }
        }
    }

    async fn run_connection(&self, tx: mpsc::Sender<UserEvent>) -> Result<()> {
        let (stream, _) = connect_async(&self.url).await?;
        let (mut writer, mut reader) = stream.split();
        writer
            .send(Message::Text(
                self.auth
                    .ws_auth_message(chrono::Utc::now().timestamp() as u64)?,
            ))
            .await?;

        while let Some(message) = reader.next().await {
            match message? {
                Message::Text(text) => {
                    if let Some(event) = parse_user_message(&text)? {
                        tx.send(event).await?;
                    }
                }
                Message::Ping(payload) => writer.send(Message::Pong(payload)).await?,
                Message::Close(_) => break,
                _ => {}
            }
        }

        Ok(())
    }
}

fn parse_user_message(text: &str) -> Result<Option<UserEvent>> {
    let value: Value = serde_json::from_str(text)?;
    if let Some(message_type) = value.get("type").and_then(Value::as_str) {
        match message_type {
            "error" => bail!(
                "auth failed: {}",
                value
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            ),
            "order" => return parse_order_update(&value).map(Some),
            "trade" => return parse_trade_execution(&value).map(Some),
            _ => {}
        }
    }
    Ok(None)
}

fn parse_order_update(value: &Value) -> Result<UserEvent> {
    let order = value
        .get("order")
        .ok_or_else(|| anyhow!("missing order payload"))?;
    let status = match order
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN")
        .to_uppercase()
        .as_str()
    {
        "LIVE" => OrderStatus::Live,
        "MATCHED" => OrderStatus::Matched,
        "CANCELLED" => OrderStatus::Cancelled,
        "EXPIRED" => OrderStatus::Expired,
        "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    };
    Ok(UserEvent::OrderUpdate {
        order_id: order
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        status,
        size_matched: parse_f64(order.get("size_matched"))?,
        avg_price: parse_f64(order.get("avg_price"))?,
    })
}

fn parse_trade_execution(value: &Value) -> Result<UserEvent> {
    let trade = value
        .get("trade")
        .ok_or_else(|| anyhow!("missing trade payload"))?;
    let side = match trade
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or("BUY")
        .to_uppercase()
        .as_str()
    {
        "SELL" => Side::Sell,
        _ => Side::Buy,
    };
    Ok(UserEvent::TradeExecution {
        trade_id: trade
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        order_id: trade
            .get("order_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        price: parse_f64(trade.get("price"))?,
        size: parse_f64(trade.get("size"))?,
        side,
        asset_id: trade
            .get("asset_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    })
}

fn parse_f64(value: Option<&Value>) -> Result<f64> {
    Ok(match value {
        Some(Value::String(s)) => s.parse()?,
        Some(Value::Number(n)) => n.as_f64().unwrap_or_default(),
        _ => 0.0,
    })
}
