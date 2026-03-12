use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use futures_util::{Sink, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    sync::{mpsc, Mutex, RwLock},
    time::{sleep, Duration},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{auth::ApiAuth, config::Config, types::PriceLevel};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookUpdate {
    pub asset_id: String,
    pub timestamp: u64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub is_snapshot: bool,
}

#[derive(Debug, Clone)]
pub struct MarketWsClient {
    url: String,
    _auth: ApiAuth,
    subscriptions: Arc<RwLock<HashSet<String>>>,
    command_tx: Arc<Mutex<Option<mpsc::UnboundedSender<SubscriptionCommand>>>>,
}

#[derive(Debug)]
enum SubscriptionCommand {
    Sync,
}

impl MarketWsClient {
    pub fn new(config: &Config, auth: &ApiAuth) -> Self {
        Self {
            url: config.api.clob_ws_url.clone(),
            _auth: auth.clone(),
            subscriptions: Arc::new(RwLock::new(HashSet::new())),
            command_tx: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn subscribe(&self, token_id: &str) -> Result<()> {
        self.subscriptions.write().await.insert(token_id.to_owned());
        if let Some(tx) = self.command_tx.lock().await.as_ref() {
            let _ = tx.send(SubscriptionCommand::Sync);
        }
        Ok(())
    }

    pub async fn unsubscribe(&self, token_id: &str) -> Result<()> {
        self.subscriptions.write().await.remove(token_id);
        if let Some(tx) = self.command_tx.lock().await.as_ref() {
            let _ = tx.send(SubscriptionCommand::Sync);
        }
        Ok(())
    }

    pub async fn run(&self, tx: mpsc::Sender<BookUpdate>) -> Result<()> {
        let mut backoff = 1_u64;
        loop {
            match self.run_connection(tx.clone()).await {
                Ok(()) => backoff = 1,
                Err(_) => {
                    sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(30);
                }
            }
        }
    }

    async fn run_connection(&self, tx: mpsc::Sender<BookUpdate>) -> Result<()> {
        let (stream, _) = connect_async(&self.url).await?;
        let (mut writer, mut reader) = stream.split();
        let (command_tx, mut command_rx) = mpsc::unbounded_channel();
        *self.command_tx.lock().await = Some(command_tx);
        self.send_subscription_snapshot(&mut writer).await?;
        let mut ping = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = ping.tick() => {
                    writer.send(Message::Ping(Vec::new())).await?;
                }
                Some(command) = command_rx.recv() => {
                    match command {
                        SubscriptionCommand::Sync => self.send_subscription_snapshot(&mut writer).await?,
                    }
                }
                incoming = reader.next() => {
                    match incoming {
                        Some(Ok(Message::Text(text))) => {
                            for update in parse_market_message(&text)? {
                                tx.send(update).await?;
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            writer.send(Message::Pong(payload)).await?;
                        }
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Ok(_)) => {}
                        Some(Err(err)) => return Err(err.into()),
                    }
                }
            }
        }
        *self.command_tx.lock().await = None;
        Ok(())
    }

    async fn send_subscription_snapshot<S>(&self, writer: &mut S) -> Result<()>
    where
        S: Sink<Message> + Unpin,
        S::Error: Into<anyhow::Error>,
    {
        let assets_ids: Vec<String> = self.subscriptions.read().await.iter().cloned().collect();
        if assets_ids.is_empty() {
            return Ok(());
        }
        let payload = serde_json::json!({
            "assets_ids": assets_ids,
            "type": "market",
            "custom_feature_enabled": true,
        });
        writer
            .send(Message::Text(payload.to_string()))
            .await
            .map_err(Into::into)
    }
}

pub fn parse_market_message(text: &str) -> Result<Vec<BookUpdate>> {
    let value: Value = serde_json::from_str(text)?;
    match value {
        Value::Array(items) => Ok(items.iter().filter_map(parse_market_value).collect()),
        other => Ok(parse_market_value(&other).into_iter().collect()),
    }
}

fn parse_market_value(value: &Value) -> Option<BookUpdate> {
    let event_type = value
        .get("event_type")
        .and_then(Value::as_str)
        .or_else(|| value.get("type").and_then(Value::as_str))?;
    let asset_id = value
        .get("asset_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)?;
    let timestamp = value
        .get("timestamp")
        .and_then(Value::as_u64)
        .unwrap_or_else(now_millis);
    match event_type {
        "book" => Some(BookUpdate {
            asset_id,
            timestamp,
            bids: parse_levels(value.get("bids")),
            asks: parse_levels(value.get("asks")),
            is_snapshot: true,
        }),
        "price_change" => {
            let changes = value.get("changes")?.as_array()?;
            let mut bids = Vec::new();
            let mut asks = Vec::new();
            for change in changes {
                let side = change
                    .get("side")
                    .and_then(Value::as_str)
                    .unwrap_or("BUY")
                    .to_uppercase();
                let level = parse_level(change)?;
                if side == "SELL" || side == "ASK" {
                    asks.push(level);
                } else {
                    bids.push(level);
                }
            }
            Some(BookUpdate {
                asset_id,
                timestamp,
                bids,
                asks,
                is_snapshot: false,
            })
        }
        _ => None,
    }
}

fn parse_levels(value: Option<&Value>) -> Vec<PriceLevel> {
    value
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(parse_level).collect())
        .unwrap_or_default()
}

fn parse_level(value: &Value) -> Option<PriceLevel> {
    if let Some(array) = value.as_array() {
        let price = parse_value_number(array.first()?)?;
        let size = parse_value_number(array.get(1)?)?;
        return Some(PriceLevel::new(price, size));
    }
    Some(PriceLevel::new(
        parse_value_number(value.get("price")?)?,
        parse_value_number(value.get("size")?)?,
    ))
}

fn now_millis() -> u64 {
    chrono::Utc::now().timestamp_millis() as u64
}

fn parse_value_number(value: &Value) -> Option<f64> {
    match value {
        Value::String(raw) => raw.parse().ok(),
        Value::Number(raw) => raw.as_f64(),
        _ => None,
    }
}
