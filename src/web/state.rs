use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::{broadcast, RwLock};

use crate::{
    arb_engine::ArbSignal,
    market_discovery::MarketInfo,
    orderbook::OrderBookManager,
    risk::RiskSnapshot,
    store::{DailyStats, LifetimeStats, Store},
};

#[derive(Debug, Clone)]
pub struct AppState {
    pub store: Store,
    pub live_tx: broadcast::Sender<LiveMessage>,
    pub current_books: Arc<RwLock<OrderBookManager>>,
    pub current_markets: Arc<RwLock<Vec<MarketInfo>>>,
    pub bot_status: Arc<RwLock<BotStatus>>,
    pub risk_state: Arc<RwLock<RiskSnapshot>>,
    pub current_signal: Arc<RwLock<Option<ArbSignal>>>,
}

impl AppState {
    pub fn new(store: Store, live_tx: broadcast::Sender<LiveMessage>) -> Self {
        Self {
            store,
            live_tx,
            current_books: Arc::new(RwLock::new(OrderBookManager::default())),
            current_markets: Arc::new(RwLock::new(Vec::new())),
            bot_status: Arc::new(RwLock::new(BotStatus::Starting)),
            risk_state: Arc::new(RwLock::new(RiskSnapshot::default())),
            current_signal: Arc::new(RwLock::new(None)),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "state", content = "data")]
pub enum BotStatus {
    Starting,
    Watching,
    Executing,
    Cooldown(DateTime<Utc>),
    Stopped(String),
}

impl BotStatus {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Watching => "watching",
            Self::Executing => "executing",
            Self::Cooldown(_) => "cooldown",
            Self::Stopped(_) => "stopped",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum LiveMessage {
    Ticker(TickerMessage),
    ArbTriggered(ArbTriggeredMessage),
    OrderUpdate(OrderUpdateMessage),
    BalanceUpdate(BalanceUpdateMessage),
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct TickerMessage {
    pub ask_up: Option<f64>,
    pub ask_down: Option<f64>,
    pub sum: Option<f64>,
    pub net_profit_potential: Option<f64>,
    pub condition_id: Option<String>,
    pub time_to_expiry_sec: Option<i64>,
    pub bot_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArbTriggeredMessage {
    pub condition_id: String,
    pub ask_up: f64,
    pub ask_down: f64,
    pub quantity: f64,
    pub expected_profit: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderUpdateMessage {
    pub trade_id: i64,
    pub status: String,
    pub profit: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BalanceUpdateMessage {
    pub usdc_balance: f64,
    pub total_equity: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusResponse {
    pub bot_status: BotStatus,
    pub tracked_books: usize,
    pub tracked_markets: usize,
    pub risk: RiskSnapshot,
    pub last_signal: Option<ArbSignal>,
    pub today: DailyStats,
    pub lifetime: LifetimeStats,
}

pub fn broadcast_live(tx: &broadcast::Sender<LiveMessage>, msg: LiveMessage) {
    let _ = tx.send(msg);
}
