use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::{Duration, Utc};
use serde::Deserialize;
use tokio::sync::broadcast;

use super::state::{AppState, StatusResponse};
use crate::{
    orderbook::OrderBookSnapshot,
    store::{ArbTradeRecord, BalanceSnapshot, DailyStats, LifetimeStats, OpportunityRecord},
};

#[derive(Debug, Deserialize)]
pub struct TradeQuery {
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct DaysQuery {
    pub days: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct LimitQuery {
    pub limit: Option<u32>,
}

pub async fn get_status(State(state): State<Arc<AppState>>) -> Json<StatusResponse> {
    let today = state.store.today_stats().await.unwrap_or_default();
    let lifetime = state.store.lifetime_stats().await.unwrap_or_default();
    let bot_status = state.bot_status.read().await.clone();
    let tracked_books = state.current_books.read().await.len();
    let tracked_markets = state.current_markets.read().await.len();
    let risk = state.risk_state.read().await.clone();
    let last_signal = state.current_signal.read().await.clone();
    Json(StatusResponse {
        bot_status,
        tracked_books,
        tracked_markets,
        risk,
        last_signal,
        today,
        lifetime,
    })
}

pub async fn get_trades(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TradeQuery>,
) -> Json<Vec<ArbTradeRecord>> {
    Json(
        state
            .store
            .recent_trades(query.limit.unwrap_or(50))
            .await
            .unwrap_or_default(),
    )
}

pub async fn get_trade_detail(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<ArbTradeRecord>, StatusCode> {
    state
        .store
        .trade_by_id(id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub async fn get_today_stats(State(state): State<Arc<AppState>>) -> Json<DailyStats> {
    Json(state.store.today_stats().await.unwrap_or_default())
}

pub async fn get_daily_stats(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DaysQuery>,
) -> Json<Vec<DailyStats>> {
    let days = query.days.unwrap_or(30).max(1);
    let to = Utc::now().format("%Y-%m-%d").to_string();
    let from = (Utc::now() - Duration::days(days as i64 - 1))
        .format("%Y-%m-%d")
        .to_string();
    Json(
        state
            .store
            .daily_stats_range(&from, &to)
            .await
            .unwrap_or_default(),
    )
}

pub async fn get_lifetime_stats(State(state): State<Arc<AppState>>) -> Json<LifetimeStats> {
    Json(state.store.lifetime_stats().await.unwrap_or_default())
}

pub async fn get_balance_history(State(state): State<Arc<AppState>>) -> Json<Vec<BalanceSnapshot>> {
    Json(state.store.balance_history(100).await.unwrap_or_default())
}

pub async fn get_orderbook(State(state): State<Arc<AppState>>) -> Json<Vec<OrderBookSnapshot>> {
    Json(state.current_books.read().await.snapshots())
}

pub async fn get_opportunities(
    State(state): State<Arc<AppState>>,
    Query(query): Query<LimitQuery>,
) -> Json<Vec<OpportunityRecord>> {
    Json(
        state
            .store
            .recent_opportunities(query.limit.unwrap_or(100))
            .await
            .unwrap_or_default(),
    )
}

pub async fn ws_live(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state.live_tx.subscribe()))
}

async fn handle_socket(
    mut socket: WebSocket,
    mut rx: broadcast::Receiver<super::state::LiveMessage>,
) {
    loop {
        match rx.recv().await {
            Ok(message) => {
                if socket
                    .send(Message::Text(
                        serde_json::to_string(&message).unwrap_or_default(),
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(_) => break,
        }
    }
}
