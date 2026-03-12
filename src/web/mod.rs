use std::sync::Arc;

use anyhow::Result;
use axum::{response::Html, routing::get, Router};
use rust_embed::RustEmbed;
use tower_http::cors::CorsLayer;

pub mod handlers;
pub mod state;

use handlers::*;
use state::AppState;

#[derive(RustEmbed)]
#[folder = "src/web/static/"]
struct StaticAssets;

pub struct WebServer {
    state: Arc<AppState>,
}

impl WebServer {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub async fn run(&self, bind: &str) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(bind).await?;
        axum::serve(listener, app(self.state.clone())).await?;
        Ok(())
    }
}

pub fn app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/status", get(get_status))
        .route("/api/trades", get(get_trades))
        .route("/api/trades/{id}", get(get_trade_detail))
        .route("/api/stats/today", get(get_today_stats))
        .route("/api/stats/daily", get(get_daily_stats))
        .route("/api/stats/lifetime", get(get_lifetime_stats))
        .route("/api/balance/history", get(get_balance_history))
        .route("/api/orderbook", get(get_orderbook))
        .route("/api/opportunities", get(get_opportunities))
        .route("/ws/live", get(ws_live))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn index() -> Html<String> {
    let html = StaticAssets::get("index.html")
        .map(|asset| String::from_utf8(asset.data.into_owned()).unwrap_or_default())
        .unwrap_or_else(|| "<h1>poly-arb dashboard missing</h1>".to_owned());
    Html(html)
}
