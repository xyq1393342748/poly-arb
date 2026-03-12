use std::{cmp::Ordering, collections::HashMap, env, path::Path, sync::Arc};

use anyhow::Result;
use chrono::Utc;
use tokio::{
    select,
    sync::{broadcast, mpsc},
};

use poly_arb::{
    arb_engine::{ArbEngine, ArbSignal, MarketPair},
    auth::{ApiAuth, OrderSigner},
    config::Config,
    logger::{init_tracing, Logger},
    market_discovery::{MarketDiscovery, MarketEvent, MarketInfo},
    order_manager::{ArbExecution, ArbStatus, OrderManager},
    orderbook::OrderBookManager,
    risk::{RiskManager, RiskVeto},
    store::{ArbTradeRecord, DailyStats, OpportunityRecord, Store},
    web::{
        state::{
            broadcast_live, AppState, ArbTriggeredMessage, BalanceUpdateMessage, BotStatus,
            LiveMessage, OrderUpdateMessage, TickerMessage,
        },
        WebServer,
    },
    ws_market::MarketWsClient,
    ws_user::UserWsClient,
};

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_owned());
    let config = if Path::new(&config_path).exists() {
        Config::load(&config_path)?
    } else {
        Config::default()
    };
    init_tracing(&config.mode.log_level);

    let base_dir = Path::new(&config_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let db_path = config.db_path(base_dir);
    let store = Store::new(&db_path).await?;
    store.run_migrations().await?;

    let signer = if config.wallet.private_key.trim().is_empty() {
        None
    } else {
        Some(OrderSigner::new(
            &config.wallet.private_key,
            config.wallet.chain_id,
        )?)
    };
    let auth = ApiAuth::new(
        config.api.api_key.clone(),
        config.api.api_secret.clone(),
        config.api.passphrase.clone(),
        signer.as_ref().map(|signer| signer.address.to_string()),
    );

    let (live_tx, _) = broadcast::channel(256);
    let state = Arc::new(AppState::new(store.clone(), live_tx.clone()));
    if config.web.enabled {
        let server = WebServer::new(state.clone());
        let bind = config.web.bind.clone();
        tokio::spawn(async move {
            let _ = server.run(&bind).await;
        });
    }

    let logger = Logger::default();
    let discovery = MarketDiscovery::new(&config);
    let market_ws = MarketWsClient::new(&config, &auth);
    let user_ws = UserWsClient::new(&config, &auth);
    let order_manager = OrderManager::new(&config, auth.clone(), signer.clone());
    let mut risk = RiskManager::new(&config.risk);
    let mut books = OrderBookManager::default();
    let mut engine = ArbEngine::new(config.strategy.clone());
    let mut latest_signal: Option<ArbSignal> = None;
    let mut last_triggered: HashMap<String, i64> = HashMap::new();

    let (market_tx, mut market_rx) = mpsc::channel(128);
    let (book_tx, mut book_rx) = mpsc::channel(2048);
    let (user_tx, mut user_rx) = mpsc::channel(1024);

    tokio::spawn({
        let discovery = discovery.clone();
        async move {
            let _ = discovery.run(market_tx).await;
        }
    });
    tokio::spawn({
        let market_ws = market_ws.clone();
        async move {
            let _ = market_ws.run(book_tx).await;
        }
    });
    if !config.api.api_key.is_empty() {
        tokio::spawn(async move {
            let _ = user_ws.run(user_tx).await;
        });
    }

    *state.bot_status.write().await = BotStatus::Watching;
    let mut ticker = tokio::time::interval(tokio::time::Duration::from_millis(
        config.web.ws_push_interval_ms,
    ));

    loop {
        select! {
            Some(event) = market_rx.recv() => {
                match event {
                    MarketEvent::NewMarket(market) => {
                        register_market(&mut engine, &state, &market).await;
                        let _ = market_ws.subscribe(&market.token_id_up).await;
                        let _ = market_ws.subscribe(&market.token_id_down).await;
                    }
                    MarketEvent::MarketExpiring(condition_id) => {
                        engine.remove_market(&condition_id);
                        state.current_markets.write().await.retain(|item| item.condition_id != condition_id);
                    }
                }
            }
            Some(update) = book_rx.recv() => {
                books.process_update(update);
                *state.current_books.write().await = books.clone();
                let market_pairs = engine.markets().cloned().collect::<Vec<_>>();
                let _ = order_manager.refresh_presigned_orders(&books, &market_pairs).await;
                let best_signal = engine
                    .evaluate(&books)
                    .into_iter()
                    .max_by(|a, b| a.net_profit.partial_cmp(&b.net_profit).unwrap_or(Ordering::Equal));
                *state.current_signal.write().await = best_signal.clone();
                latest_signal = best_signal.clone();

                if let Some(signal) = best_signal {
                    let now_ms = Utc::now().timestamp_millis();
                    let is_duplicate = last_triggered
                        .get(&signal.condition_id)
                        .map(|last| now_ms - *last < 2_000)
                        .unwrap_or(false);
                    if is_duplicate {
                        continue;
                    }

                    logger.log_opportunity(&signal);
                    match risk.check_pre_trade(&signal) {
                        RiskVeto::Approved => {
                            last_triggered.insert(signal.condition_id.clone(), now_ms);
                            *state.bot_status.write().await = BotStatus::Executing;
                            let opportunity = OpportunityRecord {
                                id: 0,
                                created_at: String::new(),
                                condition_id: signal.condition_id.clone(),
                                ask_up: signal.ask_up,
                                ask_down: signal.ask_down,
                                net_profit: signal.net_profit,
                                executed: 1,
                                skip_reason: None,
                            };
                            let _ = store.insert_opportunity(&opportunity).await;
                            broadcast_live(&live_tx, LiveMessage::ArbTriggered(ArbTriggeredMessage {
                                condition_id: signal.condition_id.clone(),
                                ask_up: signal.ask_up,
                                ask_down: signal.ask_down,
                                quantity: signal.max_quantity,
                                expected_profit: signal.net_profit * signal.max_quantity,
                            }));

                            let mut execution = order_manager.execute_arb(signal.clone()).await?;
                            if matches!(execution.status, ArbStatus::OneSideFilled { .. }) {
                                order_manager
                                    .handle_one_side_fill(&mut execution, &books)
                                    .await?;
                            }
                            let trade_id = persist_execution(&store, &execution).await?;
                            logger.log_execution(&execution);
                            apply_execution_risk(&mut risk, &execution);
                            let updated_stats = update_daily_stats(&store, &execution).await?;
                            *state.risk_state.write().await = risk.snapshot(updated_stats.trades as u32);

                            let profit = execution_realized_profit(&execution);
                            broadcast_live(&live_tx, LiveMessage::OrderUpdate(OrderUpdateMessage {
                                trade_id,
                                status: execution_status_label(&execution).to_owned(),
                                profit,
                            }));
                            if let Some(profit) = profit {
                                broadcast_live(&live_tx, LiveMessage::BalanceUpdate(BalanceUpdateMessage {
                                    usdc_balance: 0.0,
                                    total_equity: profit,
                                }));
                            }
                            *state.bot_status.write().await = BotStatus::Watching;
                        }
                        RiskVeto::Rejected(reason) => {
                            logger.log_risk_veto(&reason);
                            let opportunity = OpportunityRecord {
                                id: 0,
                                created_at: String::new(),
                                condition_id: signal.condition_id.clone(),
                                ask_up: signal.ask_up,
                                ask_down: signal.ask_down,
                                net_profit: signal.net_profit,
                                executed: 0,
                                skip_reason: Some(reason),
                            };
                            let _ = store.insert_opportunity(&opportunity).await;
                            *state.risk_state.write().await = risk.snapshot(store.today_stats().await.unwrap_or_default().trades as u32);
                        }
                    }
                }
            }
            Some(user_event) = user_rx.recv() => {
                let _ = order_manager.on_user_event(user_event).await;
            }
            _ = ticker.tick() => {
                let bot_status = state.bot_status.read().await.clone();
                // 直接从 orderbook 读取当前 best ask，不依赖套利信号
                let markets = state.current_markets.read().await.clone();
                let (ask_up, ask_down, sum, condition_id, time_to_expiry_sec) = if let Some(market) = markets.first() {
                    let pair = books.get_pair_asks(&market.token_id_up, &market.token_id_down);
                    let (up_price, down_price) = match pair {
                        Some((up, _, down, _)) => (Some(up), Some(down)),
                        None => (
                            books.get_book(&market.token_id_up).and_then(|b| b.best_ask()).map(|(p, _)| p),
                            books.get_book(&market.token_id_down).and_then(|b| b.best_ask()).map(|(p, _)| p),
                        ),
                    };
                    let s = match (up_price, down_price) {
                        (Some(u), Some(d)) => Some(u + d),
                        _ => None,
                    };
                    let ttl = (market.end_date - Utc::now()).num_seconds();
                    (up_price, down_price, s, Some(market.condition_id.clone()), Some(ttl))
                } else {
                    (None, None, None, None, None)
                };
                let net_profit = match sum {
                    Some(s) if s < 1.0 => Some(1.0 - s - config.strategy.taker_fee_rate * 2.0),
                    Some(s) => Some(1.0 - s),
                    None => None,
                };
                let ticker_message = TickerMessage {
                    ask_up,
                    ask_down,
                    sum,
                    net_profit_potential: net_profit,
                    condition_id,
                    time_to_expiry_sec,
                    bot_status: bot_status.label().to_owned(),
                };
                broadcast_live(&live_tx, LiveMessage::Ticker(ticker_message));
            }
            _ = tokio::signal::ctrl_c() => {
                *state.bot_status.write().await = BotStatus::Stopped("shutdown requested".to_owned());
                break;
            }
        }
    }

    Ok(())
}

async fn register_market(engine: &mut ArbEngine, state: &AppState, market: &MarketInfo) {
    engine.register_market(MarketPair {
        condition_id: market.condition_id.clone(),
        question: market.question.clone(),
        token_id_up: market.token_id_up.clone(),
        token_id_down: market.token_id_down.clone(),
        end_date: market.end_date,
    });
    let mut markets = state.current_markets.write().await;
    if markets
        .iter()
        .all(|item| item.condition_id != market.condition_id)
    {
        markets.push(market.clone());
    }
}

async fn persist_execution(store: &Store, execution: &ArbExecution) -> Result<i64> {
    let net_profit = execution_realized_profit(execution);
    let status = execution_status_label(execution).to_owned();
    let trade = ArbTradeRecord {
        id: 0,
        created_at: String::new(),
        condition_id: execution.signal.condition_id.clone(),
        market_question: Some(execution.signal.question.clone()),
        ask_up: execution.signal.ask_up,
        ask_down: execution.signal.ask_down,
        total_cost: execution.signal.total_cost,
        quantity: execution.signal.max_quantity,
        fees: execution.signal.total_cost * execution.signal.max_quantity * 0.001,
        gas: 0.014,
        status,
        net_profit,
        settled: if net_profit.is_some() { 1 } else { 0 },
        order_id_up: execution.order_id_up.clone(),
        order_id_down: execution.order_id_down.clone(),
        note: None,
    };
    store.insert_trade(&trade).await
}

fn apply_execution_risk(risk: &mut RiskManager, execution: &ArbExecution) {
    match &execution.status {
        ArbStatus::BothFilled { profit } => {
            risk.record_both_filled();
            risk.record_trade_result(*profit);
        }
        ArbStatus::OneSideFilled { .. } => risk.record_one_side(),
        ArbStatus::Rescued { actual_profit } => {
            risk.record_one_side();
            risk.record_trade_result(*actual_profit);
        }
        ArbStatus::LossStopped { loss } => {
            risk.record_one_side();
            risk.record_trade_result(-loss);
        }
        ArbStatus::Pending | ArbStatus::BothCancelled | ArbStatus::Error(_) => {}
    }
}

async fn update_daily_stats(store: &Store, execution: &ArbExecution) -> Result<DailyStats> {
    let mut stats = store.today_stats().await?;
    stats.date = Utc::now().format("%Y-%m-%d").to_string();
    stats.trades += 1;
    stats.total_volume += execution.signal.position_notional;
    match &execution.status {
        ArbStatus::BothFilled { profit } => {
            stats.wins += 1;
            stats.total_profit += *profit;
        }
        ArbStatus::Rescued { actual_profit } => {
            stats.wins += 1;
            stats.total_profit += *actual_profit;
        }
        ArbStatus::LossStopped { loss } => {
            stats.losses += 1;
            stats.total_profit -= *loss;
        }
        ArbStatus::OneSideFilled { .. } => stats.losses += 1,
        ArbStatus::BothCancelled => stats.cancelled += 1,
        ArbStatus::Pending | ArbStatus::Error(_) => {}
    }
    store.upsert_daily_stats(&stats).await?;
    Ok(stats)
}

fn execution_status_label(execution: &ArbExecution) -> &'static str {
    match execution.status {
        ArbStatus::Pending => "PENDING",
        ArbStatus::BothFilled { .. } => "BOTH_FILLED",
        ArbStatus::OneSideFilled { .. } => "ONE_SIDE",
        ArbStatus::Rescued { .. } => "RESCUED",
        ArbStatus::LossStopped { .. } => "LOSS_STOPPED",
        ArbStatus::BothCancelled => "BOTH_CANCELLED",
        ArbStatus::Error(_) => "ERROR",
    }
}

fn execution_realized_profit(execution: &ArbExecution) -> Option<f64> {
    match &execution.status {
        ArbStatus::BothFilled { profit } => Some(*profit),
        ArbStatus::Rescued { actual_profit } => Some(*actual_profit),
        ArbStatus::LossStopped { loss } => Some(-*loss),
        ArbStatus::Pending
        | ArbStatus::OneSideFilled { .. }
        | ArbStatus::BothCancelled
        | ArbStatus::Error(_) => None,
    }
}
