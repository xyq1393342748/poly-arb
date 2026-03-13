use std::{collections::HashMap, env, path::Path, str::FromStr, sync::Arc};

use anyhow::Result;
use chrono::Utc;
use tokio::{
    select,
    sync::{broadcast, mpsc},
    time::Duration,
};

use poly_arb::{
    arb_engine::{ArbEngine, ArbSignal, MarketPair},
    auth::{ApiAuth, OrderSigner},
    config::Config,
    fees,
    logger::{init_tracing, Logger},
    market_discovery::{MarketDiscovery, MarketEvent, MarketInfo},
    merger::{MergeResult, Merger},
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

struct ExecResult {
    signal: ArbSignal,
    execution: ArbExecution,
}

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
    let merger = Arc::new(Merger::new(&config));

    // Startup: check and set CTF approval for merge
    if config.merge.enabled && !config.mode.dry_run {
        if let Some(signer_ref) = &signer {
            let owner = signer_ref.address;
            let ctf = alloy_primitives::Address::from_str("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")
                .expect("invalid CTF address");
            match merger.check_approval(owner, ctf).await {
                Ok(true) => tracing::info!("CTF approval already set"),
                Ok(false) => {
                    tracing::info!("Setting CTF approval for merge...");
                    match merger.ensure_approval(ctf).await {
                        Ok(tx) => tracing::info!(tx_hash = tx, "CTF approval set"),
                        Err(e) => tracing::warn!(error = %e, "failed to set CTF approval, merge may fail"),
                    }
                }
                Err(e) => tracing::warn!(error = %e, "failed to check CTF approval"),
            }
        }
    }

    let mut risk = RiskManager::new(&config.risk);
    let mut books = OrderBookManager::default();
    let mut engine = ArbEngine::new(config.strategy.clone());
    let mut last_triggered: HashMap<String, i64> = HashMap::new();
    let mut executing = false;
    let mut market_pairs_buf: Vec<MarketPair> = Vec::new();
    let mut cached_signal: Option<ArbSignal> = None;

    let (market_tx, mut market_rx) = mpsc::channel(128);
    let (book_tx, mut book_rx) = mpsc::channel(2048);
    let (user_tx, mut user_rx) = mpsc::channel(1024);
    let (merge_done_tx, mut merge_done_rx) = mpsc::channel::<(i64, MergeResult)>(16);
    let (exec_done_tx, mut exec_done_rx) = mpsc::channel::<ExecResult>(4);

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
    let mut ticker = tokio::time::interval(Duration::from_millis(config.web.ws_push_interval_ms));
    let mut presign_ticker = tokio::time::interval(Duration::from_secs(3));

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
                // Drain: process all pending book updates at once
                books.process_update(update);
                while let Ok(queued) = book_rx.try_recv() {
                    books.process_update(queued);
                }

                // Skip evaluation while an execution is in flight
                if executing {
                    continue;
                }

                let best_signal = engine.evaluate_best(&books);
                cached_signal = best_signal.clone();

                if let Some(signal) = best_signal {
                    let now_ms = Utc::now().timestamp_millis();
                    let is_duplicate = last_triggered
                        .get(&signal.condition_id)
                        .map(|last| now_ms - *last < 500)
                        .unwrap_or(false);
                    if is_duplicate {
                        continue;
                    }

                    logger.log_opportunity(&signal);
                    match risk.check_pre_trade(&signal) {
                        RiskVeto::Approved => {
                            last_triggered.insert(signal.condition_id.clone(), now_ms);
                            executing = true;
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

                            // Async execution: spawn order execution
                            let om = order_manager.clone();
                            let sig = signal.clone();
                            let books_snap = books.clone();
                            let tx = exec_done_tx.clone();
                            tokio::spawn(async move {
                                let exec_result = async {
                                    let mut execution = om.execute_arb(sig.clone(), Some(&books_snap)).await?;
                                    if matches!(execution.status, ArbStatus::OneSideFilled { .. }) {
                                        om.handle_one_side_fill(&mut execution, &books_snap).await?;
                                    }
                                    Ok::<_, anyhow::Error>(execution)
                                }.await;
                                match exec_result {
                                    Ok(execution) => {
                                        let _ = tx.send(ExecResult { signal: sig, execution }).await;
                                    }
                                    Err(e) => {
                                        tracing::error!(error = %e, "async execution failed");
                                        // Send error result back so executing flag gets cleared
                                        let err_exec = om.make_error_execution(sig.clone(), &e.to_string());
                                        let _ = tx.send(ExecResult { signal: sig, execution: err_exec }).await;
                                    }
                                }
                            });
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
            Some(result) = exec_done_rx.recv() => {
                executing = false;
                let ExecResult { signal, execution } = result;

                logger.log_execution(&execution);

                let trade_id = match persist_execution(&store, &execution, &config).await {
                    Ok(id) => id,
                    Err(e) => {
                        tracing::error!(error = %e, status = execution_status_label(&execution), "persist_execution failed, trade not recorded");
                        apply_execution_risk(&mut risk, &execution);
                        *state.bot_status.write().await = BotStatus::Watching;
                        continue;
                    }
                };

                let _ = store.update_trade_extra(
                    trade_id,
                    execution.order_up.size,
                    1,
                    signal.vwap_up,
                    signal.vwap_down,
                ).await;

                // MERGE: async spawn if both sides filled
                if config.merge.enabled && matches!(execution.status, ArbStatus::BothFilled { .. }) {
                    let merger_ref = merger.clone();
                    let cond_id = signal.condition_id.clone();
                    let filled_qty = execution.order_up.size;
                    let tx = merge_done_tx.clone();
                    let tid = trade_id;
                    tokio::spawn(async move {
                        let result = merger_ref.merge_positions(&cond_id, filled_qty).await
                            .unwrap_or_else(|e| MergeResult::failed(&e.to_string()));
                        let _ = tx.send((tid, result)).await;
                    });
                }

                apply_execution_risk(&mut risk, &execution);
                let updated_stats = match update_daily_stats(&store, &execution).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(error = %e, "update_daily_stats failed");
                        store.today_stats().await.unwrap_or_default()
                    }
                };
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
            _ = presign_ticker.tick() => {
                market_pairs_buf.clear();
                market_pairs_buf.extend(engine.markets().cloned());
                let _ = order_manager.refresh_presigned_orders(&books, &market_pairs_buf).await;
            }
            Some(user_event) = user_rx.recv() => {
                let _ = order_manager.on_user_event(user_event).await;
            }
            Some((trade_id, merge_result)) = merge_done_rx.recv() => {
                let (merge_status, tx_hash, gas_cost) = if merge_result.success {
                    tracing::info!(trade_id, tx_hash = ?merge_result.tx_hash, "merge completed");
                    ("MERGED", merge_result.tx_hash.clone(), Some(merge_result.gas_cost))
                } else {
                    tracing::warn!(trade_id, error = ?merge_result.error, "merge failed");
                    ("MERGE_FAILED", merge_result.tx_hash.clone(), Some(merge_result.gas_cost))
                };
                let _ = store.update_trade_merge(
                    trade_id,
                    merge_status,
                    tx_hash.as_deref(),
                    gas_cost,
                ).await;
                broadcast_live(&live_tx, LiveMessage::OrderUpdate(OrderUpdateMessage {
                    trade_id,
                    status: merge_status.to_owned(),
                    profit: None, // profit already reported
                }));
            }
            _ = ticker.tick() => {
                *state.current_books.write().await = books.clone();
                *state.current_signal.write().await = cached_signal.clone();
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
                let net_profit = match (ask_up, ask_down) {
                    (Some(u), Some(d)) => {
                        let curve_fees = fees::arb_fees(
                            u, d,
                            config.strategy.fee_curve_rate,
                            config.strategy.fee_curve_exponent,
                        );
                        let gas = config.strategy.gas_per_order * 2.0;
                        Some(1.0 - u - d - curve_fees - gas)
                    }
                    _ => None,
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

async fn persist_execution(store: &Store, execution: &ArbExecution, config: &Config) -> Result<i64> {
    let net_profit = execution_realized_profit(execution);
    let status = execution_status_label(execution).to_owned();
    let filled_qty = execution.order_up.size;
    let curve_fees = fees::arb_fees(
        execution.signal.ask_up,
        execution.signal.ask_down,
        config.strategy.fee_curve_rate,
        config.strategy.fee_curve_exponent,
    );
    let gas_cost = config.strategy.gas_per_order * 2.0;
    let trade = ArbTradeRecord {
        id: 0,
        created_at: String::new(),
        condition_id: execution.signal.condition_id.clone(),
        market_question: Some(execution.signal.question.clone()),
        ask_up: execution.signal.ask_up,
        ask_down: execution.signal.ask_down,
        total_cost: execution.signal.total_cost,
        quantity: filled_qty,
        fees: curve_fees * filled_qty,
        gas: gas_cost,
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
        ArbStatus::Merged { profit, .. } => {
            risk.record_both_filled();
            risk.record_trade_result(*profit);
        }
        ArbStatus::MergeFailed { profit, .. } => {
            risk.record_both_filled();
            risk.record_trade_result(*profit);
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
        ArbStatus::Merged { profit, .. } => {
            stats.wins += 1;
            stats.total_profit += *profit;
        }
        ArbStatus::MergeFailed { profit, .. } => {
            stats.wins += 1;
            stats.total_profit += *profit;
        }
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
        ArbStatus::Merged { .. } => "MERGED",
        ArbStatus::MergeFailed { .. } => "MERGE_FAILED",
        ArbStatus::BothCancelled => "BOTH_CANCELLED",
        ArbStatus::Error(_) => "ERROR",
    }
}

fn execution_realized_profit(execution: &ArbExecution) -> Option<f64> {
    match &execution.status {
        ArbStatus::BothFilled { profit } => Some(*profit),
        ArbStatus::Merged { profit, .. } => Some(*profit),
        ArbStatus::MergeFailed { profit, .. } => Some(*profit),
        ArbStatus::Rescued { actual_profit } => Some(*actual_profit),
        ArbStatus::LossStopped { loss } => Some(-*loss),
        ArbStatus::Pending
        | ArbStatus::OneSideFilled { .. }
        | ArbStatus::BothCancelled
        | ArbStatus::Error(_) => None,
    }
}
