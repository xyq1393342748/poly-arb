use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{config::StrategyConfig, fees, orderbook::{DepthAnalysis, OrderBookManager}};

const STALE_THRESHOLD_MS: u64 = 5_000;

/// Lightweight scoring result — no String clones, just numbers.
struct PairScore<'a> {
    pair: &'a MarketPair,
    depth: DepthAnalysis,
    net_profit: f64,
    total_cost: f64,
    max_quantity: f64,
    books_stale: bool,
    time_to_expiry_sec: i64,
}

#[derive(Debug, Clone)]
pub struct ArbEngine {
    markets: HashMap<String, MarketPair>,
    config: StrategyConfig,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketPair {
    pub condition_id: String,
    pub question: String,
    pub token_id_up: String,
    pub token_id_down: String,
    pub end_date: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbSignal {
    pub condition_id: String,
    pub question: String,
    pub token_id_up: String,
    pub token_id_down: String,
    pub ask_up: f64,
    pub ask_down: f64,
    pub total_cost: f64,
    pub net_profit: f64,
    pub max_quantity: f64,
    pub position_notional: f64,
    pub profit_rate: f64,
    pub time_to_expiry_sec: i64,
    pub vwap_up: f64,
    pub vwap_down: f64,
    pub books_stale: bool,
    pub timestamp: DateTime<Utc>,
}

impl ArbEngine {
    pub fn new(config: StrategyConfig) -> Self {
        Self {
            markets: HashMap::new(),
            config,
        }
    }

    pub fn register_market(&mut self, pair: MarketPair) {
        self.markets.insert(pair.condition_id.clone(), pair);
    }

    pub fn remove_market(&mut self, condition_id: &str) {
        self.markets.remove(condition_id);
    }

    /// Find the single best signal, zero-alloc scoring. Scores all pairs numerically first (no String clones),
    /// then materializes only the winner into a full ArbSignal.
    pub fn evaluate_best(&self, books: &OrderBookManager) -> Option<ArbSignal> {
        let now = Utc::now();
        let best = self.markets
            .values()
            .filter_map(|pair| self.score_pair(pair, books, now))
            .max_by(|a, b| a.net_profit.partial_cmp(&b.net_profit).unwrap_or(std::cmp::Ordering::Equal))?;
        Some(self.materialize_signal(best, now))
    }

    /// Numeric-only scoring — no String allocations.
    fn score_pair<'a>(
        &self,
        pair: &'a MarketPair,
        books: &OrderBookManager,
        now: DateTime<Utc>,
    ) -> Option<PairScore<'a>> {
        let (ask_up, _size_up, ask_down, _size_down) =
            books.get_pair_asks(&pair.token_id_up, &pair.token_id_down)?;
        let up_book = books.get_book(&pair.token_id_up)?;
        let down_book = books.get_book(&pair.token_id_down)?;
        let books_stale = up_book.is_stale(std::time::Duration::from_millis(STALE_THRESHOLD_MS))
            || down_book.is_stale(std::time::Duration::from_millis(STALE_THRESHOLD_MS));

        let gas = self.config.gas_per_order * 2.0;
        let position_limited = if (ask_up + ask_down) > 0.0 {
            self.config.max_position_usd / (ask_up + ask_down)
        } else {
            0.0
        };

        let depth = books.get_pair_depth(
            &pair.token_id_up,
            &pair.token_id_down,
            position_limited,
            self.config.fee_curve_rate,
            self.config.fee_curve_exponent,
            gas,
        )?;

        let total_cost = depth.vwap_up + depth.vwap_down;
        let fees = fees::arb_fees(
            depth.vwap_up,
            depth.vwap_down,
            self.config.fee_curve_rate,
            self.config.fee_curve_exponent,
        );
        let net_profit = 1.0 - total_cost - fees - gas;
        if net_profit <= self.config.min_profit {
            return None;
        }

        let time_to_expiry_sec = (pair.end_date - now).num_seconds();
        if time_to_expiry_sec <= 0 {
            return None;
        }

        let max_quantity = round4(depth.max_profitable_size.min(position_limited));
        if max_quantity <= 0.0 {
            return None;
        }

        Some(PairScore {
            pair,
            depth,
            net_profit,
            total_cost,
            max_quantity,
            books_stale,
            time_to_expiry_sec,
        })
    }

    /// Materialize the full ArbSignal (with String clones) only for the chosen winner.
    fn materialize_signal(&self, score: PairScore<'_>, now: DateTime<Utc>) -> ArbSignal {
        let pair = score.pair;
        ArbSignal {
            condition_id: pair.condition_id.clone(),
            question: pair.question.clone(),
            token_id_up: pair.token_id_up.clone(),
            token_id_down: pair.token_id_down.clone(),
            ask_up: score.depth.best_ask_up,
            ask_down: score.depth.best_ask_down,
            total_cost: score.total_cost,
            net_profit: score.net_profit,
            max_quantity: score.max_quantity,
            position_notional: round4(score.max_quantity * score.total_cost),
            profit_rate: if score.total_cost > 0.0 {
                round4(score.net_profit / score.total_cost)
            } else {
                0.0
            },
            vwap_up: score.depth.vwap_up,
            vwap_down: score.depth.vwap_down,
            time_to_expiry_sec: score.time_to_expiry_sec,
            books_stale: score.books_stale,
            timestamp: now,
        }
    }

    pub fn markets(&self) -> impl Iterator<Item = &MarketPair> {
        self.markets.values()
    }
}

fn round4(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}
