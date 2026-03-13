use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{config::StrategyConfig, orderbook::OrderBookManager};

const STALE_THRESHOLD_MS: u64 = 5_000;

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

    pub fn evaluate(&self, books: &OrderBookManager) -> Vec<ArbSignal> {
        self.markets
            .values()
            .filter_map(|pair| self.evaluate_pair(pair, books))
            .collect()
    }

    pub fn evaluate_pair(&self, pair: &MarketPair, books: &OrderBookManager) -> Option<ArbSignal> {
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
            self.config.taker_fee_rate,
            gas,
        )?;

        let total_cost = depth.vwap_up + depth.vwap_down;
        let fees = total_cost * self.config.taker_fee_rate;
        let net_profit = 1.0 - total_cost - fees - gas;
        if net_profit <= self.config.min_profit {
            return None;
        }

        let time_to_expiry_sec = (pair.end_date - Utc::now()).num_seconds();
        if time_to_expiry_sec <= 0 {
            return None;
        }

        let depth_limited = depth.max_profitable_size;
        let max_quantity = round4(depth_limited.min(position_limited));
        if max_quantity <= 0.0 {
            return None;
        }

        Some(ArbSignal {
            condition_id: pair.condition_id.clone(),
            question: pair.question.clone(),
            token_id_up: pair.token_id_up.clone(),
            token_id_down: pair.token_id_down.clone(),
            ask_up,
            ask_down,
            total_cost,
            net_profit,
            max_quantity,
            position_notional: round4(max_quantity * total_cost),
            profit_rate: if total_cost > 0.0 {
                round4(net_profit / total_cost)
            } else {
                0.0
            },
            vwap_up: depth.vwap_up,
            vwap_down: depth.vwap_down,
            time_to_expiry_sec,
            books_stale,
            timestamp: Utc::now(),
        })
    }

    pub fn markets(&self) -> impl Iterator<Item = &MarketPair> {
        self.markets.values()
    }
}

fn round4(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}
