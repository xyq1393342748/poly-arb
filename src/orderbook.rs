use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};

use ordered_float::OrderedFloat;
use serde::Serialize;

use crate::{types::PriceLevel, ws_market::BookUpdate};

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub asset_id: String,
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
    last_update: Instant,
    last_update_ms: u64,
}

impl OrderBook {
    pub fn new(asset_id: &str) -> Self {
        Self {
            asset_id: asset_id.to_owned(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update: Instant::now(),
            last_update_ms: 0,
        }
    }

    pub fn apply_snapshot(&mut self, update: &BookUpdate) {
        self.bids.clear();
        self.asks.clear();
        self.apply_levels(&update.bids, true);
        self.apply_levels(&update.asks, false);
        self.touch(update.timestamp);
    }

    pub fn apply_delta(&mut self, update: &BookUpdate) {
        self.apply_levels(&update.bids, true);
        self.apply_levels(&update.asks, false);
        self.touch(update.timestamp);
    }

    pub fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks
            .iter()
            .next()
            .map(|(price, size)| (price.into_inner(), *size))
    }

    pub fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, size)| (price.into_inner(), *size))
    }

    pub fn available_size_at_ask(&self, max_price: f64) -> f64 {
        self.asks
            .iter()
            .filter(|(price, _)| price.into_inner() <= max_price)
            .map(|(_, size)| *size)
            .sum()
    }

    pub fn is_stale(&self, threshold: Duration) -> bool {
        self.last_update.elapsed() > threshold
    }

    pub fn summary(&self) -> OrderBookSnapshot {
        OrderBookSnapshot {
            asset_id: self.asset_id.clone(),
            best_bid: self.best_bid().map(|(price, _)| price),
            best_ask: self.best_ask().map(|(price, _)| price),
            last_update_ms: self.last_update_ms,
            bid_levels: self
                .bids
                .iter()
                .rev()
                .take(5)
                .map(|(price, size)| PriceLevel::new(price.into_inner(), *size))
                .collect(),
            ask_levels: self
                .asks
                .iter()
                .take(5)
                .map(|(price, size)| PriceLevel::new(price.into_inner(), *size))
                .collect(),
        }
    }

    fn apply_levels(&mut self, levels: &[PriceLevel], is_bid: bool) {
        let book = if is_bid {
            &mut self.bids
        } else {
            &mut self.asks
        };

        for level in levels {
            let price = OrderedFloat(level.price);
            if level.size <= 0.0 {
                book.remove(&price);
            } else {
                book.insert(price, level.size);
            }
        }
    }

    fn touch(&mut self, timestamp: u64) {
        self.last_update = Instant::now();
        self.last_update_ms = timestamp;
    }
}

#[derive(Debug, Clone, Default)]
pub struct OrderBookManager {
    books: HashMap<String, OrderBook>,
}

impl OrderBookManager {
    pub fn process_update(&mut self, update: BookUpdate) {
        let book = self
            .books
            .entry(update.asset_id.clone())
            .or_insert_with(|| OrderBook::new(&update.asset_id));
        if update.is_snapshot {
            book.apply_snapshot(&update);
        } else {
            book.apply_delta(&update);
        }
    }

    pub fn get_book(&self, asset_id: &str) -> Option<&OrderBook> {
        self.books.get(asset_id)
    }

    pub fn get_pair_asks(&self, up_id: &str, down_id: &str) -> Option<(f64, f64, f64, f64)> {
        let up = self.books.get(up_id)?.best_ask()?;
        let down = self.books.get(down_id)?.best_ask()?;
        Some((up.0, up.1, down.0, down.1))
    }

    pub fn len(&self) -> usize {
        self.books.len()
    }

    pub fn snapshots(&self) -> Vec<OrderBookSnapshot> {
        self.books.values().map(OrderBook::summary).collect()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderBookSnapshot {
    pub asset_id: String,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub last_update_ms: u64,
    pub bid_levels: Vec<PriceLevel>,
    pub ask_levels: Vec<PriceLevel>,
}
