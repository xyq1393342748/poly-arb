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

    /// 扫描多级 ask，返回各级 (price, size)，累计成本不超过 max_cost
    pub fn sweep_asks(&self, max_cost: f64) -> Vec<(f64, f64)> {
        let mut result = Vec::new();
        let mut total = 0.0;
        for (price, size) in &self.asks {
            let p = price.into_inner();
            let cost = p * *size;
            if total + cost > max_cost {
                let remaining = max_cost - total;
                if remaining > 0.0 && p > 0.0 {
                    result.push((p, remaining / p));
                }
                break;
            }
            result.push((p, *size));
            total += cost;
        }
        result
    }

    /// 计算买入 target_size 份的加权平均成本
    pub fn vwap_ask(&self, target_size: f64) -> Option<(f64, f64)> {
        if target_size <= 0.0 {
            return None;
        }
        let mut remaining = target_size;
        let mut total_cost = 0.0;
        let mut filled = 0.0;
        for (price, size) in &self.asks {
            let p = price.into_inner();
            let take = size.min(remaining);
            total_cost += p * take;
            filled += take;
            remaining -= take;
            if remaining <= 0.0 {
                break;
            }
        }
        if filled <= 0.0 {
            return None;
        }
        Some((total_cost / filled, filled))
    }

    /// 暴露 ask levels 给同 crate 的其他模块
    pub fn ask_levels(&self) -> impl Iterator<Item = (f64, f64)> + '_ {
        self.asks.iter().map(|(p, s)| (p.into_inner(), *s))
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

    pub fn get_pair_depth(
        &self,
        up_id: &str,
        down_id: &str,
        target_size: f64,
        fee_rate: f64,
        gas: f64,
    ) -> Option<DepthAnalysis> {
        let up_book = self.books.get(up_id)?;
        let down_book = self.books.get(down_id)?;
        let (best_ask_up, best_size_up) = up_book.best_ask()?;
        let (best_ask_down, best_size_down) = down_book.best_ask()?;

        let up_asks: Vec<(f64, f64)> = up_book.ask_levels().collect();
        let down_asks: Vec<(f64, f64)> = down_book.ask_levels().collect();

        // 双指针扫描计算 max_profitable_size
        let threshold = 1.0 - fee_rate * 2.0 - gas;
        let mut ui = 0usize;
        let mut di = 0usize;
        let mut u_remaining = if !up_asks.is_empty() {
            up_asks[0].1
        } else {
            0.0
        };
        let mut d_remaining = if !down_asks.is_empty() {
            down_asks[0].1
        } else {
            0.0
        };
        let mut profitable_size = 0.0;
        let mut total_up_cost = 0.0;
        let mut total_down_cost = 0.0;

        while ui < up_asks.len() && di < down_asks.len() {
            let up_price = up_asks[ui].0;
            let down_price = down_asks[di].0;
            if up_price + down_price >= threshold {
                break;
            }
            let take = u_remaining
                .min(d_remaining)
                .min(target_size - profitable_size);
            if take <= 0.0 {
                break;
            }
            profitable_size += take;
            total_up_cost += up_price * take;
            total_down_cost += down_price * take;
            u_remaining -= take;
            d_remaining -= take;
            if u_remaining <= 0.0 {
                ui += 1;
                if ui < up_asks.len() {
                    u_remaining = up_asks[ui].1;
                }
            }
            if d_remaining <= 0.0 {
                di += 1;
                if di < down_asks.len() {
                    d_remaining = down_asks[di].1;
                }
            }
            if profitable_size >= target_size {
                break;
            }
        }

        if profitable_size <= 0.0 {
            return None;
        }

        Some(DepthAnalysis {
            vwap_up: total_up_cost / profitable_size,
            vwap_down: total_down_cost / profitable_size,
            size_up: profitable_size,
            size_down: profitable_size,
            best_ask_up,
            best_ask_down,
            best_size_up,
            best_size_down,
            max_profitable_size: profitable_size,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DepthAnalysis {
    pub vwap_up: f64,
    pub vwap_down: f64,
    pub size_up: f64,
    pub size_down: f64,
    pub best_ask_up: f64,
    pub best_ask_down: f64,
    pub best_size_up: f64,
    pub best_size_down: f64,
    pub max_profitable_size: f64,
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
