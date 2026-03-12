use chrono::{DateTime, Duration, Utc};
use serde::Serialize;

use crate::{arb_engine::ArbSignal, config::RiskConfig};

#[derive(Debug, Clone)]
pub struct RiskManager {
    config: RiskConfig,
    daily_pnl: f64,
    consecutive_losses: u32,
    last_loss_at: Option<DateTime<Utc>>,
    paused_until: Option<DateTime<Utc>>,
    open_orders: usize,
    one_side_count_today: u32,
    one_side_consecutive: u32,
    last_one_side_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskVeto {
    Approved,
    Rejected(String),
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct RiskSnapshot {
    pub daily_pnl: f64,
    pub daily_trades: u32,
    pub consecutive_losses: u32,
    pub is_trading_allowed: bool,
    pub open_orders: usize,
    pub one_side_count_today: u32,
    pub one_side_consecutive: u32,
}

impl RiskManager {
    pub fn new(config: &RiskConfig) -> Self {
        Self {
            config: config.clone(),
            daily_pnl: 0.0,
            consecutive_losses: 0,
            last_loss_at: None,
            paused_until: None,
            open_orders: 0,
            one_side_count_today: 0,
            one_side_consecutive: 0,
            last_one_side_at: None,
        }
    }

    pub fn check_pre_trade(&self, signal: &ArbSignal) -> RiskVeto {
        self.check_pre_trade_at(signal, Utc::now())
    }

    pub fn check_pre_trade_at(&self, signal: &ArbSignal, now: DateTime<Utc>) -> RiskVeto {
        if let Some(until) = self.paused_until {
            if now < until {
                return RiskVeto::Rejected("trading paused after consecutive losses".to_owned());
            }
        }
        if self.daily_pnl <= -self.config.max_daily_loss {
            return RiskVeto::Rejected("daily loss limit reached".to_owned());
        }
        if self.open_orders >= self.config.max_open_orders {
            return RiskVeto::Rejected("max open orders reached".to_owned());
        }
        if self.one_side_count_today >= self.config.max_one_side_per_day {
            return RiskVeto::Rejected("daily one-side fill limit reached".to_owned());
        }
        if self.one_side_consecutive >= self.config.max_one_side_consecutive {
            // If timestamp is missing but counter exceeds limit, still reject
            // (defense-in-depth: don't bypass cooldown due to missing timestamp).
            let cooldown_active = match self.last_one_side_at {
                Some(at) => now < at + Duration::seconds(self.config.one_side_cooldown_sec as i64),
                None => true,
            };
            if cooldown_active {
                return RiskVeto::Rejected("one-side fill cooldown active".to_owned());
            }
        }
        if let Some(last_loss_at) = self.last_loss_at {
            let cooldown = Duration::seconds(self.config.cooldown_after_loss_sec as i64);
            if now < last_loss_at + cooldown {
                return RiskVeto::Rejected("cooldown after loss active".to_owned());
            }
        }
        if signal.profit_rate < self.config.min_profit_rate {
            return RiskVeto::Rejected("profit rate below minimum threshold".to_owned());
        }
        if signal.books_stale {
            return RiskVeto::Rejected("orderbook is stale".to_owned());
        }
        if signal.time_to_expiry_sec <= self.config.min_time_to_expiry_sec {
            return RiskVeto::Rejected("market too close to expiry".to_owned());
        }
        if signal.position_notional <= 0.0 {
            return RiskVeto::Rejected("position notional must be positive".to_owned());
        }
        RiskVeto::Approved
    }

    pub fn record_trade_result(&mut self, profit: f64) {
        self.daily_pnl += profit;
        if profit < 0.0 {
            self.consecutive_losses += 1;
            self.last_loss_at = Some(Utc::now());
            if self.consecutive_losses >= 3 {
                self.paused_until = Some(Utc::now() + Duration::minutes(5));
            }
        } else {
            self.consecutive_losses = 0;
            self.last_loss_at = None;
            self.paused_until = None;
        }
    }

    pub fn is_trading_allowed(&self) -> bool {
        self.check_pre_trade_at(
            &ArbSignal {
                condition_id: String::new(),
                question: String::new(),
                token_id_up: String::new(),
                token_id_down: String::new(),
                ask_up: 0.0,
                ask_down: 0.0,
                total_cost: 0.0,
                net_profit: 0.0,
                max_quantity: 0.0,
                position_notional: 1.0,
                profit_rate: f64::MAX,
                time_to_expiry_sec: 60,
                books_stale: false,
                timestamp: Utc::now(),
            },
            Utc::now(),
        ) == RiskVeto::Approved
    }

    pub fn record_one_side(&mut self) {
        self.one_side_count_today += 1;
        self.one_side_consecutive += 1;
        self.last_one_side_at = Some(Utc::now());
    }

    pub fn record_both_filled(&mut self) {
        self.one_side_consecutive = 0;
        self.last_one_side_at = None;
    }

    pub fn daily_pnl(&self) -> f64 {
        self.daily_pnl
    }

    pub fn reset_daily(&mut self) {
        self.daily_pnl = 0.0;
        self.consecutive_losses = 0;
        self.last_loss_at = None;
        self.paused_until = None;
        self.open_orders = 0;
        self.one_side_count_today = 0;
        self.one_side_consecutive = 0;
        self.last_one_side_at = None;
    }

    pub fn set_open_orders(&mut self, open_orders: usize) {
        self.open_orders = open_orders;
    }

    pub fn increment_open_orders(&mut self) {
        self.open_orders += 1;
    }

    pub fn decrement_open_orders(&mut self) {
        self.open_orders = self.open_orders.saturating_sub(1);
    }

    pub fn snapshot(&self, daily_trades: u32) -> RiskSnapshot {
        RiskSnapshot {
            daily_pnl: self.daily_pnl,
            daily_trades,
            consecutive_losses: self.consecutive_losses,
            is_trading_allowed: self.is_trading_allowed(),
            open_orders: self.open_orders,
            one_side_count_today: self.one_side_count_today,
            one_side_consecutive: self.one_side_consecutive,
        }
    }
}
