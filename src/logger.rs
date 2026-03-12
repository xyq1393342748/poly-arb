use tracing::{error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};

use crate::{
    arb_engine::ArbSignal,
    order_manager::{ArbExecution, OrderRequest},
};

#[derive(Debug, Clone, Default)]
pub struct Logger;

pub fn init_tracing(level: &str) {
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt().with_env_filter(filter).with_target(false).try_init();
}

impl Logger {
    pub fn log_opportunity(&self, signal: &ArbSignal) {
        info!(
            condition_id = %signal.condition_id,
            total_cost = signal.total_cost,
            net_profit = signal.net_profit,
            quantity = signal.max_quantity,
            "arbitrage opportunity detected"
        );
    }

    pub fn log_order_sent(&self, req: &OrderRequest) {
        info!(
            token_id = %req.token_id,
            side = req.side.as_str(),
            price = req.price,
            size = req.size,
            order_type = req.order_type.as_str(),
            "order submitted"
        );
    }

    pub fn log_execution(&self, exec: &ArbExecution) {
        info!(
            condition_id = %exec.signal.condition_id,
            status = ?exec.status,
            "arb execution updated"
        );
    }

    pub fn log_risk_veto(&self, reason: &str) {
        warn!(reason, "risk veto");
    }

    pub fn log_error(&self, module: &str, err: &str) {
        error!(module, err, "module error");
    }
}
