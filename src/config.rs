use std::{fs, path::Path};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub api: ApiConfig,
    pub wallet: WalletConfig,
    pub market: MarketConfig,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    #[serde(default)]
    pub optimization: OptimizationConfig,
    pub mode: ModeConfig,
    pub web: WebConfig,
    pub store: StoreConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api: ApiConfig::default(),
            wallet: WalletConfig::default(),
            market: MarketConfig::default(),
            strategy: StrategyConfig::default(),
            risk: RiskConfig::default(),
            optimization: OptimizationConfig::default(),
            mode: ModeConfig::default(),
            web: WebConfig::default(),
            store: StoreConfig::default(),
        }
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let raw = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&raw)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.wallet.chain_id == 0 {
            bail!("wallet.chain_id must be > 0");
        }
        if self.market.poll_interval_sec == 0 {
            bail!("market.poll_interval_sec must be > 0");
        }
        if self.strategy.min_profit < 0.0 {
            bail!("strategy.min_profit must be >= 0");
        }
        if self.strategy.max_position_usd <= 0.0 {
            bail!("strategy.max_position_usd must be > 0");
        }
        if !(0.0..=0.1).contains(&self.strategy.taker_fee_rate) {
            bail!("strategy.taker_fee_rate must be between 0 and 0.1");
        }
        if self.strategy.gas_per_order < 0.0 {
            bail!("strategy.gas_per_order must be >= 0");
        }
        if self.risk.max_daily_loss < 0.0 {
            bail!("risk.max_daily_loss must be >= 0");
        }
        if self.risk.max_open_orders == 0 {
            bail!("risk.max_open_orders must be > 0");
        }
        if self.risk.min_profit_rate < 0.0 {
            bail!("risk.min_profit_rate must be >= 0");
        }
        if self.risk.recovery_retry_timeout_ms == 0 {
            bail!("risk.recovery_retry_timeout_ms must be > 0");
        }
        if self.risk.sell_price_step <= 0.0 {
            bail!("risk.sell_price_step must be > 0");
        }
        if self.risk.min_time_to_expiry_sec < 0 {
            bail!("risk.min_time_to_expiry_sec must be >= 0");
        }
        if self.optimization.connection_pool_size == 0 {
            bail!("optimization.connection_pool_size must be > 0");
        }
        if self.optimization.ws_redundant_connections == 0 {
            bail!("optimization.ws_redundant_connections must be > 0");
        }
        if self.web.enabled && self.web.bind.trim().is_empty() {
            bail!("web.bind must be set when web is enabled");
        }
        if self.web.ws_push_interval_ms == 0 {
            bail!("web.ws_push_interval_ms must be > 0");
        }
        if self.store.db_path.trim().is_empty() {
            bail!("store.db_path must not be empty");
        }

        if !self.mode.dry_run {
            if self.api.api_key.trim().is_empty()
                || self.api.api_secret.trim().is_empty()
                || self.api.passphrase.trim().is_empty()
            {
                bail!("api credentials are required when dry_run = false");
            }
            if self.wallet.private_key.trim().is_empty() {
                bail!("wallet.private_key is required when dry_run = false");
            }
        }

        Ok(())
    }

    pub fn db_path(&self, base_dir: &Path) -> String {
        let db_path = Path::new(&self.store.db_path);
        if db_path.is_absolute() {
            db_path.display().to_string()
        } else {
            base_dir.join(db_path).display().to_string()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ApiConfig {
    pub clob_rest_url: String,
    pub clob_ws_url: String,
    pub clob_user_ws_url: String,
    pub gamma_rest_url: String,
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            clob_rest_url: "https://clob.polymarket.com".to_owned(),
            clob_ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
            clob_user_ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/user".to_owned(),
            gamma_rest_url: "https://gamma-api.polymarket.com".to_owned(),
            api_key: String::new(),
            api_secret: String::new(),
            passphrase: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WalletConfig {
    pub private_key: String,
    pub chain_id: u64,
}

impl Default for WalletConfig {
    fn default() -> Self {
        Self {
            private_key: String::new(),
            chain_id: 137,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MarketConfig {
    pub asset: String,
    pub duration: String,
    pub poll_interval_sec: u64,
}

impl Default for MarketConfig {
    fn default() -> Self {
        Self {
            asset: "BTC".to_owned(),
            duration: "5m".to_owned(),
            poll_interval_sec: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StrategyConfig {
    pub min_profit: f64,
    pub max_position_usd: f64,
    pub taker_fee_rate: f64,
    pub gas_per_order: f64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            min_profit: 0.005,
            max_position_usd: 50.0,
            taker_fee_rate: 0.001,
            gas_per_order: 0.007,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RiskConfig {
    pub max_daily_loss: f64,
    pub max_open_orders: usize,
    pub cooldown_after_loss_sec: u64,
    pub max_one_side_per_day: u32,
    pub max_one_side_consecutive: u32,
    pub one_side_cooldown_sec: u64,
    pub min_profit_rate: f64,
    pub recovery_retry_timeout_ms: u64,
    pub sell_max_retries: u32,
    pub sell_price_step: f64,
    pub min_time_to_expiry_sec: i64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_daily_loss: 20.0,
            max_open_orders: 4,
            cooldown_after_loss_sec: 60,
            max_one_side_per_day: 5,
            max_one_side_consecutive: 2,
            one_side_cooldown_sec: 900,
            min_profit_rate: 0.01,
            recovery_retry_timeout_ms: 500,
            sell_max_retries: 3,
            sell_price_step: 0.01,
            min_time_to_expiry_sec: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OptimizationConfig {
    pub use_batch_orders: bool,
    pub presign_enabled: bool,
    pub presign_price_offsets: u32,
    pub presign_ttl_sec: u64,
    pub ws_redundant_connections: usize,
    pub http2_enabled: bool,
    pub connection_pool_size: u32,
    pub tcp_nodelay: bool,
}

impl Default for OptimizationConfig {
    fn default() -> Self {
        Self {
            use_batch_orders: true,
            presign_enabled: true,
            presign_price_offsets: 3,
            presign_ttl_sec: 30,
            ws_redundant_connections: 2,
            http2_enabled: true,
            connection_pool_size: 4,
            tcp_nodelay: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ModeConfig {
    pub dry_run: bool,
    pub log_level: String,
}

impl Default for ModeConfig {
    fn default() -> Self {
        Self {
            dry_run: true,
            log_level: "info".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WebConfig {
    pub enabled: bool,
    pub bind: String,
    pub ws_push_interval_ms: u64,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: "0.0.0.0:3721".to_owned(),
            ws_push_interval_ms: 500,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StoreConfig {
    pub db_path: String,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            db_path: "data/poly_arb.db".to_owned(),
        }
    }
}
