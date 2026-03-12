use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use alloy_primitives::U256;
use anyhow::{bail, Result};
use rand::RngCore;
use serde_json::Value;

use crate::{
    arb_engine::MarketPair,
    auth::{OrderData, OrderSigner, ZERO_ADDRESS},
    config::{OptimizationConfig, StrategyConfig},
    orderbook::OrderBookManager,
    types::{OrderType, Side},
};

#[derive(Debug, Clone)]
pub struct PreSignedOrderPool {
    cache: HashMap<(String, u32), PreSignedEntry>,
    signer: OrderSigner,
    config: StrategyConfig,
    optimization: OptimizationConfig,
}

#[derive(Debug, Clone)]
struct PreSignedEntry {
    payload: Value,
    created_at: Instant,
}

impl PreSignedOrderPool {
    pub fn new(
        signer: OrderSigner,
        config: StrategyConfig,
        optimization: OptimizationConfig,
    ) -> Self {
        Self {
            cache: HashMap::new(),
            signer,
            config,
            optimization,
        }
    }

    pub fn refresh(&mut self, books: &OrderBookManager, markets: &[MarketPair]) {
        self.evict_stale();
        let mut refreshed_keys = HashSet::new();

        for market in markets {
            let Some((ask_up, size_up, ask_down, size_down)) =
                books.get_pair_asks(&market.token_id_up, &market.token_id_down)
            else {
                continue;
            };
            let total_cost = ask_up + ask_down;
            if total_cost <= 0.0 {
                continue;
            }

            let depth_limited = size_up.min(size_down);
            let position_limited = self.config.max_position_usd / total_cost;
            let quantity = round_to(depth_limited.min(position_limited), 2);
            if quantity <= 0.0 {
                continue;
            }

            for price in price_levels(ask_up, self.optimization.presign_price_offsets) {
                if let Ok(payload) = self.sign_payload(&market.token_id_up, price, quantity) {
                    let key = (market.token_id_up.clone(), price_to_cents(price));
                    refreshed_keys.insert(key.clone());
                    self.cache.insert(
                        key,
                        PreSignedEntry {
                            payload,
                            created_at: Instant::now(),
                        },
                    );
                }
            }
            for price in price_levels(ask_down, self.optimization.presign_price_offsets) {
                if let Ok(payload) = self.sign_payload(&market.token_id_down, price, quantity) {
                    let key = (market.token_id_down.clone(), price_to_cents(price));
                    refreshed_keys.insert(key.clone());
                    self.cache.insert(
                        key,
                        PreSignedEntry {
                            payload,
                            created_at: Instant::now(),
                        },
                    );
                }
            }
        }

        // Only evict stale entries; don't aggressively drop entries for
        // prices that didn't appear in this refresh cycle — the ask may
        // return on the next book update and the cached signature is still valid.
        self.evict_stale();
    }

    pub fn get_or_sign(&mut self, token_id: &str, price: f64, size: f64) -> Result<Value> {
        self.evict_stale();
        let key = (token_id.to_owned(), price_to_cents(price));
        if let Some(entry) = self.cache.get(&key) {
            let expected_size = scale_to_u256(round_to(size, 2), 6)?.to_string();
            let signed_size = entry
                .payload
                .pointer("/order/takerAmount")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if signed_size == expected_size {
                return Ok(entry.payload.clone());
            }
        }

        let payload = self.sign_payload(token_id, price, size)?;
        self.cache.insert(
            key,
            PreSignedEntry {
                payload: payload.clone(),
                created_at: Instant::now(),
            },
        );
        Ok(payload)
    }

    pub fn evict_stale(&mut self) {
        // Keep a 5-second safety margin so we never submit an order that
        // expires on-chain within seconds of submission.
        let ttl = Duration::from_secs(self.optimization.presign_ttl_sec.saturating_sub(5));
        self.cache
            .retain(|_, entry| entry.created_at.elapsed() <= ttl);
    }

    fn sign_payload(&self, token_id: &str, price: f64, size: f64) -> Result<Value> {
        let price = round_to(price, 2);
        let size = round_to(size, 2);
        if price <= 0.0 || size <= 0.0 {
            bail!("price and size must be positive");
        }

        let salt = random_u64();
        let nonce = now_ts();
        let fee_rate_bps = (self.config.taker_fee_rate * 10_000.0).round() as u64;
        let collateral_units = scale_to_u256(price * size, 6)?;
        let conditional_units = scale_to_u256(size, 6)?;
        let expiration = now_ts() + self.optimization.presign_ttl_sec.max(1);
        let order_data = OrderData {
            salt: U256::from(salt),
            maker: self.signer.address,
            signer: self.signer.address,
            taker: ZERO_ADDRESS,
            token_id: token_id.parse()?,
            maker_amount: collateral_units,
            taker_amount: conditional_units,
            expiration: U256::from(expiration),
            nonce: U256::from(nonce),
            fee_rate_bps: U256::from(fee_rate_bps),
            side: 0,
            signature_type: 0,
            chain_id: self.signer.chain_id,
            verifying_contract: OrderSigner::default_exchange(self.signer.chain_id, false)?,
        };
        let signature = self.signer.sign_order(&order_data)?;
        Ok(serde_json::json!({
            "deferExec": false,
            "order": {
                "salt": salt,
                "maker": self.signer.address.to_string(),
                "signer": self.signer.address.to_string(),
                "taker": ZERO_ADDRESS.to_string(),
                "tokenId": token_id,
                "makerAmount": order_data.maker_amount.to_string(),
                "takerAmount": order_data.taker_amount.to_string(),
                "side": Side::Buy.as_str(),
                "expiration": expiration.to_string(),
                "nonce": nonce.to_string(),
                "feeRateBps": fee_rate_bps.to_string(),
                "signatureType": 0,
                "signature": signature,
            },
            "owner": "",
            "orderType": OrderType::Fok.as_str(),
        }))
    }
}

fn price_levels(base_price: f64, offsets: u32) -> Vec<f64> {
    let mut prices = Vec::with_capacity((offsets as usize) * 2 + 1);
    let mut seen = HashSet::new();
    for offset in 0..=offsets {
        let step = offset as f64 * 0.01;
        for candidate in [base_price - step, base_price + step] {
            let rounded = round_to(candidate, 2);
            if rounded < 0.01 {
                continue;
            }
            let cents = price_to_cents(rounded);
            if seen.insert(cents) {
                prices.push(rounded);
            }
        }
    }
    prices
}

fn scale_to_u256(value: f64, decimals: u32) -> Result<U256> {
    if value.is_sign_negative() {
        bail!("negative values are not supported");
    }
    let scaled = (value * 10_f64.powi(decimals as i32)).round() as u128;
    Ok(U256::from(scaled))
}

fn price_to_cents(price: f64) -> u32 {
    (round_to(price, 2) * 100.0).round() as u32
}

fn now_ts() -> u64 {
    chrono::Utc::now().timestamp() as u64
}

fn random_u64() -> u64 {
    let mut rng = rand::thread_rng();
    rng.next_u64()
}

fn round_to(value: f64, decimals: u32) -> f64 {
    let factor = 10_f64.powi(decimals as i32);
    (value * factor).round() / factor
}
