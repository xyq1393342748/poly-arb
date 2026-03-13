use alloy_primitives::Address;
use anyhow::Result;
use reqwest::Client;
use std::str::FromStr;

use crate::config::Config;

const CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const USDC_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

pub struct Merger {
    rpc_url: String,
    private_key: String,
    chain_id: u64,
    ctf_address: Address,
    client: Client,
    dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct MergeResult {
    pub success: bool,
    pub tx_hash: Option<String>,
    pub amount_recovered: f64,
    pub gas_cost: f64,
    pub error: Option<String>,
}

impl Merger {
    pub fn new(config: &Config) -> Self {
        Self {
            rpc_url: config.merge.polygon_rpc_url.clone(),
            private_key: config.wallet.private_key.clone(),
            chain_id: config.wallet.chain_id,
            ctf_address: Address::from_str(CTF_ADDRESS).expect("invalid CTF address"),
            client: Client::new(),
            dry_run: config.mode.dry_run,
        }
    }

    pub async fn merge_positions(&self, _condition_id: &str, amount: f64) -> Result<MergeResult> {
        if self.dry_run {
            return Ok(MergeResult {
                success: true,
                tx_hash: Some(format!("dry-merge-{}", uuid::Uuid::new_v4())),
                amount_recovered: amount,
                gas_cost: 0.001,
                error: None,
            });
        }

        // Live mode: not yet implemented
        // Will need: ABI encode mergePositions call, sign tx, send via JSON-RPC
        // For now return failure without blocking the main flow
        Ok(MergeResult {
            success: false,
            tx_hash: None,
            amount_recovered: 0.0,
            gas_cost: 0.0,
            error: Some("MERGE not yet implemented for live mode".to_string()),
        })
    }
}
