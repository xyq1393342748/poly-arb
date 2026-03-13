use alloy_primitives::{keccak256, Address};
use anyhow::{anyhow, bail, Result};
use k256::ecdsa::signature::hazmat::PrehashSigner;
use k256::ecdsa::{RecoveryId, Signature as K256Signature, SigningKey, VerifyingKey};
use reqwest::Client;
use serde_json::Value;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::config::Config;

const CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const USDC_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
const USDC_DECIMALS: u32 = 6;

pub struct Merger {
    rpc_url: String,
    signing_key: Option<Arc<SigningKey>>,
    address: Address,
    chain_id: u64,
    ctf_address: Address,
    usdc_address: Address,
    gas_limit: u64,
    max_gas_price_gwei: f64,
    merge_timeout_sec: u64,
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

impl MergeResult {
    pub fn failed(reason: &str) -> Self {
        Self {
            success: false,
            tx_hash: None,
            amount_recovered: 0.0,
            gas_cost: 0.0,
            error: Some(reason.to_string()),
        }
    }
}

impl Merger {
    pub fn new(config: &Config) -> Self {
        let pk_hex = config.wallet.private_key.trim();
        let (signing_key, address) = if pk_hex.is_empty() {
            (None, Address::ZERO)
        } else {
            let normalized = pk_hex.trim_start_matches("0x");
            let raw = hex::decode(normalized).expect("invalid private key hex");
            let sk = SigningKey::from_slice(&raw).expect("invalid private key");
            let vk = VerifyingKey::from(&sk);
            let addr = public_key_to_address(&vk);
            (Some(Arc::new(sk)), addr)
        };

        Self {
            rpc_url: config.merge.polygon_rpc_url.clone(),
            signing_key,
            address,
            chain_id: config.wallet.chain_id,
            ctf_address: Address::from_str(CTF_ADDRESS).expect("invalid CTF address"),
            usdc_address: Address::from_str(USDC_ADDRESS).expect("invalid USDC address"),
            gas_limit: config.merge.merge_gas_limit,
            max_gas_price_gwei: config.merge.max_gas_price_gwei,
            merge_timeout_sec: config.merge.merge_timeout_sec,
            client: Client::new(),
            dry_run: config.mode.dry_run,
        }
    }

    /// Merge conditional token positions back into collateral (USDC).
    pub async fn merge_positions(
        &self,
        condition_id: &str,
        amount: f64,
    ) -> Result<MergeResult> {
        if self.dry_run {
            return Ok(MergeResult {
                success: true,
                tx_hash: Some(format!("dry-merge-{}", uuid::Uuid::new_v4())),
                amount_recovered: amount,
                gas_cost: 0.001,
                error: None,
            });
        }

        let signing_key = self
            .signing_key
            .as_ref()
            .ok_or_else(|| anyhow!("signing key not configured for live merge"))?;

        // Scale amount to USDC decimals (1e6)
        let amount_raw = (amount * 10f64.powi(USDC_DECIMALS as i32)) as u64;
        if amount_raw == 0 {
            return Ok(MergeResult::failed("merge amount rounds to zero"));
        }

        // Parse condition_id to bytes32
        let cond_bytes = parse_bytes32(condition_id)?;

        // Build mergePositions calldata
        let calldata = encode_merge_positions(self.usdc_address, cond_bytes, amount_raw);

        // Get nonce
        let nonce = self.get_nonce().await?;
        debug!(nonce, "got nonce for merge tx");

        // Get gas price
        let gas_price = self.get_gas_price().await?;
        let gas_price_gwei = gas_price as f64 / 1e9;
        if gas_price_gwei > self.max_gas_price_gwei {
            return Ok(MergeResult::failed(&format!(
                "gas price {gas_price_gwei:.1} gwei exceeds max {:.1} gwei",
                self.max_gas_price_gwei
            )));
        }
        info!(gas_price_gwei = format!("{gas_price_gwei:.1}"), "merge gas price");

        // Sign transaction
        let signed_tx = sign_legacy_tx(
            signing_key,
            self.chain_id,
            nonce,
            gas_price,
            self.gas_limit,
            self.ctf_address,
            calldata,
        )?;

        // Send raw transaction
        let tx_hash = self.send_raw_transaction(&signed_tx).await?;
        info!(tx_hash, "merge tx sent");

        // Wait for receipt
        let receipt = self
            .wait_for_receipt(&tx_hash, self.merge_timeout_sec)
            .await?;

        // Parse receipt
        let status = receipt
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("0x0");
        let success = status == "0x1";

        let gas_used = parse_hex_u64(
            receipt
                .get("gasUsed")
                .and_then(|v| v.as_str())
                .unwrap_or("0x0"),
        );
        let effective_gas_price = parse_hex_u64(
            receipt
                .get("effectiveGasPrice")
                .and_then(|v| v.as_str())
                .unwrap_or(&format!("0x{:x}", gas_price)),
        );
        let gas_cost_wei = gas_used as u128 * effective_gas_price as u128;
        let gas_cost_matic = gas_cost_wei as f64 / 1e18;

        if success {
            info!(
                tx_hash,
                amount,
                gas_cost_matic = format!("{gas_cost_matic:.6}"),
                "merge succeeded"
            );
            Ok(MergeResult {
                success: true,
                tx_hash: Some(tx_hash),
                amount_recovered: amount,
                gas_cost: gas_cost_matic,
                error: None,
            })
        } else {
            warn!(tx_hash, "merge tx reverted");
            Ok(MergeResult {
                success: false,
                tx_hash: Some(tx_hash),
                amount_recovered: 0.0,
                gas_cost: gas_cost_matic,
                error: Some("transaction reverted".to_string()),
            })
        }
    }

    /// Check if `owner` has approved `operator` on the CTF contract.
    pub async fn check_approval(&self, owner: Address, operator: Address) -> Result<bool> {
        // isApprovedForAll(address,address) selector
        let selector = &keccak256("isApprovedForAll(address,address)".as_bytes())[..4];
        let mut calldata = Vec::with_capacity(68);
        calldata.extend_from_slice(selector);
        calldata.extend_from_slice(&address_word(owner));
        calldata.extend_from_slice(&address_word(operator));

        let result = self.eth_call(self.ctf_address, &calldata).await?;
        // Result is a 32-byte bool
        if result.len() >= 32 {
            Ok(result[31] == 1)
        } else {
            Ok(false)
        }
    }

    /// Send setApprovalForAll(operator, true) on the CTF contract.
    pub async fn ensure_approval(&self, operator: Address) -> Result<String> {
        let signing_key = self
            .signing_key
            .as_ref()
            .ok_or_else(|| anyhow!("signing key not configured"))?;

        // setApprovalForAll(address,bool) selector
        let selector = &keccak256("setApprovalForAll(address,bool)".as_bytes())[..4];
        let mut calldata = Vec::with_capacity(68);
        calldata.extend_from_slice(selector);
        calldata.extend_from_slice(&address_word(operator));
        calldata.extend_from_slice(&bool_word(true));

        let nonce = self.get_nonce().await?;
        let gas_price = self.get_gas_price().await?;

        let signed_tx = sign_legacy_tx(
            signing_key,
            self.chain_id,
            nonce,
            gas_price,
            60_000, // approval uses less gas
            self.ctf_address,
            calldata,
        )?;

        let tx_hash = self.send_raw_transaction(&signed_tx).await?;
        info!(tx_hash, "setApprovalForAll tx sent");

        let receipt = self
            .wait_for_receipt(&tx_hash, self.merge_timeout_sec)
            .await?;
        let status = receipt
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("0x0");
        if status != "0x1" {
            bail!("setApprovalForAll tx reverted: {tx_hash}");
        }

        Ok(tx_hash)
    }

    // ---- JSON-RPC helpers ----

    async fn rpc_call(&self, method: &str, params: Value) -> Result<Value> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        });
        let resp = self
            .client
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .await?
            .json::<Value>()
            .await?;
        if let Some(err) = resp.get("error") {
            bail!("RPC error in {method}: {err}");
        }
        resp.get("result")
            .cloned()
            .ok_or_else(|| anyhow!("no result in RPC response for {method}"))
    }

    async fn get_nonce(&self) -> Result<u64> {
        let result = self
            .rpc_call(
                "eth_getTransactionCount",
                serde_json::json!([format!("0x{}", hex::encode(self.address.as_slice())), "latest"]),
            )
            .await?;
        let hex_str = result.as_str().ok_or_else(|| anyhow!("nonce not a string"))?;
        Ok(parse_hex_u64(hex_str))
    }

    async fn get_gas_price(&self) -> Result<u64> {
        let result = self
            .rpc_call("eth_gasPrice", serde_json::json!([]))
            .await?;
        let hex_str = result
            .as_str()
            .ok_or_else(|| anyhow!("gasPrice not a string"))?;
        Ok(parse_hex_u64(hex_str))
    }

    async fn send_raw_transaction(&self, signed_tx: &[u8]) -> Result<String> {
        let result = self
            .rpc_call(
                "eth_sendRawTransaction",
                serde_json::json!([format!("0x{}", hex::encode(signed_tx))]),
            )
            .await?;
        result
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("tx hash not a string"))
    }

    async fn wait_for_receipt(&self, tx_hash: &str, timeout_sec: u64) -> Result<Value> {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(timeout_sec);
        loop {
            let result = self
                .rpc_call(
                    "eth_getTransactionReceipt",
                    serde_json::json!([tx_hash]),
                )
                .await?;
            if !result.is_null() {
                return Ok(result);
            }
            if start.elapsed() > timeout {
                bail!("timeout waiting for tx receipt: {tx_hash}");
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }

    async fn eth_call(&self, to: Address, data: &[u8]) -> Result<Vec<u8>> {
        let result = self
            .rpc_call(
                "eth_call",
                serde_json::json!([
                    {
                        "to": format!("0x{}", hex::encode(to.as_slice())),
                        "data": format!("0x{}", hex::encode(data))
                    },
                    "latest"
                ]),
            )
            .await?;
        let hex_str = result
            .as_str()
            .ok_or_else(|| anyhow!("eth_call result not a string"))?;
        let normalized = hex_str.trim_start_matches("0x");
        Ok(hex::decode(normalized)?)
    }
}

// ---- ABI encoding ----

/// Encode mergePositions(address,bytes32,bytes32,uint256[],uint256) calldata.
fn encode_merge_positions(
    collateral: Address,
    condition_id: [u8; 32],
    amount: u64,
) -> Vec<u8> {
    // Function selector: keccak256("mergePositions(address,bytes32,bytes32,uint256[],uint256)")
    let selector =
        &keccak256("mergePositions(address,bytes32,bytes32,uint256[],uint256)".as_bytes())[..4];

    // Calldata: selector + 8 words
    let mut data = Vec::with_capacity(260);
    data.extend_from_slice(selector);
    // word0: collateral address
    data.extend_from_slice(&address_word(collateral));
    // word1: parentCollectionId = bytes32(0)
    data.extend_from_slice(&[0u8; 32]);
    // word2: conditionId
    data.extend_from_slice(&condition_id);
    // word3: offset to partition array = 160 (0xa0) — 5 words from start of params
    data.extend_from_slice(&u256_word(160));
    // word4: amount
    data.extend_from_slice(&u256_word(amount as u128));
    // word5: array length = 2
    data.extend_from_slice(&u256_word(2));
    // word6: partition[0] = 1
    data.extend_from_slice(&u256_word(1));
    // word7: partition[1] = 2
    data.extend_from_slice(&u256_word(2));

    data
}

// ---- RLP encoding ----

/// RLP-encode a single byte string.
fn rlp_encode_bytes(data: &[u8]) -> Vec<u8> {
    if data.len() == 1 && data[0] < 0x80 {
        // Single byte, no prefix
        vec![data[0]]
    } else if data.is_empty() {
        vec![0x80]
    } else if data.len() <= 55 {
        let mut out = Vec::with_capacity(1 + data.len());
        out.push(0x80 + data.len() as u8);
        out.extend_from_slice(data);
        out
    } else {
        let len_bytes = to_be_bytes_trimmed(data.len() as u64);
        let mut out = Vec::with_capacity(1 + len_bytes.len() + data.len());
        out.push(0xb7 + len_bytes.len() as u8);
        out.extend_from_slice(&len_bytes);
        out.extend_from_slice(data);
        out
    }
}

/// RLP-encode a u64 as a byte string (minimal big-endian, no leading zeros).
fn rlp_encode_u64(value: u64) -> Vec<u8> {
    if value == 0 {
        vec![0x80] // empty byte string
    } else {
        let bytes = to_be_bytes_trimmed(value);
        rlp_encode_bytes(&bytes)
    }
}

/// RLP-encode a list from already-encoded items.
fn rlp_encode_list(encoded_items: &[Vec<u8>]) -> Vec<u8> {
    let payload: Vec<u8> = encoded_items.iter().flat_map(|v| v.iter().copied()).collect();
    let payload_len = payload.len();
    if payload_len <= 55 {
        let mut out = Vec::with_capacity(1 + payload_len);
        out.push(0xc0 + payload_len as u8);
        out.extend_from_slice(&payload);
        out
    } else {
        let len_bytes = to_be_bytes_trimmed(payload_len as u64);
        let mut out = Vec::with_capacity(1 + len_bytes.len() + payload_len);
        out.push(0xf7 + len_bytes.len() as u8);
        out.extend_from_slice(&len_bytes);
        out.extend_from_slice(&payload);
        out
    }
}

/// Return big-endian bytes with no leading zeros.
fn to_be_bytes_trimmed(value: u64) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(7);
    bytes[start..].to_vec()
}

// ---- Transaction signing (EIP-155 legacy) ----

fn sign_legacy_tx(
    signing_key: &SigningKey,
    chain_id: u64,
    nonce: u64,
    gas_price: u64,
    gas_limit: u64,
    to: Address,
    data: Vec<u8>,
) -> Result<Vec<u8>> {
    // 1. RLP encode for signing: [nonce, gasPrice, gasLimit, to, value=0, data, chainId, 0, 0]
    let items_for_signing = vec![
        rlp_encode_u64(nonce),
        rlp_encode_u64(gas_price),
        rlp_encode_u64(gas_limit),
        rlp_encode_bytes(to.as_slice()),        // 20 bytes
        rlp_encode_u64(0),                       // value = 0
        rlp_encode_bytes(&data),
        rlp_encode_u64(chain_id),
        rlp_encode_u64(0),                       // EIP-155 empty r
        rlp_encode_u64(0),                       // EIP-155 empty s
    ];
    let unsigned_rlp = rlp_encode_list(&items_for_signing);

    // 2. keccak256 hash
    let hash: [u8; 32] = keccak256(&unsigned_rlp).into();

    // 3. Sign (same pattern as auth.rs)
    let (signature, recovery_id): (K256Signature, RecoveryId) = signing_key
        .sign_prehash(&hash)
        .map_err(|err| anyhow!("signing failed: {err}"))?;

    let sig_bytes = signature.to_bytes();
    let r = &sig_bytes[..32];
    let s = &sig_bytes[32..64];

    // 4. Compute v = recovery_id + 35 + 2 * chain_id (EIP-155)
    let v = recovery_id.to_byte() as u64 + 35 + 2 * chain_id;

    // 5. RLP encode signed tx: [nonce, gasPrice, gasLimit, to, value=0, data, v, r, s]
    let r_trimmed = trim_leading_zeros(r);
    let s_trimmed = trim_leading_zeros(s);

    let signed_items = vec![
        rlp_encode_u64(nonce),
        rlp_encode_u64(gas_price),
        rlp_encode_u64(gas_limit),
        rlp_encode_bytes(to.as_slice()),
        rlp_encode_u64(0), // value = 0
        rlp_encode_bytes(&data),
        rlp_encode_u64(v),
        rlp_encode_bytes(r_trimmed),
        rlp_encode_bytes(s_trimmed),
    ];

    Ok(rlp_encode_list(&signed_items))
}

// ---- Utilities ----

fn trim_leading_zeros(data: &[u8]) -> &[u8] {
    let start = data.iter().position(|&b| b != 0).unwrap_or(data.len());
    if start == data.len() {
        &data[data.len() - 1..] // keep at least one zero byte
    } else {
        &data[start..]
    }
}

fn parse_bytes32(hex_str: &str) -> Result<[u8; 32]> {
    let normalized = hex_str.trim_start_matches("0x");
    let bytes = hex::decode(normalized)?;
    if bytes.len() != 32 {
        bail!(
            "expected 32 bytes for condition_id, got {}",
            bytes.len()
        );
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn parse_hex_u64(hex_str: &str) -> u64 {
    let normalized = hex_str.trim_start_matches("0x");
    u64::from_str_radix(normalized, 16).unwrap_or(0)
}

fn public_key_to_address(verifying_key: &VerifyingKey) -> Address {
    let encoded = verifying_key.to_encoded_point(false);
    let hash = keccak256(&encoded.as_bytes()[1..]);
    Address::from_slice(&hash[12..])
}

fn address_word(addr: Address) -> [u8; 32] {
    let mut word = [0u8; 32];
    word[12..].copy_from_slice(addr.as_slice());
    word
}

fn bool_word(val: bool) -> [u8; 32] {
    let mut word = [0u8; 32];
    word[31] = val as u8;
    word
}

fn u256_word(value: u128) -> [u8; 32] {
    let mut word = [0u8; 32];
    word[16..].copy_from_slice(&value.to_be_bytes());
    word
}
