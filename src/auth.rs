use std::str::FromStr;

use alloy_primitives::{address, keccak256, Address, U256};
use anyhow::{anyhow, bail, Result};
use base64::engine::general_purpose::{STANDARD, URL_SAFE};
use base64::Engine;
use hmac::{Hmac, Mac};
use k256::ecdsa::signature::hazmat::PrehashSigner;
use k256::ecdsa::{RecoveryId, Signature as K256Signature, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub type Signature = String;

pub const CLOB_AUTH_MESSAGE: &str = "This message attests that I control the given wallet";
pub const CLOB_DOMAIN_NAME: &str = "ClobAuthDomain";
pub const CLOB_DOMAIN_VERSION: &str = "1";
pub const POLYMARKET_PROTOCOL_NAME: &str = "Polymarket CTF Exchange";
pub const POLYMARKET_PROTOCOL_VERSION: &str = "1";
pub const ZERO_ADDRESS: Address = address!("0x0000000000000000000000000000000000000000");
pub const POLYGON_EXCHANGE: Address = address!("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E");
pub const POLYGON_NEG_RISK_EXCHANGE: Address =
    address!("0xC5d563A36AE78145C45a50134d48A1215220f80a");
pub const AMOY_EXCHANGE: Address = address!("0xdFE02Eb6733538f8Ea35D585af8DE5958AD99E40");
pub const AMOY_NEG_RISK_EXCHANGE: Address = address!("0xC5d563A36AE78145C45a50134d48A1215220f80a");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthHeaders {
    pub poly_address: String,
    pub poly_signature: String,
    pub poly_timestamp: String,
    pub poly_api_key: String,
    pub poly_passphrase: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L1AuthHeaders {
    pub poly_address: String,
    pub poly_signature: String,
    pub poly_timestamp: String,
    pub poly_nonce: String,
}

#[derive(Debug, Clone)]
pub struct ApiAuth {
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
    pub address: Option<String>,
}

impl ApiAuth {
    pub fn new(
        api_key: impl Into<String>,
        secret: impl Into<String>,
        passphrase: impl Into<String>,
        address: Option<String>,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            secret: secret.into(),
            passphrase: passphrase.into(),
            address,
        }
    }

    pub fn sign_request(
        &self,
        method: &str,
        path: &str,
        body: &str,
        timestamp: u64,
    ) -> Result<AuthHeaders> {
        let secret = decode_secret(&self.secret)?;
        let message = format!("{timestamp}{}{path}{body}", method.to_uppercase());
        let mut mac =
            HmacSha256::new_from_slice(&secret).map_err(|_| anyhow!("invalid HMAC secret"))?;
        mac.update(message.as_bytes());
        let signature = URL_SAFE.encode(mac.finalize().into_bytes());
        Ok(AuthHeaders {
            poly_address: self.address.clone().unwrap_or_default(),
            poly_signature: signature,
            poly_timestamp: timestamp.to_string(),
            poly_api_key: self.api_key.clone(),
            poly_passphrase: self.passphrase.clone(),
        })
    }

    pub fn ws_auth_message(&self, timestamp: u64) -> Result<String> {
        let headers = self.sign_request("GET", "/ws/user", "", timestamp)?;
        Ok(serde_json::json!({
            "type": "auth",
            "api_key": headers.poly_api_key,
            "passphrase": headers.poly_passphrase,
            "timestamp": headers.poly_timestamp,
            "signature": headers.poly_signature,
            "address": headers.poly_address,
        })
        .to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderData {
    pub salt: U256,
    pub maker: Address,
    pub signer: Address,
    pub taker: Address,
    pub token_id: U256,
    pub maker_amount: U256,
    pub taker_amount: U256,
    pub expiration: U256,
    pub nonce: U256,
    pub fee_rate_bps: U256,
    pub side: u8,
    pub signature_type: u8,
    pub chain_id: u64,
    pub verifying_contract: Address,
}

#[derive(Debug, Clone)]
pub struct OrderSigner {
    private_key: SigningKey,
    pub chain_id: u64,
    pub address: Address,
}

impl OrderSigner {
    pub fn new(private_key_hex: &str, chain_id: u64) -> Result<Self> {
        let normalized = private_key_hex.trim_start_matches("0x");
        let raw = hex::decode(normalized)?;
        let signing_key = SigningKey::from_slice(&raw)?;
        let verifying_key = VerifyingKey::from(&signing_key);
        let address = public_key_to_address(&verifying_key);
        Ok(Self {
            private_key: signing_key,
            chain_id,
            address,
        })
    }

    pub fn default_exchange(chain_id: u64, neg_risk: bool) -> Result<Address> {
        match (chain_id, neg_risk) {
            (137, false) => Ok(POLYGON_EXCHANGE),
            (137, true) => Ok(POLYGON_NEG_RISK_EXCHANGE),
            (80002, false) => Ok(AMOY_EXCHANGE),
            (80002, true) => Ok(AMOY_NEG_RISK_EXCHANGE),
            _ => bail!("unsupported chain_id {chain_id}"),
        }
    }

    pub fn clob_l1_headers(&self, timestamp: u64, nonce: u64) -> Result<L1AuthHeaders> {
        let digest = clob_l1_digest(self.address, self.chain_id, timestamp, nonce);
        let signature = sign_digest(&self.private_key, digest)?;
        Ok(L1AuthHeaders {
            poly_address: self.address.to_checksum(None),
            poly_signature: signature,
            poly_timestamp: timestamp.to_string(),
            poly_nonce: nonce.to_string(),
        })
    }

    pub fn sign_order(&self, order: &OrderData) -> Result<Signature> {
        let digest = order_eip712_digest(order);
        sign_digest(&self.private_key, digest)
    }

    pub fn parse_address(value: &str) -> Result<Address> {
        Address::from_str(value).map_err(Into::into)
    }

    pub fn parse_u256(value: &str) -> Result<U256> {
        U256::from_str(value).map_err(Into::into)
    }
}

fn decode_secret(secret: &str) -> Result<Vec<u8>> {
    STANDARD
        .decode(secret)
        .or_else(|_| URL_SAFE.decode(secret))
        .map_err(Into::into)
}

fn sign_digest(signing_key: &SigningKey, digest: [u8; 32]) -> Result<Signature> {
    let (signature, recovery_id): (K256Signature, RecoveryId) = signing_key
        .sign_prehash(&digest)
        .map_err(|err| anyhow!(err))?;
    let mut out = Vec::with_capacity(65);
    out.extend_from_slice(&signature.to_bytes());
    out.push(recovery_id.to_byte() + 27);
    Ok(format!("0x{}", hex::encode(out)))
}

fn public_key_to_address(verifying_key: &VerifyingKey) -> Address {
    let encoded = verifying_key.to_encoded_point(false);
    let hash = keccak256(&encoded.as_bytes()[1..]);
    Address::from_slice(&hash[12..])
}

fn clob_l1_digest(address: Address, chain_id: u64, timestamp: u64, nonce: u64) -> [u8; 32] {
    let domain_typehash =
        keccak256("EIP712Domain(string name,string version,uint256 chainId)".as_bytes());
    let clob_auth_typehash = keccak256(
        "ClobAuth(address address,string timestamp,uint256 nonce,string message)".as_bytes(),
    );
    let domain_separator = keccak256(abi_encode_words([
        domain_typehash.into(),
        keccak256(CLOB_DOMAIN_NAME.as_bytes()).into(),
        keccak256(CLOB_DOMAIN_VERSION.as_bytes()).into(),
        u256_word(U256::from(chain_id)),
    ]));
    let struct_hash = keccak256(abi_encode_words([
        clob_auth_typehash.into(),
        address_word(address),
        keccak256(timestamp.to_string().as_bytes()).into(),
        u256_word(U256::from(nonce)),
        keccak256(CLOB_AUTH_MESSAGE.as_bytes()).into(),
    ]));
    eip712_envelope(domain_separator.into(), struct_hash.into())
}

fn order_eip712_digest(order: &OrderData) -> [u8; 32] {
    let domain_typehash = keccak256(
        "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
            .as_bytes(),
    );
    let order_typehash = keccak256("Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)".as_bytes());
    let domain_separator = keccak256(abi_encode_words([
        domain_typehash.into(),
        keccak256(POLYMARKET_PROTOCOL_NAME.as_bytes()).into(),
        keccak256(POLYMARKET_PROTOCOL_VERSION.as_bytes()).into(),
        u256_word(U256::from(order.chain_id)),
        address_word(order.verifying_contract),
    ]));
    let struct_hash = keccak256(abi_encode_words([
        order_typehash.into(),
        u256_word(order.salt),
        address_word(order.maker),
        address_word(order.signer),
        address_word(order.taker),
        u256_word(order.token_id),
        u256_word(order.maker_amount),
        u256_word(order.taker_amount),
        u256_word(order.expiration),
        u256_word(order.nonce),
        u256_word(order.fee_rate_bps),
        u8_word(order.side),
        u8_word(order.signature_type),
    ]));
    eip712_envelope(domain_separator.into(), struct_hash.into())
}

fn abi_encode_words<const N: usize>(words: [[u8; 32]; N]) -> Vec<u8> {
    words.into_iter().flatten().collect()
}

fn eip712_envelope(domain_separator: [u8; 32], struct_hash: [u8; 32]) -> [u8; 32] {
    let mut buf = Vec::with_capacity(66);
    buf.extend_from_slice(&[0x19, 0x01]);
    buf.extend_from_slice(&domain_separator);
    buf.extend_from_slice(&struct_hash);
    keccak256(buf).into()
}

fn u256_word(value: U256) -> [u8; 32] {
    value.to_be_bytes::<32>()
}

fn u8_word(value: u8) -> [u8; 32] {
    let mut word = [0_u8; 32];
    word[31] = value;
    word
}

fn address_word(value: Address) -> [u8; 32] {
    let mut word = [0_u8; 32];
    word[12..].copy_from_slice(value.as_slice());
    word
}
