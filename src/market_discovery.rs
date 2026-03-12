use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};

use crate::config::Config;

#[derive(Debug, Clone, Serialize)]
pub struct MarketInfo {
    pub condition_id: String,
    pub question: String,
    pub token_id_up: String,
    pub token_id_down: String,
    pub end_date: DateTime<Utc>,
    pub min_tick_size: f64,
}

#[derive(Debug, Clone)]
pub struct MarketDiscovery {
    client: Client,
    config: Config,
}

#[derive(Debug, Clone)]
pub enum MarketEvent {
    NewMarket(MarketInfo),
    MarketExpiring(String),
}

impl MarketDiscovery {
    pub fn new(config: &Config) -> Self {
        Self {
            client: Client::new(),
            config: config.clone(),
        }
    }

    pub async fn fetch_active_markets(&self) -> Result<Vec<MarketInfo>> {
        let asset_tag = self.config.market.asset.to_lowercase();
        let url = format!(
            "{}/markets",
            self.config.api.gamma_rest_url.trim_end_matches('/')
        );
        let response = self
            .client
            .get(url)
            .query(&[
                ("limit", "200"),
                ("closed", "false"),
                ("active", "true"),
                ("tag", asset_tag.as_str()),
            ])
            .send()
            .await?
            .error_for_status()?;
        let body = response.text().await?;
        let raw_markets = parse_market_response(&body)?;
        Ok(filter_active_markets(
            &raw_markets,
            &self.config.market.asset,
            &self.config.market.duration,
        ))
    }

    pub async fn run(&self, tx: mpsc::Sender<MarketEvent>) -> Result<()> {
        let mut known: HashMap<String, MarketInfo> = HashMap::new();
        loop {
            let markets = self.fetch_active_markets().await?;
            let mut current = HashMap::new();
            for market in markets {
                current.insert(market.condition_id.clone(), market.clone());
                if !known.contains_key(&market.condition_id) {
                    tx.send(MarketEvent::NewMarket(market.clone())).await?;
                }
                if (market.end_date - Utc::now()).num_seconds() <= 30 {
                    tx.send(MarketEvent::MarketExpiring(market.condition_id.clone()))
                        .await?;
                }
            }

            for condition_id in known.keys() {
                if !current.contains_key(condition_id) {
                    tx.send(MarketEvent::MarketExpiring(condition_id.clone()))
                        .await?;
                }
            }

            known = current;
            sleep(Duration::from_secs(self.config.market.poll_interval_sec)).await;
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct GammaMarket {
    #[serde(alias = "conditionId")]
    condition_id: Option<String>,
    question: Option<String>,
    #[serde(alias = "clobTokenIds")]
    clob_token_ids: Option<StringListField>,
    outcomes: Option<StringListField>,
    #[serde(alias = "endDate")]
    end_date: Option<String>,
    #[serde(alias = "minimum_tick_size", alias = "minimumTickSize")]
    min_tick_size: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum StringListField {
    Json(String),
    Vec(Vec<String>),
}

impl StringListField {
    fn into_vec(self) -> Vec<String> {
        match self {
            Self::Json(value) => serde_json::from_str(&value).unwrap_or_default(),
            Self::Vec(value) => value,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MarketResponse {
    Bare(Vec<GammaMarket>),
    Wrapped { data: Vec<GammaMarket> },
}

fn parse_market_response(body: &str) -> Result<Vec<GammaMarket>> {
    let response: MarketResponse = serde_json::from_str(body)?;
    Ok(match response {
        MarketResponse::Bare(markets) => markets,
        MarketResponse::Wrapped { data } => data,
    })
}

pub fn filter_active_markets(
    raw_markets: &[GammaMarket],
    asset: &str,
    duration: &str,
) -> Vec<MarketInfo> {
    raw_markets
        .iter()
        .filter_map(|market| {
            let question = market.question.clone()?;
            if !is_target_market(&question, asset, duration) {
                return None;
            }
            let condition_id = market.condition_id.clone()?;
            let token_ids = market.clob_token_ids.clone()?.into_vec();
            let outcomes = market.outcomes.clone()?.into_vec();
            if token_ids.len() != 2 || outcomes.len() != 2 {
                return None;
            }
            let end_date = market
                .end_date
                .as_ref()
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())?
                .with_timezone(&Utc);
            let (token_id_up, token_id_down) = ordered_token_pair(&outcomes, &token_ids)?;
            Some(MarketInfo {
                condition_id,
                question,
                token_id_up,
                token_id_down,
                end_date,
                min_tick_size: market.min_tick_size.unwrap_or(0.01),
            })
        })
        .collect()
}

fn is_target_market(question: &str, asset: &str, duration: &str) -> bool {
    let q = question.to_lowercase();
    let asset_match = q.contains(&asset.to_lowercase());
    let up_down_match = q.contains("up or down")
        || (q.contains("up") && q.contains("down"))
        || (q.contains("above") && q.contains("below"));
    let duration_match = match duration.to_lowercase().as_str() {
        "5m" | "5min" | "5 minute" | "5 minutes" => {
            q.contains("5 minute") || q.contains("5 min") || q.contains("5m")
        }
        "15m" | "15min" | "15 minute" | "15 minutes" => {
            q.contains("15 minute") || q.contains("15 min") || q.contains("15m")
        }
        other => q.contains(other),
    };
    asset_match && up_down_match && duration_match
}

fn ordered_token_pair(outcomes: &[String], token_ids: &[String]) -> Option<(String, String)> {
    let mut pairs: Vec<(String, String)> = outcomes
        .iter()
        .cloned()
        .zip(token_ids.iter().cloned())
        .collect();
    if pairs.len() != 2 {
        return None;
    }
    pairs.sort_by_key(|(outcome, _)| outcome_priority(outcome));
    Some((pairs[0].1.clone(), pairs[1].1.clone()))
}

fn outcome_priority(outcome: &str) -> u8 {
    let lowered = outcome.to_lowercase();
    if lowered.contains("up") || lowered.contains("above") || lowered == "yes" {
        0
    } else {
        1
    }
}
