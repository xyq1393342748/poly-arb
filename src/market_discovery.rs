use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};
use tracing::{debug, info, warn};

use crate::config::Config;

#[derive(Debug, Clone, Serialize)]
pub struct MarketInfo {
    pub condition_id: String,
    pub slug: String,
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

/// 根据当前时间推算即将到来的市场 slug
/// 格式: {asset}-updown-{horizon}m-{startEpoch}
/// startEpoch = floor(now / interval) * interval (市场开始时间)
/// endTime = startEpoch + interval (市场结束时间)
struct SlugCandidate {
    slug: String,
    end_epoch: i64,
    minutes_left: f64,
}

fn generate_upcoming_slugs(asset: &str, duration: &str) -> Vec<SlugCandidate> {
    let horizon_sec: i64 = match duration.to_lowercase().as_str() {
        "5m" | "5min" => 300,
        "15m" | "15min" => 900,
        "1h" | "60m" => 3600,
        _ => 300,
    };
    let horizon_min = horizon_sec / 60;
    let now_sec = Utc::now().timestamp();
    let sym = asset.to_lowercase();

    let current_start = (now_sec / horizon_sec) * horizon_sec;
    let mut results = Vec::new();

    // 当前周期 + 下一个周期
    for &start_epoch in &[current_start, current_start + horizon_sec] {
        let end_epoch = start_epoch + horizon_sec;
        let minutes_left = (end_epoch - now_sec) as f64 / 60.0;
        // 跳过快过期的(<18秒)和太远的(>horizon+3分钟)
        if minutes_left < 0.3 || minutes_left > (horizon_min as f64 + 3.0) {
            continue;
        }
        results.push(SlugCandidate {
            slug: format!("{}-updown-{}m-{}", sym, horizon_min, start_epoch),
            end_epoch,
            minutes_left,
        });
    }
    results
}

impl MarketDiscovery {
    pub fn new(config: &Config) -> Self {
        Self {
            client: Client::new(),
            config: config.clone(),
        }
    }

    /// 用推算出的 slug 精确查询 Gamma API，拿到 condition_id 和 token_id
    async fn fetch_market_by_slug(&self, slug: &str) -> Result<Option<MarketInfo>> {
        let url = format!(
            "{}/markets",
            self.config.api.gamma_rest_url.trim_end_matches('/')
        );
        let response = self
            .client
            .get(&url)
            .query(&[("slug", slug)])
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await?
            .error_for_status()?;
        let body = response.text().await?;
        let raw_markets = parse_market_response(&body)?;
        let market = match raw_markets.into_iter().next() {
            Some(m) => m,
            None => return Ok(None),
        };

        let condition_id = match market.condition_id {
            Some(id) => id,
            None => return Ok(None),
        };
        let question = market.question.unwrap_or_default();
        let token_ids = match market.clob_token_ids {
            Some(f) => f.into_vec(),
            None => return Ok(None),
        };
        let outcomes = match market.outcomes {
            Some(f) => f.into_vec(),
            None => return Ok(None),
        };
        if token_ids.len() != 2 || outcomes.len() != 2 {
            return Ok(None);
        }
        let end_date = market
            .end_date
            .as_ref()
            .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);
        let (token_id_up, token_id_down) = match ordered_token_pair(&outcomes, &token_ids) {
            Some(pair) => pair,
            None => return Ok(None),
        };

        Ok(Some(MarketInfo {
            condition_id,
            slug: slug.to_string(),
            question,
            token_id_up,
            token_id_down,
            end_date,
            min_tick_size: market.min_tick_size.unwrap_or(0.01),
        }))
    }

    /// 推算当前和下一个周期的 slug，查询返回活跃市场
    pub async fn fetch_active_markets(&self) -> Result<Vec<MarketInfo>> {
        let slugs = generate_upcoming_slugs(
            &self.config.market.asset,
            &self.config.market.duration,
        );
        let mut markets = Vec::new();
        for candidate in &slugs {
            debug!("尝试 slug: {} (剩余 {:.1} 分钟)", candidate.slug, candidate.minutes_left);
            match self.fetch_market_by_slug(&candidate.slug).await {
                Ok(Some(market)) => {
                    info!("发现市场: {} → {}", candidate.slug, market.condition_id);
                    markets.push(market);
                }
                Ok(None) => {
                    debug!("slug {} 未找到市场（可能尚未创建）", candidate.slug);
                }
                Err(e) => {
                    warn!("查询 slug {} 失败: {}", candidate.slug, e);
                }
            }
        }
        Ok(markets)
    }

    pub async fn run(&self, tx: mpsc::Sender<MarketEvent>) -> Result<()> {
        let mut known: HashMap<String, MarketInfo> = HashMap::new();
        loop {
            let markets = self.fetch_active_markets().await?;
            let mut current = HashMap::new();
            for market in markets {
                current.insert(market.condition_id.clone(), market.clone());
                if !known.contains_key(&market.condition_id) {
                    info!("新市场: {} [{}]", market.slug, market.condition_id);
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

// ─── Gamma API 响应解析（保留，slug 查询也用同样的格式）──────────────────

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slug_generation() {
        let slugs = generate_upcoming_slugs("BTC", "5m");
        assert!(!slugs.is_empty(), "应该至少生成一个 slug");
        for s in &slugs {
            assert!(s.slug.starts_with("btc-updown-5m-"), "slug 格式错误: {}", s.slug);
            assert!(s.minutes_left > 0.0, "剩余时间应 > 0");
            // startEpoch 应该是 300 的整数倍
            let epoch_str = s.slug.strip_prefix("btc-updown-5m-").unwrap();
            let epoch: i64 = epoch_str.parse().unwrap();
            assert_eq!(epoch % 300, 0, "epoch 应该是 300 的整数倍");
        }
    }

    #[test]
    fn test_slug_generation_15m() {
        let slugs = generate_upcoming_slugs("ETH", "15m");
        assert!(!slugs.is_empty());
        for s in &slugs {
            assert!(s.slug.starts_with("eth-updown-15m-"));
            let epoch_str = s.slug.strip_prefix("eth-updown-15m-").unwrap();
            let epoch: i64 = epoch_str.parse().unwrap();
            assert_eq!(epoch % 900, 0, "epoch 应该是 900 的整数倍");
        }
    }

    #[test]
    fn test_ordered_token_pair() {
        let outcomes = vec!["Down".to_string(), "Up".to_string()];
        let tokens = vec!["token_down".to_string(), "token_up".to_string()];
        let (up, down) = ordered_token_pair(&outcomes, &tokens).unwrap();
        assert_eq!(up, "token_up");
        assert_eq!(down, "token_down");
    }
}
