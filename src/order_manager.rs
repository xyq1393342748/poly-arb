use std::{collections::HashMap, sync::Arc, time::Duration};

use alloy_primitives::U256;
use anyhow::{anyhow, bail, Result};
use rand::RngCore;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Client,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{sync::Mutex, time::timeout};
use uuid::Uuid;

use crate::{
    arb_engine::{ArbSignal, MarketPair},
    auth::{ApiAuth, OrderData, OrderSigner, ZERO_ADDRESS},
    config::Config,
    fees,
    orderbook::OrderBookManager,
    presigner::PreSignedOrderPool,
    types::{OrderStatus, OrderType, Side},
    ws_user::UserEvent,
};

#[derive(Debug, Clone)]
pub struct OrderManager {
    config: Config,
    client: Client,
    auth: ApiAuth,
    signer: Option<OrderSigner>,
    pending: Arc<Mutex<HashMap<String, PendingExecution>>>,
    presigner: Option<Arc<Mutex<PreSignedOrderPool>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub token_id: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub order_type: OrderType,
    pub expiration: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbExecution {
    pub signal: ArbSignal,
    pub order_up: OrderRequest,
    pub order_down: OrderRequest,
    pub status: ArbStatus,
    pub order_id_up: Option<String>,
    pub order_id_down: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ArbStatus {
    Pending,
    BothFilled {
        profit: f64,
    },
    OneSideFilled {
        filled_side: String,
        recovery_order_id: String,
    },
    Rescued {
        actual_profit: f64,
    },
    LossStopped {
        loss: f64,
    },
    BothCancelled,
    Merged {
        profit: f64,
        tx_hash: String,
    },
    MergeFailed {
        profit: f64,
        reason: String,
    },
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BatchOrderResult {
    pub success: bool,
    #[serde(alias = "orderID", alias = "order_id", alias = "id")]
    pub order_id: Option<String>,
    pub status: Option<String>,
}

impl Default for BatchOrderResult {
    fn default() -> Self {
        Self {
            success: false,
            order_id: None,
            status: None,
        }
    }
}

#[derive(Debug, Clone)]
struct PendingExecution {
    order_id_up: String,
    order_id_down: String,
    token_id_up: String,
    token_id_down: String,
    size: f64,
    matched_up: bool,
    matched_down: bool,
}

impl OrderManager {
    pub fn new(config: &Config, auth: ApiAuth, signer: Option<OrderSigner>) -> Self {
        let client_builder = Client::builder()
            .pool_max_idle_per_host(config.optimization.connection_pool_size as usize)
            .pool_idle_timeout(Duration::from_secs(60))
            .tcp_nodelay(config.optimization.tcp_nodelay)
            .tcp_keepalive(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10));
        let client_builder = if config.optimization.http2_enabled {
            client_builder.http2_adaptive_window(true)
        } else {
            client_builder
        };
        let client = client_builder.build().unwrap_or_else(|_| Client::new());
        let presigner = signer.clone().and_then(|signer| {
            config.optimization.presign_enabled.then(|| {
                Arc::new(Mutex::new(PreSignedOrderPool::new(
                    signer,
                    config.strategy.clone(),
                    config.optimization.clone(),
                )))
            })
        });

        Self {
            config: config.clone(),
            client,
            auth,
            signer,
            pending: Arc::new(Mutex::new(HashMap::new())),
            presigner,
        }
    }

    pub async fn refresh_presigned_orders(
        &self,
        books: &OrderBookManager,
        markets: &[MarketPair],
    ) -> Result<()> {
        if let Some(presigner) = &self.presigner {
            presigner.lock().await.refresh(books, markets);
        }
        Ok(())
    }

    pub async fn execute_arb(
        &self,
        signal: ArbSignal,
        books: Option<&OrderBookManager>,
    ) -> Result<ArbExecution> {
        if self.config.strategy.batch_enabled {
            if let Some(books) = books {
                return self
                    .execute_arb_batched(
                        signal,
                        books,
                        self.config.strategy.batch_chunk_usd,
                        self.config.strategy.batch_max_chunks,
                        self.config.strategy.batch_delay_ms,
                    )
                    .await;
            }
        }

        let expiration = self.default_expiration();
        let order_up = OrderRequest {
            token_id: signal.token_id_up.clone(),
            side: Side::Buy,
            price: signal.ask_up,
            size: signal.max_quantity,
            order_type: OrderType::Fok,
            expiration,
        };
        let order_down = OrderRequest {
            token_id: signal.token_id_down.clone(),
            side: Side::Buy,
            price: signal.ask_down,
            size: signal.max_quantity,
            order_type: OrderType::Fok,
            expiration,
        };

        if self.config.mode.dry_run {
            return Ok(ArbExecution {
                signal: signal.clone(),
                order_up,
                order_down,
                status: ArbStatus::BothFilled {
                    profit: round4(signal.net_profit * signal.max_quantity),
                },
                order_id_up: Some(format!("dry-{}", Uuid::new_v4())),
                order_id_down: Some(format!("dry-{}", Uuid::new_v4())),
            });
        }

        let (up_result, down_result) = if self.config.optimization.use_batch_orders {
            let results = self
                .submit_orders_batch(&[order_up.clone(), order_down.clone()])
                .await?;
            if results.len() != 2 {
                bail!("expected 2 batch order results, got {}", results.len());
            }
            (results[0].clone(), results[1].clone())
        } else {
            let (up_result, down_result) = tokio::join!(
                self.submit_single_order(
                    &order_up.token_id,
                    Side::Buy,
                    order_up.price,
                    order_up.size
                ),
                self.submit_single_order(
                    &order_down.token_id,
                    Side::Buy,
                    order_down.price,
                    order_down.size,
                ),
            );
            (up_result?, down_result?)
        };

        let mut execution = ArbExecution {
            signal: signal.clone(),
            order_up: order_up.clone(),
            order_down: order_down.clone(),
            status: ArbStatus::Pending,
            order_id_up: up_result.order_id.clone(),
            order_id_down: down_result.order_id.clone(),
        };

        let up_active = result_is_active(&up_result);
        let down_active = result_is_active(&down_result);
        let up_filled = result_is_filled(&up_result);
        let down_filled = result_is_filled(&down_result);

        execution.status = if up_filled && down_filled {
            ArbStatus::BothFilled {
                profit: round4(signal.net_profit * signal.max_quantity),
            }
        } else if !up_active && !down_active {
            ArbStatus::BothCancelled
        } else if up_active ^ down_active {
            let (filled_side, recovery_order_id) = if up_active {
                (
                    "UP".to_owned(),
                    up_result
                        .order_id
                        .clone()
                        .unwrap_or_else(|| format!("missing-up-{}", Uuid::new_v4())),
                )
            } else {
                (
                    "DOWN".to_owned(),
                    down_result
                        .order_id
                        .clone()
                        .unwrap_or_else(|| format!("missing-down-{}", Uuid::new_v4())),
                )
            };
            ArbStatus::OneSideFilled {
                filled_side,
                recovery_order_id,
            }
        } else {
            if let (Some(order_id_up), Some(order_id_down)) = (
                execution.order_id_up.clone(),
                execution.order_id_down.clone(),
            ) {
                self.insert_pending_execution(
                    &signal,
                    &order_up,
                    &order_down,
                    &order_id_up,
                    &order_id_down,
                    up_filled,
                    down_filled,
                )
                .await?;
            }
            ArbStatus::Pending
        };

        Ok(execution)
    }

    pub async fn execute_arb_batched(
        &self,
        signal: ArbSignal,
        books: &OrderBookManager,
        chunk_usd: f64,
        max_chunks: u32,
        delay_ms: u64,
    ) -> Result<ArbExecution> {
        if self.config.mode.dry_run {
            return Ok(ArbExecution {
                signal: signal.clone(),
                order_up: OrderRequest {
                    token_id: signal.token_id_up.clone(),
                    side: Side::Buy,
                    price: signal.ask_up,
                    size: signal.max_quantity,
                    order_type: OrderType::Fok,
                    expiration: self.default_expiration(),
                },
                order_down: OrderRequest {
                    token_id: signal.token_id_down.clone(),
                    side: Side::Buy,
                    price: signal.ask_down,
                    size: signal.max_quantity,
                    order_type: OrderType::Fok,
                    expiration: self.default_expiration(),
                },
                status: ArbStatus::BothFilled {
                    profit: round4(signal.net_profit * signal.max_quantity),
                },
                order_id_up: Some(format!("dry-batch-{}", Uuid::new_v4())),
                order_id_down: Some(format!("dry-batch-{}", Uuid::new_v4())),
            });
        }

        let chunk_size = round_to(chunk_usd / signal.total_cost, 2).max(1.0);
        let mut total_filled = 0.0;
        let mut last_order_id_up = None;
        let mut last_order_id_down = None;
        let mut total_profit = 0.0;

        for i in 0..max_chunks {
            let remaining = signal.max_quantity - total_filled;
            if remaining <= 0.0 {
                break;
            }
            let batch_size = round_to(chunk_size.min(remaining), 2);
            if batch_size <= 0.0 {
                break;
            }

            // 重新检查当前 best ask
            let Some((ask_up, _, ask_down, _)) =
                books.get_pair_asks(&signal.token_id_up, &signal.token_id_down)
            else {
                break;
            };
            let sum = ask_up + ask_down;
            let curve_fees = fees::arb_fees(
                ask_up,
                ask_down,
                self.config.strategy.fee_curve_rate,
                self.config.strategy.fee_curve_exponent,
            );
            let gas = self.config.strategy.gas_per_order * 2.0;
            if sum + curve_fees + gas >= 1.0 {
                break;
            }

            let expiration = self.default_expiration();
            let order_up = OrderRequest {
                token_id: signal.token_id_up.clone(),
                side: Side::Buy,
                price: ask_up,
                size: batch_size,
                order_type: OrderType::Fok,
                expiration,
            };
            let order_down = OrderRequest {
                token_id: signal.token_id_down.clone(),
                side: Side::Buy,
                price: ask_down,
                size: batch_size,
                order_type: OrderType::Fok,
                expiration,
            };

            let (up_result, down_result) = if self.config.optimization.use_batch_orders {
                let results = self
                    .submit_orders_batch(&[order_up.clone(), order_down.clone()])
                    .await?;
                if results.len() != 2 {
                    break;
                }
                (results[0].clone(), results[1].clone())
            } else {
                let (ur, dr) = tokio::join!(
                    self.submit_single_order(&signal.token_id_up, Side::Buy, ask_up, batch_size),
                    self.submit_single_order(
                        &signal.token_id_down,
                        Side::Buy,
                        ask_down,
                        batch_size
                    ),
                );
                (ur?, dr?)
            };

            let up_filled = result_is_filled(&up_result);
            let down_filled = result_is_filled(&down_result);
            if up_filled && down_filled {
                total_filled += batch_size;
                total_profit += (1.0 - sum - curve_fees - gas) * batch_size;
                last_order_id_up = up_result.order_id.clone();
                last_order_id_down = down_result.order_id.clone();
            } else if up_filled != down_filled {
                // 一侧成交另一侧未成交：返回 OneSideFilled 让 handle_one_side_fill 处理
                let (filled_side, recovery_order_id) = if up_filled {
                    ("UP".to_owned(), up_result.order_id.clone().unwrap_or_default())
                } else {
                    ("DOWN".to_owned(), down_result.order_id.clone().unwrap_or_default())
                };
                return Ok(ArbExecution {
                    signal: signal.clone(),
                    order_up: OrderRequest {
                        token_id: signal.token_id_up.clone(),
                        side: Side::Buy,
                        price: ask_up,
                        size: batch_size,
                        order_type: OrderType::Fok,
                        expiration,
                    },
                    order_down: OrderRequest {
                        token_id: signal.token_id_down.clone(),
                        side: Side::Buy,
                        price: ask_down,
                        size: batch_size,
                        order_type: OrderType::Fok,
                        expiration,
                    },
                    status: ArbStatus::OneSideFilled {
                        filled_side,
                        recovery_order_id,
                    },
                    order_id_up: up_result.order_id,
                    order_id_down: down_result.order_id,
                });
            } else {
                // 两侧都未成交，停止后续批次
                break;
            }

            if i + 1 < max_chunks && delay_ms > 0 {
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            }
        }

        let status = if total_filled > 0.0 {
            ArbStatus::BothFilled {
                profit: round4(total_profit),
            }
        } else {
            ArbStatus::BothCancelled
        };

        Ok(ArbExecution {
            signal: signal.clone(),
            order_up: OrderRequest {
                token_id: signal.token_id_up.clone(),
                side: Side::Buy,
                price: signal.ask_up,
                size: total_filled,
                order_type: OrderType::Fok,
                expiration: self.default_expiration(),
            },
            order_down: OrderRequest {
                token_id: signal.token_id_down.clone(),
                side: Side::Buy,
                price: signal.ask_down,
                size: total_filled,
                order_type: OrderType::Fok,
                expiration: self.default_expiration(),
            },
            status,
            order_id_up: last_order_id_up,
            order_id_down: last_order_id_down,
        })
    }

    pub async fn handle_one_side_fill(
        &self,
        execution: &mut ArbExecution,
        books: &OrderBookManager,
    ) -> Result<()> {
        let filled_side = match &execution.status {
            ArbStatus::OneSideFilled { filled_side, .. } => filled_side.clone(),
            _ => return Ok(()),
        };

        if execution.signal.time_to_expiry_sec <= self.config.risk.min_time_to_expiry_sec {
            return Ok(());
        }

        let (filled_order, unfilled_order, is_up_filled) = if filled_side.eq_ignore_ascii_case("UP")
        {
            (
                execution.order_up.clone(),
                execution.order_down.clone(),
                true,
            )
        } else {
            (
                execution.order_down.clone(),
                execution.order_up.clone(),
                false,
            )
        };

        if let Some((ask, available)) = books
            .get_book(&unfilled_order.token_id)
            .and_then(|book| book.best_ask())
        {
            let total = filled_order.price + ask;
            let rescue_fees = fees::arb_fees(
                filled_order.price,
                ask,
                self.config.strategy.fee_curve_rate,
                self.config.strategy.fee_curve_exponent,
            );
            if available >= filled_order.size && total + rescue_fees < 1.0 {
                let rescue_future = self.submit_single_order(
                    &unfilled_order.token_id,
                    Side::Buy,
                    ask,
                    filled_order.size,
                );
                if let Ok(Ok(result)) = timeout(
                    Duration::from_millis(self.config.risk.recovery_retry_timeout_ms),
                    rescue_future,
                )
                .await
                {
                    if result_is_filled(&result) || result_is_active(&result) {
                        if is_up_filled {
                            execution.order_id_down = result.order_id.clone();
                        } else {
                            execution.order_id_up = result.order_id.clone();
                        }
                        execution.status = ArbStatus::Rescued {
                            actual_profit: round4((1.0 - total - rescue_fees) * filled_order.size),
                        };
                        return Ok(());
                    }
                }
            }
        }

        for retry in 0..self.config.risk.sell_max_retries {
            let Some((bid, _)) = books
                .get_book(&filled_order.token_id)
                .and_then(|book| book.best_bid())
            else {
                break;
            };
            let sell_price = round_to(bid - retry as f64 * self.config.risk.sell_price_step, 2);
            if sell_price <= 0.0 {
                break;
            }

            let result = self
                .submit_single_order(
                    &filled_order.token_id,
                    Side::Sell,
                    sell_price,
                    filled_order.size,
                )
                .await?;
            if result_is_filled(&result) || result_is_active(&result) {
                let sell_fees = fees::arb_fees(
                    filled_order.price,
                    sell_price,
                    self.config.strategy.fee_curve_rate,
                    self.config.strategy.fee_curve_exponent,
                ) * filled_order.size;
                let loss =
                    round4(((filled_order.price - sell_price).max(0.0) * filled_order.size) + sell_fees);
                execution.status = ArbStatus::LossStopped { loss };
                return Ok(());
            }
        }

        Ok(())
    }

    pub async fn submit_single_order(
        &self,
        token_id: &str,
        side: Side,
        price: f64,
        size: f64,
    ) -> Result<BatchOrderResult> {
        if self.config.mode.dry_run {
            return Ok(BatchOrderResult {
                success: true,
                order_id: Some(format!("dry-{}", Uuid::new_v4())),
                status: Some("MATCHED".to_owned()),
            });
        }

        let request = OrderRequest {
            token_id: token_id.to_owned(),
            side,
            price,
            size,
            order_type: OrderType::Fok,
            expiration: self.default_expiration(),
        };
        self.submit_order_result(&request).await
    }

    pub async fn submit_orders_batch(
        &self,
        requests: &[OrderRequest],
    ) -> Result<Vec<BatchOrderResult>> {
        if self.config.mode.dry_run {
            return Ok(requests
                .iter()
                .map(|_| BatchOrderResult {
                    success: true,
                    order_id: Some(format!("dry-{}", Uuid::new_v4())),
                    status: Some("MATCHED".to_owned()),
                })
                .collect());
        }

        let mut payloads = Vec::with_capacity(requests.len());
        for request in requests {
            payloads.push(self.build_payload_for_request(request).await?);
        }

        let mut body = String::with_capacity(payloads.iter().map(String::len).sum::<usize>() + 2 + payloads.len().saturating_sub(1));
        body.push('[');
        for (index, payload) in payloads.iter().enumerate() {
            if index > 0 {
                body.push(',');
            }
            body.push_str(payload);
        }
        body.push(']');
        let headers = self.auth.sign_request("POST", "/orders", &body, now_ts())?;
        let response = self
            .client
            .post(format!(
                "{}/orders",
                self.config.api.clob_rest_url.trim_end_matches('/')
            ))
            .headers(to_headers(&headers)?)
            .body(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json().await?)
    }

    pub async fn on_user_event(&self, event: UserEvent) -> Result<()> {
        if let UserEvent::OrderUpdate {
            order_id, status, ..
        } = event
        {
            // Collect emergency sells needed, then drop the lock before awaiting HTTP.
            let mut sells: Vec<(String, f64)> = Vec::new();
            {
                let mut pending = self.pending.lock().await;
                for execution in pending.values_mut() {
                    if execution.order_id_up == order_id {
                        execution.matched_up = status == OrderStatus::Matched;
                    }
                    if execution.order_id_down == order_id {
                        execution.matched_down = status == OrderStatus::Matched;
                    }
                    if (execution.order_id_up == order_id || execution.order_id_down == order_id)
                        && status == OrderStatus::Cancelled
                        && (execution.matched_up || execution.matched_down)
                    {
                        let token_id = if execution.matched_up {
                            execution.token_id_up.clone()
                        } else {
                            execution.token_id_down.clone()
                        };
                        sells.push((token_id, execution.size));
                    }
                }
            } // lock released here

            for (token_id, size) in sells {
                let _ = self.emergency_sell(&token_id, size).await;
            }
        }
        Ok(())
    }

    pub async fn emergency_sell(&self, token_id: &str, size: f64) -> Result<String> {
        if self.config.mode.dry_run {
            return Ok(format!("dry-recovery-{}", Uuid::new_v4()));
        }
        let order = OrderRequest {
            token_id: token_id.to_owned(),
            side: Side::Sell,
            price: 0.01,
            size,
            order_type: OrderType::Fok,
            expiration: self.default_expiration(),
        };
        self.submit_order(&order).await
    }

    pub async fn query_order_status(&self, order_id: &str) -> Result<OrderStatus> {
        if self.config.mode.dry_run {
            return Ok(OrderStatus::Matched);
        }
        let path = format!("/data/order/{order_id}");
        let headers = self.auth.sign_request("GET", &path, "", now_ts())?;
        let response = self
            .client
            .get(format!("{}{}", self.config.api.clob_rest_url, path))
            .headers(to_headers(&headers)?)
            .send()
            .await?
            .error_for_status()?;
        let body: Value = response.json().await?;
        let status = body
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN");
        Ok(if status.eq_ignore_ascii_case("LIVE") {
            OrderStatus::Live
        } else if status.eq_ignore_ascii_case("MATCHED") {
            OrderStatus::Matched
        } else if status.eq_ignore_ascii_case("CANCELLED") {
            OrderStatus::Cancelled
        } else if status.eq_ignore_ascii_case("EXPIRED") {
            OrderStatus::Expired
        } else if status.eq_ignore_ascii_case("REJECTED") {
            OrderStatus::Rejected
        } else {
            OrderStatus::Unknown
        })
    }

    async fn insert_pending_execution(
        &self,
        signal: &ArbSignal,
        order_up: &OrderRequest,
        order_down: &OrderRequest,
        order_id_up: &str,
        order_id_down: &str,
        matched_up: bool,
        matched_down: bool,
    ) -> Result<()> {
        self.pending.lock().await.insert(
            signal.condition_id.clone(),
            PendingExecution {
                order_id_up: order_id_up.to_owned(),
                order_id_down: order_id_down.to_owned(),
                token_id_up: order_up.token_id.clone(),
                token_id_down: order_down.token_id.clone(),
                size: signal.max_quantity,
                matched_up,
                matched_down,
            },
        );
        Ok(())
    }

    async fn build_payload_for_request(&self, request: &OrderRequest) -> Result<String> {
        if request.side == Side::Buy {
            if let Some(presigner) = &self.presigner {
                let payload = presigner.lock().await.get_or_sign(
                    &request.token_id,
                    request.price,
                    request.size,
                )?;
                return hydrate_payload(&payload, &self.auth.api_key, request.order_type);
            }
        }

        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| anyhow!("real mode requires wallet signer"))?;
        build_order_payload(
            request,
            signer,
            &self.auth.api_key,
            self.config.strategy.fee_curve_rate,
            self.config.strategy.fee_curve_exponent,
        )
    }

    async fn submit_order_result(&self, request: &OrderRequest) -> Result<BatchOrderResult> {
        let body = self.build_payload_for_request(request).await?;
        let headers = self.auth.sign_request("POST", "/order", &body, now_ts())?;
        let response = self
            .client
            .post(format!(
                "{}/order",
                self.config.api.clob_rest_url.trim_end_matches('/')
            ))
            .headers(to_headers(&headers)?)
            .body(body)
            .send()
            .await?
            .error_for_status()?;
        let value: Value = response.json().await?;
        Ok(parse_batch_order_result(&value))
    }

    async fn submit_order(&self, request: &OrderRequest) -> Result<String> {
        let result = self.submit_order_result(request).await?;
        result
            .order_id
            .ok_or_else(|| anyhow!("missing order id in response"))
    }

    fn default_expiration(&self) -> u64 {
        now_ts() + self.config.optimization.presign_ttl_sec.max(1)
    }

    pub fn make_error_execution(&self, signal: ArbSignal, error: &str) -> ArbExecution {
        ArbExecution {
            order_up: OrderRequest {
                token_id: signal.token_id_up.clone(),
                side: Side::Buy,
                price: signal.ask_up,
                size: 0.0,
                order_type: OrderType::Fok,
                expiration: self.default_expiration(),
            },
            order_down: OrderRequest {
                token_id: signal.token_id_down.clone(),
                side: Side::Buy,
                price: signal.ask_down,
                size: 0.0,
                order_type: OrderType::Fok,
                expiration: self.default_expiration(),
            },
            signal,
            status: ArbStatus::Error(error.to_owned()),
            order_id_up: None,
            order_id_down: None,
        }
    }
}

fn build_order_payload(
    request: &OrderRequest,
    signer: &OrderSigner,
    owner: &str,
    fee_curve_rate: f64,
    fee_curve_exponent: u32,
) -> Result<String> {
    let price = round_to(request.price, 2);
    let size = round_to(request.size, 2);
    let collateral_units = scale_to_u256(price * size, 6)?;
    let conditional_units = scale_to_u256(size, 6)?;
    let side = match request.side {
        Side::Buy => 0,
        Side::Sell => 1,
    };
    let salt = random_u64();
    let nonce = now_ts();
    let fee_rate_bps = fees::fee_bps_for_price(price, fee_curve_rate, fee_curve_exponent, 50);
    let order_data = OrderData {
        salt: U256::from(salt),
        maker: signer.address,
        signer: signer.address,
        taker: ZERO_ADDRESS,
        token_id: request.token_id.parse()?,
        maker_amount: if request.side == Side::Buy {
            collateral_units
        } else {
            conditional_units
        },
        taker_amount: if request.side == Side::Buy {
            conditional_units
        } else {
            collateral_units
        },
        expiration: U256::from(request.expiration),
        nonce: U256::from(nonce),
        fee_rate_bps: U256::from(fee_rate_bps),
        side,
        signature_type: 0,
        chain_id: signer.chain_id,
        verifying_contract: OrderSigner::default_exchange(signer.chain_id, false)?,
    };
    let signature = signer.sign_order(&order_data)?;
    let payload = serde_json::json!({
        "deferExec": false,
        "order": {
            "salt": salt,
            "maker": signer.address.to_string(),
            "signer": signer.address.to_string(),
            "taker": ZERO_ADDRESS.to_string(),
            "tokenId": request.token_id,
            "makerAmount": order_data.maker_amount.to_string(),
            "takerAmount": order_data.taker_amount.to_string(),
            "side": request.side.as_str(),
            "expiration": request.expiration.to_string(),
            "nonce": nonce.to_string(),
            "feeRateBps": fee_rate_bps.to_string(),
            "signatureType": 0,
            "signature": signature,
        },
        "owner": owner,
        "orderType": request.order_type.as_str(),
    });
    Ok(serde_json::to_string(&payload)?)
}

fn hydrate_payload(payload: &str, owner: &str, order_type: OrderType) -> Result<String> {
    if !payload.contains("\"__OWNER__\"") || !payload.contains("\"__ORDER_TYPE__\"") {
        bail!("signed payload missing hydration placeholders");
    }
    let owner = serde_json::to_string(owner)?;
    let order_type = serde_json::to_string(order_type.as_str())?;
    Ok(payload
        .replace("\"__OWNER__\"", &owner)
        .replace("\"__ORDER_TYPE__\"", &order_type))
}

fn parse_batch_order_result(value: &Value) -> BatchOrderResult {
    let order_id = value
        .get("id")
        .or_else(|| value.get("orderID"))
        .or_else(|| value.get("order_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let success = value
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| order_id.is_some());
    BatchOrderResult {
        success,
        order_id,
        status,
    }
}

fn result_is_active(result: &BatchOrderResult) -> bool {
    result.success && result.order_id.is_some()
}

fn result_is_filled(result: &BatchOrderResult) -> bool {
    if !result.success {
        return false;
    }
    match result.status.as_deref() {
        Some(status) => status.eq_ignore_ascii_case("MATCHED") || status.eq_ignore_ascii_case("FILLED"),
        None => false,
    }
}

fn scale_to_u256(value: f64, decimals: u32) -> Result<U256> {
    if value.is_sign_negative() {
        bail!("negative values are not supported");
    }
    let scaled = (value * 10_f64.powi(decimals as i32)).round() as u128;
    Ok(U256::from(scaled))
}

fn to_headers(auth: &crate::auth::AuthHeaders) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    insert_header(
        &mut headers,
        HeaderName::from_static("poly_address"),
        &auth.poly_address,
    )?;
    insert_header(
        &mut headers,
        HeaderName::from_static("poly_signature"),
        &auth.poly_signature,
    )?;
    insert_header(
        &mut headers,
        HeaderName::from_static("poly_timestamp"),
        &auth.poly_timestamp,
    )?;
    insert_header(
        &mut headers,
        HeaderName::from_static("poly_api_key"),
        &auth.poly_api_key,
    )?;
    insert_header(
        &mut headers,
        HeaderName::from_static("poly_passphrase"),
        &auth.poly_passphrase,
    )?;
    insert_header(
        &mut headers,
        HeaderName::from_static("content-type"),
        "application/json",
    )?;
    Ok(headers)
}

fn insert_header(headers: &mut HeaderMap, name: HeaderName, value: &str) -> Result<()> {
    headers.insert(name, HeaderValue::from_str(value)?);
    Ok(())
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

fn round4(value: f64) -> f64 {
    round_to(value, 4)
}
