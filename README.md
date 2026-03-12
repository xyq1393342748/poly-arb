# poly-arb

Polymarket 二元期权套利机器人（Rust），专注于 5 分钟 BTC Up/Down 市场的"总和小于 1"买入套利策略。

## 策略原理

在 Polymarket 的二元结果市场中，当两个互补结果（Up/Down）的最佳卖价之和小于 $1.00 时，同时买入两个结果即可锁定无风险利润。结算时必有一方价值 $1.00，另一方归零。

```
例: Up ask = $0.48, Down ask = $0.49
总成本 = $0.97, 结算收入 = $1.00
净利润 = $0.03 - fees ≈ $0.025 (2.5% 利润率)
```

## 架构

```
MarketDiscovery ──→ ArbEngine ──→ OrderManager ──→ CLOB REST API
       ↑                ↑              ↑
   Gamma API      OrderBookManager   PreSigner
       ↑                ↑              ↑
   (poll)         WS Market Feed    EIP-712
                        ↑
                  WS User Feed ──→ 订单状态追踪
                        ↑
                     RiskManager ──→ 三级止损
                        ↑
                    WebServer ──→ Dashboard
                        ↑
                   SQLite Store
```

### 核心模块

| 模块 | 文件 | 功能 |
|------|------|------|
| 配置 | `config.rs` | TOML 配置加载与验证 |
| 认证 | `auth.rs` | HMAC-SHA256 API 签名 + EIP-712 订单签名 |
| 市场发现 | `market_discovery.rs` | 从 Gamma API 发现活跃的 BTC 5m 市场 |
| WS 行情 | `ws_market.rs` | WebSocket 订阅 orderbook 快照和增量更新 |
| WS 用户 | `ws_user.rs` | WebSocket 接收订单成交/取消通知 |
| 订单簿 | `orderbook.rs` | 本地维护多资产 orderbook |
| 套利引擎 | `arb_engine.rs` | 检测总和 < 1 的套利机会 |
| 订单管理 | `order_manager.rs` | 批量下单 + 三级止损 + 预签名集成 |
| 预签名 | `presigner.rs` | 预签名订单池，减少下单延迟 |
| 风控 | `risk.rs` | 日亏损限额、单边成交追踪、连续狙击检测 |
| 存储 | `store.rs` | SQLite 交易记录、日统计、余额快照 |
| Web | `web/` | Axum Dashboard + WebSocket 实时推送 |

## 延迟优化

| 优化项 | 延迟收益 | 说明 |
|--------|---------|------|
| 部署 AWS us-east-1 | -150~400ms | CLOB 服务器同区域 |
| 批量 POST /orders | -0~25ms | 2 个订单合并为 1 次 HTTP |
| 预签名订单池 | -1~5ms | EIP-712 签名提前完成 |
| HTTP/2 + 连接池 | -5~15ms | 复用 TCP+TLS 连接 |
| TCP_NODELAY | -0.5~2ms | 禁用 Nagle 算法 |

## 三级止损

当批量下单导致单边成交时：

1. **Level 1 — 补单**：检查另一边 orderbook，如果仍有利润空间则补买（500ms 超时）
2. **Level 2 — 卖出**：以 best_bid 卖出已成交一边，最多降价重试 3 次
3. **Level 3 — 持有到期**：前两级都失败，持有到 5 分钟后结算

## 快速开始

```bash
# 编译
cargo build --release

# 配置
cp config.toml.example config.toml
# 编辑 config.toml 填入 API 密钥和钱包私钥

# Dry run 模式运行（默认）
cargo run

# 查看 Dashboard
# 浏览器打开 http://localhost:3721
```

## 配置说明

参考 `config.toml.example`，关键配置：

- `[api]` — Polymarket CLOB API 凭据
- `[wallet]` — 以太坊钱包私钥（用于 EIP-712 签名）
- `[strategy]` — 最小利润率、最大仓位
- `[risk]` — 日亏损限额、单边成交限制
- `[optimization]` — 批量下单、预签名、HTTP/2
- `[mode]` — `dry_run = true` 模拟交易

## 部署

```bash
# 在 AWS us-east-1 上部署
sudo bash deploy/tuning.sh  # Linux TCP 调优
cargo build --release
./target/release/poly-arb config.toml
```

## 测试

```bash
cargo test
```

## 免责声明

本项目仅供学习和研究目的。使用真实资金交易前请充分了解风险。作者不对任何交易损失负责。

## License

MIT
