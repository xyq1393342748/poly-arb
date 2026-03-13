use std::{path::Path, str::FromStr};

use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePoolOptions, FromRow, Row, SqlitePool};

#[derive(Debug, Clone)]
pub struct Store {
    pool: SqlitePool,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ArbTradeRecord {
    pub id: i64,
    pub created_at: String,
    pub condition_id: String,
    pub market_question: Option<String>,
    pub ask_up: f64,
    pub ask_down: f64,
    pub total_cost: f64,
    pub quantity: f64,
    pub fees: f64,
    pub gas: f64,
    pub status: String,
    pub net_profit: Option<f64>,
    pub settled: i64,
    pub order_id_up: Option<String>,
    pub order_id_down: Option<String>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct BalanceSnapshot {
    pub id: i64,
    pub created_at: String,
    pub usdc_balance: f64,
    pub open_positions_value: f64,
    pub total_equity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, Default)]
pub struct DailyStats {
    pub date: String,
    pub trades: i64,
    pub wins: i64,
    pub losses: i64,
    pub cancelled: i64,
    pub total_profit: f64,
    pub total_volume: f64,
    pub max_drawdown: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LifetimeStats {
    pub trades: i64,
    pub wins: i64,
    pub losses: i64,
    pub cancelled: i64,
    pub total_profit: f64,
    pub total_volume: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct OpportunityRecord {
    pub id: i64,
    pub created_at: String,
    pub condition_id: String,
    pub ask_up: f64,
    pub ask_down: f64,
    pub net_profit: f64,
    pub executed: i64,
    pub skip_reason: Option<String>,
}

impl Store {
    pub async fn new(db_path: &str) -> Result<Self> {
        if !db_path.starts_with("sqlite:") && db_path != ":memory:" {
            if let Some(parent) = Path::new(db_path).parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        let url = if db_path == ":memory:" || db_path.starts_with("sqlite:") {
            db_path.to_owned()
        } else {
            format!("sqlite://{db_path}?mode=rwc")
        };
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn run_migrations(&self) -> Result<()> {
        for statement in MIGRATIONS {
            sqlx::query(statement).execute(&self.pool).await?;
        }
        // Schema extensions — ignore errors if columns already exist
        for alter in SCHEMA_EXTENSIONS {
            let _ = sqlx::query(alter).execute(&self.pool).await;
        }
        Ok(())
    }

    pub async fn insert_trade(&self, trade: &ArbTradeRecord) -> Result<i64> {
        let result = sqlx::query(
            "INSERT INTO arb_trades (
                condition_id, market_question, ask_up, ask_down, total_cost, quantity,
                fees, gas, status, net_profit, settled, order_id_up, order_id_down, note
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        )
        .bind(&trade.condition_id)
        .bind(&trade.market_question)
        .bind(trade.ask_up)
        .bind(trade.ask_down)
        .bind(trade.total_cost)
        .bind(trade.quantity)
        .bind(trade.fees)
        .bind(trade.gas)
        .bind(&trade.status)
        .bind(trade.net_profit)
        .bind(trade.settled)
        .bind(&trade.order_id_up)
        .bind(&trade.order_id_down)
        .bind(&trade.note)
        .execute(&self.pool)
        .await?;
        Ok(result.last_insert_rowid())
    }

    pub async fn update_trade_settled(&self, id: i64, profit: f64) -> Result<()> {
        sqlx::query("UPDATE arb_trades SET net_profit = ?1, settled = 1 WHERE id = ?2")
            .bind(profit)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn insert_balance_snapshot(&self, snap: &BalanceSnapshot) -> Result<()> {
        sqlx::query(
            "INSERT INTO balance_snapshots (usdc_balance, open_positions_value, total_equity)
             VALUES (?1, ?2, ?3)",
        )
        .bind(snap.usdc_balance)
        .bind(snap.open_positions_value)
        .bind(snap.total_equity)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_daily_stats(&self, stats: &DailyStats) -> Result<()> {
        sqlx::query(
            "INSERT INTO daily_stats (date, trades, wins, losses, cancelled, total_profit, total_volume, max_drawdown)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(date) DO UPDATE SET
               trades = excluded.trades,
               wins = excluded.wins,
               losses = excluded.losses,
               cancelled = excluded.cancelled,
               total_profit = excluded.total_profit,
               total_volume = excluded.total_volume,
               max_drawdown = excluded.max_drawdown",
        )
        .bind(&stats.date)
        .bind(stats.trades)
        .bind(stats.wins)
        .bind(stats.losses)
        .bind(stats.cancelled)
        .bind(stats.total_profit)
        .bind(stats.total_volume)
        .bind(stats.max_drawdown)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_opportunity(&self, opp: &OpportunityRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO opportunities (condition_id, ask_up, ask_down, net_profit, executed, skip_reason)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(&opp.condition_id)
        .bind(opp.ask_up)
        .bind(opp.ask_down)
        .bind(opp.net_profit)
        .bind(opp.executed)
        .bind(&opp.skip_reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn recent_trades(&self, limit: u32) -> Result<Vec<ArbTradeRecord>> {
        sqlx::query_as::<_, ArbTradeRecord>("SELECT * FROM arb_trades ORDER BY id DESC LIMIT ?1")
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn trade_by_id(&self, id: i64) -> Result<Option<ArbTradeRecord>> {
        sqlx::query_as::<_, ArbTradeRecord>("SELECT * FROM arb_trades WHERE id = ?1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn daily_stats_range(&self, from: &str, to: &str) -> Result<Vec<DailyStats>> {
        sqlx::query_as::<_, DailyStats>(
            "SELECT * FROM daily_stats WHERE date >= ?1 AND date <= ?2 ORDER BY date ASC",
        )
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn balance_history(&self, limit: u32) -> Result<Vec<BalanceSnapshot>> {
        sqlx::query_as::<_, BalanceSnapshot>(
            "SELECT * FROM balance_snapshots ORDER BY id DESC LIMIT ?1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn today_stats(&self) -> Result<DailyStats> {
        let today = Utc::now().format("%Y-%m-%d").to_string();
        Ok(
            sqlx::query_as::<_, DailyStats>("SELECT * FROM daily_stats WHERE date = ?1")
                .bind(&today)
                .fetch_optional(&self.pool)
                .await?
                .unwrap_or(DailyStats {
                    date: today,
                    ..DailyStats::default()
                }),
        )
    }

    pub async fn lifetime_stats(&self) -> Result<LifetimeStats> {
        let row = sqlx::query(
            "SELECT
                COUNT(*) AS trades,
                SUM(CASE WHEN status = 'BOTH_FILLED' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN status = 'ONE_SIDE' THEN 1 ELSE 0 END) AS losses,
                SUM(CASE WHEN status = 'BOTH_CANCELLED' THEN 1 ELSE 0 END) AS cancelled,
                CAST(COALESCE(SUM(net_profit), 0) AS REAL) AS total_profit,
                CAST(COALESCE(SUM(total_cost * quantity), 0) AS REAL) AS total_volume
             FROM arb_trades",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(LifetimeStats {
            trades: row.get("trades"),
            wins: row.get("wins"),
            losses: row.get("losses"),
            cancelled: row.get("cancelled"),
            total_profit: row.get("total_profit"),
            total_volume: row.get("total_volume"),
        })
    }

    pub async fn update_trade_merge(
        &self,
        id: i64,
        merge_status: &str,
        tx_hash: Option<&str>,
        gas_cost: Option<f64>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE arb_trades SET merge_status = ?1, merge_tx_hash = ?2, merge_gas_cost = ?3 WHERE id = ?4",
        )
        .bind(merge_status)
        .bind(tx_hash)
        .bind(gas_cost)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_trade_extra(
        &self,
        id: i64,
        filled_qty: f64,
        batch_count: u32,
        vwap_up: f64,
        vwap_down: f64,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE arb_trades SET filled_quantity = ?1, batch_count = ?2, vwap_up = ?3, vwap_down = ?4 WHERE id = ?5",
        )
        .bind(filled_qty)
        .bind(batch_count as i64)
        .bind(vwap_up)
        .bind(vwap_down)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn recent_opportunities(&self, limit: u32) -> Result<Vec<OpportunityRecord>> {
        sqlx::query_as::<_, OpportunityRecord>(
            "SELECT * FROM opportunities ORDER BY id DESC LIMIT ?1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }
}

const MIGRATIONS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS arb_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        condition_id TEXT NOT NULL,
        market_question TEXT,
        ask_up REAL NOT NULL,
        ask_down REAL NOT NULL,
        total_cost REAL NOT NULL,
        quantity REAL NOT NULL,
        fees REAL NOT NULL,
        gas REAL NOT NULL,
        status TEXT NOT NULL,
        net_profit REAL,
        settled INTEGER NOT NULL DEFAULT 0,
        order_id_up TEXT,
        order_id_down TEXT,
        note TEXT
    )",
    "CREATE TABLE IF NOT EXISTS balance_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        usdc_balance REAL NOT NULL,
        open_positions_value REAL NOT NULL DEFAULT 0,
        total_equity REAL NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS daily_stats (
        date TEXT PRIMARY KEY,
        trades INTEGER NOT NULL DEFAULT 0,
        wins INTEGER NOT NULL DEFAULT 0,
        losses INTEGER NOT NULL DEFAULT 0,
        cancelled INTEGER NOT NULL DEFAULT 0,
        total_profit REAL NOT NULL DEFAULT 0,
        total_volume REAL NOT NULL DEFAULT 0,
        max_drawdown REAL NOT NULL DEFAULT 0
    )",
    "CREATE TABLE IF NOT EXISTS opportunities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        condition_id TEXT NOT NULL,
        ask_up REAL NOT NULL,
        ask_down REAL NOT NULL,
        net_profit REAL NOT NULL,
        executed INTEGER NOT NULL DEFAULT 0,
        skip_reason TEXT
    )",
];

const SCHEMA_EXTENSIONS: &[&str] = &[
    "ALTER TABLE arb_trades ADD COLUMN merge_status TEXT DEFAULT NULL",
    "ALTER TABLE arb_trades ADD COLUMN merge_tx_hash TEXT DEFAULT NULL",
    "ALTER TABLE arb_trades ADD COLUMN merge_gas_cost REAL DEFAULT NULL",
    "ALTER TABLE arb_trades ADD COLUMN filled_quantity REAL DEFAULT NULL",
    "ALTER TABLE arb_trades ADD COLUMN batch_count INTEGER DEFAULT NULL",
    "ALTER TABLE arb_trades ADD COLUMN vwap_up REAL DEFAULT NULL",
    "ALTER TABLE arb_trades ADD COLUMN vwap_down REAL DEFAULT NULL",
];
