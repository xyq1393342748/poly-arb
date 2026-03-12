use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use poly_arb::{
    arb_engine::MarketPair,
    auth::OrderSigner,
    config::{OptimizationConfig, StrategyConfig},
    orderbook::OrderBookManager,
    presigner::PreSignedOrderPool,
    types::PriceLevel,
    ws_market::BookUpdate,
};

fn signer() -> OrderSigner {
    OrderSigner::new(
        "0x1111111111111111111111111111111111111111111111111111111111111111",
        137,
    )
    .expect("signer")
}

fn setup_books() -> OrderBookManager {
    let mut books = OrderBookManager::default();
    books.process_update(BookUpdate {
        asset_id: "1".into(),
        timestamp: 1,
        bids: vec![PriceLevel::new(0.47, 10.0)],
        asks: vec![PriceLevel::new(0.48, 10.0)],
        is_snapshot: true,
    });
    books.process_update(BookUpdate {
        asset_id: "2".into(),
        timestamp: 1,
        bids: vec![PriceLevel::new(0.47, 8.0)],
        asks: vec![PriceLevel::new(0.48, 8.0)],
        is_snapshot: true,
    });
    books
}

fn setup_market() -> MarketPair {
    MarketPair {
        condition_id: "c1".into(),
        question: "BTC up or down".into(),
        token_id_up: "1".into(),
        token_id_down: "2".into(),
        end_date: Utc::now() + ChronoDuration::minutes(5),
    }
}

#[test]
fn presigner_hits_cache_for_matching_price_and_size() {
    let books = setup_books();
    let market = setup_market();
    let mut pool = PreSignedOrderPool::new(
        signer(),
        StrategyConfig::default(),
        OptimizationConfig {
            presign_price_offsets: 0,
            ..OptimizationConfig::default()
        },
    );

    pool.refresh(&books, &[market]);

    let first = pool.get_or_sign("1", 0.48, 8.0).expect("payload");
    let second = pool.get_or_sign("1", 0.48, 8.0).expect("cached payload");

    assert_eq!(first, second);
}

#[test]
fn presigner_evicts_stale_entries() {
    let books = setup_books();
    let market = setup_market();
    let mut pool = PreSignedOrderPool::new(
        signer(),
        StrategyConfig::default(),
        OptimizationConfig {
            presign_price_offsets: 0,
            presign_ttl_sec: 1,
            ..OptimizationConfig::default()
        },
    );

    pool.refresh(&books, &[market]);
    let cached = pool.get_or_sign("1", 0.48, 8.0).expect("payload");

    std::thread::sleep(Duration::from_millis(1_100));
    pool.evict_stale();

    let refreshed = pool.get_or_sign("1", 0.48, 8.0).expect("refreshed payload");
    assert_ne!(cached, refreshed);
}
