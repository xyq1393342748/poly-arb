use chrono::{Duration, Utc};
use poly_arb::{
    arb_engine::{ArbEngine, MarketPair},
    config::StrategyConfig,
    orderbook::OrderBookManager,
    types::PriceLevel,
    ws_market::BookUpdate,
};

#[test]
fn profitable_pair_triggers_signal() {
    let mut engine = ArbEngine::new(StrategyConfig {
        min_profit: 0.005,
        max_position_usd: 50.0,
        taker_fee_rate: 0.001,
        gas_per_order: 0.007,
    });
    engine.register_market(MarketPair {
        condition_id: "c1".into(),
        question: "BTC up or down".into(),
        token_id_up: "up".into(),
        token_id_down: "down".into(),
        end_date: Utc::now() + Duration::minutes(4),
    });
    let mut books = OrderBookManager::default();
    books.process_update(BookUpdate {
        asset_id: "up".into(),
        timestamp: 1,
        bids: vec![],
        asks: vec![PriceLevel::new(0.48, 10.0)],
        is_snapshot: true,
    });
    books.process_update(BookUpdate {
        asset_id: "down".into(),
        timestamp: 1,
        bids: vec![],
        asks: vec![PriceLevel::new(0.48, 8.0)],
        is_snapshot: true,
    });

    let signal = engine.evaluate(&books).pop().expect("signal");
    assert_eq!(signal.condition_id, "c1");
    assert!(signal.net_profit > 0.005);
    assert_eq!(signal.max_quantity, 8.0);
}

#[test]
fn flat_pair_does_not_trigger() {
    let mut engine = ArbEngine::new(StrategyConfig::default());
    engine.register_market(MarketPair {
        condition_id: "c1".into(),
        question: "BTC up or down".into(),
        token_id_up: "up".into(),
        token_id_down: "down".into(),
        end_date: Utc::now() + Duration::minutes(4),
    });
    let mut books = OrderBookManager::default();
    books.process_update(BookUpdate {
        asset_id: "up".into(),
        timestamp: 1,
        bids: vec![],
        asks: vec![PriceLevel::new(0.50, 10.0)],
        is_snapshot: true,
    });
    books.process_update(BookUpdate {
        asset_id: "down".into(),
        timestamp: 1,
        bids: vec![],
        asks: vec![PriceLevel::new(0.50, 8.0)],
        is_snapshot: true,
    });

    assert!(engine.evaluate(&books).is_empty());
}
