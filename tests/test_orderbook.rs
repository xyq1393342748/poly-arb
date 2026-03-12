use poly_arb::{orderbook::OrderBookManager, types::PriceLevel, ws_market::BookUpdate};

#[test]
fn snapshot_and_delta_update_best_prices() {
    let mut manager = OrderBookManager::default();
    manager.process_update(BookUpdate {
        asset_id: "token-a".into(),
        timestamp: 1,
        bids: vec![PriceLevel::new(0.48, 10.0)],
        asks: vec![PriceLevel::new(0.52, 8.0), PriceLevel::new(0.53, 5.0)],
        is_snapshot: true,
    });
    let book = manager.get_book("token-a").expect("book");
    assert_eq!(book.best_bid(), Some((0.48, 10.0)));
    assert_eq!(book.best_ask(), Some((0.52, 8.0)));

    manager.process_update(BookUpdate {
        asset_id: "token-a".into(),
        timestamp: 2,
        bids: vec![],
        asks: vec![PriceLevel::new(0.52, 0.0), PriceLevel::new(0.51, 4.0)],
        is_snapshot: false,
    });
    let book = manager.get_book("token-a").expect("book");
    assert_eq!(book.best_ask(), Some((0.51, 4.0)));
}
