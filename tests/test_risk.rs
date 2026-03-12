use chrono::Utc;
use poly_arb::{
    arb_engine::ArbSignal,
    config::RiskConfig,
    risk::{RiskManager, RiskVeto},
};

#[test]
fn cooldown_blocks_trade_after_loss() {
    let mut risk = RiskManager::new(&RiskConfig {
        max_daily_loss: 20.0,
        max_open_orders: 4,
        cooldown_after_loss_sec: 60,
        ..RiskConfig::default()
    });
    risk.record_trade_result(-1.0);
    let signal = ArbSignal {
        condition_id: "c1".into(),
        question: "q".into(),
        token_id_up: "up".into(),
        token_id_down: "down".into(),
        ask_up: 0.48,
        ask_down: 0.48,
        total_cost: 0.96,
        net_profit: 0.02,
        max_quantity: 10.0,
        position_notional: 9.6,
        profit_rate: 0.02,
        time_to_expiry_sec: 120,
        books_stale: false,
        timestamp: Utc::now(),
    };
    assert!(matches!(
        risk.check_pre_trade(&signal),
        RiskVeto::Rejected(_)
    ));
}

#[test]
fn one_side_consecutive_blocks_until_both_filled() {
    let mut risk = RiskManager::new(&RiskConfig {
        max_one_side_consecutive: 2,
        one_side_cooldown_sec: 900,
        ..RiskConfig::default()
    });
    let signal = ArbSignal {
        condition_id: "c1".into(),
        question: "q".into(),
        token_id_up: "up".into(),
        token_id_down: "down".into(),
        ask_up: 0.48,
        ask_down: 0.48,
        total_cost: 0.96,
        net_profit: 0.02,
        max_quantity: 10.0,
        position_notional: 9.6,
        profit_rate: 0.02,
        time_to_expiry_sec: 120,
        books_stale: false,
        timestamp: Utc::now(),
    };

    risk.record_one_side();
    risk.record_one_side();
    assert!(matches!(
        risk.check_pre_trade(&signal),
        RiskVeto::Rejected(_)
    ));

    risk.record_both_filled();
    assert_eq!(risk.check_pre_trade(&signal), RiskVeto::Approved);
}

#[test]
fn one_side_daily_limit_blocks_trade() {
    let mut risk = RiskManager::new(&RiskConfig {
        max_one_side_per_day: 1,
        ..RiskConfig::default()
    });
    let signal = ArbSignal {
        condition_id: "c1".into(),
        question: "q".into(),
        token_id_up: "up".into(),
        token_id_down: "down".into(),
        ask_up: 0.48,
        ask_down: 0.48,
        total_cost: 0.96,
        net_profit: 0.02,
        max_quantity: 10.0,
        position_notional: 9.6,
        profit_rate: 0.02,
        time_to_expiry_sec: 120,
        books_stale: false,
        timestamp: Utc::now(),
    };

    risk.record_one_side();
    assert!(matches!(
        risk.check_pre_trade(&signal),
        RiskVeto::Rejected(_)
    ));
}
