use poly_arb::config::Config;

#[test]
fn defaults_enable_dry_run() {
    let config = Config::default();
    assert!(config.mode.dry_run);
    assert!(config.optimization.use_batch_orders);
    assert_eq!(config.risk.max_one_side_per_day, 5);
    assert!(config.validate().is_ok());
}

#[test]
fn live_mode_requires_credentials() {
    let mut config = Config::default();
    config.mode.dry_run = false;
    let err = config
        .validate()
        .expect_err("expected missing creds failure");
    assert!(err.to_string().contains("api credentials"));
}
