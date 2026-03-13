/// Polymarket fee curve implementation.
///
/// Formula: `fee_per_share = price × rate × (price × (1 − price))^exponent`
///
/// For crypto 5-min markets: rate = 0.25, exponent = 2.

/// Compute the fee per share for buying at `price`.
pub fn polymarket_fee(price: f64, rate: f64, exponent: u32) -> f64 {
    if price <= 0.0 || price >= 1.0 || rate <= 0.0 {
        return 0.0;
    }
    let base = price * (1.0 - price);
    price * rate * base.powi(exponent as i32)
}

/// Compute total fees for an arb trade (buy Up at `ask_up` + buy Down at `ask_down`).
pub fn arb_fees(ask_up: f64, ask_down: f64, rate: f64, exponent: u32) -> f64 {
    polymarket_fee(ask_up, rate, exponent) + polymarket_fee(ask_down, rate, exponent)
}

/// Compute fee rate in basis points for a given price (for order signing).
/// Returns the effective fee rate as bps, with a minimum floor.
pub fn fee_bps_for_price(price: f64, rate: f64, exponent: u32, min_bps: u64) -> u64 {
    if price <= 0.0 || price >= 1.0 {
        return min_bps;
    }
    let fee = polymarket_fee(price, rate, exponent);
    let effective_rate = fee / price;
    let bps = (effective_rate * 10_000.0).ceil() as u64;
    bps.max(min_bps)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_at_midpoint() {
        // p=0.50, rate=0.25, exp=2
        // fee = 0.50 * 0.25 * (0.50 * 0.50)^2 = 0.50 * 0.25 * 0.0625 = 0.0078125
        let fee = polymarket_fee(0.50, 0.25, 2);
        assert!((fee - 0.0078125).abs() < 1e-10);
    }

    #[test]
    fn fee_bps_at_midpoint() {
        // effective_rate = 0.25 * (0.25)^2 = 0.015625 → 157 bps (ceil)
        let bps = fee_bps_for_price(0.50, 0.25, 2, 10);
        assert_eq!(bps, 157); // ceil(156.25) = 157
    }

    #[test]
    fn arb_fees_symmetric() {
        let fees = arb_fees(0.50, 0.50, 0.25, 2);
        assert!((fees - 0.015625).abs() < 1e-10);
    }

    #[test]
    fn fee_at_boundary() {
        assert_eq!(polymarket_fee(0.0, 0.25, 2), 0.0);
        assert_eq!(polymarket_fee(1.0, 0.25, 2), 0.0);
    }
}
