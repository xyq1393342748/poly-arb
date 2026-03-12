use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderType {
    Gtc,
    Fok,
    Gtd,
    Fak,
}

impl OrderType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gtc => "GTC",
            Self::Fok => "FOK",
            Self::Gtd => "GTD",
            Self::Fak => "FAK",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    Live,
    Matched,
    Cancelled,
    Expired,
    Rejected,
    Unknown,
}

impl OrderStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Live => "LIVE",
            Self::Matched => "MATCHED",
            Self::Cancelled => "CANCELLED",
            Self::Expired => "EXPIRED",
            Self::Rejected => "REJECTED",
            Self::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: f64,
    pub size: f64,
}

impl PriceLevel {
    pub const fn new(price: f64, size: f64) -> Self {
        Self { price, size }
    }
}
