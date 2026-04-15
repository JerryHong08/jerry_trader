// rust/src/factor/data.rs
//
// Data structures for factor computation.
// Bar, Trade, Quote structs used as inputs to factor traits.

/// Completed bar data (OHLCV).
///
/// Used as input to BarFactor traits.
#[derive(Debug, Clone, Copy)]
pub struct Bar {
    /// Bar close timestamp in milliseconds (ET or UTC depending on context)
    pub ts_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,
}

impl Bar {
    /// Create a new bar.
    pub fn new(ts_ms: i64, open: f64, high: f64, low: f64, close: f64, volume: i64) -> Self {
        Self {
            ts_ms,
            open,
            high,
            low,
            close,
            volume,
        }
    }
}

/// Trade tick data.
///
/// Used as input to TickFactor traits.
#[derive(Debug, Clone, Copy)]
pub struct Trade {
    /// Trade timestamp in milliseconds
    pub ts_ms: i64,
    /// Trade price
    pub price: f64,
    /// Trade size (number of shares)
    pub size: i64,
}

impl Trade {
    /// Create a new trade tick.
    pub fn new(ts_ms: i64, price: f64, size: i64) -> Self {
        Self { ts_ms, price, size }
    }
}

/// Quote (bid/ask) data.
///
/// Used as input to QuoteFactor traits.
#[derive(Debug, Clone, Copy)]
pub struct Quote {
    /// Quote timestamp in milliseconds
    pub ts_ms: i64,
    /// Best bid price
    pub bid: f64,
    /// Best ask price
    pub ask: f64,
    /// Bid size (shares at bid price)
    pub bid_size: i64,
    /// Ask size (shares at ask price)
    pub ask_size: i64,
}

impl Quote {
    /// Create a new quote.
    pub fn new(ts_ms: i64, bid: f64, ask: f64, bid_size: i64, ask_size: i64) -> Self {
        Self {
            ts_ms,
            bid,
            ask,
            bid_size,
            ask_size,
        }
    }

    /// Compute mid price.
    pub fn mid(&self) -> f64 {
        (self.bid + self.ask) / 2.0
    }

    /// Compute spread percentage (ask - bid) / mid.
    pub fn spread_pct(&self) -> f64 {
        if self.mid() > 0.0 {
            (self.ask - self.bid) / self.mid()
        } else {
            0.0
        }
    }
}
