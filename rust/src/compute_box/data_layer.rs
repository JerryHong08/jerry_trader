// rust/src/compute_box/data_layer.rs
//
// DataLayer - the single source of truth for trades, bars, and bootstrap state.
//
// Design goals:
//   1. Trades never leave Rust memory (no FFI copy to Python)
//   2. bars_buffer provides fast recent-bar queries (no ClickHouse hit)
//   3. BootstrapStatus tracks per-ticker warmup state
//
// See roadmap/rust-compute-box-architecture.md for full design.

use crate::bars::CompletedBar;
use std::collections::{HashMap, VecDeque};

// ── Trade ───────────────────────────────────────────────────────────────

/// A single trade tick.
///
/// Timestamps are in ET milliseconds (epoch ms in US/Eastern timezone).
#[derive(Debug, Clone)]
pub struct Trade {
    pub timestamp_ms: i64,
    pub price: f64,
    pub size: f64,
}

impl Trade {
    pub fn new(timestamp_ms: i64, price: f64, size: f64) -> Self {
        Self {
            timestamp_ms,
            price,
            size,
        }
    }
}

// ── BootstrapStatus ─────────────────────────────────────────────────────

/// Bootstrap state machine for a ticker.
///
/// Tracks whether the ticker has loaded historical data and is ready
/// for real-time factor computation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapStatus {
    /// No bootstrap has been started.
    NotStarted,
    /// Bootstrap is in progress (loading trades, building bars).
    InProgress,
    /// Bootstrap completed successfully, bars_buffer has data.
    Ready,
    /// Bootstrap failed with an error message.
    Failed(String),
}

impl Default for BootstrapStatus {
    fn default() -> Self {
        BootstrapStatus::NotStarted
    }
}

// ── DataLayer ───────────────────────────────────────────────────────────

/// The single source of truth for all real-time data.
///
/// Contains:
///   - trades: per-ticker trade storage (for FactorEngine warmup)
///   - bars_buffer: recent completed bars (for fast history queries)
///   - bootstrap_status: per-ticker warmup state
///
/// All data is kept in Rust memory. Python accesses data via FFI getters
/// (e.g., get_recent_bars) rather than receiving full collections.
pub struct DataLayer {
    /// Per-ticker trade storage.
    /// Key: ticker symbol (e.g., "AAPL")
    /// Value: VecDeque of trades, newest at back
    trades: HashMap<String, VecDeque<Trade>>,

    /// Recent completed bars buffer.
    /// Key: (ticker, timeframe_label)
    /// Value: VecDeque of bars, newest at back
    bars_buffer: HashMap<(String, &'static str), VecDeque<CompletedBar>>,

    /// Per-ticker bootstrap status.
    bootstrap_status: HashMap<String, BootstrapStatus>,

    /// Maximum trades to keep per ticker (for FactorEngine warmup).
    /// Older trades are evicted when limit is reached.
    max_trades_per_ticker: usize,

    /// Maximum bars to keep per ticker+timeframe.
    /// Older bars are evicted when limit is reached.
    max_bars_per_key: usize,
}

impl DataLayer {
    /// Create a new DataLayer with default capacity limits.
    pub fn new() -> Self {
        Self {
            trades: HashMap::new(),
            bars_buffer: HashMap::new(),
            bootstrap_status: HashMap::new(),
            max_trades_per_ticker: 10_000,
            max_bars_per_key: 1_000,
        }
    }

    /// Create a new DataLayer with custom capacity limits.
    pub fn with_capacity(max_trades: usize, max_bars: usize) -> Self {
        Self {
            trades: HashMap::new(),
            bars_buffer: HashMap::new(),
            bootstrap_status: HashMap::new(),
            max_trades_per_ticker: max_trades,
            max_bars_per_key: max_bars,
        }
    }

    // ── Trades ──────────────────────────────────────────────────────────

    /// Push a trade for a ticker.
    ///
    /// Evicts oldest trades if max_trades_per_ticker is exceeded.
    pub fn push_trade(&mut self, ticker: &str, trade: Trade) {
        let deque = self
            .trades
            .entry(ticker.to_string())
            .or_insert_with(|| VecDeque::with_capacity(self.max_trades_per_ticker));

        deque.push_back(trade);

        // Evict oldest if over capacity
        while deque.len() > self.max_trades_per_ticker {
            deque.pop_front();
        }
    }

    /// Push multiple trades for a ticker (bulk load).
    ///
    /// More efficient than calling push_trade N times.
    pub fn push_trades_batch(&mut self, ticker: &str, trades: Vec<Trade>) {
        let deque = self
            .trades
            .entry(ticker.to_string())
            .or_insert_with(|| VecDeque::with_capacity(self.max_trades_per_ticker));

        for trade in trades {
            deque.push_back(trade);
        }

        // Evict oldest if over capacity
        while deque.len() > self.max_trades_per_ticker {
            deque.pop_front();
        }
    }

    /// Get the number of trades stored for a ticker.
    pub fn trades_count(&self, ticker: &str) -> usize {
        self.trades.get(ticker).map(|d| d.len()).unwrap_or(0)
    }

    /// Get all trades for a ticker (for FactorEngine warmup).
    ///
    /// Returns a slice reference to the internal VecDeque.
    pub fn get_trades(&self, ticker: &str) -> Option<&VecDeque<Trade>> {
        self.trades.get(ticker)
    }

    /// Clear all trades for a ticker.
    pub fn clear_trades(&mut self, ticker: &str) {
        self.trades.remove(ticker);
    }

    // ── Bars Buffer ─────────────────────────────────────────────────────

    /// Push a completed bar to the buffer.
    ///
    /// Evicts oldest bars if max_bars_per_key is exceeded.
    pub fn push_bar(&mut self, ticker: &str, timeframe: &'static str, bar: CompletedBar) {
        let key = (ticker.to_string(), timeframe);
        let deque = self
            .bars_buffer
            .entry(key)
            .or_insert_with(|| VecDeque::with_capacity(self.max_bars_per_key));

        deque.push_back(bar);

        // Evict oldest if over capacity
        while deque.len() > self.max_bars_per_key {
            deque.pop_front();
        }
    }

    /// Push multiple bars to the buffer (bulk load from bootstrap).
    pub fn push_bars_batch(&mut self, ticker: &str, bars: Vec<CompletedBar>) {
        for bar in bars {
            self.push_bar(ticker, bar.timeframe, bar);
        }
    }

    /// Get recent bars for a ticker+timeframe.
    ///
    /// Returns up to `count` bars, newest bars first (reverse order).
    /// If buffer has fewer bars than requested, returns all available.
    pub fn get_recent_bars(&self, ticker: &str, timeframe: &'static str, count: usize) -> Vec<&CompletedBar> {
        let key = (ticker.to_string(), timeframe);
        let deque = match self.bars_buffer.get(&key) {
            Some(d) => d,
            None => return Vec::new(),
        };

        // Return newest bars first (iterate from back)
        let available = deque.len().min(count);
        let mut result = Vec::with_capacity(available);
        for bar in deque.iter().rev().take(available) {
            result.push(bar);
        }
        result
    }

    /// Get the number of bars stored for a ticker+timeframe.
    pub fn bars_count(&self, ticker: &str, timeframe: &str) -> usize {
        let key = (ticker.to_string(), timeframe);
        self.bars_buffer.get(&key).map(|d| d.len()).unwrap_or(0)
    }

    /// Clear all bars for a ticker.
    pub fn clear_bars(&mut self, ticker: &str) {
        self.bars_buffer.retain(|(t, _), _| t != ticker);
    }

    // ── Bootstrap Status ────────────────────────────────────────────────

    /// Get the bootstrap status for a ticker.
    pub fn get_bootstrap_status(&self, ticker: &str) -> &BootstrapStatus {
        self.bootstrap_status
            .get(ticker)
            .unwrap_or(&BootstrapStatus::NotStarted)
    }

    /// Set the bootstrap status for a ticker.
    pub fn set_bootstrap_status(&mut self, ticker: &str, status: BootstrapStatus) {
        self.bootstrap_status.insert(ticker.to_string(), status);
    }

    /// Check if a ticker is ready (bootstrap completed).
    pub fn is_ready(&self, ticker: &str) -> bool {
        matches!(
            self.bootstrap_status.get(ticker),
            Some(BootstrapStatus::Ready) | None
        )
    }

    /// Check if a ticker is currently bootstrapping.
    pub fn is_bootstrapping(&self, ticker: &str) -> bool {
        matches!(
            self.bootstrap_status.get(ticker),
            Some(BootstrapStatus::InProgress)
        )
    }

    /// Clear bootstrap status for a ticker.
    pub fn clear_bootstrap_status(&mut self, ticker: &str) {
        self.bootstrap_status.remove(ticker);
    }

    // ── Ticker Management ───────────────────────────────────────────────

    /// Get all tickers with data in the layer.
    pub fn active_tickers(&self) -> Vec<String> {
        // Combine tickers from trades and bars_buffer
        let mut tickers: Vec<String> = self.trades.keys().cloned().collect();
        for (ticker, _) in self.bars_buffer.keys() {
            if !tickers.contains(ticker) {
                tickers.push(ticker.clone());
            }
        }
        tickers
    }

    /// Remove all data for a ticker (trades, bars, bootstrap status).
    pub fn remove_ticker(&mut self, ticker: &str) {
        self.trades.remove(ticker);
        self.bars_buffer.retain(|(t, _), _| t != ticker);
        self.bootstrap_status.remove(ticker);
    }

    /// Clear all data from the layer.
    pub fn clear(&mut self) {
        self.trades.clear();
        self.bars_buffer.clear();
        self.bootstrap_status.clear();
    }
}

impl Default for DataLayer {
    fn default() -> Self {
        Self::new()
    }
}

// ── Unit Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_creation() {
        let trade = Trade::new(1234567890, 100.5, 500.0);
        assert_eq!(trade.timestamp_ms, 1234567890);
        assert_eq!(trade.price, 100.5);
        assert_eq!(trade.size, 500.0);
    }

    #[test]
    fn test_bootstrap_status_default() {
        let status = BootstrapStatus::default();
        assert_eq!(status, BootstrapStatus::NotStarted);
    }

    #[test]
    fn test_data_layer_push_trade() {
        let mut layer = DataLayer::new();
        layer.push_trade("AAPL", Trade::new(1000, 100.0, 50.0));
        layer.push_trade("AAPL", Trade::new(1001, 101.0, 60.0));

        assert_eq!(layer.trades_count("AAPL"), 2);
        assert_eq!(layer.trades_count("MSFT"), 0);
    }

    #[test]
    fn test_data_layer_trade_eviction() {
        let mut layer = DataLayer::with_capacity(3, 10); // max 3 trades

        for i in 0..5 {
            layer.push_trade("AAPL", Trade::new(i, 100.0 + i as f64, 50.0));
        }

        // Should have evicted trades 0, 1
        assert_eq!(layer.trades_count("AAPL"), 3);

        let trades = layer.get_trades("AAPL").unwrap();
        assert_eq!(trades[0].timestamp_ms, 2); // oldest kept
        assert_eq!(trades[2].timestamp_ms, 4); // newest
    }

    #[test]
    fn test_data_layer_push_bar() {
        let bar = CompletedBar {
            ticker: "AAPL".to_string(),
            timeframe: "1m",
            open: 100.0,
            high: 105.0,
            low: 99.0,
            close: 102.0,
            volume: 5000.0,
            trade_count: 100,
            vwap: 101.0,
            bar_start: 1000,
            bar_end: 1060,
            session: "regular",
        };

        let mut layer = DataLayer::new();
        layer.push_bar("AAPL", "1m", bar.clone());

        assert_eq!(layer.bars_count("AAPL", "1m"), 1);
    }

    #[test]
    fn test_data_layer_get_recent_bars() {
        let mut layer = DataLayer::new();

        // Push 5 bars
        for i in 0..5 {
            layer.push_bar(
                "AAPL",
                "1m",
                CompletedBar {
                    ticker: "AAPL".to_string(),
                    timeframe: "1m",
                    open: 100.0 + i as f64,
                    high: 105.0 + i as f64,
                    low: 99.0 + i as f64,
                    close: 102.0 + i as f64,
                    volume: 5000.0,
                    trade_count: 100,
                    vwap: 101.0 + i as f64,
                    bar_start: 1000 + i * 60,
                    bar_end: 1060 + i * 60,
                    session: "regular",
                },
            );
        }

        // Get 3 most recent bars
        let recent = layer.get_recent_bars("AAPL", "1m", 3);
        assert_eq!(recent.len(), 3);

        // Newest first
        assert_eq!(recent[0].bar_start, 1000 + 4 * 60); // bar 4
        assert_eq!(recent[1].bar_start, 1000 + 3 * 60); // bar 3
        assert_eq!(recent[2].bar_start, 1000 + 2 * 60); // bar 2
    }

    #[test]
    fn test_data_layer_bootstrap_status() {
        let mut layer = DataLayer::new();

        assert!(!layer.is_bootstrapping("AAPL"));
        assert!(layer.is_ready("AAPL")); // NotStarted counts as ready for initial state

        layer.set_bootstrap_status("AAPL", BootstrapStatus::InProgress);
        assert!(layer.is_bootstrapping("AAPL"));
        assert!(!layer.is_ready("AAPL"));

        layer.set_bootstrap_status("AAPL", BootstrapStatus::Ready);
        assert!(!layer.is_bootstrapping("AAPL"));
        assert!(layer.is_ready("AAPL"));

        layer.set_bootstrap_status("AAPL", BootstrapStatus::Failed("No trades".to_string()));
        assert!(!layer.is_bootstrapping("AAPL"));
        assert!(!layer.is_ready("AAPL"));
    }

    #[test]
    fn test_data_layer_remove_ticker() {
        let mut layer = DataLayer::new();
        layer.push_trade("AAPL", Trade::new(1000, 100.0, 50.0));
        layer.push_bar(
            "AAPL",
            "1m",
            CompletedBar {
                ticker: "AAPL".to_string(),
                timeframe: "1m",
                open: 100.0,
                high: 105.0,
                low: 99.0,
                close: 102.0,
                volume: 5000.0,
                trade_count: 100,
                vwap: 101.0,
                bar_start: 1000,
                bar_end: 1060,
                session: "regular",
            },
        );
        layer.set_bootstrap_status("AAPL", BootstrapStatus::Ready);

        layer.remove_ticker("AAPL");

        assert_eq!(layer.trades_count("AAPL"), 0);
        assert_eq!(layer.bars_count("AAPL", "1m"), 0);
        assert_eq!(layer.get_bootstrap_status("AAPL"), &BootstrapStatus::NotStarted);
    }
}
