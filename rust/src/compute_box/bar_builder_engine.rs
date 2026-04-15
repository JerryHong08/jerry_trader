// rust/src/compute_box/bar_builder_engine.rs
//
// BarBuilderEngine - wraps BarBuilder with DataLayer and CH integration.
//
// When a bar completes, it:
//   1. Writes to DataLayer bars_buffer (for fast history queries)
//   2. Writes to ClickHouse (async, via CHWriter)
//   3. Broadcasts to factor_stream (for FactorEngineCore)
//   4. Broadcasts to ws_stream (for WSPublisher)
//
// See roadmap/rust-compute-box-architecture.md for full design.

use crate::bars::{BarBuilder, CompletedBar};
use crate::compute_box::data_layer::DataLayer;
use tokio::sync::broadcast;

// ── BarBuilderEngine ────────────────────────────────────────────────────

/// Bar building engine with internal storage and broadcast.
///
/// Wraps the existing BarBuilder and adds:
///   - bars_buffer storage (via DataLayer)
///   - bar stream broadcast (for FactorEngineCore and WSPublisher)
///
/// ClickHouse writing is done asynchronously via a background task.
pub struct BarBuilderEngine {
    /// Core bar builder (tick-to-OHLCV aggregation).
    builder: BarBuilder,

    /// Data layer for internal storage.
    data_layer: DataLayer,

    /// Broadcast channel for completed bars.
    /// Used by FactorEngineCore and WSPublisher.
    bar_tx: broadcast::Sender<CompletedBar>,
}

impl BarBuilderEngine {
    /// Create a new BarBuilderEngine.
    pub fn new(
        timeframes: Option<Vec<String>>,
        max_trades: usize,
        max_bars: usize,
    ) -> Self {
        let builder = BarBuilder::new_pub(timeframes);
        let data_layer = DataLayer::with_capacity(max_trades, max_bars);
        let (bar_tx, _) = broadcast::channel(256);

        Self {
            builder,
            data_layer,
            bar_tx,
        }
    }

    /// Get the bar broadcast sender.
    /// FactorEngineCore and WSPublisher subscribe to this.
    pub fn bar_sender(&self) -> broadcast::Sender<CompletedBar> {
        self.bar_tx.clone()
    }

    /// Get reference to the DataLayer.
    pub fn data_layer(&self) -> &DataLayer {
        &self.data_layer
    }

    /// Get mutable reference to the DataLayer.
    pub fn data_layer_mut(&mut self) -> &mut DataLayer {
        &mut self.data_layer
    }

    /// Ingest a single trade and process completed bars.
    ///
    /// Completed bars are:
    ///   1. Stored in bars_buffer
    ///   2. Broadcast to subscribers
    ///
    /// This method uses Python::attach to acquire the GIL
    /// for calling the underlying BarBuilder.
    pub fn ingest_trade(
        &mut self,
        ticker: &str,
        price: f64,
        size: f64,
        timestamp_ms: i64,
    ) -> Vec<CompletedBar> {
        // Call BarBuilder pub method (populates completed_queue)
        self.builder.ingest_trade_pub(ticker, price, size, timestamp_ms);

        // Drain completed_queue as CompletedBar structs
        let completed = self.builder.drain_completed_raw();

        // Process each completed bar
        for bar in &completed {
            // 1. Store in bars_buffer
            self.data_layer.push_bar(ticker, bar.timeframe, bar.clone());

            // 2. Broadcast to subscribers (FactorEngine, WSPublisher)
            // Ignore send errors (no subscribers is OK)
            let _ = self.bar_tx.send(bar.clone());
        }

        completed
    }

    /// Bulk ingest trades for a ticker.
    ///
    /// More efficient than calling ingest_trade N times.
    pub fn ingest_trades_batch(
        &mut self,
        ticker: &str,
        trades: Vec<(i64, f64, f64)>,  // (timestamp_ms, price, size)
    ) -> Vec<CompletedBar> {
        self.builder.ingest_trades_batch_pub(ticker, trades);

        let completed = self.builder.drain_completed_raw();

        for bar in &completed {
            self.data_layer.push_bar(ticker, bar.timeframe, bar.clone());
            let _ = self.bar_tx.send(bar.clone());
        }

        completed
    }

    /// Advance time and close expired bars.
    pub fn advance(&mut self, now_ms: i64) -> Vec<CompletedBar> {
        self.builder.advance_pub(now_ms);

        let completed = self.builder.drain_completed_raw();

        for bar in &completed {
            self.data_layer.push_bar(&bar.ticker, bar.timeframe, bar.clone());
            let _ = self.bar_tx.send(bar.clone());
        }

        completed
    }

    /// Flush all open bars (force complete).
    pub fn flush(&mut self) -> Vec<CompletedBar> {
        self.builder.flush_pub();

        let completed = self.builder.drain_completed_raw();

        for bar in &completed {
            self.data_layer.push_bar(&bar.ticker, bar.timeframe, bar.clone());
            let _ = self.bar_tx.send(bar.clone());
        }

        completed
    }

    /// Get recent bars for a ticker+timeframe.
    pub fn get_recent_bars(&self, ticker: &str, timeframe: &'static str, count: usize) -> Vec<&CompletedBar> {
        self.data_layer.get_recent_bars(ticker, timeframe, count)
    }

    /// Get bootstrap status for a ticker.
    pub fn get_bootstrap_status(&self, ticker: &str) -> &crate::compute_box::data_layer::BootstrapStatus {
        self.data_layer.get_bootstrap_status(ticker)
    }

    /// Set bootstrap status for a ticker.
    pub fn set_bootstrap_status(&mut self, ticker: &str, status: crate::compute_box::data_layer::BootstrapStatus) {
        self.data_layer.set_bootstrap_status(ticker, status);
    }

    /// Get active tickers.
    pub fn active_tickers(&self) -> Vec<String> {
        self.builder.active_tickers_pub()
    }
}

// ── Unit Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute_box::data_layer::BootstrapStatus;

    #[test]
    fn test_bar_builder_engine_creation() {
        let engine = BarBuilderEngine::new(Some(vec!["1m".to_string()]), 1000, 100);
        assert!(engine.data_layer().bars_count("AAPL", "1m") == 0);
    }

    #[test]
    fn test_bar_builder_engine_bootstrap_status() {
        let mut engine = BarBuilderEngine::new(None, 1000, 100);

        engine.set_bootstrap_status("AAPL", BootstrapStatus::InProgress);
        assert!(matches!(engine.get_bootstrap_status("AAPL"), BootstrapStatus::InProgress));

        engine.set_bootstrap_status("AAPL", BootstrapStatus::Ready);
        assert!(matches!(engine.get_bootstrap_status("AAPL"), BootstrapStatus::Ready));
    }
}
