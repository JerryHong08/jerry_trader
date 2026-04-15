// rust/src/compute_box/factor_engine_core.rs
//
// FactorEngineCore - subscribes bar stream and computes factors.
//
// On each bar:
//   1. Compute factors via BarFactor.update()
//   2. Write to ClickHouse via CHWriter
//   3. Broadcast to WS subscribers
//
// See roadmap/rust-compute-box-architecture.md for full design.

use crate::bars::CompletedBar;
use crate::compute_box::ch_writer::{CHFactor, CHWriter};
use crate::factor::{Bar, BarFactor};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

// ── FactorInstance ──────────────────────────────────────────────────────

/// A single factor instance for a ticker.
///
/// Holds the factor state (wrapped in Arc<Mutex> for thread safety).
pub struct FactorInstance {
    /// Factor name (e.g., "ema20", "relative_volume")
    name: String,

    /// Factor implementation (wrapped for concurrent access).
    factor: Arc<Mutex<dyn BarFactor>>,
}

impl FactorInstance {
    pub fn new(name: String, factor: Arc<Mutex<dyn BarFactor>>) -> Self {
        Self { name, factor }
    }

    /// Compute factor value for a bar.
    ///
    /// Returns None if warmup incomplete, Some(value) when ready.
    pub fn compute(&self, bar: &Bar) -> Option<f64> {
        let mut guard = self.factor.lock().unwrap();
        guard.update(bar)
    }

    /// Reset factor state.
    pub fn reset(&self) {
        let mut guard = self.factor.lock().unwrap();
        guard.reset();
    }

    /// Check if warmup complete.
    pub fn is_ready(&self) -> bool {
        let guard = self.factor.lock().unwrap();
        guard.is_ready()
    }
}

// ── FactorEngineCore ────────────────────────────────────────────────────

/// Factor computation engine.
///
/// Subscribes to bar stream and computes factors on each bar.
/// Results are written to ClickHouse and broadcast to WS.
pub struct FactorEngineCore {
    /// Bar stream receiver (from BarBuilderEngine).
    bar_rx: broadcast::Receiver<CompletedBar>,

    /// Factor instances per ticker.
    /// Key: ticker → Vec<FactorInstance>
    factors: HashMap<String, Vec<FactorInstance>>,

    /// ClickHouse writer (async).
    ch_writer: CHWriter,

    /// Factor broadcast sender (for WSPublisher).
    factor_tx: broadcast::Sender<CHFactor>,
}

impl FactorEngineCore {
    /// Create a new FactorEngineCore.
    pub fn new(
        bar_rx: broadcast::Receiver<CompletedBar>,
        factor_tx: broadcast::Sender<CHFactor>,
    ) -> Self {
        Self {
            bar_rx,
            factors: HashMap::new(),
            ch_writer: CHWriter::new(),
            factor_tx,
        }
    }

    /// Add a factor for a ticker.
    pub fn add_factor(&mut self, ticker: &str, instance: FactorInstance) {
        self.factors
            .entry(ticker.to_string())
            .or_insert_with(Vec::new)
            .push(instance);
    }

    /// Remove all factors for a ticker.
    pub fn remove_factors(&mut self, ticker: &str) {
        self.factors.remove(ticker);
    }

    /// Process a single bar and compute factors.
    ///
    /// Returns computed factor values.
    pub fn process_bar(&mut self, bar: &CompletedBar) -> Vec<CHFactor> {
        let instances = self.factors.get(&bar.ticker);
        if instances.is_none() || instances.unwrap().is_empty() {
            return Vec::new();
        }

        // Convert CompletedBar to factor::Bar
        let factor_bar = Bar {
            ts_ms: bar.bar_end,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume as i64,
        };

        let mut results: Vec<CHFactor> = Vec::new();

        for instance in instances.unwrap() {
            if let Some(value) = instance.compute(&factor_bar) {
                let ch_factor = CHFactor {
                    ticker: bar.ticker.clone(),
                    name: instance.name.clone(),
                    timeframe: bar.timeframe.to_string(),
                    value,
                    ts: bar.bar_end,
                };

                // Broadcast to WS subscribers (before moving to results)
                let _ = self.factor_tx.send(ch_factor.clone());

                results.push(ch_factor);
            }
        }

        results
    }

    /// Run the factor engine loop (async).
    ///
    /// Continuously receives bars from stream and processes them.
    /// CH writing is done asynchronously.
    pub async fn run(&mut self) {
        loop {
            // Async recv from bar stream
            let result = self.bar_rx.recv().await;

            match result {
                Ok(bar) => {
                    // Process bar and get factor results
                    let factors = self.process_bar(&bar);

                    // Write to ClickHouse asynchronously
                    if !factors.is_empty() {
                        if let Err(e) = self.ch_writer.write_factors_batch(&factors).await {
                            log::warn!("CH factor write error: {}", e);
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    log::info!("Bar stream closed, FactorEngineCore stopping");
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    log::warn!("FactorEngineCore lagged by {} bars", n);
                    // Continue processing, lag is acceptable for factor computation
                }
            }
        }
    }

    /// Get factor sender (for WSPublisher to subscribe).
    pub fn factor_sender(&self) -> broadcast::Sender<CHFactor> {
        self.factor_tx.clone()
    }
}

// ── Unit Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factor_engine_core_creation() {
        let (bar_tx, bar_rx) = broadcast::channel(16);
        let (factor_tx, _) = broadcast::channel(16);

        let engine = FactorEngineCore::new(bar_rx, factor_tx);
        assert!(engine.factors.is_empty());
    }

    #[test]
    fn test_process_bar_no_factors() {
        let (bar_tx, bar_rx) = broadcast::channel(16);
        let (factor_tx, _) = broadcast::channel(16);

        let mut engine = FactorEngineCore::new(bar_rx, factor_tx);

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

        let results = engine.process_bar(&bar);
        assert!(results.is_empty());
    }
}
