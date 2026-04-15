// rust/src/factor/trait.rs
//
// Core trait definitions for the unified Factor architecture.
// Supports both batch (backtest/warmup) and incremental (live) computation modes.

use super::data::{Bar, Quote, Trade};

/// Factor type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FactorType {
    Bar,
    Tick,
    Quote,
}

/// Base trait for all factors.
///
/// Every factor must implement this base trait, which provides
/// common functionality for name, type, reset, and readiness checks.
pub trait Factor: Send + Sync {
    /// Factor name for registry lookup and logging.
    fn name(&self) -> &str;

    /// Factor type: bar, tick, or quote.
    fn factor_type(&self) -> FactorType;

    /// Reset state for a new ticker/session.
    /// Called when switching tickers or at session start.
    fn reset(&mut self);

    /// Check if warmup is complete (ready to produce valid output).
    /// Returns true when enough data has been processed.
    fn is_ready(&self) -> bool;
}

/// Bar-based factors (EMA, RSI, RelativeVolume, etc.)
///
/// Updated incrementally on each bar close in live mode,
/// or computed in batch for backtest/warmup scenarios.
pub trait BarFactor: Factor {
    /// Batch computation: given historical bars, compute factor values.
    ///
    /// Returns Vec<Option<f64>> with same length as input.
    /// - Some(value) when warmup complete for that position
    /// - None during warmup period
    ///
    /// Used for:
    /// - Backtest: compute entire history in one call
    /// - Warmup: bootstrap from ClickHouse historical bars
    fn compute_batch(&self, bars: &[Bar]) -> Vec<Option<f64>>;

    /// Incremental update: process a single bar, return current factor value.
    ///
    /// Called in live mode when a new bar closes.
    /// Returns None if warmup incomplete, Some(value) when ready.
    fn update(&mut self, bar: &Bar) -> Option<f64>;
}

/// Tick-based factors (TradeRate, LargeTradeRatio, etc.)
///
/// Ingests ticks continuously in live mode, computes on demand.
/// For backtest, uses batch computation from historical trades.
pub trait TickFactor: Factor {
    /// Batch computation: given historical trades, compute factor values at specified timestamps.
    ///
    /// `trades`: Historical trade ticks
    /// `compute_ts`: Timestamps (in ms) where we want factor values
    ///
    /// Returns Vec<Option<f64>> with same length as compute_ts.
    fn compute_batch(&self, trades: &[Trade], compute_ts: &[i64]) -> Vec<Option<f64>>;

    /// Incremental update: process a single trade tick.
    ///
    /// Called in live mode for each incoming trade.
    /// Factor may buffer internally and compute on demand later.
    fn on_tick(&mut self, tick: &Trade) -> Option<f64>;
}

/// Quote-based factors (Spread, MidPrice, etc.)
///
/// Updated on quote updates (bid/ask changes).
pub trait QuoteFactor: Factor {
    /// Batch computation: given historical quotes, compute factor values.
    fn compute_batch(&self, quotes: &[Quote]) -> Vec<Option<f64>>;

    /// Incremental update: process a single quote update.
    fn on_quote(&mut self, quote: &Quote) -> Option<f64>;
}
