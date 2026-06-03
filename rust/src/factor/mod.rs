// rust/src/factor/mod.rs
//
// Unified Factor architecture module.
// Supports both batch (backtest/warmup) and incremental (live) computation.

mod bar_factors;
mod batch_compute;
mod data;
mod factor_trait;
mod py_engine;
mod quote_factors;
mod registry;
mod trade_factors;

// Re-export public types
pub use batch_compute::{
    compute_all_bar_factors, compute_all_quote_factors, compute_signal_exits_batch,
    BarFactorConfig, QuoteFactorConfig, SignalExitResult,
};
pub use data::{Bar, Quote, Trade};
pub use factor_trait::{BarFactor, Factor, FactorType, QuoteFactor, TradeFactor};
pub use py_engine::{PyBar, PyFactorEngine, PyTrade};
pub use registry::FactorRegistry;
