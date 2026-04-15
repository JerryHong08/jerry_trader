// rust/src/factor/mod.rs
//
// Unified Factor architecture module.
// Supports both batch (backtest/warmup) and incremental (live) computation.

mod data;
mod factor_trait;
mod py_engine;
mod registry;

// Re-export public types
pub use data::{Bar, Quote, Trade};
pub use factor_trait::{BarFactor, Factor, FactorType, QuoteFactor, TickFactor};
pub use py_engine::{PyBar, PyFactorEngine, PyTrade};
pub use registry::FactorRegistry;
