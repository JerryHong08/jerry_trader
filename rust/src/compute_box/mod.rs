// rust/src/compute_box/mod.rs
//
// Rust Compute Box - unified data layer for bars and factors.
//
// The ComputeBox encapsulates all real-time data flow:
//   trades → bars → factors → ClickHouse → WebSocket
//
// All data stays in Rust memory, eliminating Python↔Rust FFI overhead.

mod bar_builder_engine;
mod ch_writer;
mod data_layer;
mod factor_engine_core;
mod polygon_fetcher;
mod ws_publisher;

pub use bar_builder_engine::BarBuilderEngine;
pub use ch_writer::{CHWriter, CHBar, CHFactor};
pub use data_layer::{DataLayer, Trade, BootstrapStatus};
pub use factor_engine_core::{FactorEngineCore, FactorInstance};
pub use polygon_fetcher::PolygonFetcher;
pub use ws_publisher::{WSPublisher, WsCommand, WsMessage};
