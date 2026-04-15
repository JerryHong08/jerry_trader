// rust/src/compute_box/mod.rs
//
// Rust Compute Box - unified data layer for bars and factors.
//
// The ComputeBox encapsulates all real-time data flow:
//   trades → bars → factors → ClickHouse → WebSocket
//
// All data stays in Rust memory, eliminating Python↔Rust FFI overhead.

mod data_layer;

pub use data_layer::{DataLayer, Trade, BootstrapStatus};
