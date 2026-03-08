// rust/src/replayer/stats.rs
//
// Lightweight replay statistics.

use std::collections::HashMap;

/// Aggregate statistics for the current replay session.
#[derive(Debug, Clone, Default)]
pub struct ReplayStats {
    /// Total messages delivered across all symbols.
    pub total_messages: usize,
    /// Messages delivered per symbol.
    pub messages_per_symbol: HashMap<String, usize>,
    /// Messages skipped (before start_time or past data) per symbol.
    pub skipped_messages: HashMap<String, usize>,
}

impl ReplayStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment_messages(&mut self, symbol: &str) {
        self.total_messages += 1;
        *self
            .messages_per_symbol
            .entry(symbol.to_string())
            .or_insert(0) += 1;
    }

    pub fn increment_skipped(&mut self, symbol: &str, count: usize) {
        *self
            .skipped_messages
            .entry(symbol.to_string())
            .or_insert(0) += count;
    }
}
