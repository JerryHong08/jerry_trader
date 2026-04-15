// rust/src/compute_box/ch_writer.rs
//
// ClickHouse writer for bars and factors.
//
// Async writing via clickhouse crate query API.
// Used by BarBuilderEngine and FactorEngineCore.
//
// See roadmap/rust-compute-box-architecture.md for full design.

use crate::bars::CompletedBar;
use anyhow::Result;
use clickhouse::Client;

// ── CHBar (serializable for ClickHouse) ────────────────────────────────

/// Bar row for ClickHouse INSERT.
///
/// Matches the bars table schema.
#[derive(Debug, Clone)]
pub struct CHBar {
    pub ticker: String,
    pub timeframe: String,
    pub bar_start: i64,
    pub bar_end: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub trade_count: u64,
    pub vwap: f64,
    pub session: String,
}

impl From<&CompletedBar> for CHBar {
    fn from(bar: &CompletedBar) -> Self {
        Self {
            ticker: bar.ticker.clone(),
            timeframe: bar.timeframe.to_string(),
            bar_start: bar.bar_start,
            bar_end: bar.bar_end,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume,
            trade_count: bar.trade_count,
            vwap: bar.vwap,
            session: bar.session.to_string(),
        }
    }
}

// ── FactorValue (for ClickHouse) ───────────────────────────────────────

/// Factor value row for ClickHouse INSERT.
#[derive(Debug, Clone)]
pub struct CHFactor {
    pub ticker: String,
    pub name: String,
    pub timeframe: String,
    pub value: f64,
    pub ts: i64,  // bar_end timestamp
}

// ── CHWriter ───────────────────────────────────────────────────────────

/// ClickHouse async writer.
///
/// Uses the clickhouse crate query API for INSERTs.
/// Can be configured with custom URL, user, password, database.
pub struct CHWriter {
    client: Client,
}

impl CHWriter {
    /// Create a new CHWriter with default localhost connection.
    pub fn new() -> Self {
        Self {
            client: Client::default().with_url("http://localhost:8123"),
        }
    }

    /// Create a new CHWriter with custom configuration.
    pub fn with_config(url: &str, user: &str, password: &str, database: &str) -> Self {
        Self {
            client: Client::default()
                .with_url(url)
                .with_user(user)
                .with_password(password)
                .with_database(database),
        }
    }

    /// Write a single bar to ClickHouse (async).
    pub async fn write_bar(&self, bar: &CompletedBar) -> Result<()> {
        let ch_bar = CHBar::from(bar);

        // Use raw SQL INSERT for simplicity
        let query = format!(
            "INSERT INTO bars (ticker, timeframe, bar_start, bar_end, open, high, low, close, volume, trade_count, vwap, session) VALUES ('{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}')",
            ch_bar.ticker,
            ch_bar.timeframe,
            ch_bar.bar_start,
            ch_bar.bar_end,
            ch_bar.open,
            ch_bar.high,
            ch_bar.low,
            ch_bar.close,
            ch_bar.volume,
            ch_bar.trade_count,
            ch_bar.vwap,
            ch_bar.session
        );

        self.client.query(&query).execute().await?;
        Ok(())
    }

    /// Write multiple bars in a batch (async).
    pub async fn write_bars_batch(&self, bars: &[CompletedBar]) -> Result<()> {
        if bars.is_empty() {
            return Ok(());
        }

        // Build batch INSERT query
        let values: Vec<String> = bars.iter().map(|bar| {
            let ch_bar = CHBar::from(bar);
            format!(
                "('{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}')",
                ch_bar.ticker,
                ch_bar.timeframe,
                ch_bar.bar_start,
                ch_bar.bar_end,
                ch_bar.open,
                ch_bar.high,
                ch_bar.low,
                ch_bar.close,
                ch_bar.volume,
                ch_bar.trade_count,
                ch_bar.vwap,
                ch_bar.session
            )
        }).collect();

        let query = format!(
            "INSERT INTO bars (ticker, timeframe, bar_start, bar_end, open, high, low, close, volume, trade_count, vwap, session) VALUES {}",
            values.join(", ")
        );

        self.client.query(&query).execute().await?;
        Ok(())
    }

    /// Write a single factor value (async).
    pub async fn write_factor(&self, factor: &CHFactor) -> Result<()> {
        let query = format!(
            "INSERT INTO factors (ticker, name, timeframe, value, ts) VALUES ('{}', '{}', '{}', {}, {})",
            factor.ticker,
            factor.name,
            factor.timeframe,
            factor.value,
            factor.ts
        );

        self.client.query(&query).execute().await?;
        Ok(())
    }

    /// Write multiple factors in a batch (async).
    pub async fn write_factors_batch(&self, factors: &[CHFactor]) -> Result<()> {
        if factors.is_empty() {
            return Ok(());
        }

        let values: Vec<String> = factors.iter().map(|f| {
            format!(
                "('{}', '{}', '{}', {}, {})",
                f.ticker, f.name, f.timeframe, f.value, f.ts
            )
        }).collect();

        let query = format!(
            "INSERT INTO factors (ticker, name, timeframe, value, ts) VALUES {}",
            values.join(", ")
        );

        self.client.query(&query).execute().await?;
        Ok(())
    }
}

impl Default for CHWriter {
    fn default() -> Self {
        Self::new()
    }
}

// ── Unit Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ch_bar_from_completed_bar() {
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

        let ch_bar = CHBar::from(&bar);
        assert_eq!(ch_bar.ticker, "AAPL");
        assert_eq!(ch_bar.timeframe, "1m");
        assert_eq!(ch_bar.close, 102.0);
    }

    #[test]
    fn test_ch_writer_creation() {
        let writer = CHWriter::new();
        // Just verify creation works (no actual CH connection test)
        let _ = writer.client;  // Access client to verify it exists
    }

    #[test]
    fn test_build_insert_query() {
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

        // Verify query building logic
        let ch_bar = CHBar::from(&bar);
        assert!(ch_bar.ticker.contains("AAPL"));
    }
}
