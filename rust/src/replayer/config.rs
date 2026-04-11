// rust/src/replayer/config.rs
//
// Internal replay configuration (not exposed to Python directly).

use std::path::{Path, PathBuf};

/// ClickHouse connection configuration.
#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    /// HTTP URL, e.g. `"http://localhost:8123"`.
    pub url: String,
    pub user: String,
    pub password: String,
    pub database: String,
}

/// Replay configuration — constructed from Python arguments in
/// `TickDataReplayer::new()`.
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// Replay date in YYYYMMDD format.
    pub replay_date: String,
    /// Optional start time as epoch nanoseconds (pre-parsed from HH:MM:SS).
    pub start_timestamp_ns: Option<i64>,
    /// Base data-lake directory (e.g. `/mnt/blackdisk/quant_data/polygon_data/lake`).
    pub lake_data_dir: String,
    /// Threshold for logging large market gaps (ms).  `None` = disabled.
    pub max_gap_ms: Option<u64>,
    /// ClickHouse connection config. When `Some`, replayer queries CH first
    /// and falls back to Parquet files on failure.
    pub clickhouse: Option<ClickHouseConfig>,
}

impl ReplayConfig {
    /// Replay date in ISO format: `"2026-03-13"`.
    pub fn replay_date_iso(&self) -> String {
        let y = &self.replay_date[0..4];
        let m = &self.replay_date[4..6];
        let d = &self.replay_date[6..8];
        format!("{}-{}-{}", y, m, d)
    }

    /// Build the Parquet path for a given data subdir (e.g. `"quotes_v1"`).
    ///
    /// Pattern: `{lake_data_dir}/us_stocks_sip/{subdir}/{YYYY}/{MM}/{YYYY-MM-DD}.parquet`
    pub fn parquet_path(&self, subdir: &str) -> PathBuf {
        let year = &self.replay_date[0..4];
        let month = &self.replay_date[4..6];
        let day = &self.replay_date[6..8];
        let date_iso = format!("{}-{}-{}", year, month, day);

        Path::new(&self.lake_data_dir)
            .join("us_stocks_sip")
            .join(subdir)
            .join(year)
            .join(month)
            .join(format!("{}.parquet", date_iso))
    }

    /// Build the partitioned Parquet path for a specific ticker.
    ///
    /// Pattern: `{lake_data_dir}/us_stocks_sip/{subdir}_partitioned/{ticker}/{YYYY-MM-DD}.parquet`
    pub fn parquet_path_partitioned(&self, subdir: &str, ticker: &str) -> PathBuf {
        let year = &self.replay_date[0..4];
        let month = &self.replay_date[4..6];
        let day = &self.replay_date[6..8];
        let date_iso = format!("{}-{}-{}", year, month, day);

        Path::new(&self.lake_data_dir)
            .join("us_stocks_sip")
            .join(format!("{}_partitioned", subdir))
            .join(ticker)
            .join(format!("{}.parquet", date_iso))
    }
}
