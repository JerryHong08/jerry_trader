use pyo3::prelude::*;

mod bars;
mod clock;
mod factor;
mod factors;
mod replayer;
mod snapshot;

/// The Rust computation core, exposed to Python as `jerry_trader._rust`.
#[pymodule]
mod _rust {
    use super::*;

    /// Initialize pyo3-log bridge on module load.
    /// This routes all Rust `log::info!` / `log::warn!` to Python's logging module.
    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        pyo3_log::init();
        // Optional: set Rust log level filter (defaults to follow Python's level)
        // pyo3_log::reset_level(log::LevelFilter::Debug);
        log::info!("Rust logging bridge initialized - Rust logs will appear in Python logs");
        Ok(())
    }

    // ── Factor compute functions ────────────────────────────────────
    #[pyfunction]
    #[pyo3(name = "z_score")]
    fn py_z_score(value: f64, history: Vec<f64>) -> Option<f64> {
        factors::z_score(value, history)
    }

    #[pyfunction]
    #[pyo3(name = "price_accel")]
    fn py_price_accel<'py>(
        recent: &Bound<'py, pyo3::types::PyList>,
        older: &Bound<'py, pyo3::types::PyList>,
    ) -> PyResult<f64> {
        factors::price_accel(recent, older)
    }

    // ── Bar-based factor functions ───────────────────────────────────
    #[pyfunction]
    #[pyo3(name = "momentum")]
    fn py_momentum(closes: Vec<f64>, window: usize) -> f64 {
        factors::momentum(closes, window)
    }

    #[pyfunction]
    #[pyo3(name = "volatility")]
    fn py_volatility(closes: Vec<f64>, window: usize) -> f64 {
        factors::volatility(closes, window)
    }

    #[pyfunction]
    #[pyo3(name = "rsi")]
    fn py_rsi(closes: Vec<f64>, window: usize) -> f64 {
        factors::rsi(closes, window)
    }

    #[pyfunction]
    #[pyo3(name = "vwap_deviation")]
    fn py_vwap_deviation(bars: Vec<(f64, i64)>, window: usize) -> f64 {
        factors::vwap_deviation(bars, window)
    }

    #[pyfunction]
    #[pyo3(name = "volume_ratio")]
    fn py_volume_ratio(volumes: Vec<i64>, window: usize) -> f64 {
        factors::volume_ratio(volumes, window)
    }

    // ── Tick-based indicator functions ────────────────────────────────
    #[pyfunction]
    #[pyo3(name = "trade_rate")]
    #[pyo3(signature = (timestamps, current_ms, window_ms, min_trades = 5))]
    fn py_trade_rate(
        timestamps: Vec<i64>,
        current_ms: i64,
        window_ms: i64,
        min_trades: usize,
    ) -> Option<f64> {
        factors::trade_rate(timestamps, current_ms, window_ms, min_trades)
    }

    #[pyfunction]
    #[pyo3(name = "bootstrap_trade_rate")]
    #[pyo3(signature = (timestamps, window_ms = 20_000, min_trades = 5, compute_interval_ms = 1_000))]
    fn py_bootstrap_trade_rate(
        timestamps: Vec<i64>,
        window_ms: i64,
        min_trades: usize,
        compute_interval_ms: i64,
    ) -> PyResult<factors::TradeRateBootstrapResult> {
        Ok(factors::bootstrap_trade_rate(timestamps, window_ms, min_trades, compute_interval_ms))
    }

    // ── Quote factor batch computation ───────────────────────────────
    #[pymodule_export]
    use super::factors::QuoteFactorsResult;

    #[pyfunction]
    #[pyo3(name = "compute_quote_factors")]
    fn py_compute_quote_factors(
        quotes: Vec<(i64, f64, f64)>,
    ) -> factors::QuoteFactorsResult {
        factors::py_compute_quote_factors(quotes)
    }

    // ── Bar builder ─────────────────────────────────────────────────
    #[pymodule_export]
    use super::bars::BarBuilder;

    // ── Replay clock ────────────────────────────────────────────────
    #[pymodule_export]
    use super::clock::ReplayClock;
    #[pymodule_export]
    use super::clock::SharedTimeHandle;

    // ── Tick-data replayer ───────────────────────────────────────────
    #[pymodule_export]
    use super::replayer::TickDataReplayer;

    // ── Snapshot compute tracker ─────────────────────────────────────
    #[pymodule_export]
    use super::snapshot::VolumeTracker;

    // ── Unified Factor Engine ──────────────────────────────────────
    #[pymodule_export]
    use super::factor::PyFactorEngine;
    #[pymodule_export]
    use super::factor::PyBar;
    #[pymodule_export]
    use super::factor::PyTrade;

    // ── Parquet trade loader (for 10s bootstrap) ────────────────────
    #[pyfunction]
    #[pyo3(name = "load_trades_from_parquet")]
    #[pyo3(signature = (lake_data_dir, symbol, date_yyyymmdd, end_ts_ms=0, start_ts_ms=0))]
    fn py_load_trades_from_parquet(
        lake_data_dir: &str,
        symbol: &str,
        date_yyyymmdd: &str,
        end_ts_ms: i64,
        start_ts_ms: i64,
    ) -> PyResult<Vec<(i64, f64, i64)>> {
        replayer::loader::load_trades_from_parquet_sync(
            lake_data_dir,
            symbol,
            date_yyyymmdd,
            end_ts_ms,
            start_ts_ms,
        )
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    // ── Unified trade loader (CH first, Parquet fallback) ───────────────
    #[pyfunction]
    #[pyo3(name = "load_trades")]
    #[pyo3(signature = (lake_data_dir, symbol, date_yyyymmdd, end_ts_ms=0, start_ts_ms=0, clickhouse_url=None, clickhouse_user=None, clickhouse_password=None, clickhouse_database=None))]
    fn py_load_trades(
        lake_data_dir: &str,
        symbol: &str,
        date_yyyymmdd: &str,
        end_ts_ms: i64,
        start_ts_ms: i64,
        clickhouse_url: Option<&str>,
        clickhouse_user: Option<&str>,
        clickhouse_password: Option<&str>,
        clickhouse_database: Option<&str>,
    ) -> PyResult<Vec<(i64, f64, i64)>> {
        replayer::loader::load_trades_sync(
            lake_data_dir,
            symbol,
            date_yyyymmdd,
            end_ts_ms,
            start_ts_ms,
            clickhouse_url,
            clickhouse_user,
            clickhouse_password,
            clickhouse_database,
        )
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    // ── Parquet quote loader (for backtest) ─────────────────────────
    #[pyfunction]
    #[pyo3(name = "load_quotes_from_parquet")]
    #[pyo3(signature = (lake_data_dir, symbol, date_yyyymmdd, end_ts_ms=0, start_ts_ms=0))]
    fn py_load_quotes_from_parquet(
        lake_data_dir: &str,
        symbol: &str,
        date_yyyymmdd: &str,
        end_ts_ms: i64,
        start_ts_ms: i64,
    ) -> PyResult<Vec<(i64, f64, f64, i64, i64)>> {
        replayer::loader::load_quotes_from_parquet_sync(
            lake_data_dir,
            symbol,
            date_yyyymmdd,
            end_ts_ms,
            start_ts_ms,
        )
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }
}
