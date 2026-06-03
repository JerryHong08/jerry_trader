use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

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

    // ── Forward-fill for batch backtest ──────────────────────────────
    #[pymodule_export]
    use super::factors::ForwardFillResult;

    #[pyfunction]
    #[pyo3(name = "forward_fill_factors")]
    fn py_forward_fill_factors(
        ts: Vec<(i64, std::collections::HashMap<String, f64>)>,
    ) -> factors::ForwardFillResult {
        factors::py_forward_fill_factors(ts)
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

    // ── Unified quote loader (CH first, Parquet fallback) ────────────
    #[pyfunction]
    #[pyo3(name = "load_quotes")]
    #[pyo3(signature = (lake_data_dir, symbol, date_yyyymmdd, end_ts_ms=0, start_ts_ms=0, clickhouse_url=None, clickhouse_user=None, clickhouse_password=None, clickhouse_database=None))]
    fn py_load_quotes(
        lake_data_dir: &str,
        symbol: &str,
        date_yyyymmdd: &str,
        end_ts_ms: i64,
        start_ts_ms: i64,
        clickhouse_url: Option<&str>,
        clickhouse_user: Option<&str>,
        clickhouse_password: Option<&str>,
        clickhouse_database: Option<&str>,
    ) -> PyResult<Vec<(i64, f64, f64, i64, i64)>> {
        replayer::loader::load_quotes_sync(
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

    // ═══════════════════════════════════════════════════════════════════════════
    // Batch Factor Compute (B1 Optimization)
    // ═══════════════════════════════════════════════════════════════════════════

    /// Batch compute bar factors.
    ///
    /// Args:
    ///   bars: List of (ts_ms, open, high, low, close, volume) tuples
    ///   configs: List of dicts with keys: factor_type, name, params (dict)
    ///
    /// Returns: List of (ts_ms, [(factor_name, value)]) tuples
    #[pyfunction]
    #[pyo3(name = "compute_bar_factors_batch")]
    fn py_compute_bar_factors_batch(
        bars: Vec<(i64, f64, f64, f64, f64, i64)>,
        configs: Vec<std::collections::HashMap<String, f64>>,
        factor_types: Vec<String>,
        names: Vec<String>,
    ) -> PyResult<Vec<(i64, Vec<(String, Option<f64>)>)>> {
        // Convert to Bar structs
        let rust_bars: Vec<factor::Bar> = bars
            .iter()
            .map(|(ts_ms, open, high, low, close, volume)| {
                factor::Bar::new(*ts_ms, *open, *high, *low, *close, *volume)
            })
            .collect();

        // Build configs
        let rust_configs: Vec<factor::BarFactorConfig> = factor_types
            .iter()
            .zip(names.iter())
            .zip(configs.iter())
            .map(|((ft, name), params)| factor::BarFactorConfig {
                factor_type: ft.clone(),
                name: name.clone(),
                params: params.clone(),
            })
            .collect();

        Ok(factor::compute_all_bar_factors(&rust_bars, &rust_configs))
    }

    /// Batch compute quote factors (optimized O(1) per compute).
    ///
    /// Args:
    ///   quotes: List of (ts_ms, bid, ask, bid_size, ask_size) tuples
    ///   configs: List of dicts with keys: factor_type, name, params (dict)
    ///
    /// Returns: List of (ts_ms, [(factor_name, value)]) tuples
    #[pyfunction]
    #[pyo3(name = "compute_quote_factors_batch")]
    fn py_compute_quote_factors_batch(
        quotes: Vec<(i64, f64, f64, i64, i64)>,
        configs: Vec<std::collections::HashMap<String, f64>>,
        factor_types: Vec<String>,
        names: Vec<String>,
    ) -> PyResult<Vec<(i64, Vec<(String, Option<f64>)>)>> {
        // Convert to Quote structs
        let rust_quotes: Vec<factor::Quote> = quotes
            .iter()
            .map(|(ts_ms, bid, ask, bid_size, ask_size)| {
                factor::Quote::new(*ts_ms, *bid, *ask, *bid_size, *ask_size)
            })
            .collect();

        // Build configs
        let rust_configs: Vec<factor::QuoteFactorConfig> = factor_types
            .iter()
            .zip(names.iter())
            .zip(configs.iter())
            .map(|((ft, name), params)| factor::QuoteFactorConfig {
                factor_type: ft.clone(),
                name: name.clone(),
                params: params.clone(),
            })
            .collect();

        Ok(factor::compute_all_quote_factors(&rust_quotes, &rust_configs))
    }

    /// Batch compute signal exit statistics.
    ///
    /// Args:
    ///   trades: List of (ts_ms, price) tuples - MUST be sorted by ts_ms
    ///   signals: List of (entry_time_ms, hold_duration_ms) tuples
    ///
    /// Returns: List of (entry_price, exit_price, max_price, min_price,
    ///                   time_to_max_ms, time_to_min_ms, return_pct) tuples
    ///          or None if no entry_price found
    #[pyfunction]
    #[pyo3(name = "compute_signal_exits_batch")]
    fn py_compute_signal_exits_batch(
        trades: Vec<(i64, f64)>,
        signals: Vec<(i64, i64)>,
    ) -> PyResult<Vec<Option<(f64, f64, f64, f64, i64, i64, f64)>>> {
        let results = factor::compute_signal_exits_batch(&trades, &signals);

        // Convert SignalExitResult to Python-friendly tuple
        let py_results: Vec<Option<(f64, f64, f64, f64, i64, i64, f64)>> = results
            .iter()
            .map(|r| {
                r.as_ref().map(|r| {
                    (
                        r.entry_price,
                        r.exit_price,
                        r.max_price,
                        r.min_price,
                        r.time_to_max_ms,
                        r.time_to_min_ms,
                        r.return_pct,
                    )
                })
            })
            .collect();

        Ok(py_results)
    }
}
