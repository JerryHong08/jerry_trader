use pyo3::prelude::*;

mod factors;
mod bars;
mod clock;
mod replayer;

/// The Rust computation core, exposed to Python as `jerry_trader._rust`.
#[pymodule]
mod _rust {
    use super::*;

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

    // ── Bar builder ─────────────────────────────────────────────────
    #[pymodule_export]
    use super::bars::BarBuilder;

    // ── Replay clock ────────────────────────────────────────────────
    #[pymodule_export]
    use super::clock::ReplayClock;

    // ── Tick-data replayer ───────────────────────────────────────────
    #[pymodule_export]
    use super::replayer::TickDataReplayer;

    // ── Parquet trade loader (for 10s bootstrap) ────────────────────
    #[pyfunction]
    #[pyo3(name = "load_trades_from_parquet")]
    #[pyo3(signature = (lake_data_dir, symbol, date_yyyymmdd, end_ts_ms=0))]
    fn py_load_trades_from_parquet(
        lake_data_dir: &str,
        symbol: &str,
        date_yyyymmdd: &str,
        end_ts_ms: i64,
    ) -> PyResult<Vec<(i64, f64, i64)>> {
        replayer::loader::load_trades_from_parquet_sync(
            lake_data_dir,
            symbol,
            date_yyyymmdd,
            end_ts_ms,
        )
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }
}
