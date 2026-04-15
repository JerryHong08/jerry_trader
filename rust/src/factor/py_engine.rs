// rust/src/factor/py_engine.rs
//
// PyO3 wrapper for FactorEngine.
// Python shell calls this for factor state management and computation.

use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::data::Bar;
use super::factor_trait::{BarFactor, TickFactor};
use super::registry::FactorRegistry;

// ═══════════════════════════════════════════════════════════════════════════
// PyO3 Data Wrappers
// ═══════════════════════════════════════════════════════════════════════════

/// PyO3 Bar wrapper for passing bar data from Python.
#[pyclass]
#[derive(Debug, Clone)]
pub struct PyBar {
    #[pyo3(get, set)]
    pub ts_ms: i64,
    #[pyo3(get, set)]
    pub open: f64,
    #[pyo3(get, set)]
    pub high: f64,
    #[pyo3(get, set)]
    pub low: f64,
    #[pyo3(get, set)]
    pub close: f64,
    #[pyo3(get, set)]
    pub volume: i64,
}

impl From<PyBar> for Bar {
    fn from(py_bar: PyBar) -> Bar {
        Bar::new(
            py_bar.ts_ms,
            py_bar.open,
            py_bar.high,
            py_bar.low,
            py_bar.close,
            py_bar.volume,
        )
    }
}

impl From<&PyBar> for Bar {
    fn from(py_bar: &PyBar) -> Bar {
        Bar::new(
            py_bar.ts_ms,
            py_bar.open,
            py_bar.high,
            py_bar.low,
            py_bar.close,
            py_bar.volume,
        )
    }
}

#[pymethods]
impl PyBar {
    #[new]
    fn new(ts_ms: i64, open: f64, high: f64, low: f64, close: f64, volume: i64) -> Self {
        Self {
            ts_ms,
            open,
            high,
            low,
            close,
            volume,
        }
    }
}

/// PyO3 Trade wrapper for passing trade data from Python.
#[pyclass]
#[derive(Debug, Clone)]
pub struct PyTrade {
    #[pyo3(get, set)]
    pub ts_ms: i64,
    #[pyo3(get, set)]
    pub price: f64,
    #[pyo3(get, set)]
    pub size: i64,
}

#[pymethods]
impl PyTrade {
    #[new]
    fn new(ts_ms: i64, price: f64, size: i64) -> Self {
        Self { ts_ms, price, size }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// PyFactorEngine
// ═══════════════════════════════════════════════════════════════════════════

/// PyO3 FactorEngine wrapper.
///
/// Manages per-ticker factor instances and provides computation methods.
/// Python FactorEngine shell calls this for:
/// - create_factor: Create factor instance for a ticker
/// - update_bar: Incremental update (live mode)
/// - compute_batch_bar: Batch computation (backtest/warmup)
/// - reset_factor: Reset factor state
/// - is_ready: Check warmup status
#[pyclass]
pub struct PyFactorEngine {
    registry: FactorRegistry,
    // ticker → factor_name → factor instance
    bar_factors: Mutex<HashMap<String, HashMap<String, Arc<Mutex<dyn BarFactor>>>>>,
    tick_factors: Mutex<HashMap<String, HashMap<String, Arc<Mutex<dyn TickFactor>>>>>,
}

#[pymethods]
impl PyFactorEngine {
    #[new]
    fn new() -> Self {
        Self {
            registry: FactorRegistry::new(),
            bar_factors: Mutex::new(HashMap::new()),
            tick_factors: Mutex::new(HashMap::new()),
        }
    }

    /// Create a bar factor instance for a ticker.
    ///
    /// Args:
    ///     ticker: Ticker symbol (e.g., "AAPL")
    ///     factor_name: Factor name (e.g., "ema", "relative_volume")
    ///     params: Optional parameter dict (e.g., {"period": 20.0})
    ///
    /// Raises:
    ///     KeyError: If factor factory not found
    #[pyo3(signature = (ticker, factor_name, params=None))]
    fn create_bar_factor(
        &self,
        ticker: String,
        factor_name: String,
        params: Option<HashMap<String, f64>>,
    ) -> PyResult<()> {
        let factory = self.registry.get_bar_factory(&factor_name).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Factor '{}' not found in registry. Available: {}",
                factor_name,
                self.registry.bar_factor_names().join(", ")
            ))
        })?;

        let params = params.unwrap_or_default();
        let factor = factory.create(&params);

        let mut factors = self.bar_factors.lock().unwrap();
        factors
            .entry(ticker)
            .or_insert_with(HashMap::new)
            .insert(factor_name, factor);

        Ok(())
    }

    /// Update a bar factor with a new bar (incremental mode).
    ///
    /// Args:
    ///     ticker: Ticker symbol
    ///     factor_name: Factor name
    ///     bar: PyBar instance
    ///
    /// Returns:
    ///     Optional[float]: Factor value if ready, None if warmup incomplete
    fn update_bar(&self, ticker: String, factor_name: String, bar: PyBar) -> Option<f64> {
        let factors = self.bar_factors.lock().unwrap();
        let ticker_factors = factors.get(&ticker)?;
        let factor = ticker_factors.get(&factor_name)?;

        let mut factor_guard = factor.lock().unwrap();
        factor_guard.update(&Bar::from(&bar))
    }

    /// Batch compute factor values (backtest/warmup mode).
    ///
    /// Args:
    ///     factor_name: Factor name
    ///     bars: List of PyBar instances
    ///     params: Optional parameter dict
    ///
    /// Returns:
    ///     List[Optional[float]]: Factor values for each bar, None during warmup
    #[pyo3(signature = (factor_name, bars, params=None))]
    fn compute_batch_bar(
        &self,
        factor_name: String,
        bars: Vec<PyBar>,
        params: Option<HashMap<String, f64>>,
    ) -> PyResult<Vec<Option<f64>>> {
        let factory = self.registry.get_bar_factory(&factor_name).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Factor '{}' not found",
                factor_name
            ))
        })?;

        let params = params.unwrap_or_default();
        let factor = factory.create(&params);

        let rust_bars: Vec<Bar> = bars.iter().map(|b| Bar::from(b)).collect();
        let factor_guard = factor.lock().unwrap();
        Ok(factor_guard.compute_batch(&rust_bars))
    }

    /// Reset a factor's state for a ticker.
    ///
    /// Args:
    ///     ticker: Ticker symbol
    ///     factor_name: Factor name
    ///
    /// Raises:
    ///     KeyError: If ticker or factor not found
    fn reset_factor(&self, ticker: String, factor_name: String) -> PyResult<()> {
        let factors = self.bar_factors.lock().unwrap();
        let ticker_factors = factors
            .get(&ticker)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>(ticker))?;
        let factor = ticker_factors
            .get(&factor_name)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>(factor_name))?;

        factor.lock().unwrap().reset();
        Ok(())
    }

    /// Check if a factor is ready (warmup complete).
    ///
    /// Args:
    ///     ticker: Ticker symbol
    ///     factor_name: Factor name
    ///
    /// Returns:
    ///     bool: True if ready, False otherwise
    fn is_ready(&self, ticker: String, factor_name: String) -> bool {
        let factors = self.bar_factors.lock().unwrap();
        if let Some(ticker_factors) = factors.get(&ticker) {
            if let Some(factor) = ticker_factors.get(&factor_name) {
                return factor.lock().unwrap().is_ready();
            }
        }
        false
    }

    /// Remove all factors for a ticker.
    ///
    /// Args:
    ///     ticker: Ticker symbol
    fn remove_ticker(&self, ticker: String) {
        let mut bar_factors = self.bar_factors.lock().unwrap();
        bar_factors.remove(&ticker);

        let mut tick_factors = self.tick_factors.lock().unwrap();
        tick_factors.remove(&ticker);
    }

    /// List all available bar factor names.
    fn list_bar_factors(&self) -> Vec<String> {
        self.registry
            .bar_factor_names()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    /// List all available tick factor names.
    fn list_tick_factors(&self) -> Vec<String> {
        self.registry
            .tick_factor_names()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_py_bar_conversion() {
        let py_bar = PyBar::new(1000, 10.0, 11.0, 9.0, 10.5, 100);
        let bar = Bar::from(&py_bar);

        assert_eq!(bar.ts_ms, 1000);
        assert_eq!(bar.open, 10.0);
        assert_eq!(bar.close, 10.5);
        assert_eq!(bar.volume, 100);
    }

    #[test]
    fn test_py_factor_engine_creation() {
        let engine = PyFactorEngine::new();
        assert!(engine.list_bar_factors().is_empty());
        assert!(engine.list_tick_factors().is_empty());
    }
}
