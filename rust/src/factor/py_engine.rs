// rust/src/factor/py_engine.rs
//
// PyO3 wrapper for FactorEngine.
// Python shell calls this for factor state management and computation.

use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::data::{Bar, Quote, Trade};
use super::factor_trait::{BarFactor, QuoteFactor, TradeFactor};
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
    trade_factors: Mutex<HashMap<String, HashMap<String, Arc<Mutex<dyn TradeFactor>>>>>,
    quote_factors: Mutex<HashMap<String, HashMap<String, Arc<Mutex<dyn QuoteFactor>>>>>,
}

#[pymethods]
impl PyFactorEngine {
    #[new]
    fn new() -> Self {
        Self {
            registry: FactorRegistry::new(),
            bar_factors: Mutex::new(HashMap::new()),
            trade_factors: Mutex::new(HashMap::new()),
            quote_factors: Mutex::new(HashMap::new()),
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

    /// Create a trade factor instance for a ticker.
    ///
    /// Args:
    ///     ticker: Ticker symbol (e.g., "AAPL")
    ///     factor_name: Factor name (e.g., "trade_rate")
    ///     params: Optional parameter dict (e.g., {"window_ms": 20000.0, "min_trades": 5.0})
    ///
    /// Raises:
    ///     KeyError: If factor factory not found
    #[pyo3(signature = (ticker, factor_name, params=None))]
    fn create_trade_factor(
        &self,
        ticker: String,
        factor_name: String,
        params: Option<HashMap<String, f64>>,
    ) -> PyResult<()> {
        let factory = self.registry.get_trade_factory(&factor_name).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Trade factor '{}' not found in registry. Available: {}",
                factor_name,
                self.registry.trade_factor_names().join(", ")
            ))
        })?;

        let params = params.unwrap_or_default();
        let factor = factory.create(&params);

        let mut factors = self.trade_factors.lock().unwrap();
        factors
            .entry(ticker)
            .or_insert_with(HashMap::new)
            .insert(factor_name, factor);

        Ok(())
    }

    /// Create a quote factor instance for a ticker.
    ///
    /// Args:
    ///     ticker: Ticker symbol (e.g., "AAPL")
    ///     factor_name: Factor name (e.g., "bid_ask_spread")
    ///     params: Optional parameter dict (e.g., {"window_ms": 5000.0})
    ///
    /// Raises:
    ///     KeyError: If factor factory not found
    #[pyo3(signature = (ticker, factor_name, params=None))]
    fn create_quote_factor(
        &self,
        ticker: String,
        factor_name: String,
        params: Option<HashMap<String, f64>>,
    ) -> PyResult<()> {
        let factory = self.registry.get_quote_factory(&factor_name).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Quote factor '{}' not found in registry. Available: {}",
                factor_name,
                self.registry.quote_factor_names().join(", ")
            ))
        })?;

        let params = params.unwrap_or_default();
        let factor = factory.create(&params);

        let mut factors = self.quote_factors.lock().unwrap();
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

    /// Process a trade tick, updating the trade factor and returning current value.
    ///
    /// Args:
    ///     ticker: Ticker symbol
    ///     factor_name: Factor name (e.g., "trade_rate")
    ///     ts_ms: Trade timestamp in milliseconds
    ///     price: Trade price
    ///     size: Trade size
    ///
    /// Returns:
    ///     Optional[float]: Factor value if ready, None if warmup incomplete
    fn on_trade(
        &self,
        ticker: String,
        factor_name: String,
        ts_ms: i64,
        price: f64,
        size: i64,
    ) -> Option<f64> {
        let factors = self.trade_factors.lock().unwrap();
        let ticker_factors = factors.get(&ticker)?;
        let factor = ticker_factors.get(&factor_name)?;

        let trade = Trade::new(ts_ms, price, size);
        let mut factor_guard = factor.lock().unwrap();
        factor_guard.on_trade(&trade)
    }

    /// Process a quote tick, updating the quote factor and returning current value.
    ///
    /// Args:
    ///     ticker: Ticker symbol
    ///     factor_name: Factor name (e.g., "bid_ask_spread")
    ///     ts_ms: Quote timestamp in milliseconds
    ///     bid: Bid price
    ///     ask: Ask price
    ///     bid_size: Bid size
    ///     ask_size: Ask size
    ///
    /// Returns:
    ///     Optional[float]: Factor value if ready, None if warmup incomplete
    fn on_quote(
        &self,
        ticker: String,
        factor_name: String,
        ts_ms: i64,
        bid: f64,
        ask: f64,
        bid_size: i64,
        ask_size: i64,
    ) -> Option<f64> {
        let factors = self.quote_factors.lock().unwrap();
        let ticker_factors = factors.get(&ticker)?;
        let factor = ticker_factors.get(&factor_name)?;

        let quote = Quote::new(ts_ms, bid, ask, bid_size, ask_size);
        let mut factor_guard = factor.lock().unwrap();
        factor_guard.on_quote(&quote)
    }

    /// Read current factor value without ingesting new data.
    ///
    /// Args:
    ///     ticker: Ticker symbol
    ///     factor_name: Factor name
    ///     factor_type: "bar", "trade", or "quote"
    ///
    /// Returns:
    ///     Optional[float]: Current factor value, None if not ready or not found
    fn get_value(&self, ticker: String, factor_name: String, factor_type: String) -> Option<f64> {
        match factor_type.as_str() {
            "bar" => {
                let factors = self.bar_factors.lock().unwrap();
                let ticker_factors = factors.get(&ticker)?;
                let factor = ticker_factors.get(&factor_name)?;
                factor.lock().unwrap().get_value()
            }
            "trade" => {
                let factors = self.trade_factors.lock().unwrap();
                let ticker_factors = factors.get(&ticker)?;
                let factor = ticker_factors.get(&factor_name)?;
                factor.lock().unwrap().get_value()
            }
            "quote" => {
                let factors = self.quote_factors.lock().unwrap();
                let ticker_factors = factors.get(&ticker)?;
                let factor = ticker_factors.get(&factor_name)?;
                factor.lock().unwrap().get_value()
            }
            _ => None,
        }
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

    /// Batch compute trade factor values.
    ///
    /// Args:
    ///     factor_name: Factor name (e.g., "trade_rate")
    ///     trades: List of PyTrade instances (ts_ms, price, size)
    ///     compute_ts: List of compute timestamps in ms (uniform 1s grid)
    ///     params: Optional parameter dict (e.g., {"window_ms": 20000.0, "min_trades": 5.0})
    ///
    /// Returns:
    ///     List[Optional[float]]: Factor value at each compute_ts, None if not ready
    #[pyo3(signature = (factor_name, trades, compute_ts, params=None))]
    fn compute_batch_trade(
        &self,
        factor_name: String,
        trades: Vec<PyTrade>,
        compute_ts: Vec<i64>,
        params: Option<HashMap<String, f64>>,
    ) -> PyResult<Vec<Option<f64>>> {
        let factory = self.registry.get_trade_factory(&factor_name).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Trade factor '{}' not found. Available: {}",
                factor_name,
                self.registry.trade_factor_names().join(", ")
            ))
        })?;

        let params = params.unwrap_or_default();
        let factor = factory.create(&params);

        let rust_trades: Vec<Trade> = trades
            .iter()
            .map(|t| Trade::new(t.ts_ms, t.price, t.size))
            .collect();

        let factor_guard = factor.lock().unwrap();
        Ok(factor_guard.compute_batch(&rust_trades, &compute_ts))
    }

    /// Batch compute quote factor values with timestamps.
    ///
    /// Delegates directly to QuoteFactor::compute_batch() — same true-batch
    /// path as BarFactor, not an on_quote() loop.
    ///
    /// Args:
    ///     factor_name: Factor name (e.g., "bid_ask_spread", "order_imbalance", "quote_rate")
    ///     quotes: List of (ts_ms, bid, ask, bid_size, ask_size) tuples
    ///     params: Optional parameter dict (e.g., {"window_ms": 5000.0})
    ///
    /// Returns:
    ///     List[(int, Optional[float])]: (ts_ms, value) pairs at 1s compute intervals
    #[pyo3(signature = (factor_name, quotes, params=None))]
    fn compute_batch_quote(
        &self,
        factor_name: String,
        quotes: Vec<(i64, f64, f64, i64, i64)>,
        params: Option<HashMap<String, f64>>,
    ) -> PyResult<Vec<(i64, Option<f64>)>> {
        let factory = self.registry.get_quote_factory(&factor_name).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Quote factor '{}' not found. Available: {}",
                factor_name,
                self.registry.quote_factor_names().join(", ")
            ))
        })?;

        let params = params.unwrap_or_default();
        let factor = factory.create(&params);

        let rust_quotes: Vec<Quote> = quotes
            .iter()
            .map(|(ts_ms, bid, ask, bid_size, ask_size)| {
                Quote::new(*ts_ms, *bid, *ask, *bid_size, *ask_size)
            })
            .collect();

        let factor_guard = factor.lock().unwrap();
        Ok(factor_guard.compute_batch(&rust_quotes))
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
        // Search bar factors
        {
            let factors = self.bar_factors.lock().unwrap();
            if let Some(ticker_factors) = factors.get(&ticker) {
                if let Some(factor) = ticker_factors.get(&factor_name) {
                    factor.lock().unwrap().reset();
                    return Ok(());
                }
            }
        }
        // Search trade factors
        {
            let factors = self.trade_factors.lock().unwrap();
            if let Some(ticker_factors) = factors.get(&ticker) {
                if let Some(factor) = ticker_factors.get(&factor_name) {
                    factor.lock().unwrap().reset();
                    return Ok(());
                }
            }
        }
        // Search quote factors
        {
            let factors = self.quote_factors.lock().unwrap();
            if let Some(ticker_factors) = factors.get(&ticker) {
                if let Some(factor) = ticker_factors.get(&factor_name) {
                    factor.lock().unwrap().reset();
                    return Ok(());
                }
            }
        }
        Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
            "Factor '{}' not found for ticker '{}'",
            factor_name, ticker
        )))
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
        // Check bar factors
        {
            let factors = self.bar_factors.lock().unwrap();
            if let Some(ticker_factors) = factors.get(&ticker) {
                if let Some(factor) = ticker_factors.get(&factor_name) {
                    return factor.lock().unwrap().is_ready();
                }
            }
        }
        // Check trade factors
        {
            let factors = self.trade_factors.lock().unwrap();
            if let Some(ticker_factors) = factors.get(&ticker) {
                if let Some(factor) = ticker_factors.get(&factor_name) {
                    return factor.lock().unwrap().is_ready();
                }
            }
        }
        // Check quote factors
        {
            let factors = self.quote_factors.lock().unwrap();
            if let Some(ticker_factors) = factors.get(&ticker) {
                if let Some(factor) = ticker_factors.get(&factor_name) {
                    return factor.lock().unwrap().is_ready();
                }
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

        let mut trade_factors = self.trade_factors.lock().unwrap();
        trade_factors.remove(&ticker);

        let mut quote_factors = self.quote_factors.lock().unwrap();
        quote_factors.remove(&ticker);
    }

    /// List all available bar factor names.
    fn list_bar_factors(&self) -> Vec<String> {
        self.registry
            .bar_factor_names()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    /// List all available trade factor names.
    fn list_trade_factors(&self) -> Vec<String> {
        self.registry
            .trade_factor_names()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    /// List all available quote factor names.
    fn list_quote_factors(&self) -> Vec<String> {
        self.registry
            .quote_factor_names()
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
        // Registry is now populated with real factors
        assert!(!engine.list_bar_factors().is_empty());
        assert!(!engine.list_trade_factors().is_empty());
        assert!(!engine.list_quote_factors().is_empty());
        assert!(engine.list_bar_factors().contains(&"ema".to_string()));
        assert!(engine.list_trade_factors().contains(&"trade_rate".to_string()));
    }
}
