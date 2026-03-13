use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};
use std::collections::HashMap;

const MINUTE_MS: i64 = 60_000;

#[pyclass]
pub struct VolumeTracker {
    // ticker -> [(timestamp_ms, cumulative_volume)]
    history: HashMap<String, Vec<(i64, f64)>>,
}

#[pymethods]
impl VolumeTracker {
    #[new]
    fn new() -> Self {
        Self {
            history: HashMap::new(),
        }
    }

    #[getter]
    fn history<'py>(&self, py: Python<'py>) -> PyResult<Py<PyDict>> {
        let out = PyDict::new(py);
        for (ticker, entries) in &self.history {
            let py_list = PyList::empty(py);
            for (ts_ms, vol) in entries {
                py_list.append((*ts_ms, *vol))?;
            }
            out.set_item(ticker, py_list)?;
        }
        Ok(out.unbind())
    }

    fn reload_history(&mut self, ticker: String, entries: &Bound<'_, PyAny>) -> PyResult<()> {
        let list = entries.downcast::<PyList>()?;
        let slot = self.history.entry(ticker).or_default();

        for item in list.iter() {
            let tup = item.extract::<(Bound<'_, PyAny>, f64)>()?;
            let ts_ms = timestamp_ms_from_py(&tup.0)?;
            slot.push((ts_ms, tup.1));
        }

        Ok(())
    }

    fn compute_relative_volume_5min(
        &mut self,
        ticker: String,
        timestamp: &Bound<'_, PyAny>,
        volume: f64,
    ) -> PyResult<f64> {
        let ts_ms = timestamp_ms_from_py(timestamp)?;
        Ok(compute_one(&mut self.history, &ticker, ts_ms, volume))
    }

    fn compute_batch(
        &mut self,
        tickers: Vec<String>,
        timestamps: &Bound<'_, PyList>,
        volumes: Vec<f64>,
    ) -> PyResult<Vec<f64>> {
        let n = tickers.len();
        if timestamps.len() != n || volumes.len() != n {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "tickers, timestamps, and volumes must have the same length",
            ));
        }

        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let ticker = &tickers[i];
            let ts_obj = timestamps.get_item(i)?;
            let ts_ms = timestamp_ms_from_py(&ts_obj)?;
            let vol = volumes[i];
            out.push(compute_one(&mut self.history, ticker, ts_ms, vol));
        }
        Ok(out)
    }
}

fn compute_one(
    history_map: &mut HashMap<String, Vec<(i64, f64)>>,
    ticker: &str,
    timestamp_ms: i64,
    volume: f64,
) -> f64 {
    let history = history_map.entry(ticker.to_string()).or_default();
    history.push((timestamp_ms, volume));

    // Keep only last 6 minutes
    let cutoff_ms = timestamp_ms - 6 * MINUTE_MS;
    history.retain(|(ts, _)| *ts >= cutoff_ms);

    if history.len() < 2 {
        return 1.0;
    }

    // last_1min_volume = volume_now - volume_at_or_before(now-1m)
    let one_min_ago = timestamp_ms - MINUTE_MS;
    let mut volume_1min_ago: Option<f64> = None;
    for (ts, vol) in history.iter().rev() {
        if *ts <= one_min_ago {
            volume_1min_ago = Some(*vol);
            break;
        }
    }
    let v1_ref = volume_1min_ago.unwrap_or(history[0].1);
    let last_1min_volume = (volume - v1_ref).max(0.0);

    // last_5min_avg_volume
    let five_min_ago = timestamp_ms - 5 * MINUTE_MS;
    let mut volume_5min_ago: Option<f64> = None;
    for (ts, vol) in history.iter() {
        if *ts <= five_min_ago {
            volume_5min_ago = Some(*vol);
        } else {
            break;
        }
    }

    let (v5_ref, span_minutes) = if let Some(v) = volume_5min_ago {
        (v, 5.0)
    } else {
        let earliest_ts = history[0].0;
        let span = ((timestamp_ms - earliest_ts) as f64 / MINUTE_MS as f64).max(1.0);
        (history[0].1, span)
    };

    let last_5min_total_volume = (volume - v5_ref).max(0.0);
    let last_5min_avg_volume = last_5min_total_volume / span_minutes;

    if last_5min_avg_volume > 0.0 {
        last_1min_volume / last_5min_avg_volume
    } else {
        1.0
    }
}

fn timestamp_ms_from_py(obj: &Bound<'_, PyAny>) -> PyResult<i64> {
    if let Ok(ms) = obj.extract::<i64>() {
        return Ok(ms);
    }

    let ts_seconds = obj.call_method0("timestamp")?.extract::<f64>()?;
    Ok((ts_seconds * 1000.0) as i64)
}
