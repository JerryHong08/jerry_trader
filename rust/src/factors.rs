// rust/src/factors.rs
//
// Pure computation functions for the factor engine.
// Python equivalents live in jerry_trader.core.factors.compute.
//
// Exposed to Python via the `_rust` module in lib.rs.

use pyo3::prelude::*;
use pyo3::types::PyList;

/// Return the z-score of `value` relative to `history`, or `None` if < 2 samples.
///
/// z = (value - mean) / std   (population std)
///
/// Python signature:  z_score(value: float, history: list[float]) -> float | None
#[pyfunction]
pub fn z_score(value: f64, history: Vec<f64>) -> Option<f64> {
    let n = history.len();
    if n < 2 {
        return None;
    }

    let n_f = n as f64;
    let mean = history.iter().sum::<f64>() / n_f;
    let var = history.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n_f;
    let std = var.sqrt();

    if std < 1e-9 {
        return Some(0.0);
    }

    Some((value - mean) / std)
}

/// Direction-aware price acceleration.
///
/// Computes the return-rate (fractional return per second) in each half-window,
/// then returns the difference:  accel = rate_recent - rate_older.
///
///   Positive → price accelerating upward
///   Negative → price accelerating downward
///
/// Each input is a list of `[timestamp_ms, price]` pairs.
///
/// Only reads the first and last elements — avoids converting the entire list.
///
/// Python signature:
///   price_accel(recent: list[tuple[int, float]], older: list[tuple[int, float]]) -> float
#[pyfunction]
pub fn price_accel(
    recent: &Bound<'_, PyList>,
    older: &Bound<'_, PyList>,
) -> PyResult<f64> {
    Ok(return_rate_py(recent)? - return_rate_py(older)?)
}

/// Extract first/last (ts_ms, price) from a Python list without full conversion.
fn return_rate_py(trades: &Bound<'_, PyList>) -> PyResult<f64> {
    let n = trades.len();
    if n < 2 {
        return Ok(0.0);
    }

    let first: (i64, f64) = trades.get_item(0)?.extract()?;
    let last: (i64, f64) = trades.get_item(n - 1)?.extract()?;

    let dt_sec = (last.0 - first.0) as f64 / 1000.0;
    if dt_sec <= 0.0 || first.1 <= 0.0 {
        return Ok(0.0);
    }

    Ok(((last.1 / first.1) - 1.0) / dt_sec)
}

/// Compute fractional return per second for a sorted slice of (ts_ms, price).
/// Used by Rust-only tests.
#[allow(dead_code)]
fn return_rate(trades: &[(i64, f64)]) -> f64 {
    if trades.len() < 2 {
        return 0.0;
    }

    let first_price = trades[0].1;
    let last_price = trades[trades.len() - 1].1;
    let dt_sec = (trades[trades.len() - 1].0 - trades[0].0) as f64 / 1000.0;

    if dt_sec <= 0.0 || first_price <= 0.0 {
        return 0.0;
    }

    ((last_price / first_price) - 1.0) / dt_sec
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_z_score_insufficient() {
        assert_eq!(z_score(5.0, vec![]), None);
        assert_eq!(z_score(5.0, vec![3.0]), None);
    }

    #[test]
    fn test_z_score_two_samples() {
        // history=[4, 6], mean=5, var=1, std=1 → z(7)=2.0
        let result = z_score(7.0, vec![4.0, 6.0]);
        assert!((result.unwrap() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn test_z_score_zero_std() {
        let result = z_score(5.0, vec![5.0, 5.0, 5.0]);
        assert_eq!(result, Some(0.0));
    }

    #[test]
    fn test_z_score_known_values() {
        // history=[2,4,6,8,10], mean=6, var=8, std=sqrt(8)≈2.828
        // z(12) = (12-6)/2.828 ≈ 2.121
        let history = vec![2.0, 4.0, 6.0, 8.0, 10.0];
        let result = z_score(12.0, history).unwrap();
        let expected = (12.0 - 6.0) / 8.0_f64.sqrt();
        assert!((result - expected).abs() < 1e-9);
    }

    #[test]
    fn test_price_accel_empty() {
        // Both empty → both return_rate = 0 → accel = 0
        assert_eq!(return_rate(&[]) - return_rate(&[]), 0.0);
    }

    #[test]
    fn test_price_accel_accelerating() {
        // older: 100→101 in 1s, recent: 101→104 in 1s
        let older = vec![(0_i64, 100.0), (1000, 101.0)];
        let recent = vec![(1000_i64, 101.0), (2000, 104.0)];
        let result = return_rate(&recent) - return_rate(&older);
        assert!(result > 0.0, "Expected positive accel, got {result}");
    }

    #[test]
    fn test_price_accel_decelerating() {
        let older = vec![(0_i64, 100.0), (1000, 105.0)];
        let recent = vec![(1000_i64, 105.0), (2000, 106.0)];
        let result = return_rate(&recent) - return_rate(&older);
        assert!(result < 0.0, "Expected negative accel, got {result}");
    }

    #[test]
    fn test_price_accel_constant_speed() {
        // Same % return in both halves → accel ≈ 0
        let older = vec![(0_i64, 100.0), (1000, 101.0)];
        let recent = vec![(1000_i64, 101.0), (2000, 102.01)];
        let result = return_rate(&recent) - return_rate(&older);
        assert!(result.abs() < 1e-6, "Expected ~0, got {result}");
    }
}
