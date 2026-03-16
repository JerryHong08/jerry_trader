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

// ═══════════════════════════════════════════════════════════════════════════
// Bar-Based Factor Functions
// ═══════════════════════════════════════════════════════════════════════════

/// Compute price momentum over N bars.
///
/// momentum = (close_now - close_N_bars_ago) / close_N_bars_ago
///
/// Python signature: momentum(closes: list[float], window: int) -> float
#[pyfunction]
pub fn momentum(closes: Vec<f64>, window: usize) -> f64 {
    if closes.len() < window + 1 || window == 0 {
        return 0.0;
    }
    let recent = closes[closes.len() - 1];
    let old = closes[closes.len() - 1 - window];
    if old <= 0.0 {
        return 0.0;
    }
    (recent - old) / old
}

/// Compute volatility (standard deviation of returns).
///
/// Python signature: volatility(closes: list[float], window: int) -> float
#[pyfunction]
pub fn volatility(closes: Vec<f64>, window: usize) -> f64 {
    if closes.len() < window + 1 || window < 2 {
        return 0.0;
    }

    let recent_closes: Vec<f64> = closes.iter().rev().take(window + 1).copied().collect();
    let returns: Vec<f64> = (1..recent_closes.len())
        .map(|i| {
            let prev = recent_closes[i - 1];
            let curr = recent_closes[i];
            if prev > 0.0 {
                (curr - prev) / prev
            } else {
                0.0
            }
        })
        .collect();

    if returns.is_empty() {
        return 0.0;
    }

    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
    variance.sqrt()
}

/// Compute Relative Strength Index (RSI).
///
/// RSI = 100 - (100 / (1 + RS)) where RS = avg_gain / avg_loss
///
/// Python signature: rsi(closes: list[float], window: int) -> float
#[pyfunction]
pub fn rsi(closes: Vec<f64>, window: usize) -> f64 {
    if closes.len() < window + 1 || window < 2 {
        return 50.0; // Neutral RSI
    }

    let recent_closes: Vec<f64> = closes.iter().rev().take(window + 1).copied().collect();
    let mut gains = 0.0;
    let mut losses = 0.0;

    for i in 1..recent_closes.len() {
        let change = recent_closes[i] - recent_closes[i - 1];
        if change > 0.0 {
            gains += change;
        } else {
            losses += -change;
        }
    }

    let avg_gain = gains / window as f64;
    let avg_loss = losses / window as f64;

    if avg_loss == 0.0 {
        return if avg_gain > 0.0 { 100.0 } else { 50.0 };
    }

    let rs = avg_gain / avg_loss;
    100.0 - (100.0 / (1.0 + rs))
}

/// Compute VWAP deviation: (price - vwap) / vwap.
///
/// bars: Vec<(price, volume)>
///
/// Python signature: vwap_deviation(bars: list[tuple[float, int]], window: int) -> float
#[pyfunction]
pub fn vwap_deviation(bars: Vec<(f64, i64)>, window: usize) -> f64 {
    if bars.len() < window || window == 0 {
        return 0.0;
    }

    let recent_bars: Vec<(f64, i64)> = bars.iter().rev().take(window).copied().collect();
    let total_volume: i64 = recent_bars.iter().map(|(_, v)| v).sum();

    if total_volume == 0 {
        return 0.0;
    }

    let vwap = recent_bars.iter()
        .map(|(p, v)| p * (*v as f64))
        .sum::<f64>() / total_volume as f64;

    let current_price = recent_bars[0].0;
    (current_price - vwap) / vwap
}

/// Compute volume ratio: current_volume / average_volume.
///
/// Python signature: volume_ratio(volumes: list[int], window: int) -> float
#[pyfunction]
pub fn volume_ratio(volumes: Vec<i64>, window: usize) -> f64 {
    if volumes.is_empty() || window == 0 {
        return 1.0;
    }

    let recent_volumes: Vec<i64> = volumes.iter().rev().take(window + 1).copied().collect();
    if recent_volumes.is_empty() {
        return 1.0;
    }

    let current_vol = recent_volumes[0] as f64;
    let avg_vol = recent_volumes.iter().skip(1).map(|&v| v as f64).sum::<f64>() / window as f64;

    if avg_vol <= 0.0 {
        return 1.0;
    }

    current_vol / avg_vol
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
