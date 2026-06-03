// rust/src/factor/batch_compute.rs
//
// Batch factor computation for backtest.
// Optimized implementations that avoid Python loop overhead.
//
// Key optimization: use running sum + sliding window instead of
// iterating over entire deque for each compute call.

use std::collections::VecDeque;

use super::data::{Bar, Quote};

// ═══════════════════════════════════════════════════════════════════════════
// Result Types
// ═══════════════════════════════════════════════════════════════════════════

/// Result of batch bar factor computation.
///
/// Each tuple contains (ts_ms, factor_name, value).
/// ts_ms corresponds to bar close time.
pub struct BarFactorResult {
    pub ts_ms: i64,
    pub name: String,
    pub value: Option<f64>,
}

/// Result of batch quote factor computation.
///
/// Each tuple contains (ts_ms, factor_name, value).
/// ts_ms is the compute timestamp (every 1 second interval).
pub struct QuoteFactorResult {
    pub ts_ms: i64,
    pub name: String,
    pub value: Option<f64>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Bar Factor Batch Compute
// ═══════════════════════════════════════════════════════════════════════════

/// Compute EMA batch.
///
/// Args:
///   bars: List of bars (sorted by ts_ms)
///   period: EMA period (e.g., 20)
///   name: Factor name for result (e.g., "ema_20")
///
/// Returns: Vec<BarFactorResult>
pub fn compute_ema_batch(bars: &[Bar], period: usize, name: &str) -> Vec<BarFactorResult> {
    if bars.is_empty() || period == 0 {
        return vec![];
    }

    let alpha = 2.0 / (period + 1) as f64;
    let mut results: Vec<BarFactorResult> = Vec::with_capacity(bars.len());
    let mut value: Option<f64> = None;
    let mut count: usize = 0;

    for bar in bars {
        if value.is_none() {
            value = Some(bar.close);
        } else {
            let prev = value.unwrap();
            value = Some(alpha * bar.close + (1.0 - alpha) * prev);
        }

        count += 1;
        results.push(BarFactorResult {
            ts_ms: bar.ts_ms,
            name: name.to_string(),
            value: if count >= period { value } else { None },
        });
    }

    results
}

/// Compute Relative Volume batch.
///
/// Args:
///   bars: List of bars (sorted by ts_ms)
///   lookback: Number of bars to compute average (e.g., 20)
///   name: Factor name for result (e.g., "rel_vol_20")
///
/// Returns: Vec<BarFactorResult>
pub fn compute_relative_volume_batch(
    bars: &[Bar],
    lookback: usize,
    name: &str,
) -> Vec<BarFactorResult> {
    if bars.is_empty() || lookback == 0 {
        return vec![];
    }

    let mut results: Vec<BarFactorResult> = Vec::with_capacity(bars.len());
    let mut volumes: VecDeque<i64> = VecDeque::with_capacity(lookback);
    let mut count: usize = 0;

    for bar in bars {
        volumes.push_back(bar.volume);
        if volumes.len() > lookback {
            volumes.pop_front();
        }

        count += 1;

        if count < lookback {
            // Warmup period
            results.push(BarFactorResult {
                ts_ms: bar.ts_ms,
                name: name.to_string(),
                value: None,
            });
        } else {
            // Compute average of last N bars
            let avg_volume = volumes.iter().sum::<i64>() as f64 / volumes.len() as f64;

            let rel_vol = if avg_volume > 0.0 {
                bar.volume as f64 / avg_volume
            } else {
                0.0
            };

            results.push(BarFactorResult {
                ts_ms: bar.ts_ms,
                name: name.to_string(),
                value: Some(rel_vol),
            });
        }
    }

    results
}

/// Compute Price Direction batch.
///
/// direction = (close - low) / (high - low)
/// Returns None for flat bars (high == low).
pub fn compute_price_direction_batch(bars: &[Bar], name: &str) -> Vec<BarFactorResult> {
    if bars.is_empty() {
        return vec![];
    }

    bars.iter()
        .map(|bar| {
            let direction = if bar.high > bar.low {
                let d = (bar.close - bar.low) / (bar.high - bar.low);
                Some(d.clamp(0.0, 1.0))
            } else {
                None
            };

            BarFactorResult {
                ts_ms: bar.ts_ms,
                name: name.to_string(),
                value: direction,
            }
        })
        .collect()
}

/// Compute Gap Percent batch.
///
/// gap_pct = (current_open - prev_close) / prev_close * 100
/// First bar has None (no previous close).
pub fn compute_gap_percent_batch(bars: &[Bar], name: &str) -> Vec<BarFactorResult> {
    if bars.is_empty() {
        return vec![];
    }

    let mut results: Vec<BarFactorResult> = Vec::with_capacity(bars.len());
    let mut prev_close: Option<f64> = None;

    for bar in bars {
        let gap_pct = if let Some(prev) = prev_close {
            if prev > 0.0 {
                Some((bar.open - prev) / prev * 100.0)
            } else {
                None
            }
        } else {
            None
        };

        results.push(BarFactorResult {
            ts_ms: bar.ts_ms,
            name: name.to_string(),
            value: gap_pct,
        });

        prev_close = Some(bar.close);
    }

    results
}

/// Compute all bar factors in one pass.
///
/// This is the main entry point for batch bar factor computation.
/// Takes indicator configs and computes all factors efficiently.
///
/// Args:
///   bars: List of bars (sorted by ts_ms)
///   configs: List of (factor_type, params) tuples
///
/// Returns: Vec<(ts_ms, Vec<(name, value)>)> - merged results per timestamp
pub fn compute_all_bar_factors(
    bars: &[Bar],
    configs: &[BarFactorConfig],
) -> Vec<(i64, Vec<(String, Option<f64>)>)> {
    if bars.is_empty() || configs.is_empty() {
        return vec![];
    }

    // Compute each factor
    let all_results: Vec<Vec<BarFactorResult>> = configs
        .iter()
        .map(|cfg| match cfg.factor_type.as_str() {
            "ema" => compute_ema_batch(bars, *cfg.params.get("period").unwrap_or(&20.0) as usize, &cfg.name),
            "relative_volume" | "rel_vol" => {
                let lookback = *cfg.params.get("lookback").unwrap_or(&20.0) as usize;
                compute_relative_volume_batch(bars, lookback, &cfg.name)
            },
            "price_direction" => compute_price_direction_batch(bars, &cfg.name),
            "gap_percent" => compute_gap_percent_batch(bars, &cfg.name),
            _ => vec![], // Unknown factor type
        })
        .collect();

    // Merge results by timestamp
    // Each factor produces results at the same timestamps (bar close times)
    // So we can just zip them together

    let n_bars = bars.len();
    let mut merged: Vec<(i64, Vec<(String, Option<f64>)>)> = Vec::with_capacity(n_bars);

    for i in 0..n_bars {
        let ts_ms = bars[i].ts_ms;
        let factors: Vec<(String, Option<f64>)> = all_results
            .iter()
            .filter_map(|results| results.get(i))
            .map(|r| (r.name.clone(), r.value))
            .collect();

        merged.push((ts_ms, factors));
    }

    merged
}

/// Configuration for a bar factor.
#[derive(Debug, Clone)]
pub struct BarFactorConfig {
    pub factor_type: String, // "ema", "relative_volume", "price_direction", etc.
    pub name: String,        // Output name (e.g., "ema_20", "rel_vol_20")
    pub params: std::collections::HashMap<String, f64>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Quote Factor Batch Compute (Optimized with Running Sum)
// ═══════════════════════════════════════════════════════════════════════════

/// Compute Bid-Ask Spread batch with O(1) per-compute complexity.
///
/// Uses running sum + sliding window for efficiency:
/// - Add new spread to running_sum when on_quote()
/// - Subtract removed spread when pruning old entries
/// - compute() = running_sum / count (no iteration!)
///
/// Args:
///   quotes: List of quotes (sorted by ts_ms)
///   window_ms: Rolling average window in milliseconds
///   compute_interval_ms: Compute interval (default 1000ms)
///   name: Factor name for result
///
/// Returns: Vec<QuoteFactorResult>
pub fn compute_bid_ask_spread_batch(
    quotes: &[Quote],
    window_ms: i64,
    compute_interval_ms: i64,
    name: &str,
) -> Vec<QuoteFactorResult> {
    if quotes.is_empty() || window_ms <= 0 {
        return vec![];
    }

    let mut results: Vec<QuoteFactorResult> = Vec::new();

    // Sliding window with running sum (O(1) compute!)
    let mut window: VecDeque<(i64, f64)> = VecDeque::new(); // (ts_ms, spread_bps)
    let mut running_sum: f64 = 0.0;
    let mut last_compute_ms: i64 = 0;

    for quote in quotes {
        // Skip invalid quotes
        if quote.bid <= 0.0 || quote.ask <= 0.0 {
            continue;
        }

        let ts_ms = quote.ts_ms;
        let mid = quote.mid();
        let spread_bps = if mid > 0.0 {
            (quote.ask - quote.bid) / mid * 10000.0
        } else {
            0.0
        };

        // Add to window
        window.push_back((ts_ms, spread_bps));
        running_sum += spread_bps;

        // Prune old entries (O(1) amortized)
        let cutoff = ts_ms - window_ms;
        while let Some((old_ts, old_spread)) = window.front() {
            if *old_ts < cutoff {
                running_sum -= *old_spread;
                window.pop_front();
            } else {
                break;
            }
        }

        // Compute at intervals
        if ts_ms - last_compute_ms >= compute_interval_ms {
            let avg_spread = if window.len() > 0 {
                running_sum / window.len() as f64
            } else {
                0.0
            };

            results.push(QuoteFactorResult {
                ts_ms,
                name: name.to_string(),
                value: Some(avg_spread),
            });

            last_compute_ms = ts_ms;
        }
    }

    results
}

/// Compute Order Imbalance batch with O(1) per-compute complexity.
///
/// imbalance = (bid_size - ask_size) / (bid_size + ask_size)
/// Range: [-1, +1]
///
/// Uses running sum approach similar to spread.
pub fn compute_order_imbalance_batch(
    quotes: &[Quote],
    window_ms: i64,
    compute_interval_ms: i64,
    name: &str,
) -> Vec<QuoteFactorResult> {
    if quotes.is_empty() || window_ms <= 0 {
        return vec![];
    }

    let mut results: Vec<QuoteFactorResult> = Vec::new();

    // Sliding window with running sums for bid_size and ask_size
    let mut window: VecDeque<(i64, i64, i64)> = VecDeque::new(); // (ts_ms, bid_size, ask_size)
    let mut running_bid: i64 = 0;
    let mut running_ask: i64 = 0;
    let mut last_compute_ms: i64 = 0;

    for quote in quotes {
        let ts_ms = quote.ts_ms;

        // Add to window
        window.push_back((ts_ms, quote.bid_size, quote.ask_size));
        running_bid += quote.bid_size;
        running_ask += quote.ask_size;

        // Prune old entries
        let cutoff = ts_ms - window_ms;
        while let Some((old_ts, old_bid, old_ask)) = window.front() {
            if *old_ts < cutoff {
                running_bid -= *old_bid;
                running_ask -= *old_ask;
                window.pop_front();
            } else {
                break;
            }
        }

        // Compute at intervals
        if ts_ms - last_compute_ms >= compute_interval_ms {
            let imbalance = if running_bid + running_ask > 0 {
                (running_bid - running_ask) as f64 / (running_bid + running_ask) as f64
            } else {
                0.0
            };

            results.push(QuoteFactorResult {
                ts_ms,
                name: name.to_string(),
                value: Some(imbalance),
            });

            last_compute_ms = ts_ms;
        }
    }

    results
}

/// Compute Quote Rate batch.
///
/// Counts quotes per second within window.
pub fn compute_quote_rate_batch(
    quotes: &[Quote],
    window_ms: i64,
    min_quotes: usize,
    compute_interval_ms: i64,
    name: &str,
) -> Vec<QuoteFactorResult> {
    if quotes.is_empty() || window_ms <= 0 {
        return vec![];
    }

    let mut results: Vec<QuoteFactorResult> = Vec::new();

    // Sliding window with quote timestamps
    let mut window: VecDeque<i64> = VecDeque::new(); // ts_ms only
    let mut last_compute_ms: i64 = 0;

    for quote in quotes {
        let ts_ms = quote.ts_ms;

        // Add to window
        window.push_back(ts_ms);

        // Prune old entries
        let cutoff = ts_ms - window_ms;
        while let Some(&old_ts) = window.front() {
            if old_ts < cutoff {
                window.pop_front();
            } else {
                break;
            }
        }

        // Compute at intervals
        if ts_ms - last_compute_ms >= compute_interval_ms {
            let count = window.len();
            let rate = if count >= min_quotes {
                Some(count as f64 / (window_ms as f64 / 1000.0))
            } else {
                None
            };

            results.push(QuoteFactorResult {
                ts_ms,
                name: name.to_string(),
                value: rate,
            });

            last_compute_ms = ts_ms;
        }
    }

    results
}

/// Compute all quote factors in one pass.
///
/// Args:
///   quotes: List of quotes (sorted by ts_ms)
///   configs: List of QuoteFactorConfig
///
/// Returns: Vec<(ts_ms, Vec<(name, value)>)> - merged results per timestamp
pub fn compute_all_quote_factors(
    quotes: &[Quote],
    configs: &[QuoteFactorConfig],
) -> Vec<(i64, Vec<(String, Option<f64>)>)> {
    if quotes.is_empty() || configs.is_empty() {
        return vec![];
    }

    // Default compute interval: 1000ms
    let compute_interval_ms = 1000;

    // Collect all compute timestamps first
    // All quote factors compute at the same timestamps (every 1s)
    let compute_timestamps: Vec<i64> = quotes
        .iter()
        .filter_map(|q| {
            let ts = q.ts_ms;
            // Take first quote in each 1-second bucket
            if ts % compute_interval_ms < 100 {
                Some(ts)
            } else {
                None
            }
        })
        .collect();

    // Compute each factor
    let all_results: Vec<Vec<QuoteFactorResult>> = configs
        .iter()
        .map(|cfg| match cfg.factor_type.as_str() {
            "bid_ask_spread" => {
                let window_ms = *cfg.params.get("window_ms").unwrap_or(&5000.0) as i64;
                compute_bid_ask_spread_batch(quotes, window_ms, compute_interval_ms, &cfg.name)
            },
            "order_imbalance" => {
                let window_ms = *cfg.params.get("window_ms").unwrap_or(&5000.0) as i64;
                compute_order_imbalance_batch(quotes, window_ms, compute_interval_ms, &cfg.name)
            },
            "quote_rate" => {
                let window_ms = *cfg.params.get("window_ms").unwrap_or(&10000.0) as i64;
                let min_quotes = *cfg.params.get("min_quotes").unwrap_or(&10.0) as usize;
                compute_quote_rate_batch(quotes, window_ms, min_quotes, compute_interval_ms, &cfg.name)
            },
            _ => vec![],
        })
        .collect();

    // Build timestamp -> factors map
    // Quote factors may compute at different timestamps due to quote arrival pattern
    // So we need to merge properly

    let mut ts_to_factors: std::collections::BTreeMap<i64, Vec<(String, Option<f64>)>> =
        std::collections::BTreeMap::new();

    for results in all_results {
        for r in results {
            ts_to_factors
                .entry(r.ts_ms)
                .or_insert_with(Vec::new)
                .push((r.name, r.value));
        }
    }

    // Convert to sorted vec
    ts_to_factors.into_iter().collect()
}

/// Configuration for a quote factor.
#[derive(Debug, Clone)]
pub struct QuoteFactorConfig {
    pub factor_type: String, // "bid_ask_spread", "order_imbalance", "quote_rate"
    pub name: String,        // Output name
    pub params: std::collections::HashMap<String, f64>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Signal Exit Statistics (Batch Compute)
// ═══════════════════════════════════════════════════════════════════════════

/// Result of signal exit computation.
///
/// Contains all statistics for a signal's holding period.
#[derive(Debug, Clone)]
pub struct SignalExitResult {
    pub entry_price: f64,
    pub exit_price: f64,
    pub max_price: f64,
    pub min_price: f64,
    pub time_to_max_ms: i64,
    pub time_to_min_ms: i64,
    pub return_pct: f64,
}

/// Batch compute exit statistics for all signals.
///
/// This replaces Python's O(n log n) sorted() calls per signal with:
/// 1. One sort of trades upfront (shared by all signals)
/// 2. O(log n) binary search to find window boundaries per signal
/// 3. O(window_size) iteration to find max/min (small window)
///
/// Args:
///   trades: List of (ts_ms, price) tuples - MUST be sorted by ts_ms
///   signals: List of (entry_time_ms, hold_duration_ms) tuples
///
/// Returns: Vec<Option<SignalExitResult>>
///   - Some(result) if entry_price found
///   - None if no trade at/before entry_time
pub fn compute_signal_exits_batch(
    trades: &[(i64, f64)],
    signals: &[(i64, i64)],
) -> Vec<Option<SignalExitResult>> {
    if trades.is_empty() || signals.is_empty() {
        return vec![];
    }

    // Ensure trades is sorted (just in case)
    // In practice caller should pre-sort once
    let sorted_trades: Vec<(i64, f64)> = if is_sorted(trades) {
        trades.to_vec()
    } else {
        let mut t = trades.to_vec();
        t.sort_by_key(|(ts, _)| *ts);
        t
    };

    signals
        .iter()
        .map(|(entry_time, hold_ms)| {
            let exit_time = entry_time + hold_ms;

            // Find entry_price: last trade before/at entry_time
            // Binary search: O(log n)
            let entry_idx = sorted_trades.partition_point(|(t, _)| t <= entry_time);
            if entry_idx == 0 {
                return None; // No trade before entry_time
            }
            let entry_price = sorted_trades[entry_idx - 1].1;

            // Find window: trades between entry_time and exit_time
            // Binary search: O(log n)
            let window_start = entry_idx;
            let window_end = sorted_trades.partition_point(|(t, _)| t <= &exit_time);

            // Iterate window to find max/min/exit
            // O(window_size) - typically small (10 min window)
            let mut max_price = entry_price;
            let mut min_price = entry_price;
            let mut max_time = *entry_time;
            let mut min_time = *entry_time;
            let mut exit_price = entry_price;

            for i in window_start..window_end {
                let (ts, price) = sorted_trades[i];
                if price > max_price {
                    max_price = price;
                    max_time = ts;
                }
                if price < min_price {
                    min_price = price;
                    min_time = ts;
                }
                exit_price = price;
            }

            // If no trades in window, use last trade before exit_time
            if window_start >= window_end && entry_idx < sorted_trades.len() {
                // Find closest trade after entry_time
                let next_idx = sorted_trades.partition_point(|(t, _)| t <= entry_time);
                if next_idx < sorted_trades.len() {
                    exit_price = sorted_trades[next_idx].1;
                    let exit_ts = sorted_trades[next_idx].0;
                    max_time = exit_ts;
                    min_time = exit_ts;
                }
            }

            let return_pct = if entry_price > 0.0 {
                (exit_price - entry_price) / entry_price * 100.0
            } else {
                0.0
            };

            Some(SignalExitResult {
                entry_price,
                exit_price,
                max_price,
                min_price,
                time_to_max_ms: max_time - entry_time,
                time_to_min_ms: min_time - entry_time,
                return_pct,
            })
        })
        .collect()
}

/// Check if trades array is sorted by timestamp.
fn is_sorted(trades: &[(i64, f64)]) -> bool {
    for i in 1..trades.len() {
        if trades[i].0 < trades[i - 1].0 {
            return false;
        }
    }
    true
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ema_batch() {
        let bars = vec![
            Bar::new(1000, 10.0, 11.0, 9.0, 10.5, 100),
            Bar::new(2000, 10.5, 11.5, 10.0, 11.0, 150),
            Bar::new(3000, 11.0, 12.0, 10.5, 11.5, 200),
        ];

        let results = compute_ema_batch(&bars, 2, "ema_2");

        assert_eq!(results.len(), 3);
        assert!(results[0].value.is_none()); // count=1 < period=2
        assert!(results[1].value.is_some()); // count=2 >= period=2
        assert!(results[2].value.is_some());
    }

    #[test]
    fn test_relative_volume_batch() {
        let bars = vec![
            Bar::new(1000, 10.0, 11.0, 9.0, 10.5, 100),
            Bar::new(2000, 10.5, 11.5, 10.0, 11.0, 200),
            Bar::new(3000, 11.0, 12.0, 10.5, 11.5, 150),
        ];

        let results = compute_relative_volume_batch(&bars, 2, "rel_vol_2");

        assert_eq!(results.len(), 3);
        assert!(results[0].value.is_none()); // Warmup
        assert!(results[1].value.is_some());
        // Bar 1: vol=200, avg=(100+200)/2=150, rel_vol=1.33
        // Actually includes current bar: avg=(100+200)/2=150, rel_vol=200/150=1.33
    }

    #[test]
    fn test_price_direction_batch() {
        let bars = vec![
            Bar::new(1000, 10.0, 11.0, 9.0, 10.5, 100), // close at midpoint
            Bar::new(2000, 10.5, 11.5, 10.5, 11.5, 150), // close at high
            Bar::new(3000, 11.0, 11.0, 11.0, 11.0, 200), // flat bar
        ];

        let results = compute_price_direction_batch(&bars, "price_direction");

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].value, Some(0.5)); // (10.5-9)/(11-9) = 0.5
        assert_eq!(results[1].value, Some(1.0)); // (11.5-10.5)/(11.5-10.5) = 1.0
        assert!(results[2].value.is_none()); // Flat bar
    }

    #[test]
    fn test_bid_ask_spread_batch_optimized() {
        // Test that O(1) running sum approach works correctly
        let quotes = vec![
            Quote::new(1000, 10.0, 10.1, 100, 100), // spread=100bps
            Quote::new(1500, 10.0, 10.1, 100, 100),
            Quote::new(2000, 10.0, 10.2, 100, 100), // spread=200bps
            Quote::new(2500, 10.0, 10.1, 100, 100),
            Quote::new(3000, 10.0, 10.1, 100, 100),
        ];

        let results = compute_bid_ask_spread_batch(&quotes, 5000, 1000, "spread");

        // Should compute at ts=1000, 2000, 3000
        assert!(results.len() >= 1);
    }

    #[test]
    fn test_large_quote_batch_performance() {
        // Test that large quote batches don't cause O(n^2) behavior
        // Create 50,000 quotes
        let quotes: Vec<Quote> = (0..50000)
            .map(|i| Quote::new(i as i64, 10.0, 10.01 + (i % 10) as f64 * 0.001, 100, 100))
            .collect();

        // This should complete in milliseconds, not minutes!
        let start = std::time::Instant::now();
        let results = compute_bid_ask_spread_batch(&quotes, 5000, 1000, "spread");
        let elapsed = start.elapsed();

        // Should be well under 1 second for 50k quotes
        // (Python would take minutes due to O(n^2) iteration)
        assert!(elapsed.as_millis() < 100);
        assert!(results.len() > 0);
    }

    #[test]
    fn test_signal_exits_batch() {
        let trades = vec![
            (1000, 10.0),
            (2000, 10.5),
            (3000, 11.0),
            (4000, 10.8),
            (5000, 11.2),
            (6000, 10.6),
        ];

        // Signal: enter at 1500, hold for 3000ms (until 4500)
        let signals = vec![(1500, 3000)];

        let results = compute_signal_exits_batch(&trades, &signals);

        assert_eq!(results.len(), 1);
        let r = results[0].as_ref().unwrap();

        // Entry price: last trade before 1500 is at 1000, price=10.0
        assert_eq!(r.entry_price, 10.0);

        // Window: trades at 2000, 3000, 4000 (within 1500-4500)
        // Max: 11.0 at 3000, Min: 10.5 at 2000, Exit: 10.8 at 4000
        assert_eq!(r.max_price, 11.0);
        assert_eq!(r.min_price, 10.5);
        assert_eq!(r.exit_price, 10.8);

        // Time to max: 3000 - 1500 = 1500ms
        assert_eq!(r.time_to_max_ms, 1500);

        // Return: (10.8 - 10.0) / 10.0 * 100 = 8%
        assert!((r.return_pct - 8.0).abs() < 0.01);
    }

    #[test]
    fn test_signal_exits_batch_no_trades_in_window() {
        let trades = vec![
            (1000, 10.0),
            (2000, 10.5),
            (10000, 11.0), // Gap in trades
        ];

        // Signal: enter at 1500, hold for 3000ms (until 4500)
        // No trades in window [1500, 4500]
        let signals = vec![(1500, 3000)];

        let results = compute_signal_exits_batch(&trades, &signals);

        // Should still find entry_price = 10.0 (trade at 1000)
        // But no trades in window, should use next trade
        let r = results[0].as_ref().unwrap();
        assert_eq!(r.entry_price, 10.0);
    }

    #[test]
    fn test_signal_exits_batch_performance() {
        // Simulate KIDZ: 113800 trades, 10 signals
        let trades: Vec<(i64, f64)> = (0..113800)
            .map(|i| (i as i64 * 10, 10.0 + (i % 100) as f64 * 0.01))
            .collect();

        let signals: Vec<(i64, i64)> = (0..10)
            .map(|i| (100000 + i * 10000, 600000)) // 10 min hold
            .collect();

        let start = std::time::Instant::now();
        let results = compute_signal_exits_batch(&trades, &signals);
        let elapsed = start.elapsed();

        // Should be under 1ms
        // Python sorted() approach would be ~seconds
        assert!(elapsed.as_micros() < 1000);
        assert_eq!(results.len(), 10);
    }
}
