// rust/src/factor/quote_factors.rs
//
// Quote factor implementations using O(1) running-sum sliding windows.

use std::collections::VecDeque;

use super::data::Quote;
use super::factor_trait::{Factor, FactorType, QuoteFactor};

// ═══════════════════════════════════════════════════════════════════════════
// Bid-Ask Spread Factor (basis points)
// ═══════════════════════════════════════════════════════════════════════════

pub struct BidAskSpreadFactor {
    window_ms: i64,
    window: VecDeque<(i64, f64)>, // (ts_ms, spread_bps)
    running_sum: f64,
    last_value: Option<f64>,
    last_compute_ms: i64,
}

impl BidAskSpreadFactor {
    pub fn new(window_ms: i64) -> Self {
        Self {
            window_ms,
            window: VecDeque::new(),
            running_sum: 0.0,
            last_value: None,
            last_compute_ms: 0,
        }
    }
}

impl Factor for BidAskSpreadFactor {
    fn name(&self) -> &str {
        "bid_ask_spread"
    }
    fn factor_type(&self) -> FactorType {
        FactorType::Quote
    }
    fn reset(&mut self) {
        self.window.clear();
        self.running_sum = 0.0;
        self.last_value = None;
        self.last_compute_ms = 0;
    }
    fn is_ready(&self) -> bool {
        self.last_value.is_some()
    }
    fn get_value(&self) -> Option<f64> {
        self.last_value
    }
}

impl QuoteFactor for BidAskSpreadFactor {
    fn compute_batch(&self, quotes: &[Quote]) -> Vec<(i64, Option<f64>)> {
        if quotes.is_empty() || self.window_ms <= 0 {
            return vec![];
        }

        // Grid-driven: compute at every 1s boundary, not just when quotes arrive.
        // Same algorithm as bootstrap_trade_rate (grid walk) vs old data-driven loop.
        let compute_interval_ms: i64 = 1000;

        // Sort quotes by timestamp (should already be sorted, but be safe)
        let mut sorted: Vec<&Quote> = quotes.iter().collect();
        sorted.sort_by_key(|q| q.ts_ms);

        let first_ts = sorted[0].ts_ms;
        let last_ts = sorted.last().unwrap().ts_ms;

        let mut q_iter: VecDeque<&Quote> = sorted.into_iter().collect();
        let mut window: VecDeque<(i64, f64)> = VecDeque::new();
        let mut running_sum: f64 = 0.0;
        let mut results: Vec<(i64, Option<f64>)> = Vec::new();

        // Walk 1s grid from first aligned boundary to last_ts
        let mut grid_ts = first_ts - (first_ts % compute_interval_ms) + compute_interval_ms;

        while grid_ts <= last_ts {
            // Advance quotes up to this grid point
            while let Some(q) = q_iter.front() {
                if q.ts_ms <= grid_ts {
                    let q = q_iter.pop_front().unwrap();
                    if q.bid > 0.0 && q.ask > 0.0 {
                        let mid = q.mid();
                        let spread_bps = if mid > 0.0 {
                            (q.ask - q.bid) / mid * 10000.0
                        } else {
                            0.0
                        };
                        window.push_back((q.ts_ms, spread_bps));
                        running_sum += spread_bps;
                    }
                } else {
                    break;
                }
            }

            // Prune old entries outside the window
            let cutoff = grid_ts - self.window_ms;
            while let Some((old_ts, old_spread)) = window.front() {
                if *old_ts < cutoff {
                    running_sum -= *old_spread;
                    window.pop_front();
                } else {
                    break;
                }
            }

            let avg = if !window.is_empty() {
                Some(running_sum / window.len() as f64)
            } else {
                None
            };
            results.push((grid_ts, avg));

            grid_ts += compute_interval_ms;
        }
        results
    }

    fn on_quote(&mut self, quote: &Quote) -> Option<f64> {
        if quote.bid <= 0.0 || quote.ask <= 0.0 {
            return self.last_value;
        }

        let ts_ms = quote.ts_ms;
        let mid = quote.mid();
        let spread_bps = if mid > 0.0 {
            (quote.ask - quote.bid) / mid * 10000.0
        } else {
            0.0
        };

        self.window.push_back((ts_ms, spread_bps));
        self.running_sum += spread_bps;

        // Prune old
        let cutoff = ts_ms - self.window_ms;
        while let Some((old_ts, old_spread)) = self.window.front() {
            if *old_ts < cutoff {
                self.running_sum -= *old_spread;
                self.window.pop_front();
            } else {
                break;
            }
        }

        if ts_ms - self.last_compute_ms >= 1000 && !self.window.is_empty() {
            let avg = self.running_sum / self.window.len() as f64;
            self.last_value = Some(avg);
            self.last_compute_ms = ts_ms;
        }
        self.last_value
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Order Imbalance Factor [-1, +1]
// ═══════════════════════════════════════════════════════════════════════════

pub struct OrderImbalanceFactor {
    window_ms: i64,
    window: VecDeque<(i64, i64, i64)>, // (ts_ms, bid_size, ask_size)
    running_bid: i64,
    running_ask: i64,
    last_value: Option<f64>,
    last_compute_ms: i64,
}

impl OrderImbalanceFactor {
    pub fn new(window_ms: i64) -> Self {
        Self {
            window_ms,
            window: VecDeque::new(),
            running_bid: 0,
            running_ask: 0,
            last_value: None,
            last_compute_ms: 0,
        }
    }
}

impl Factor for OrderImbalanceFactor {
    fn name(&self) -> &str {
        "order_imbalance"
    }
    fn factor_type(&self) -> FactorType {
        FactorType::Quote
    }
    fn reset(&mut self) {
        self.window.clear();
        self.running_bid = 0;
        self.running_ask = 0;
        self.last_value = None;
        self.last_compute_ms = 0;
    }
    fn is_ready(&self) -> bool {
        self.last_value.is_some()
    }
    fn get_value(&self) -> Option<f64> {
        self.last_value
    }
}

impl QuoteFactor for OrderImbalanceFactor {
    fn compute_batch(&self, quotes: &[Quote]) -> Vec<(i64, Option<f64>)> {
        if quotes.is_empty() || self.window_ms <= 0 {
            return vec![];
        }

        // Grid-driven: compute at every 1s boundary across the full time range.
        let compute_interval_ms: i64 = 1000;

        let mut sorted: Vec<&Quote> = quotes.iter().collect();
        sorted.sort_by_key(|q| q.ts_ms);

        let first_ts = sorted[0].ts_ms;
        let last_ts = sorted.last().unwrap().ts_ms;

        let mut q_iter: VecDeque<&Quote> = sorted.into_iter().collect();
        let mut window: VecDeque<(i64, i64, i64)> = VecDeque::new();
        let mut running_bid: i64 = 0;
        let mut running_ask: i64 = 0;
        let mut results: Vec<(i64, Option<f64>)> = Vec::new();

        let mut grid_ts = first_ts - (first_ts % compute_interval_ms) + compute_interval_ms;

        while grid_ts <= last_ts {
            // Advance quotes up to this grid point
            while let Some(q) = q_iter.front() {
                if q.ts_ms <= grid_ts {
                    let q = q_iter.pop_front().unwrap();
                    window.push_back((q.ts_ms, q.bid_size, q.ask_size));
                    running_bid += q.bid_size;
                    running_ask += q.ask_size;
                } else {
                    break;
                }
            }

            // Prune old entries
            let cutoff = grid_ts - self.window_ms;
            while let Some((old_ts, old_bid, old_ask)) = window.front() {
                if *old_ts < cutoff {
                    running_bid -= *old_bid;
                    running_ask -= *old_ask;
                    window.pop_front();
                } else {
                    break;
                }
            }

            let imbalance = if running_bid + running_ask > 0 {
                Some((running_bid - running_ask) as f64 / (running_bid + running_ask) as f64)
            } else {
                None
            };
            results.push((grid_ts, imbalance));

            grid_ts += compute_interval_ms;
        }
        results
    }

    fn on_quote(&mut self, quote: &Quote) -> Option<f64> {
        let ts_ms = quote.ts_ms;

        self.window.push_back((ts_ms, quote.bid_size, quote.ask_size));
        self.running_bid += quote.bid_size;
        self.running_ask += quote.ask_size;

        let cutoff = ts_ms - self.window_ms;
        while let Some((old_ts, old_bid, old_ask)) = self.window.front() {
            if *old_ts < cutoff {
                self.running_bid -= *old_bid;
                self.running_ask -= *old_ask;
                self.window.pop_front();
            } else {
                break;
            }
        }

        if ts_ms - self.last_compute_ms >= 1000 {
            let imbalance = if self.running_bid + self.running_ask > 0 {
                (self.running_bid - self.running_ask) as f64
                    / (self.running_bid + self.running_ask) as f64
            } else {
                0.0
            };
            self.last_value = Some(imbalance);
            self.last_compute_ms = ts_ms;
        }
        self.last_value
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Quote Rate Factor (quotes/sec)
// ═══════════════════════════════════════════════════════════════════════════

pub struct QuoteRateFactor {
    window_ms: i64,
    min_quotes: usize,
    window: VecDeque<i64>,
    last_value: Option<f64>,
    last_compute_ms: i64,
}

impl QuoteRateFactor {
    pub fn new(window_ms: i64, min_quotes: usize) -> Self {
        Self {
            window_ms,
            min_quotes,
            window: VecDeque::new(),
            last_value: None,
            last_compute_ms: 0,
        }
    }
}

impl Factor for QuoteRateFactor {
    fn name(&self) -> &str {
        "quote_rate"
    }
    fn factor_type(&self) -> FactorType {
        FactorType::Quote
    }
    fn reset(&mut self) {
        self.window.clear();
        self.last_value = None;
        self.last_compute_ms = 0;
    }
    fn is_ready(&self) -> bool {
        self.last_value.is_some()
    }
    fn get_value(&self) -> Option<f64> {
        self.last_value
    }
}

impl QuoteFactor for QuoteRateFactor {
    fn compute_batch(&self, quotes: &[Quote]) -> Vec<(i64, Option<f64>)> {
        if quotes.is_empty() || self.window_ms <= 0 {
            return vec![];
        }

        // Grid-driven: compute at every 1s boundary across the full time range.
        // Matches bootstrap_trade_rate's algorithm — walks uniform grid instead
        // of only computing when a quote happens to trigger the interval check.
        let compute_interval_ms: i64 = 1000;

        // Extract and sort timestamps
        let mut timestamps: Vec<i64> = quotes.iter().map(|q| q.ts_ms).collect();
        timestamps.sort_unstable();

        let first_ts = timestamps[0];
        let last_ts = *timestamps.last().unwrap();

        let mut ts_iter: VecDeque<i64> = timestamps.into_iter().collect();
        let mut window: VecDeque<i64> = VecDeque::new();
        let mut results: Vec<(i64, Option<f64>)> = Vec::new();

        let mut grid_ts = first_ts - (first_ts % compute_interval_ms) + compute_interval_ms;

        while grid_ts <= last_ts {
            // Advance timestamps up to this grid point
            while let Some(&ts) = ts_iter.front() {
                if ts <= grid_ts {
                    window.push_back(ts);
                    ts_iter.pop_front();
                } else {
                    break;
                }
            }

            // Prune old timestamps outside the window
            let cutoff = grid_ts - self.window_ms;
            while window.front().map_or(false, |&ts| ts < cutoff) {
                window.pop_front();
            }

            let count = window.len();
            let rate = if count >= self.min_quotes {
                Some(count as f64 / (self.window_ms as f64 / 1000.0))
            } else {
                None
            };
            results.push((grid_ts, rate));

            grid_ts += compute_interval_ms;
        }
        results
    }

    fn on_quote(&mut self, quote: &Quote) -> Option<f64> {
        let ts_ms = quote.ts_ms;

        self.window.push_back(ts_ms);

        let cutoff = ts_ms - self.window_ms;
        while let Some(&old_ts) = self.window.front() {
            if old_ts < cutoff {
                self.window.pop_front();
            } else {
                break;
            }
        }

        if ts_ms - self.last_compute_ms >= 1000 {
            let count = self.window.len();
            let rate = if count >= self.min_quotes {
                Some(count as f64 / (self.window_ms as f64 / 1000.0))
            } else {
                None
            };
            self.last_value = rate;
            self.last_compute_ms = ts_ms;
        }
        self.last_value
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spread_batch_vs_incremental() {
        let quotes: Vec<Quote> = (0..100)
            .map(|i| {
                let spread = if i % 10 == 0 { 0.05 } else { 0.01 };
                Quote::new(i * 500, 10.0, 10.0 + spread, 100, 100)
            })
            .collect();

        // Batch mode
        let sf_batch = BidAskSpreadFactor::new(5000);
        let batch_results = sf_batch.compute_batch(&quotes);

        // Incremental mode: feed quotes one by one,
        // track last_value separately (same as live mode pattern)
        let mut sf_inc = BidAskSpreadFactor::new(5000);
        let mut inc_results: Vec<(i64, Option<f64>)> = Vec::new();
        let mut last_compute_ms = 0i64;

        for q in &quotes {
            sf_inc.on_quote(q);
            // Read only at 1s compute intervals (same throttle as batch)
            if q.ts_ms - last_compute_ms >= 1000 {
                inc_results.push((q.ts_ms, sf_inc.last_value));
                last_compute_ms = q.ts_ms;
            }
        }

        assert_eq!(batch_results.len(), inc_results.len(),
            "Batch={}, Inc={}", batch_results.len(), inc_results.len());
        for (i, ((_bts, b), (_its, inc))) in batch_results.iter().zip(inc_results.iter()).enumerate() {
            match (b, inc) {
                (Some(bv), Some(iv)) => assert!((bv - iv).abs() < 0.01, "Mismatch at {}: {} vs {}", i, bv, iv),
                (None, None) => {}
                _ => panic!("None/Some mismatch at {}: batch={:?} inc={:?}", i, b, inc),
            }
        }
    }

    #[test]
    fn test_imbalance_basic() {
        // All buy pressure
        let quotes = vec![
            Quote::new(1000, 10.0, 10.01, 1000, 100),
            Quote::new(2000, 10.0, 10.01, 800, 200),
        ];
        let mut oi = OrderImbalanceFactor::new(5000);
        let r1 = oi.on_quote(&quotes[0]);
        assert!(r1.is_some());
        // (1000-100)/(1000+100) ≈ 0.818
        assert!((r1.unwrap() - 0.818).abs() < 0.01);
    }
}
