// rust/src/factor/trade_factors.rs
//
// Trade factor implementations for the unified Factor architecture.

use std::collections::VecDeque;

use super::data::Trade;
use super::factor_trait::{Factor, FactorType, TradeFactor};

// ═══════════════════════════════════════════════════════════════════════════
// Trade Rate Factor
// ═══════════════════════════════════════════════════════════════════════════
//
// Counts trades per second within a rolling time window.

pub struct TradeRateFactor {
    window_ms: i64,
    min_trades: usize,
    timestamps: VecDeque<i64>,
    last_value: Option<f64>,
}

impl TradeRateFactor {
    pub fn new(window_ms: i64, min_trades: usize) -> Self {
        Self {
            window_ms,
            min_trades,
            timestamps: VecDeque::new(),
            last_value: None,
        }
    }
}

impl Factor for TradeRateFactor {
    fn name(&self) -> &str {
        "trade_rate"
    }
    fn factor_type(&self) -> FactorType {
        FactorType::Trade
    }
    fn reset(&mut self) {
        self.timestamps.clear();
        self.last_value = None;
    }
    fn is_ready(&self) -> bool {
        self.last_value.is_some()
    }
    fn get_value(&self) -> Option<f64> {
        self.last_value
    }
}

impl TradeFactor for TradeRateFactor {
    fn compute_batch(&self, trades: &[Trade], compute_ts: &[i64]) -> Vec<Option<f64>> {
        if trades.is_empty() || compute_ts.is_empty() {
            return vec![];
        }

        let mut results: Vec<Option<f64>> = Vec::with_capacity(compute_ts.len());
        let mut window: VecDeque<i64> = VecDeque::new();
        let mut trade_idx: usize = 0;

        for &ts in compute_ts {
            let cutoff = ts - self.window_ms;

            // Advance trades up to this compute timestamp
            while trade_idx < trades.len() && trades[trade_idx].ts_ms <= ts {
                window.push_back(trades[trade_idx].ts_ms);
                trade_idx += 1;
            }

            // Prune old trades outside window
            while let Some(&old_ts) = window.front() {
                if old_ts < cutoff {
                    window.pop_front();
                } else {
                    break;
                }
            }

            let count = window.len();
            let rate = if count >= self.min_trades {
                Some(count as f64 / (self.window_ms as f64 / 1000.0))
            } else {
                None
            };
            results.push(rate);
        }
        results
    }

    fn on_trade(&mut self, trade: &Trade) -> Option<f64> {
        self.timestamps.push_back(trade.ts_ms);

        // Prune old timestamps
        let cutoff = trade.ts_ms - self.window_ms;
        while let Some(&old_ts) = self.timestamps.front() {
            if old_ts < cutoff {
                self.timestamps.pop_front();
            } else {
                break;
            }
        }

        // Compute rate: always compute on each trade for live mode,
        // but only return Some if min_trades met
        let count = self.timestamps.len();
        if count >= self.min_trades {
            let rate = count as f64 / (self.window_ms as f64 / 1000.0);
            self.last_value = Some(rate);
            self.last_value
        } else {
            None
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Large Trade Ratio Factor
// ═══════════════════════════════════════════════════════════════════════════
//
// Detects institutional participation: ratio of large trades to total trades
// within the rolling window. Large = size > multiplier * mean_size.

pub struct LargeTradeRatioFactor {
    window_ms: i64,
    min_trades: usize,
    large_multiplier: f64,
    window: VecDeque<(i64, i64)>, // (ts_ms, size)
    running_sum: i64,
    last_value: Option<f64>,
}

impl LargeTradeRatioFactor {
    pub fn new(window_ms: i64, min_trades: usize, large_multiplier: f64) -> Self {
        Self {
            window_ms,
            min_trades,
            large_multiplier,
            window: VecDeque::new(),
            running_sum: 0,
            last_value: None,
        }
    }
}

impl Factor for LargeTradeRatioFactor {
    fn name(&self) -> &str {
        "large_trade_ratio"
    }
    fn factor_type(&self) -> FactorType {
        FactorType::Trade
    }
    fn reset(&mut self) {
        self.window.clear();
        self.running_sum = 0;
        self.last_value = None;
    }
    fn is_ready(&self) -> bool {
        self.last_value.is_some()
    }
    fn get_value(&self) -> Option<f64> {
        self.last_value
    }
}

impl TradeFactor for LargeTradeRatioFactor {
    fn compute_batch(&self, trades: &[Trade], compute_ts: &[i64]) -> Vec<Option<f64>> {
        if trades.is_empty() || compute_ts.is_empty() {
            return vec![];
        }

        let mut results: Vec<Option<f64>> = Vec::with_capacity(compute_ts.len());
        let mut window: VecDeque<(i64, i64)> = VecDeque::new();
        let mut running_sum: i64 = 0;
        let mut trade_idx: usize = 0;

        for &ts in compute_ts {
            let cutoff = ts - self.window_ms;

            // Advance trades up to this compute timestamp
            while trade_idx < trades.len() && trades[trade_idx].ts_ms <= ts {
                window.push_back((trades[trade_idx].ts_ms, trades[trade_idx].size));
                running_sum += trades[trade_idx].size;
                trade_idx += 1;
            }

            // Prune old trades outside window
            while let Some(&(old_ts, old_size)) = window.front() {
                if old_ts < cutoff {
                    running_sum -= old_size;
                    window.pop_front();
                } else {
                    break;
                }
            }

            let total = window.len();
            let ratio = if total >= self.min_trades {
                let mean = running_sum as f64 / total as f64;
                let threshold = mean * self.large_multiplier;
                let large_count = window.iter().filter(|(_, sz)| *sz as f64 > threshold).count();
                Some(large_count as f64 / total as f64)
            } else {
                None
            };
            results.push(ratio);
        }
        results
    }

    fn on_trade(&mut self, trade: &Trade) -> Option<f64> {
        self.window.push_back((trade.ts_ms, trade.size));
        self.running_sum += trade.size;

        // Prune old
        let cutoff = trade.ts_ms - self.window_ms;
        while let Some(&(old_ts, old_size)) = self.window.front() {
            if old_ts < cutoff {
                self.running_sum -= old_size;
                self.window.pop_front();
            } else {
                break;
            }
        }

        let total = self.window.len();
        if total >= self.min_trades {
            let mean = self.running_sum as f64 / total as f64;
            let threshold = mean * self.large_multiplier;
            let large_count = self.window.iter().filter(|(_, sz)| *sz as f64 > threshold).count();
            self.last_value = Some(large_count as f64 / total as f64);
            self.last_value
        } else {
            None
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Aggressor Ratio Factor
// ═══════════════════════════════════════════════════════════════════════════
//
// Ratio of buyer-initiated trades to total trades in a rolling window.
// Uses tick test (uptick = buy, downtick = sell) to infer aggressor side.

pub struct AggressorRatioFactor {
    window_ms: i64,
    min_trades: usize,
    window: VecDeque<(i64, bool)>, // (ts_ms, is_buy)
    buy_count: usize,              // running count of buys in window
    last_price: Option<f64>,       // previous trade price for tick test
    last_value: Option<f64>,
}

impl AggressorRatioFactor {
    pub fn new(window_ms: i64, min_trades: usize) -> Self {
        Self {
            window_ms,
            min_trades,
            window: VecDeque::new(),
            buy_count: 0,
            last_price: None,
            last_value: None,
        }
    }
}

impl Factor for AggressorRatioFactor {
    fn name(&self) -> &str {
        "aggressor_ratio"
    }
    fn factor_type(&self) -> FactorType {
        FactorType::Trade
    }
    fn reset(&mut self) {
        self.window.clear();
        self.buy_count = 0;
        self.last_price = None;
        self.last_value = None;
    }
    fn is_ready(&self) -> bool {
        self.last_value.is_some()
    }
    fn get_value(&self) -> Option<f64> {
        self.last_value
    }
}

impl TradeFactor for AggressorRatioFactor {
    fn compute_batch(&self, trades: &[Trade], compute_ts: &[i64]) -> Vec<Option<f64>> {
        if trades.is_empty() || compute_ts.is_empty() {
            return vec![];
        }

        let mut results: Vec<Option<f64>> = Vec::with_capacity(compute_ts.len());
        let mut window: VecDeque<(i64, bool)> = VecDeque::new();
        let mut buy_count: usize = 0;
        let mut trade_idx: usize = 0;
        let mut last_price: Option<f64> = None;

        for &ts in compute_ts {
            let cutoff = ts - self.window_ms;

            while trade_idx < trades.len() && trades[trade_idx].ts_ms <= ts {
                let price = trades[trade_idx].price;
                let is_buy = match last_price {
                    Some(prev) if price > prev => true,
                    Some(prev) if price < prev => false,
                    _ => true,
                };
                window.push_back((trades[trade_idx].ts_ms, is_buy));
                if is_buy {
                    buy_count += 1;
                }
                last_price = Some(price);
                trade_idx += 1;
            }

            while let Some(&(old_ts, is_buy)) = window.front() {
                if old_ts < cutoff {
                    if is_buy {
                        buy_count -= 1;
                    }
                    window.pop_front();
                } else {
                    break;
                }
            }

            let total = window.len();
            results.push(if total >= self.min_trades {
                Some(buy_count as f64 / total as f64)
            } else {
                None
            });
        }
        results
    }

    fn on_trade(&mut self, trade: &Trade) -> Option<f64> {
        let is_buy = match self.last_price {
            Some(prev) if trade.price > prev => true,
            Some(prev) if trade.price < prev => false,
            _ => true,
        };

        self.window.push_back((trade.ts_ms, is_buy));
        if is_buy {
            self.buy_count += 1;
        }
        self.last_price = Some(trade.price);

        let cutoff = trade.ts_ms - self.window_ms;
        while let Some(&(old_ts, is_buy)) = self.window.front() {
            if old_ts < cutoff {
                if is_buy {
                    self.buy_count -= 1;
                }
                self.window.pop_front();
            } else {
                break;
            }
        }

        let total = self.window.len();
        if total >= self.min_trades {
            let ratio = self.buy_count as f64 / total as f64;
            self.last_value = Some(ratio);
            self.last_value
        } else {
            None
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn make_trades(count: usize, interval_ms: i64, start_ms: i64) -> Vec<Trade> {
        (0..count)
            .map(|i| Trade::new(start_ms + i as i64 * interval_ms, 10.0, 100))
            .collect()
    }

    #[test]
    fn test_trade_rate_batch() {
        // 20 trades in 10 seconds = 2 trades/sec
        let trades = make_trades(20, 500, 1000);
        let compute_ts: Vec<i64> = (1000..=11000).step_by(1000).collect();

        let tr = TradeRateFactor::new(20_000, 5);
        let results = tr.compute_batch(&trades, &compute_ts);

        // First compute_ts at 1000ms: only 1 trade, should be None (<5 min)
        assert!(results[0].is_none());
        // At 11000ms: all 20 trades in 20s window = 1.0 trade/sec
        if let Some(last) = results.last().unwrap() {
            assert!(*last > 0.0);
        }
    }

    #[test]
    fn test_trade_rate_on_trade() {
        let mut tr = TradeRateFactor::new(20_000, 3);

        // First two trades: not enough for min_trades
        assert!(tr.on_trade(&Trade::new(1000, 10.0, 100)).is_none());
        assert!(tr.on_trade(&Trade::new(1500, 10.0, 100)).is_none());
        // Third trade: meets min_trades
        let result = tr.on_trade(&Trade::new(2000, 10.0, 100));
        assert!(result.is_some());
        // 3 trades in 20s window = 0.15 trades/sec
        assert!((result.unwrap() - 0.15).abs() < 0.01);
    }

    #[test]
    fn test_trade_rate_batch_vs_incremental_equivalence() {
        // Generate 100 trades over 50 seconds
        let trades: Vec<Trade> = (0..100)
            .map(|i| Trade::new(i * 500, 10.0, 100))
            .collect();

        let compute_ts: Vec<i64> = (0..100)
            .map(|i| i * 500)
            .step_by(2)
            .collect();

        // Batch mode
        let tr_batch = TradeRateFactor::new(20_000, 5);
        let batch_results = tr_batch.compute_batch(&trades, &compute_ts);

        // Incremental mode: feed trades one by one, record at compute_ts
        let mut tr_inc = TradeRateFactor::new(20_000, 5);
        let mut inc_results: Vec<Option<f64>> = Vec::new();
        let mut trade_idx: usize = 0;

        for &ts in &compute_ts {
            // Feed all trades up to this timestamp
            while trade_idx < trades.len() && trades[trade_idx].ts_ms <= ts {
                tr_inc.on_trade(&trades[trade_idx]);
                trade_idx += 1;
            }
            // Compute
            inc_results.push(tr_inc.last_value);
        }

        // Compare
        for (i, (b, inc)) in batch_results.iter().zip(inc_results.iter()).enumerate() {
            match (b, inc) {
                (Some(bv), Some(iv)) => {
                    assert!(
                        (bv - iv).abs() < 0.001,
                        "Mismatch at compute_ts {}: batch={:.6} vs inc={:.6}",
                        compute_ts[i],
                        bv,
                        iv
                    );
                }
                (None, None) => {}
                _ => panic!(
                    "None/Some mismatch at compute_ts {}: batch={:?} vs inc={:?}",
                    compute_ts[i], b, inc
                ),
            }
        }
    }

    #[test]
    fn test_large_trade_ratio_uniform_sizes() {
        // All trades same size → ratio ≈ 0 (none exceed 2x mean)
        let trades = make_trades(20, 500, 1000);
        let compute_ts: Vec<i64> = (1000..=11000).step_by(1000).collect();

        let ltr = LargeTradeRatioFactor::new(20_000, 5, 2.0);
        let results = ltr.compute_batch(&trades, &compute_ts);

        // First compute_ts: only 1 trade, < min_trades (5) → None
        assert!(results[0].is_none());
        // After enough trades, ratio should be near 0 (uniform sizes)
        if let Some(last) = results.last().unwrap() {
            assert!(*last < 0.1, "Uniform sizes should have low large-trade ratio, got {}", *last);
        }
    }

    #[test]
    fn test_large_trade_ratio_spike() {
        // Mix of small and large trades
        let mut trades = make_trades(18, 500, 1000); // size=100 each
        // Add 2 spike trades with 10x size
        trades.push(Trade::new(10000, 10.0, 1000));
        trades.push(Trade::new(10500, 10.0, 1000));
        trades.sort_by_key(|t| t.ts_ms);

        let compute_ts: Vec<i64> = (1000..=11000).step_by(1000).collect();

        let ltr = LargeTradeRatioFactor::new(20_000, 5, 2.0);
        let results = ltr.compute_batch(&trades, &compute_ts);

        // Last compute_ts includes both spike trades → ratio should be > 0
        if let Some(last) = results.last().unwrap() {
            assert!(*last > 0.0, "Should detect large trades, got {}", *last);
        }
    }

    #[test]
    fn test_large_trade_ratio_batch_vs_incremental() {
        let mut trades: Vec<Trade> = (0..80)
            .map(|i| Trade::new(i * 500, 10.0, 100))
            .collect();
        // Add some spike trades
        trades.push(Trade::new(20000, 10.0, 500));
        trades.push(Trade::new(25000, 10.0, 600));
        trades.push(Trade::new(35000, 10.0, 500));
        trades.push(Trade::new(40000, 10.0, 550));
        trades.sort_by_key(|t| t.ts_ms);

        let compute_ts: Vec<i64> = trades.iter().map(|t| t.ts_ms).collect();

        // Batch
        let ltr_batch = LargeTradeRatioFactor::new(20_000, 5, 2.0);
        let batch_results = ltr_batch.compute_batch(&trades, &compute_ts);

        // Incremental
        let mut ltr_inc = LargeTradeRatioFactor::new(20_000, 5, 2.0);
        let mut inc_results: Vec<Option<f64>> = Vec::new();
        let mut trade_idx: usize = 0;

        for &ts in &compute_ts {
            while trade_idx < trades.len() && trades[trade_idx].ts_ms <= ts {
                ltr_inc.on_trade(&trades[trade_idx]);
                trade_idx += 1;
            }
            inc_results.push(ltr_inc.last_value);
        }

        for (i, (b, inc)) in batch_results.iter().zip(inc_results.iter()).enumerate() {
            match (b, inc) {
                (Some(bv), Some(iv)) => {
                    assert!(
                        (bv - iv).abs() < 0.001,
                        "Mismatch at compute_ts {}: batch={:.6} vs inc={:.6}",
                        compute_ts[i], bv, iv
                    );
                }
                (None, None) => {}
                _ => panic!(
                    "None/Some mismatch at compute_ts {}: batch={:?} vs inc={:?}",
                    compute_ts[i], b, inc
                ),
            }
        }
    }

    fn make_uptrend_trades(count: usize, start_ms: i64) -> Vec<Trade> {
        (0..count)
            .map(|i| Trade::new(start_ms + i as i64 * 500, 100.0 + i as f64 * 0.1, 100))
            .collect()
    }

    fn make_downtrend_trades(count: usize, start_ms: i64) -> Vec<Trade> {
        (0..count)
            .map(|i| Trade::new(start_ms + i as i64 * 500, 200.0 - i as f64 * 0.1, 100))
            .collect()
    }

    #[test]
    fn test_aggressor_ratio_uptrend() {
        // Uptrend: nearly all trades are buyer-initiated → ratio close to 1.0
        let trades = make_uptrend_trades(30, 1000);
        let compute_ts: Vec<i64> = (1000..=16000).step_by(1000).collect();

        let ar = AggressorRatioFactor::new(20_000, 5);
        let results = ar.compute_batch(&trades, &compute_ts);

        // Last compute_ts includes all 30 trades → ratio should be high
        if let Some(last) = results.last().unwrap() {
            assert!(*last > 0.9, "Uptrend should have high buy ratio, got {}", *last);
        }
    }

    #[test]
    fn test_aggressor_ratio_downtrend() {
        // Downtrend: nearly all trades are seller-initiated → ratio close to 0.0
        let trades = make_downtrend_trades(30, 1000);
        let compute_ts: Vec<i64> = (1000..=16000).step_by(1000).collect();

        let ar = AggressorRatioFactor::new(20_000, 5);
        let results = ar.compute_batch(&trades, &compute_ts);

        if let Some(last) = results.last().unwrap() {
            assert!(*last < 0.1, "Downtrend should have low buy ratio, got {}", *last);
        }
    }

    #[test]
    fn test_aggressor_ratio_batch_vs_incremental() {
        // Mix up and down moves for realistic test
        let mut prices = vec![100.0];
        let steps = [0.05, 0.03, -0.02, 0.01, -0.04, 0.06, -0.03, 0.02];
        for i in 0..90 {
            let prev = prices.last().unwrap();
            prices.push(prev + steps[i % steps.len()]);
        }

        let trades: Vec<Trade> = prices
            .iter()
            .enumerate()
            .map(|(i, &p)| Trade::new(i as i64 * 500, p, 100))
            .collect();

        let compute_ts: Vec<i64> = trades.iter().map(|t| t.ts_ms).collect();

        // Batch
        let ar_batch = AggressorRatioFactor::new(20_000, 5);
        let batch_results = ar_batch.compute_batch(&trades, &compute_ts);

        // Incremental
        let mut ar_inc = AggressorRatioFactor::new(20_000, 5);
        let mut inc_results: Vec<Option<f64>> = Vec::new();
        let mut trade_idx: usize = 0;

        for &ts in &compute_ts {
            while trade_idx < trades.len() && trades[trade_idx].ts_ms <= ts {
                ar_inc.on_trade(&trades[trade_idx]);
                trade_idx += 1;
            }
            inc_results.push(ar_inc.last_value);
        }

        for (i, (b, inc)) in batch_results.iter().zip(inc_results.iter()).enumerate() {
            match (b, inc) {
                (Some(bv), Some(iv)) => {
                    assert!(
                        (bv - iv).abs() < 0.001,
                        "Mismatch at compute_ts {}: batch={:.6} vs inc={:.6}",
                        compute_ts[i], bv, iv
                    );
                }
                (None, None) => {}
                _ => panic!(
                    "None/Some mismatch at compute_ts {}: batch={:?} vs inc={:?}",
                    compute_ts[i], b, inc
                ),
            }
        }
    }
}
