// rust/src/factor/bar_factors.rs
//
// Bar factor implementations for the unified Factor architecture.
// Each struct implements BarFactor with both batch and incremental modes.

use std::collections::VecDeque;

use super::data::Bar;
use super::factor_trait::{BarFactor, Factor, FactorType};

// ═══════════════════════════════════════════════════════════════════════════
// EMA Factor
// ═══════════════════════════════════════════════════════════════════════════

pub struct EmaFactor {
    period: usize,
    alpha: f64,
    current: Option<f64>,
    count: usize,
}

impl EmaFactor {
    pub fn new(period: usize) -> Self {
        Self {
            period,
            alpha: 2.0 / (period as f64 + 1.0),
            current: None,
            count: 0,
        }
    }
}

impl Factor for EmaFactor {
    fn name(&self) -> &str {
        "ema"
    }
    fn factor_type(&self) -> FactorType {
        FactorType::Bar
    }
    fn reset(&mut self) {
        self.current = None;
        self.count = 0;
    }
    fn is_ready(&self) -> bool {
        self.count >= self.period
    }
    fn get_value(&self) -> Option<f64> {
        self.current
    }
}

impl BarFactor for EmaFactor {
    fn compute_batch(&self, bars: &[Bar]) -> Vec<Option<f64>> {
        if bars.is_empty() || self.period == 0 {
            return vec![];
        }
        let alpha = self.alpha;
        let mut results = Vec::with_capacity(bars.len());
        let mut value: Option<f64> = None;
        let mut count: usize = 0;

        for bar in bars {
            value = Some(match value {
                None => bar.close,
                Some(prev) => alpha * bar.close + (1.0 - alpha) * prev,
            });
            count += 1;
            results.push(if count >= self.period { value } else { None });
        }
        results
    }

    fn update(&mut self, bar: &Bar) -> Option<f64> {
        self.current = Some(match self.current {
            None => bar.close,
            Some(prev) => self.alpha * bar.close + (1.0 - self.alpha) * prev,
        });
        self.count += 1;
        if self.count >= self.period {
            self.current
        } else {
            None
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Relative Volume Factor
// ═══════════════════════════════════════════════════════════════════════════

pub struct RelVolFactor {
    lookback: usize,
    volumes: VecDeque<i64>,
    count: usize,
}

impl RelVolFactor {
    pub fn new(lookback: usize) -> Self {
        Self {
            lookback,
            volumes: VecDeque::with_capacity(lookback),
            count: 0,
        }
    }
}

impl Factor for RelVolFactor {
    fn name(&self) -> &str {
        "relative_volume"
    }
    fn factor_type(&self) -> FactorType {
        FactorType::Bar
    }
    fn reset(&mut self) {
        self.volumes.clear();
        self.count = 0;
    }
    fn is_ready(&self) -> bool {
        self.count >= self.lookback
    }
}

impl BarFactor for RelVolFactor {
    fn compute_batch(&self, bars: &[Bar]) -> Vec<Option<f64>> {
        if bars.is_empty() || self.lookback == 0 {
            return vec![];
        }
        let mut results = Vec::with_capacity(bars.len());
        let mut vols: VecDeque<i64> = VecDeque::with_capacity(self.lookback);
        let mut count: usize = 0;

        for bar in bars {
            vols.push_back(bar.volume);
            if vols.len() > self.lookback {
                vols.pop_front();
            }
            count += 1;
            if count < self.lookback {
                results.push(None);
            } else {
                let avg = vols.iter().sum::<i64>() as f64 / vols.len() as f64;
                let rel_vol = if avg > 0.0 {
                    bar.volume as f64 / avg
                } else {
                    0.0
                };
                results.push(Some(rel_vol));
            }
        }
        results
    }

    fn update(&mut self, bar: &Bar) -> Option<f64> {
        self.volumes.push_back(bar.volume);
        if self.volumes.len() > self.lookback {
            self.volumes.pop_front();
        }
        self.count += 1;
        if self.count < self.lookback {
            None
        } else {
            let avg = self.volumes.iter().sum::<i64>() as f64 / self.volumes.len() as f64;
            let rel_vol = if avg > 0.0 {
                bar.volume as f64 / avg
            } else {
                0.0
            };
            Some(rel_vol)
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Price Direction Factor
// ═══════════════════════════════════════════════════════════════════════════

pub struct PriceDirectionFactor;

impl Factor for PriceDirectionFactor {
    fn name(&self) -> &str {
        "price_direction"
    }
    fn factor_type(&self) -> FactorType {
        FactorType::Bar
    }
    fn reset(&mut self) {}
    fn is_ready(&self) -> bool {
        true
    }
}

impl BarFactor for PriceDirectionFactor {
    fn compute_batch(&self, bars: &[Bar]) -> Vec<Option<f64>> {
        bars.iter()
            .map(|bar| {
                if bar.high > bar.low {
                    let d = (bar.close - bar.low) / (bar.high - bar.low);
                    Some(d.clamp(0.0, 1.0))
                } else {
                    None
                }
            })
            .collect()
    }

    fn update(&mut self, bar: &Bar) -> Option<f64> {
        if bar.high > bar.low {
            let d = (bar.close - bar.low) / (bar.high - bar.low);
            Some(d.clamp(0.0, 1.0))
        } else {
            None
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap Percent Factor
// ═══════════════════════════════════════════════════════════════════════════

pub struct GapPercentFactor {
    prev_close: Option<f64>,
}

impl GapPercentFactor {
    pub fn new() -> Self {
        Self { prev_close: None }
    }
}

impl Factor for GapPercentFactor {
    fn name(&self) -> &str {
        "gap_percent"
    }
    fn factor_type(&self) -> FactorType {
        FactorType::Bar
    }
    fn reset(&mut self) {
        self.prev_close = None;
    }
    fn is_ready(&self) -> bool {
        self.prev_close.is_some()
    }
}

impl BarFactor for GapPercentFactor {
    fn compute_batch(&self, bars: &[Bar]) -> Vec<Option<f64>> {
        let mut results = Vec::with_capacity(bars.len());
        let mut prev_close: Option<f64> = None;

        for bar in bars {
            let gap = prev_close.and_then(|prev| {
                if prev > 0.0 {
                    Some((bar.open - prev) / prev * 100.0)
                } else {
                    None
                }
            });
            results.push(gap);
            prev_close = Some(bar.close);
        }
        results
    }

    fn update(&mut self, bar: &Bar) -> Option<f64> {
        let gap = self.prev_close.and_then(|prev| {
            if prev > 0.0 {
                Some((bar.open - prev) / prev * 100.0)
            } else {
                None
            }
        });
        self.prev_close = Some(bar.close);
        gap
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Volume Acceleration Factor
// ═══════════════════════════════════════════════════════════════════════════
//
// Formula: (vol_prev_short / vol_older_long) / (vol_recent / vol_prev_short)
// Measures momentum sustainability — is volume acceleration increasing or decaying?

pub struct VolAccelFactor {
    short_window: usize,
    long_window: usize,
    warmup: usize,
    volumes: VecDeque<i64>,
}

impl VolAccelFactor {
    pub fn new(short_window: usize, long_window: usize) -> Self {
        let warmup = 2 * short_window + long_window;
        Self {
            short_window,
            long_window,
            warmup,
            volumes: VecDeque::with_capacity(warmup + 1),
        }
    }
}

impl Factor for VolAccelFactor {
    fn name(&self) -> &str {
        "volume_acceleration"
    }
    fn factor_type(&self) -> FactorType {
        FactorType::Bar
    }
    fn reset(&mut self) {
        self.volumes.clear();
    }
    fn is_ready(&self) -> bool {
        self.volumes.len() > self.warmup
    }
}

impl BarFactor for VolAccelFactor {
    fn compute_batch(&self, bars: &[Bar]) -> Vec<Option<f64>> {
        let mut results = Vec::with_capacity(bars.len());

        for i in 0..bars.len() {
            if i < self.warmup {
                results.push(None);
                continue;
            }

            // vol_recent: short_window bars ending at i (exclusive, matches update())
            let vol_recent = avg_vol(bars, i.saturating_sub(self.short_window), self.short_window);

            // vol_prev_short: short_window bars ending at i - short_window
            let prev_start = i.saturating_sub(2 * self.short_window);
            let vol_prev_short = avg_vol(bars, prev_start, self.short_window);

            // vol_older_long: long_window bars ending at i - 2*short_window
            let older_start = i.saturating_sub(2 * self.short_window + self.long_window);
            let vol_older_long = avg_vol(bars, older_start, self.long_window);

            let accel = if vol_recent > 0.0 && vol_prev_short > 0.0 && vol_older_long > 0.0 {
                let accel_prev = vol_prev_short / vol_older_long;
                let accel_recent = vol_recent / vol_prev_short;
                if accel_prev > 0.0 {
                    accel_recent / accel_prev
                } else {
                    0.0
                }
            } else {
                0.0
            };

            results.push(Some(accel));
        }
        results
    }

    fn update(&mut self, bar: &Bar) -> Option<f64> {
        self.volumes.push_back(bar.volume);
        if self.volumes.len() > self.warmup + 1 {
            self.volumes.pop_front();
        }

        if self.volumes.len() <= self.warmup {
            return None;
        }

        let total = self.volumes.len();
        let vol_recent = deque_avg(&self.volumes, total - 1 - self.short_window, self.short_window);
        let vol_prev_short = deque_avg(&self.volumes, total - 1 - 2 * self.short_window, self.short_window);
        let vol_older_long = deque_avg(&self.volumes, total - 1 - 2 * self.short_window - self.long_window, self.long_window);

        let accel = if vol_recent > 0.0 && vol_prev_short > 0.0 && vol_older_long > 0.0 {
            let accel_prev = vol_prev_short / vol_older_long;
            let accel_recent = vol_recent / vol_prev_short;
            if accel_prev > 0.0 {
                accel_recent / accel_prev
            } else {
                0.0
            }
        } else {
            0.0
        };

        Some(accel)
    }
}

fn avg_vol(bars: &[Bar], start: usize, window: usize) -> f64 {
    let end = (start + window).min(bars.len());
    if end <= start {
        return 0.0;
    }
    let sum: i64 = bars[start..end].iter().map(|b| b.volume).sum();
    sum as f64 / (end - start) as f64
}

fn deque_avg(deque: &VecDeque<i64>, start: usize, count: usize) -> f64 {
    if count == 0 || start >= deque.len() {
        return 0.0;
    }
    let end = (start + count).min(deque.len());
    let sum: i64 = deque.range(start..end).sum();
    sum as f64 / (end - start) as f64
}

// ═══════════════════════════════════════════════════════════════════════════
// VWAP Deviation Factor
// ═══════════════════════════════════════════════════════════════════════════
//
// Formula: (close - vwap) / vwap * 100
// VWAP = sum(close_i * volume_i) / sum(volume_i) over the period window.
// Positive = price above VWAP (premium), Negative = price below (discount).

pub struct VwapDeviationFactor {
    period: usize,
    // Incremental state
    pv_deque: VecDeque<f64>,  // close * volume per bar
    vol_deque: VecDeque<i64>,  // volume per bar
    running_pv: f64,
    running_vol: i64,
    last_value: Option<f64>,
}

impl VwapDeviationFactor {
    pub fn new(period: usize) -> Self {
        Self {
            period,
            pv_deque: VecDeque::with_capacity(period),
            vol_deque: VecDeque::with_capacity(period),
            running_pv: 0.0,
            running_vol: 0,
            last_value: None,
        }
    }
}

impl Factor for VwapDeviationFactor {
    fn name(&self) -> &str {
        "vwap_deviation"
    }
    fn factor_type(&self) -> FactorType {
        FactorType::Bar
    }
    fn reset(&mut self) {
        self.pv_deque.clear();
        self.vol_deque.clear();
        self.running_pv = 0.0;
        self.running_vol = 0;
        self.last_value = None;
    }
    fn is_ready(&self) -> bool {
        self.pv_deque.len() >= self.period
    }
    fn get_value(&self) -> Option<f64> {
        self.last_value
    }
}

impl BarFactor for VwapDeviationFactor {
    fn compute_batch(&self, bars: &[Bar]) -> Vec<Option<f64>> {
        if bars.is_empty() || self.period == 0 {
            return vec![];
        }
        let mut results = Vec::with_capacity(bars.len());
        let mut pv_dq: VecDeque<f64> = VecDeque::with_capacity(self.period);
        let mut vol_dq: VecDeque<i64> = VecDeque::with_capacity(self.period);
        let mut pv_sum: f64 = 0.0;
        let mut vol_sum: i64 = 0;

        for bar in bars {
            let contrib = bar.close * bar.volume as f64;
            pv_sum += contrib;
            vol_sum += bar.volume;
            pv_dq.push_back(contrib);
            vol_dq.push_back(bar.volume);

            if pv_dq.len() > self.period {
                pv_sum -= pv_dq.pop_front().unwrap();
                vol_sum -= vol_dq.pop_front().unwrap();
            }

            if pv_dq.len() >= self.period && vol_sum > 0 {
                let vwap = pv_sum / vol_sum as f64;
                results.push(Some((bar.close - vwap) / vwap * 100.0));
            } else {
                results.push(None);
            }
        }
        results
    }

    fn update(&mut self, bar: &Bar) -> Option<f64> {
        let contrib = bar.close * bar.volume as f64;
        self.pv_deque.push_back(contrib);
        self.vol_deque.push_back(bar.volume);
        self.running_pv += contrib;
        self.running_vol += bar.volume;

        if self.pv_deque.len() > self.period {
            self.running_pv -= self.pv_deque.pop_front().unwrap();
            self.running_vol -= self.vol_deque.pop_front().unwrap();
        }

        if self.pv_deque.len() >= self.period && self.running_vol > 0 {
            let vwap = self.running_pv / self.running_vol as f64;
            self.last_value = Some((bar.close - vwap) / vwap * 100.0);
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

    fn make_bar(close: f64, volume: i64) -> Bar {
        Bar::new(0, close - 1.0, close + 1.0, close - 2.0, close, volume)
    }

    #[test]
    fn test_ema_warmup() {
        let mut ema = EmaFactor::new(3);
        assert!(!ema.is_ready());
        assert!(ema.update(&make_bar(10.0, 100)).is_none());
        assert!(ema.update(&make_bar(11.0, 100)).is_none());
        assert!(ema.update(&make_bar(12.0, 100)).is_some());
        assert!(ema.is_ready());
    }

    #[test]
    fn test_ema_batch() {
        let bars = vec![make_bar(10.0, 100), make_bar(11.0, 100), make_bar(12.0, 100)];
        let ema = EmaFactor::new(3);
        let results = ema.compute_batch(&bars);
        assert_eq!(results.len(), 3);
        assert!(results[0].is_none());
        assert!(results[1].is_none());
        assert!(results[2].is_some());
    }

    #[test]
    fn test_ema_batch_vs_incremental() {
        let bars = vec![
            make_bar(10.0, 100),
            make_bar(10.5, 150),
            make_bar(11.0, 200),
            make_bar(10.8, 120),
            make_bar(11.2, 180),
        ];

        // Batch
        let ema_batch = EmaFactor::new(3);
        let batch_results = ema_batch.compute_batch(&bars);

        // Incremental
        let mut ema_inc = EmaFactor::new(3);
        let inc_results: Vec<Option<f64>> = bars.iter().map(|b| ema_inc.update(b)).collect();

        for (i, (b, inc)) in batch_results.iter().zip(inc_results.iter()).enumerate() {
            assert_eq!(b, inc, "Mismatch at bar {}: batch={:?} vs inc={:?}", i, b, inc);
        }
    }

    #[test]
    fn test_rel_vol_warmup() {
        let mut rv = RelVolFactor::new(2);
        assert!(rv.update(&make_bar(10.0, 100)).is_none());
        let result = rv.update(&make_bar(10.5, 200));
        assert!(result.is_some());
        // avg=(100+200)/2=150, rel_vol=200/150≈1.333
        assert!((result.unwrap() - 1.333).abs() < 0.01);
    }

    #[test]
    fn test_price_direction() {
        let bar = Bar::new(0, 10.0, 12.0, 8.0, 10.0, 100);
        let mut pd = PriceDirectionFactor;
        let result = pd.update(&bar);
        // (10-8)/(12-8) = 0.5
        assert!((result.unwrap() - 0.5).abs() < 0.001);

        // Flat bar
        let flat = Bar::new(0, 10.0, 10.0, 10.0, 10.0, 50);
        assert!(pd.update(&flat).is_none());
    }

    #[test]
    fn test_gap_percent() {
        let bar1 = Bar::new(0, 10.0, 11.0, 9.0, 10.5, 100);
        let bar2 = Bar::new(0, 11.0, 12.0, 10.0, 11.5, 150);

        let mut gp = GapPercentFactor::new();
        assert!(gp.update(&bar1).is_none()); // No prev close
        let gap = gp.update(&bar2);
        // (11.0-10.5)/10.5*100 ≈ 4.76%
        assert!((gap.unwrap() - 4.76).abs() < 0.1);
    }

    #[test]
    fn test_vol_accel_warmup() {
        let va = VolAccelFactor::new(5, 15);
        let bars: Vec<Bar> = (0..40).map(|i| make_bar(10.0, 100 + i * 10)).collect();
        let results = va.compute_batch(&bars);
        assert_eq!(results.len(), 40);
        // Warmup: 2*short_window + long_window = 2*5+15 = 25
        assert!(results[24].is_none());
        assert!(results[25].is_some());
    }

    #[test]
    fn test_vol_accel_batch_vs_incremental() {
        let short = 5;
        let long = 15;
        let bars: Vec<Bar> = (0..50).map(|i| make_bar(10.0, 100 + i * 10)).collect();

        // Batch
        let va_batch = VolAccelFactor::new(short, long);
        let batch_results = va_batch.compute_batch(&bars);

        // Incremental
        let mut va_inc = VolAccelFactor::new(short, long);
        let inc_results: Vec<Option<f64>> = bars.iter().map(|b| va_inc.update(b)).collect();

        assert_eq!(batch_results.len(), inc_results.len());
        for (i, (b, inc)) in batch_results.iter().zip(inc_results.iter()).enumerate() {
            match (b, inc) {
                (Some(bv), Some(iv)) => {
                    assert!(
                        (bv - iv).abs() < 0.001,
                        "Mismatch at bar {}: batch={} vs inc={}", i, bv, iv
                    );
                }
                (None, None) => {}
                _ => panic!(
                    "None/Some mismatch at bar {}: batch={:?} inc={:?}",
                    i, b, inc
                ),
            }
        }
    }

    #[test]
    fn test_vwap_deviation_basic() {
        // 3 bars: close=10@100v, close=12@200v, close=11@100v
        // VWAP = (10*100+12*200+11*100)/(100+200+100) = 4500/400 = 11.25
        // deviation = (11-11.25)/11.25*100 = -2.222...
        let bars = vec![
            Bar::new(0, 9.0, 11.0, 8.0, 10.0, 100),
            Bar::new(0, 11.0, 13.0, 10.0, 12.0, 200),
            Bar::new(0, 10.0, 12.0, 9.0, 11.0, 100),
        ];

        let vd = VwapDeviationFactor::new(3);
        let results = vd.compute_batch(&bars);
        assert_eq!(results.len(), 3);
        assert!(results[0].is_none());
        assert!(results[1].is_none());
        let dev = results[2].unwrap();
        let expected = (11.0 - 11.25) / 11.25 * 100.0;
        assert!((dev - expected).abs() < 0.001);
    }

    #[test]
    fn test_vwap_deviation_warmup() {
        let mut vd = VwapDeviationFactor::new(3);
        assert!(vd.update(&make_bar(10.0, 100)).is_none());
        assert!(vd.update(&make_bar(11.0, 100)).is_none());
        assert!(vd.update(&make_bar(12.0, 100)).is_some());
        assert!(vd.is_ready());
    }

    #[test]
    fn test_vwap_deviation_batch_vs_incremental() {
        let bars: Vec<Bar> = (0..30)
            .map(|i| {
                let close = 100.0 + i as f64 * 0.5;
                let volume = 100 + (i % 5) * 50;
                Bar::new(i * 60_000, close - 0.5, close + 0.8, close - 1.0, close, volume)
            })
            .collect();

        let vd_batch = VwapDeviationFactor::new(5);
        let batch_results = vd_batch.compute_batch(&bars);

        let mut vd_inc = VwapDeviationFactor::new(5);
        let inc_results: Vec<Option<f64>> = bars.iter().map(|b| vd_inc.update(b)).collect();

        assert_eq!(batch_results.len(), inc_results.len());
        for (i, (b, inc)) in batch_results.iter().zip(inc_results.iter()).enumerate() {
            match (b, inc) {
                (Some(bv), Some(iv)) => {
                    assert!(
                        (bv - iv).abs() < 0.001,
                        "Mismatch at bar {}: batch={} vs inc={}", i, bv, iv
                    );
                }
                (None, None) => {}
                _ => panic!(
                    "None/Some mismatch at bar {}: batch={:?} inc={:?}",
                    i, b, inc
                ),
            }
        }
    }
}
