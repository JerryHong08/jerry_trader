// rust/src/bars.rs
//
// High-performance bar builder for tick-to-OHLCV aggregation.
// Maintains per-ticker, per-timeframe rolling bar state with
// session-aware boundaries (premarket / regular / afterhours).
//
// Exposed to Python as `jerry_trader._rust.BarBuilder` via PyO3.

use pyo3::prelude::*;
use pyo3::Py;
use std::collections::HashMap;

// ── Timeframe ───────────────────────────────────────────────────────

/// Supported bar timeframes with their duration in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Timeframe {
    Sec10,
    Min1,
    Min5,
    Min15,
    Min30,
    Hour1,
    Hour4,
    Day1,
    Week1,
}

impl Timeframe {
    /// Duration of this timeframe in milliseconds.
    pub fn duration_ms(&self) -> i64 {
        match self {
            Timeframe::Sec10 => 10_000,
            Timeframe::Min1 => 60_000,
            Timeframe::Min5 => 300_000,
            Timeframe::Min15 => 900_000,
            Timeframe::Min30 => 1_800_000,
            Timeframe::Hour1 => 3_600_000,
            Timeframe::Hour4 => 14_400_000,
            Timeframe::Day1 => 86_400_000,
            Timeframe::Week1 => 604_800_000,
        }
    }

    /// All supported timeframes in order.
    pub fn all() -> &'static [Timeframe] {
        &[
            Timeframe::Sec10,
            Timeframe::Min1,
            Timeframe::Min5,
            Timeframe::Min15,
            Timeframe::Min30,
            Timeframe::Hour1,
            Timeframe::Hour4,
            Timeframe::Day1,
            Timeframe::Week1,
        ]
    }

    /// Parse from a string label (e.g. "1m", "5m", "10s").
    pub fn from_label(s: &str) -> Option<Timeframe> {
        match s {
            "10s" => Some(Timeframe::Sec10),
            "1m" => Some(Timeframe::Min1),
            "5m" => Some(Timeframe::Min5),
            "15m" => Some(Timeframe::Min15),
            "30m" => Some(Timeframe::Min30),
            "1h" => Some(Timeframe::Hour1),
            "4h" => Some(Timeframe::Hour4),
            "1d" => Some(Timeframe::Day1),
            "1w" => Some(Timeframe::Week1),
            _ => None,
        }
    }

    /// String label for this timeframe.
    pub fn label(&self) -> &'static str {
        match self {
            Timeframe::Sec10 => "10s",
            Timeframe::Min1 => "1m",
            Timeframe::Min5 => "5m",
            Timeframe::Min15 => "15m",
            Timeframe::Min30 => "30m",
            Timeframe::Hour1 => "1h",
            Timeframe::Hour4 => "4h",
            Timeframe::Day1 => "1d",
            Timeframe::Week1 => "1w",
        }
    }
}

// ── Session ─────────────────────────────────────────────────────────

/// US market session classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Session {
    Premarket,  // 04:00 – 09:30 ET
    Regular,    // 09:30 – 16:00 ET
    Afterhours, // 16:00 – 20:00 ET
    Closed,     // Outside all sessions
}

impl Session {
    pub fn label(&self) -> &'static str {
        match self {
            Session::Premarket => "premarket",
            Session::Regular => "regular",
            Session::Afterhours => "afterhours",
            Session::Closed => "closed",
        }
    }
}

// ── SessionCalendar ─────────────────────────────────────────────────

/// Session boundary calculator.
///
/// All timestamps are in **milliseconds since Unix epoch** and assumed to be
/// in US/Eastern (the caller is responsible for conversion — in practice
/// Polygon timestamps are already Eastern-aligned for equity markets).
///
/// For simplicity we store boundaries as ms-offsets from midnight.
/// We deliberately do NOT handle DST or holidays here — the Python service
/// can pass an ET-aligned epoch and skip feeding ticks on market holidays.
pub struct SessionCalendar;

impl SessionCalendar {
    // Session boundaries as ms from midnight ET.
    const PREMARKET_START: i64 = 4 * 3_600_000;           // 04:00
    const REGULAR_START: i64 = 9 * 3_600_000 + 30 * 60_000; // 09:30
    const AFTERHOURS_START: i64 = 16 * 3_600_000;          // 16:00
    const AFTERHOURS_END: i64 = 20 * 3_600_000;            // 20:00
    const DAY_MS: i64 = 86_400_000;

    /// Classify which session a timestamp falls into.
    /// `ts_ms` is ms since Unix epoch (assumed ET-aligned).
    pub fn classify(ts_ms: i64) -> Session {
        let ms_of_day = ts_ms.rem_euclid(Self::DAY_MS);
        if ms_of_day >= Self::PREMARKET_START && ms_of_day < Self::REGULAR_START {
            Session::Premarket
        } else if ms_of_day >= Self::REGULAR_START && ms_of_day < Self::AFTERHOURS_START {
            Session::Regular
        } else if ms_of_day >= Self::AFTERHOURS_START && ms_of_day < Self::AFTERHOURS_END {
            Session::Afterhours
        } else {
            Session::Closed
        }
    }

    /// Returns the ms-epoch of the next session boundary AFTER `ts_ms`.
    /// This is where a bar must be truncated if it would otherwise cross.
    pub fn next_session_boundary(ts_ms: i64) -> i64 {
        let day_start = ts_ms - ts_ms.rem_euclid(Self::DAY_MS);
        let ms_of_day = ts_ms.rem_euclid(Self::DAY_MS);

        if ms_of_day < Self::PREMARKET_START {
            day_start + Self::PREMARKET_START
        } else if ms_of_day < Self::REGULAR_START {
            day_start + Self::REGULAR_START
        } else if ms_of_day < Self::AFTERHOURS_START {
            day_start + Self::AFTERHOURS_START
        } else if ms_of_day < Self::AFTERHOURS_END {
            day_start + Self::AFTERHOURS_END
        } else {
            // After 20:00 → next day premarket at 04:00
            day_start + Self::DAY_MS + Self::PREMARKET_START
        }
    }

    /// Compute the bar-start timestamp for a given timestamp and timeframe.
    ///
    /// For intraday timeframes (≤ 4h): aligns to the start of the current
    /// session, then snaps to the nearest timeframe boundary within that session.
    ///
    /// For daily: aligns to midnight of the day.
    /// For weekly: aligns to Monday midnight.
    pub fn bar_start(ts_ms: i64, tf: Timeframe) -> i64 {
        match tf {
            Timeframe::Week1 => {
                // Align to Monday 00:00 UTC.
                // 1970-01-01 (epoch day 0) was a Thursday → weekday 3 (0=Mon).
                // day_of_week = (days_since_epoch + 3) % 7, where 0 = Monday.
                let day_ms = Self::DAY_MS;
                let days = ts_ms.div_euclid(day_ms);
                let dow = (days + 3).rem_euclid(7); // 0=Mon … 6=Sun
                (days - dow) * day_ms
            }
            Timeframe::Day1 => {
                ts_ms - ts_ms.rem_euclid(Self::DAY_MS)
            }
            _ => {
                // Intraday: align within session.
                let session_start_ms = Self::session_start_of(ts_ms);
                let elapsed = ts_ms - session_start_ms;
                let dur = tf.duration_ms();
                session_start_ms + (elapsed / dur) * dur
            }
        }
    }

    /// Returns the ms-epoch of the start of the session that `ts_ms` falls in.
    fn session_start_of(ts_ms: i64) -> i64 {
        let day_start = ts_ms - ts_ms.rem_euclid(Self::DAY_MS);
        let ms_of_day = ts_ms.rem_euclid(Self::DAY_MS);

        if ms_of_day >= Self::AFTERHOURS_START {
            day_start + Self::AFTERHOURS_START
        } else if ms_of_day >= Self::REGULAR_START {
            day_start + Self::REGULAR_START
        } else if ms_of_day >= Self::PREMARKET_START {
            day_start + Self::PREMARKET_START
        } else {
            // Before 04:00 — belongs to previous day's afterhours (or closed).
            // Snap to previous day afterhours end → treat as "closed" start.
            day_start
        }
    }

    /// Effective bar end: min(bar_start + duration, next_session_boundary).
    /// Bars never cross session boundaries.
    pub fn bar_end(bar_start_ms: i64, tf: Timeframe) -> i64 {
        match tf {
            Timeframe::Day1 | Timeframe::Week1 => {
                // Daily/weekly bars are not session-clipped.
                bar_start_ms + tf.duration_ms()
            }
            _ => {
                let natural_end = bar_start_ms + tf.duration_ms();
                let boundary = Self::next_session_boundary(bar_start_ms);
                natural_end.min(boundary)
            }
        }
    }
}

// ── CompletedBar ────────────────────────────────────────────────────

/// A fully completed OHLCV bar, returned to the Python caller.
#[derive(Debug, Clone)]
pub struct CompletedBar {
    pub ticker: String,
    pub timeframe: &'static str,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub trade_count: u64,
    pub vwap: f64,
    pub bar_start: i64,   // epoch ms
    pub bar_end: i64,     // epoch ms
    pub session: &'static str,
}

impl CompletedBar {
    /// Convert to a Python dict for easy consumption.
    pub fn to_py_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("ticker", &self.ticker)?;
        dict.set_item("timeframe", self.timeframe)?;
        dict.set_item("open", self.open)?;
        dict.set_item("high", self.high)?;
        dict.set_item("low", self.low)?;
        dict.set_item("close", self.close)?;
        dict.set_item("volume", self.volume)?;
        dict.set_item("trade_count", self.trade_count)?;
        dict.set_item("vwap", self.vwap)?;
        dict.set_item("bar_start", self.bar_start)?;
        dict.set_item("bar_end", self.bar_end)?;
        dict.set_item("session", self.session)?;
        Ok(dict.into())
    }
}

// ── BarState (internal rolling bar) ─────────────────────────────────

/// Rolling state for a single bar being built.
#[derive(Debug, Clone)]
struct BarState {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    trade_count: u64,
    vwap_numer: f64,   // Σ(price × size) for VWAP
    vwap_denom: f64,   // Σ(size) for VWAP
    bar_start: i64,    // epoch ms — aligned bar start
    bar_end: i64,      // epoch ms — when this bar completes
    session: Session,
}

impl BarState {
    /// Create a new bar starting with the given trade.
    fn new(price: f64, size: f64, bar_start: i64, bar_end: i64, session: Session) -> Self {
        Self {
            open: price,
            high: price,
            low: price,
            close: price,
            volume: size,
            trade_count: 1,
            vwap_numer: price * size,
            vwap_denom: size,
            bar_start,
            bar_end,
            session,
        }
    }

    /// Update this bar with a new trade.
    #[inline]
    fn update(&mut self, price: f64, size: f64) {
        if price > self.high {
            self.high = price;
        }
        if price < self.low {
            self.low = price;
        }
        self.close = price;
        self.volume += size;
        self.trade_count += 1;
        self.vwap_numer += price * size;
        self.vwap_denom += size;
    }

    /// Compute VWAP for this bar.
    fn vwap(&self) -> f64 {
        if self.vwap_denom > 0.0 {
            self.vwap_numer / self.vwap_denom
        } else {
            self.close
        }
    }

    /// Convert to a CompletedBar.
    fn to_completed(&self, ticker: &str, tf: Timeframe) -> CompletedBar {
        CompletedBar {
            ticker: ticker.to_string(),
            timeframe: tf.label(),
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            trade_count: self.trade_count,
            vwap: self.vwap(),
            bar_start: self.bar_start,
            bar_end: self.bar_end,
            session: self.session.label(),
        }
    }
}

// ── TickerBars (per-ticker state across all timeframes) ─────────────

/// All rolling bar states for a single ticker.
struct TickerBars {
    /// Active bar per timeframe.  `None` = no bar in progress.
    bars: HashMap<Timeframe, BarState>,
}

impl TickerBars {
    fn new() -> Self {
        Self {
            bars: HashMap::with_capacity(8),
        }
    }
}

// ── BarBuilder (#[pyclass]) ─────────────────────────────────────────

/// High-performance bar builder exposed to Python.
///
/// Usage from Python:
/// ```python
/// from jerry_trader._rust import BarBuilder
///
/// builder = BarBuilder()                      # all 8 timeframes
/// builder = BarBuilder(["1m", "5m", "15m"])   # subset
///
/// completed = builder.ingest_trade("AAPL", 150.25, 100.0, 1709820000000)
/// # completed is a list of dicts, one per completed bar (may be empty)
///
/// partial = builder.get_current_bar("AAPL", "1m")
/// # partial is a dict or None
///
/// all_completed = builder.flush()
/// # force-complete all open bars
/// ```
#[pyclass]
pub struct BarBuilder {
    /// Per-ticker rolling state.
    tickers: HashMap<String, TickerBars>,
    /// Which timeframes this builder is tracking.
    timeframes: Vec<Timeframe>,
}

#[pymethods]
impl BarBuilder {
    /// Create a new BarBuilder.
    ///
    /// `timeframes`: optional list of timeframe labels (e.g. ["1m", "5m"]).
    ///               If None or empty, all 8 timeframes are used.
    #[new]
    #[pyo3(signature = (timeframes=None))]
    fn new(timeframes: Option<Vec<String>>) -> PyResult<Self> {
        let tfs = match timeframes {
            Some(labels) if !labels.is_empty() => {
                let mut parsed = Vec::with_capacity(labels.len());
                for label in &labels {
                    match Timeframe::from_label(label) {
                        Some(tf) => parsed.push(tf),
                        None => {
                            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                                "Unknown timeframe: '{}'. Valid: 10s, 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w",
                                label
                            )));
                        }
                    }
                }
                parsed
            }
            _ => Timeframe::all().to_vec(),
        };

        Ok(Self {
            tickers: HashMap::new(),
            timeframes: tfs,
        })
    }

    /// Ingest a single trade and return any completed bars.
    ///
    /// Arguments:
    ///   ticker:       Ticker symbol (e.g. "AAPL")
    ///   price:        Trade price
    ///   size:         Trade size (shares/volume)
    ///   timestamp_ms: Trade timestamp in ms since Unix epoch (ET-aligned)
    ///
    /// Returns:
    ///   List of completed bar dicts (may be empty).
    fn ingest_trade(
        &mut self,
        py: Python<'_>,
        ticker: &str,
        price: f64,
        size: f64,
        timestamp_ms: i64,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let session = SessionCalendar::classify(timestamp_ms);
        if session == Session::Closed {
            // Outside trading hours — drop the tick silently.
            return Ok(Vec::new());
        }

        let ticker_bars = self
            .tickers
            .entry(ticker.to_string())
            .or_insert_with(TickerBars::new);

        let mut completed = Vec::new();

        for &tf in &self.timeframes {
            let bar_start_ts = SessionCalendar::bar_start(timestamp_ms, tf);
            let bar_end_ts = SessionCalendar::bar_end(bar_start_ts, tf);

            match ticker_bars.bars.get_mut(&tf) {
                Some(bar) => {
                    if timestamp_ms >= bar.bar_end {
                        // Current bar is completed — emit it.
                        let cb = bar.to_completed(ticker, tf);
                        completed.push(cb.to_py_dict(py)?);

                        // Start a new bar.
                        *bar = BarState::new(price, size, bar_start_ts, bar_end_ts, session);
                    } else {
                        // Same bar — update in place.
                        bar.update(price, size);
                    }
                }
                None => {
                    // First trade for this ticker+timeframe.
                    ticker_bars.bars.insert(
                        tf,
                        BarState::new(price, size, bar_start_ts, bar_end_ts, session),
                    );
                }
            }
        }

        Ok(completed)
    }

    /// Get the current (partial) bar for a ticker+timeframe.
    ///
    /// Returns a dict with OHLCV fields, or None if no bar is in progress.
    fn get_current_bar(
        &self,
        py: Python<'_>,
        ticker: &str,
        timeframe: &str,
    ) -> PyResult<Option<Py<PyAny>>> {
        let tf = Timeframe::from_label(timeframe).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "Unknown timeframe: '{}'",
                timeframe
            ))
        })?;

        let bar = self
            .tickers
            .get(ticker)
            .and_then(|tb| tb.bars.get(&tf));

        match bar {
            Some(b) => Ok(Some(b.to_completed(ticker, tf).to_py_dict(py)?)),
            None => Ok(None),
        }
    }

    /// Flush all open bars — force-complete and return them.
    ///
    /// Useful at session end or shutdown.
    fn flush(&mut self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let mut completed = Vec::new();

        for (ticker, ticker_bars) in &self.tickers {
            for (&tf, bar) in &ticker_bars.bars {
                let cb = bar.to_completed(ticker, tf);
                completed.push(cb.to_py_dict(py)?);
            }
        }

        // Clear all state.
        self.tickers.clear();

        Ok(completed)
    }

    /// Get all active tickers.
    fn active_tickers(&self) -> Vec<String> {
        self.tickers.keys().cloned().collect()
    }

    /// Remove a ticker and flush its bars.
    fn remove_ticker(
        &mut self,
        py: Python<'_>,
        ticker: &str,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let mut completed = Vec::new();

        if let Some(ticker_bars) = self.tickers.remove(ticker) {
            for (&tf, bar) in &ticker_bars.bars {
                let cb = bar.to_completed(ticker, tf);
                completed.push(cb.to_py_dict(py)?);
            }
        }

        Ok(completed)
    }

    /// Number of tickers currently tracked.
    fn ticker_count(&self) -> usize {
        self.tickers.len()
    }

    /// String representation.
    fn __repr__(&self) -> String {
        let tf_labels: Vec<&str> = self.timeframes.iter().map(|tf| tf.label()).collect();
        format!(
            "BarBuilder(tickers={}, timeframes={:?})",
            self.tickers.len(),
            tf_labels,
        )
    }
}

// ════════════════════════════════════════════════════════════════════
// Unit tests
// ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: create a ms timestamp for a given hour:minute on an arbitrary day.
    // Using 2025-01-06 (Monday) = epoch ms 1736121600000.
    const BASE_DAY_MS: i64 = 1_736_121_600_000; // 2025-01-06 00:00 UTC

    fn ts(hour: i64, min: i64, sec: i64) -> i64 {
        BASE_DAY_MS + hour * 3_600_000 + min * 60_000 + sec * 1_000
    }

    // ── Session classification ──────────────────────────────────────

    #[test]
    fn test_session_classify_premarket() {
        assert_eq!(SessionCalendar::classify(ts(4, 0, 0)), Session::Premarket);
        assert_eq!(SessionCalendar::classify(ts(8, 30, 0)), Session::Premarket);
        assert_eq!(SessionCalendar::classify(ts(9, 29, 59)), Session::Premarket);
    }

    #[test]
    fn test_session_classify_regular() {
        assert_eq!(SessionCalendar::classify(ts(9, 30, 0)), Session::Regular);
        assert_eq!(SessionCalendar::classify(ts(12, 0, 0)), Session::Regular);
        assert_eq!(SessionCalendar::classify(ts(15, 59, 59)), Session::Regular);
    }

    #[test]
    fn test_session_classify_afterhours() {
        assert_eq!(SessionCalendar::classify(ts(16, 0, 0)), Session::Afterhours);
        assert_eq!(SessionCalendar::classify(ts(19, 59, 59)), Session::Afterhours);
    }

    #[test]
    fn test_session_classify_closed() {
        assert_eq!(SessionCalendar::classify(ts(3, 59, 59)), Session::Closed);
        assert_eq!(SessionCalendar::classify(ts(20, 0, 0)), Session::Closed);
        assert_eq!(SessionCalendar::classify(ts(23, 0, 0)), Session::Closed);
    }

    // ── Session boundaries ──────────────────────────────────────────

    #[test]
    fn test_next_session_boundary() {
        // In premarket → boundary is regular start (09:30)
        assert_eq!(
            SessionCalendar::next_session_boundary(ts(5, 0, 0)),
            ts(9, 30, 0)
        );
        // In regular → boundary is afterhours start (16:00)
        assert_eq!(
            SessionCalendar::next_session_boundary(ts(10, 0, 0)),
            ts(16, 0, 0)
        );
        // In afterhours → boundary is afterhours end (20:00)
        assert_eq!(
            SessionCalendar::next_session_boundary(ts(17, 0, 0)),
            ts(20, 0, 0)
        );
    }

    // ── Bar start alignment ─────────────────────────────────────────

    #[test]
    fn test_bar_start_1m() {
        // 09:32:45 → bar starts at 09:32:00
        let t = ts(9, 32, 45);
        let bs = SessionCalendar::bar_start(t, Timeframe::Min1);
        assert_eq!(bs, ts(9, 32, 0));
    }

    #[test]
    fn test_bar_start_5m_aligned_to_session() {
        // 09:37:00 → session starts at 09:30, elapsed = 7min → bar_start = 09:35
        let t = ts(9, 37, 0);
        let bs = SessionCalendar::bar_start(t, Timeframe::Min5);
        assert_eq!(bs, ts(9, 35, 0));
    }

    #[test]
    fn test_bar_start_5m_premarket() {
        // 04:12:00 → session starts at 04:00, elapsed = 12min → bar_start = 04:10
        let t = ts(4, 12, 0);
        let bs = SessionCalendar::bar_start(t, Timeframe::Min5);
        assert_eq!(bs, ts(4, 10, 0));
    }

    #[test]
    fn test_bar_start_daily() {
        let t = ts(10, 0, 0);
        let bs = SessionCalendar::bar_start(t, Timeframe::Day1);
        assert_eq!(bs, BASE_DAY_MS); // midnight
    }

    // ── Bar end with session clipping ───────────────────────────────

    #[test]
    fn test_bar_end_clips_at_session_boundary() {
        // 5m bar starting at 09:28. Natural end = 09:33.
        // But session boundary = 09:30. Bar should end at 09:30.
        let bar_start = ts(9, 28, 0);
        let bar_end = SessionCalendar::bar_end(bar_start, Timeframe::Min5);
        assert_eq!(bar_end, ts(9, 30, 0));
    }

    #[test]
    fn test_bar_end_no_clip() {
        // 5m bar at 10:00 → natural end 10:05, no boundary crossing.
        let bar_start = ts(10, 0, 0);
        let bar_end = SessionCalendar::bar_end(bar_start, Timeframe::Min5);
        assert_eq!(bar_end, ts(10, 5, 0));
    }

    #[test]
    fn test_bar_end_daily_not_clipped() {
        // Daily bar not clipped by session boundaries.
        let bar_start = BASE_DAY_MS;
        let bar_end = SessionCalendar::bar_end(bar_start, Timeframe::Day1);
        assert_eq!(bar_end, BASE_DAY_MS + 86_400_000);
    }

    // ── BarState ────────────────────────────────────────────────────

    #[test]
    fn test_bar_state_new_and_update() {
        let mut bar = BarState::new(100.0, 50.0, ts(10, 0, 0), ts(10, 1, 0), Session::Regular);
        assert_eq!(bar.open, 100.0);
        assert_eq!(bar.high, 100.0);
        assert_eq!(bar.low, 100.0);
        assert_eq!(bar.close, 100.0);
        assert_eq!(bar.volume, 50.0);
        assert_eq!(bar.trade_count, 1);

        bar.update(105.0, 30.0);
        assert_eq!(bar.high, 105.0);
        assert_eq!(bar.close, 105.0);
        assert_eq!(bar.volume, 80.0);
        assert_eq!(bar.trade_count, 2);

        bar.update(95.0, 20.0);
        assert_eq!(bar.low, 95.0);
        assert_eq!(bar.close, 95.0);
        assert_eq!(bar.volume, 100.0);
        assert_eq!(bar.trade_count, 3);

        // VWAP = (100*50 + 105*30 + 95*20) / (50+30+20) = 9050 / 100 = 90.5...
        // Actually: 5000 + 3150 + 1900 = 10050 / 100 = 100.5
        let expected_vwap = (100.0 * 50.0 + 105.0 * 30.0 + 95.0 * 20.0) / 100.0;
        assert!((bar.vwap() - expected_vwap).abs() < 1e-9);
    }

    // ── Weekly bar start alignment ──────────────────────────────────

    #[test]
    fn test_bar_start_weekly() {
        // BASE_DAY is 2025-01-06 (Monday). Any ts that day should align to Monday 00:00.
        let t = ts(10, 0, 0);
        let bs = SessionCalendar::bar_start(t, Timeframe::Week1);
        assert_eq!(bs, BASE_DAY_MS);

        // Wednesday (BASE_DAY + 2 days) should also align to the same Monday.
        let wed = BASE_DAY_MS + 2 * 86_400_000 + 10 * 3_600_000;
        let bs_wed = SessionCalendar::bar_start(wed, Timeframe::Week1);
        assert_eq!(bs_wed, BASE_DAY_MS);
    }
}
