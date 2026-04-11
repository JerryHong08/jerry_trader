// rust/src/bars.rs
//
// High-performance bar builder for tick-to-OHLCV aggregation.
// Maintains per-ticker, per-timeframe rolling bar state with
// session-aware boundaries (premarket / regular / afterhours).
//
// Exposed to Python as `jerry_trader._rust.BarBuilder` via PyO3.

use pyo3::prelude::*;
use pyo3::Py;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::VecDeque;

// ── Timeframe ───────────────────────────────────────────────────────

/// Supported bar timeframes with their duration in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
/// **IMPORTANT CONTRACT**: All timestamps passed to this calculator MUST be
/// **milliseconds since Unix epoch in US/Eastern time** (not UTC).
///
/// The Python layer is responsible for converting UTC → ET using proper
/// timezone-aware libraries (pytz/zoneinfo) that handle DST correctly before
/// passing timestamps to Rust.
///
/// Session boundaries (premarket 04:00, regular 09:30, afterhours 16:00-20:00)
/// are defined as simple time-of-day offsets from midnight. No timezone
/// conversion or DST handling is performed in Rust.
///
/// Market holidays should be handled by the Python layer (simply don't feed
/// ticks on those days).
pub struct SessionCalendar;

impl SessionCalendar {
    // Session boundaries as ms from midnight ET.
    const PREMARKET_START: i64 = 4 * 3_600_000;           // 04:00 ET
    const REGULAR_START: i64 = 9 * 3_600_000 + 30 * 60_000; // 09:30 ET
    const AFTERHOURS_START: i64 = 16 * 3_600_000;          // 16:00 ET
    const AFTERHOURS_END: i64 = 20 * 3_600_000;            // 20:00 ET
    const DAY_MS: i64 = 86_400_000;

    /// Classify which session a timestamp falls into.
    /// `et_ms` is ms since Unix epoch in US/Eastern time.
    pub fn classify(et_ms: i64) -> Session {
        let ms_of_day = et_ms.rem_euclid(Self::DAY_MS);
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

    /// Returns the ms-epoch (ET) of the next session boundary AFTER `et_ms`.
    /// This is where a bar must be truncated if it would otherwise cross.
    pub fn next_session_boundary(et_ms: i64) -> i64 {
        let day_start = et_ms - et_ms.rem_euclid(Self::DAY_MS);
        let ms_of_day = et_ms.rem_euclid(Self::DAY_MS);

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

    /// Compute the bar-start timestamp (ET) for a given timestamp and timeframe.
    ///
    /// For intraday timeframes (≤ 4h): aligns to the start of the current
    /// session, then snaps to the nearest timeframe boundary within that session.
    ///
    /// For daily: aligns to midnight ET of the day.
    /// For weekly: aligns to Monday midnight ET.
    pub fn bar_start(et_ms: i64, tf: Timeframe) -> i64 {
        match tf {
            Timeframe::Week1 => {
                // Align to Monday 00:00 ET.
                // 1970-01-01 (epoch day 0) was a Thursday → weekday 3 (0=Mon).
                // day_of_week = (days_since_epoch + 3) % 7, where 0 = Monday.
                let day_ms = Self::DAY_MS;
                let days = et_ms.div_euclid(day_ms);
                let dow = (days + 3).rem_euclid(7); // 0=Mon … 6=Sun
                (days - dow) * day_ms
            }
            Timeframe::Day1 => {
                et_ms - et_ms.rem_euclid(Self::DAY_MS)
            }
            _ => {
                // Intraday: align within session.
                let session_start_ms = Self::session_start_of(et_ms);
                let elapsed = et_ms - session_start_ms;
                let dur = tf.duration_ms();
                session_start_ms + (elapsed / dur) * dur
            }
        }
    }

    /// Returns the ms-epoch (ET) of the start of the session that `et_ms` falls in.
    fn session_start_of(et_ms: i64) -> i64 {
        let day_start = et_ms - et_ms.rem_euclid(Self::DAY_MS);
        let ms_of_day = et_ms.rem_euclid(Self::DAY_MS);

        if ms_of_day >= Self::AFTERHOURS_START {
            day_start + Self::AFTERHOURS_START
        } else if ms_of_day >= Self::REGULAR_START {
            day_start + Self::REGULAR_START
        } else if ms_of_day >= Self::PREMARKET_START {
            day_start + Self::PREMARKET_START
        } else {
            // Before 04:00 ET — belongs to previous day's afterhours (or closed).
            // Snap to previous day afterhours end → treat as "closed" start.
            day_start
        }
    }

    /// Effective bar end: min(bar_start + duration, next_session_boundary).
    /// Bars never cross session boundaries.
    /// All timestamps in ET.
    pub fn bar_end(bar_start_et: i64, tf: Timeframe) -> i64 {
        match tf {
            Timeframe::Day1 | Timeframe::Week1 => {
                // Daily/weekly bars are not session-clipped.
                bar_start_et + tf.duration_ms()
            }
            _ => {
                let natural_end = bar_start_et + tf.duration_ms();
                let boundary = Self::next_session_boundary(bar_start_et);
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
    seq: u64,
}

impl BarState {
    /// Create a new bar starting with the given trade.
    fn new(
        price: f64,
        size: f64,
        bar_start: i64,
        bar_end: i64,
        session: Session,
        seq: u64,
    ) -> Self {
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
            seq,
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

    /// Merge another bar into this one.
    ///
    /// Used for meeting bar merge: combine REST partial with WS partial.
    /// The resulting bar has:
    ///   - open from the earlier bar (by bar_start)
    ///   - high/low from both bars
    ///   - close from the later bar
    ///   - volume/trade_count/vwap summed
    fn merge(&mut self, other: &BarState) {
        // Take open from the bar that started earlier
        // (REST partial starts before WS partial)
        // We assume self is REST, other is WS

        // High/low: take the extremes
        if other.high > self.high {
            self.high = other.high;
        }
        if other.low < self.low {
            self.low = other.low;
        }

        // Close: take from WS (the later bar)
        self.close = other.close;

        // Volume and trade count: sum
        self.volume += other.volume;
        self.trade_count += other.trade_count;

        // VWAP: recompute from numer/denom
        self.vwap_numer += other.vwap_numer;
        self.vwap_denom += other.vwap_denom;
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
    max_event_ts: i64,
    /// Tracks the `bar_end` of the last closed bar per timeframe.
    /// Used to prevent late-arriving trades from re-creating an already-closed bar.
    last_closed_end: HashMap<Timeframe, i64>,
    /// Close price of the last completed bar per timeframe.
    /// Used as reference price for forward-fill continuation bars.
    last_close_price: HashMap<Timeframe, f64>,
    /// Session of the last completed bar per timeframe.
    /// Used for forward-fill continuation bars.
    last_close_session: HashMap<Timeframe, Session>,
}

impl TickerBars {
    fn new() -> Self {
        Self {
            bars: HashMap::with_capacity(8),
            max_event_ts: 0,
            last_closed_end: HashMap::with_capacity(8),
            last_close_price: HashMap::with_capacity(8),
            last_close_session: HashMap::with_capacity(8),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ExpiryEntry {
    bar_end: i64,
    ticker: String,
    tf: Timeframe,
    seq: u64,
}

// ── MergeResult (internal) ────────────────────────────────────────────

/// Result of attempting to merge a meeting bar.
enum MergeResult {
    /// Not a meeting bar — return the original WS bar unchanged.
    NotAMeetingBar,
    /// Successfully merged REST + WS.
    Merged(BarState),
    /// REST partial not ready — WS bar deferred, will be merged when REST arrives.
    Deferred,
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
///
/// ## Meeting Bar Merge (Bootstrap → Live Transition)
///
/// When transitioning from REST bootstrap to live WebSocket ticks, the bar
/// that straddles the boundary needs to be merged from two sources:
///   - REST partial: [bar_start ... first_ws_tick_ts)
///   - WS partial:   [first_ws_tick_ts ... bar_end)
///
/// The BarBuilder handles this internally:
///   1. `set_first_ws_tick_ts()` records when WS stream starts
///   2. `detect_meeting_bars()` identifies which bars straddle the boundary
///   3. `set_rest_partial()` stores the REST half from trades_backfill
///   4. `drain_completed()` merges REST + WS halves before returning
///
/// This eliminates the race condition between trades_backfill (Thread B)
/// and _flush_loop (Thread A) — all merge logic is single-threaded in Rust.
#[pyclass]
pub struct BarBuilder {
    /// Per-ticker rolling state.
    tickers: HashMap<String, TickerBars>,
    /// Which timeframes this builder is tracking.
    timeframes: Vec<Timeframe>,
    /// Completed bars waiting for caller to drain.
    completed_queue: VecDeque<CompletedBar>,
    /// Expiry scheduler keyed by earliest bar_end.
    expiry_heap: BinaryHeap<Reverse<ExpiryEntry>>,
    /// Monotonic sequence for stale-heap-entry detection.
    next_seq: u64,
    /// Keep bars open this long for late-arriving trades (ms).
    late_arrival_ms: i64,
    /// If ticker is idle longer than this, wall-time closes bars (ms).
    idle_close_ms: i64,

    // ── Meeting bar merge state ─────────────────────────────────────
    /// First WS tick timestamp per ticker (epoch ms, ET timezone).
    /// Used to detect which bars straddle the REST→WS boundary.
    first_ws_tick_ts: HashMap<String, i64>,
    /// REST partial bars from trades_backfill.
    /// Key: (ticker, timeframe), Value: bar state from REST data.
    rest_partials: HashMap<(String, Timeframe), BarState>,
    /// Which bars are meeting bars (straddle REST→WS boundary).
    /// Key: (ticker, timeframe), Value: bar_start timestamp.
    meeting_bar_starts: HashMap<(String, Timeframe), i64>,
    /// Pending WS meeting bars waiting for REST partial.
    /// Key: (ticker, timeframe), Value: WS bar state.
    /// When REST partial arrives via set_rest_partial(), we merge immediately.
    pending_ws_meeting_bars: HashMap<(String, Timeframe), BarState>,
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
            completed_queue: VecDeque::new(),
            expiry_heap: BinaryHeap::new(),
            next_seq: 1,
            late_arrival_ms: 200,
            idle_close_ms: 2_000,
            // Meeting bar merge state
            first_ws_tick_ts: HashMap::new(),
            rest_partials: HashMap::new(),
            meeting_bar_starts: HashMap::new(),
            pending_ws_meeting_bars: HashMap::new(),
        })
    }

    /// Configure late-arrival hold window and idle close timeout.
    #[pyo3(signature = (late_arrival_ms=200, idle_close_ms=2000))]
    fn configure_watermark(&mut self, late_arrival_ms: i64, idle_close_ms: i64) {
        self.late_arrival_ms = late_arrival_ms.max(0);
        self.idle_close_ms = idle_close_ms.max(1);
    }

    /// Ingest a single trade and return any completed bars.
    ///
    /// **IMPORTANT**: `timestamp_ms` MUST be in US/Eastern time, not UTC.
    /// Python must convert UTC → ET before calling this method.
    ///
    /// Arguments:
    ///   ticker:       Ticker symbol (e.g. "AAPL")
    ///   price:        Trade price
    ///   size:         Trade size (shares/volume)
    ///   timestamp_ms: Trade timestamp in ms since Unix epoch **in US/Eastern time**
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
        if timestamp_ms > ticker_bars.max_event_ts {
            ticker_bars.max_event_ts = timestamp_ms;
        }

        let mut completed = Vec::new();

        for &tf in &self.timeframes {
            let bar_start_ts = SessionCalendar::bar_start(timestamp_ms, tf);
            let bar_end_ts = SessionCalendar::bar_end(bar_start_ts, tf);

            match ticker_bars.bars.get_mut(&tf) {
                Some(bar) => {
                    if timestamp_ms >= bar.bar_end {
                        // Current bar is completed — emit it.
                        // Record close price for forward-fill reference
                        let close_price = bar.close;
                        let close_session = bar.session;

                        let cb = bar.to_completed(ticker, tf);
                        self.completed_queue.push_back(cb.clone());
                        completed.push(cb.to_py_dict(py)?);
                        ticker_bars.last_closed_end.insert(tf, bar.bar_end);
                        ticker_bars.last_close_price.insert(tf, close_price);
                        ticker_bars.last_close_session.insert(tf, close_session);

                        // Forward-fill gap between closed bar and new trade's bar
                        let gap_bars = Self::forward_fill_gap(close_price, bar.bar_end, bar_start_ts, tf, ticker);
                        for gap_cb in gap_bars {
                            completed.push(gap_cb.to_py_dict(py)?);
                        }

                        // Start a new bar.
                        let seq = self.next_seq;
                        self.next_seq += 1;
                        *bar = BarState::new(
                            price,
                            size,
                            bar_start_ts,
                            bar_end_ts,
                            session,
                            seq,
                        );
                        self.expiry_heap.push(Reverse(ExpiryEntry {
                            bar_end: bar_end_ts,
                            ticker: ticker.to_string(),
                            tf,
                            seq,
                        }));
                    } else {
                        // Same bar — update in place.
                        bar.update(price, size);
                    }
                }
                None => {
                    // Don't re-create a bar for an already-closed period.
                    if bar_end_ts <= *ticker_bars.last_closed_end.get(&tf).unwrap_or(&0) {
                        continue;
                    }
                    // Forward-fill gap using last close price (if available)
                    if let (Some(&ref_price), Some(&last_end)) = (
                        ticker_bars.last_close_price.get(&tf),
                        ticker_bars.last_closed_end.get(&tf),
                    ) {
                        let gap_bars = Self::forward_fill_gap(ref_price, last_end, bar_start_ts, tf, ticker);
                        for gap_cb in gap_bars {
                            completed.push(gap_cb.to_py_dict(py)?);
                        }
                    }

                    // First trade for this ticker+timeframe.
                    let seq = self.next_seq;
                    self.next_seq += 1;
                    ticker_bars.bars.insert(
                        tf,
                        BarState::new(
                            price,
                            size,
                            bar_start_ts,
                            bar_end_ts,
                            session,
                            seq,
                        ),
                    );
                    self.expiry_heap.push(Reverse(ExpiryEntry {
                        bar_end: bar_end_ts,
                        ticker: ticker.to_string(),
                        tf,
                        seq,
                    }));
                }
            }
        }

        Ok(completed)
    }

    /// Bulk-ingest trades for a single ticker. Returns all completed bars.
    ///
    /// Much faster than calling `ingest_trade()` N times from Python because
    /// the FFI boundary is crossed only once.  Trades are processed in pure
    /// Rust with no per-trade GIL interaction — Python dicts are built only
    /// for the completed bars at the end.
    ///
    /// **IMPORTANT**: All `timestamp_ms` values MUST be in US/Eastern time, not UTC.
    /// Python must convert UTC → ET before calling this method.
    ///
    /// Arguments:
    ///   ticker:  Ticker symbol (e.g. "AAPL")
    ///   trades:  List of `(timestamp_ms, price, size)` tuples where timestamp_ms
    ///            is in **US/Eastern time**. Should be sorted ascending by timestamp
    ///            for correct bar alignment.
    ///
    /// Returns:
    ///   List of completed bar dicts (may be empty).
    fn ingest_trades_batch(
        &mut self,
        py: Python<'_>,
        ticker: &str,
        trades: Vec<(i64, f64, f64)>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        // Accumulate CompletedBars in Rust, convert to Python at the end.
        let mut completed_bars: Vec<CompletedBar> = Vec::new();

        let ticker_bars = self
            .tickers
            .entry(ticker.to_string())
            .or_insert_with(TickerBars::new);

        for (timestamp_ms, price, size) in trades {
            let session = SessionCalendar::classify(timestamp_ms);
            if session == Session::Closed {
                continue;
            }
            if timestamp_ms > ticker_bars.max_event_ts {
                ticker_bars.max_event_ts = timestamp_ms;
            }

            for &tf in &self.timeframes {
                let bar_start_ts = SessionCalendar::bar_start(timestamp_ms, tf);
                let bar_end_ts = SessionCalendar::bar_end(bar_start_ts, tf);

                match ticker_bars.bars.get_mut(&tf) {
                    Some(bar) => {
                        if timestamp_ms >= bar.bar_end {
                            // Record close price for forward-fill
                            let close_price = bar.close;
                            let close_session = bar.session;

                            let cb = bar.to_completed(ticker, tf);
                            self.completed_queue.push_back(cb.clone());
                            completed_bars.push(cb);
                            ticker_bars.last_closed_end.insert(tf, bar.bar_end);
                            ticker_bars.last_close_price.insert(tf, close_price);
                            ticker_bars.last_close_session.insert(tf, close_session);

                            // Forward-fill gap
                            let gap_bars = Self::forward_fill_gap(close_price, bar.bar_end, bar_start_ts, tf, ticker);
                            for gap_cb in gap_bars {
                                self.completed_queue.push_back(gap_cb.clone());
                                completed_bars.push(gap_cb);
                            }

                            let seq = self.next_seq;
                            self.next_seq += 1;
                            *bar = BarState::new(
                                price,
                                size,
                                bar_start_ts,
                                bar_end_ts,
                                session,
                                seq,
                            );
                            self.expiry_heap.push(Reverse(ExpiryEntry {
                                bar_end: bar_end_ts,
                                ticker: ticker.to_string(),
                                tf,
                                seq,
                            }));
                        } else {
                            bar.update(price, size);
                        }
                    }
                    None => {
                        // Don't re-create a bar for an already-closed period.
                        if bar_end_ts <= *ticker_bars.last_closed_end.get(&tf).unwrap_or(&0) {
                            continue;
                        }
                        // Forward-fill gap using last close price (if available)
                        if let (Some(&ref_price), Some(&last_end)) = (
                            ticker_bars.last_close_price.get(&tf),
                            ticker_bars.last_closed_end.get(&tf),
                        ) {
                            let gap_bars = Self::forward_fill_gap(ref_price, last_end, bar_start_ts, tf, ticker);
                            for gap_cb in gap_bars {
                                self.completed_queue.push_back(gap_cb.clone());
                                completed_bars.push(gap_cb);
                            }
                        }

                        let seq = self.next_seq;
                        self.next_seq += 1;
                        ticker_bars.bars.insert(
                            tf,
                            BarState::new(
                                price,
                                size,
                                bar_start_ts,
                                bar_end_ts,
                                session,
                                seq,
                            ),
                        );
                        self.expiry_heap.push(Reverse(ExpiryEntry {
                            bar_end: bar_end_ts,
                            ticker: ticker.to_string(),
                            tf,
                            seq,
                        }));
                    }
                }
            }
        }

        // Convert to Python dicts only once at the end
        let mut result = Vec::with_capacity(completed_bars.len());
        for cb in completed_bars {
            result.push(cb.to_py_dict(py)?);
        }

        Ok(result)
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

    /// Check all open bars and complete any whose `bar_end <= now_ms`.
    ///
    /// This enables **wall-time driven** bar completion: call this
    /// periodically with `clock.now_ms()` so that bars close at the correct
    /// boundary even when no trade arrives.
    ///
    /// Expired bars are removed from the rolling state (the next trade will
    /// start a fresh bar).
    ///
    /// Returns:
    ///   List of completed bar dicts (may be empty).
    fn advance(
        &mut self,
        py: Python<'_>,
        now_ms: i64,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let mut completed = Vec::new();
        let mut deferred: Vec<ExpiryEntry> = Vec::new();
        let max_threshold = now_ms - self.late_arrival_ms;

        while let Some(Reverse(entry)) = self.expiry_heap.peek().cloned() {
            if entry.bar_end > max_threshold {
                break;
            }
            let _ = self.expiry_heap.pop();

            let Some(ticker_bars) = self.tickers.get_mut(&entry.ticker) else {
                continue;
            };
            let Some(active) = ticker_bars.bars.get(&entry.tf) else {
                continue;
            };
            if active.seq != entry.seq {
                continue;
            }

            let ticker_threshold = if now_ms - ticker_bars.max_event_ts >= self.idle_close_ms {
                now_ms - self.late_arrival_ms
            } else {
                ticker_bars.max_event_ts - self.late_arrival_ms
            };

            if entry.bar_end > ticker_threshold {
                deferred.push(entry);
                continue;
            }

            if let Some(bar) = ticker_bars.bars.remove(&entry.tf) {
                ticker_bars.last_closed_end.insert(entry.tf, bar.bar_end);
                ticker_bars.last_close_price.insert(entry.tf, bar.close);
                ticker_bars.last_close_session.insert(entry.tf, bar.session);
                let cb = bar.to_completed(&entry.ticker, entry.tf);
                self.completed_queue.push_back(cb.clone());
                completed.push(cb.to_py_dict(py)?);
            }
        }

        for entry in deferred {
            self.expiry_heap.push(Reverse(entry));
        }

        Ok(completed)
    }

    /// Drain and return all completed bars currently queued.
    ///
    /// For meeting bars (straddling REST→WS boundary):
    ///   - If REST partial is available: merge and return the merged bar
    ///   - If REST partial not ready: defer the WS bar (not returned yet)
    ///   - If not a meeting bar: return unchanged
    fn drain_completed(&mut self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let mut out = Vec::with_capacity(self.completed_queue.len());

        while let Some(cb) = self.completed_queue.pop_front() {
            let key = (cb.ticker.clone(), match Timeframe::from_label(cb.timeframe) {
                Some(tf) => tf,
                None => {
                    // Unknown timeframe — return as-is
                    out.push(cb.to_py_dict(py)?);
                    continue;
                }
            });

            // Check if we need to rebuild the BarState for merge checking
            // We only do this if there are meeting bar starts registered
            let needs_merge_check = self.meeting_bar_starts.contains_key(&key);

            if !needs_merge_check {
                out.push(cb.to_py_dict(py)?);
                continue;
            }

            // Reconstruct BarState from CompletedBar for merge logic
            let ws_bar_state = BarState {
                open: cb.open,
                high: cb.high,
                low: cb.low,
                close: cb.close,
                volume: cb.volume,
                trade_count: cb.trade_count,
                vwap_numer: cb.vwap * cb.volume,
                vwap_denom: cb.volume,
                bar_start: cb.bar_start,
                bar_end: cb.bar_end,
                session: match cb.session {
                    "premarket" => Session::Premarket,
                    "regular" => Session::Regular,
                    "afterhours" => Session::Afterhours,
                    _ => Session::Closed,
                },
                seq: 0,
            };

            let tf = key.1;
            match self.try_merge_meeting_bar(&cb.ticker, tf, &ws_bar_state) {
                MergeResult::NotAMeetingBar => {
                    out.push(cb.to_py_dict(py)?);
                }
                MergeResult::Merged(merged) => {
                    let merged_cb = merged.to_completed(&cb.ticker, tf);
                    out.push(merged_cb.to_py_dict(py)?);
                }
                MergeResult::Deferred => {
                    // WS bar deferred — don't return it.
                    // It will be returned later when set_rest_partial() merges it
                    // and adds it back to completed_queue.
                }
            }
        }

        Ok(out)
    }

    /// Flush all open bars — force-complete and return them.
    ///
    /// Useful at session end or shutdown.
    fn flush(&mut self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let mut completed = Vec::new();

        for (ticker, ticker_bars) in &self.tickers {
            for (&tf, bar) in &ticker_bars.bars {
                let cb = bar.to_completed(ticker, tf);
                self.completed_queue.push_back(cb.clone());
                completed.push(cb.to_py_dict(py)?);
            }
        }

        // Clear all state.
        self.tickers.clear();

        Ok(completed)
    }

    // ════════════════════════════════════════════════════════════════════
    // Meeting Bar Merge (Bootstrap → Live Transition)
    // ════════════════════════════════════════════════════════════════════

    /// Record the first WebSocket tick timestamp for a ticker.
    ///
    /// This is called from Python when the first WS tick arrives.
    /// The timestamp is used to detect which bars straddle the REST→WS boundary.
    ///
    /// Returns true if this was the first WS tick (new), false if already set.
    fn set_first_ws_tick_ts(&mut self, ticker: &str, ts_ms: i64) -> bool {
        if self.first_ws_tick_ts.contains_key(ticker) {
            return false;
        }
        self.first_ws_tick_ts.insert(ticker.to_string(), ts_ms);
        true
    }

    /// Get the first WS tick timestamp for a ticker.
    fn get_first_ws_tick_ts(&self, ticker: &str) -> Option<i64> {
        self.first_ws_tick_ts.get(ticker).copied()
    }

    /// Detect and record meeting bars for a ticker.
    ///
    /// Called after the first WS tick arrives. Checks all active timeframes
    /// to see if the current bar straddles the REST→WS boundary.
    ///
    /// Returns a dict mapping timeframe label → bar_start for meeting bars.
    fn detect_meeting_bars(&mut self, py: Python<'_>, ticker: &str) -> PyResult<Option<Py<PyAny>>> {
        let first_ws_ts = match self.first_ws_tick_ts.get(ticker) {
            Some(&ts) => ts,
            None => return Ok(None),
        };

        let ticker_bars = match self.tickers.get(ticker) {
            Some(tb) => tb,
            None => return Ok(None),
        };

        let mut meeting_starts: Vec<(&'static str, i64)> = Vec::new();

        for (&tf, bar) in &ticker_bars.bars {
            // A meeting bar is one where bar_start < first_ws_ts < bar_end
            // i.e., the first WS tick falls within this bar
            if bar.bar_start < first_ws_ts && first_ws_ts < bar.bar_end {
                let key = (ticker.to_string(), tf);
                self.meeting_bar_starts.insert(key.clone(), bar.bar_start);
                meeting_starts.push((tf.label(), bar.bar_start));
            }
        }

        if meeting_starts.is_empty() {
            return Ok(None);
        }

        let dict = pyo3::types::PyDict::new(py);
        for (tf_label, bar_start) in meeting_starts {
            dict.set_item(tf_label, bar_start)?;
        }
        Ok(Some(dict.into()))
    }

    /// Store a REST partial bar for deferred merge.
    ///
    /// Called from trades_backfill (Python) after building bars from
    /// historical REST data. The REST partial will be merged with the
    /// WS partial when the WS bar completes.
    ///
    /// Args:
    ///   ticker: Ticker symbol
    ///   timeframe: Timeframe label (e.g., "1m")
    ///   bar_dict: Bar dict from Python with OHLCV fields
    ///
    /// Returns true if stored successfully.
    fn set_rest_partial(
        &mut self,
        ticker: &str,
        timeframe: &str,
        bar_dict: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let tf = match Timeframe::from_label(timeframe) {
            Some(t) => t,
            None => return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Unknown timeframe: '{}'", timeframe
            ))),
        };

        // Extract bar fields from Python dict
        let dict = bar_dict.cast::<pyo3::types::PyDict>()?;

        let open: f64 = dict.get_item("open")?.and_then(|v| v.extract().ok()).unwrap_or(0.0);
        let high: f64 = dict.get_item("high")?.and_then(|v| v.extract().ok()).unwrap_or(0.0);
        let low: f64 = dict.get_item("low")?.and_then(|v| v.extract().ok()).unwrap_or(0.0);
        let close: f64 = dict.get_item("close")?.and_then(|v| v.extract().ok()).unwrap_or(0.0);
        let volume: f64 = dict.get_item("volume")?.and_then(|v| v.extract().ok()).unwrap_or(0.0);
        let trade_count: u64 = dict.get_item("trade_count")?.and_then(|v| v.extract().ok()).unwrap_or(0);
        let vwap: f64 = dict.get_item("vwap")?.and_then(|v| v.extract().ok()).unwrap_or(close);
        let bar_start: i64 = dict.get_item("bar_start")?.and_then(|v| v.extract().ok()).unwrap_or(0);
        let bar_end: i64 = dict.get_item("bar_end")?.and_then(|v| v.extract().ok()).unwrap_or(0);
        let session_str: String = dict.get_item("session")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| "regular".to_string());
        let session = match session_str.as_str() {
            "premarket" => Session::Premarket,
            "regular" => Session::Regular,
            "afterhours" => Session::Afterhours,
            _ => Session::Closed,
        };

        // Create BarState from the dict
        let bar_state = BarState {
            open,
            high,
            low,
            close,
            volume,
            trade_count,
            vwap_numer: vwap * volume, // Reconstruct from VWAP
            vwap_denom: volume,
            bar_start,
            bar_end,
            session,
            seq: 0, // Not used for REST partials
        };

        let key = (ticker.to_string(), tf);

        // Check if there's a pending WS meeting bar waiting for this REST partial
        if let Some(ws_bar) = self.pending_ws_meeting_bars.remove(&key) {
            // Merge immediately and add to completed queue
            let mut merged = bar_state;
            merged.merge(&ws_bar);

            // Remove meeting bar start marker
            self.meeting_bar_starts.remove(&key);

            // Add merged bar to completed queue
            let cb = merged.to_completed(ticker, tf);
            self.completed_queue.push_back(cb);

            return Ok(true);
        }

        // No pending WS bar, just store the REST partial
        self.rest_partials.insert(key, bar_state);

        Ok(true)
    }

    /// Get all pending REST partials for a ticker.
    ///
    /// Returns a dict mapping timeframe → bar_dict for all stored REST partials.
    fn get_rest_partials(&self, py: Python<'_>, ticker: &str) -> PyResult<Option<Py<PyAny>>> {
        let mut partials: Vec<(&'static str, &BarState)> = Vec::new();

        for ((t, tf), bar) in &self.rest_partials {
            if t == ticker {
                partials.push((tf.label(), bar));
            }
        }

        if partials.is_empty() {
            return Ok(None);
        }

        let dict = pyo3::types::PyDict::new(py);
        for (tf_label, bar) in partials {
            let bar_dict = pyo3::types::PyDict::new(py);
            bar_dict.set_item("ticker", ticker)?;
            bar_dict.set_item("timeframe", tf_label)?;
            bar_dict.set_item("open", bar.open)?;
            bar_dict.set_item("high", bar.high)?;
            bar_dict.set_item("low", bar.low)?;
            bar_dict.set_item("close", bar.close)?;
            bar_dict.set_item("volume", bar.volume)?;
            bar_dict.set_item("trade_count", bar.trade_count)?;
            bar_dict.set_item("vwap", bar.vwap())?;
            bar_dict.set_item("bar_start", bar.bar_start)?;
            bar_dict.set_item("bar_end", bar.bar_end)?;
            bar_dict.set_item("session", bar.session.label())?;
            dict.set_item(tf_label, bar_dict)?;
        }
        Ok(Some(dict.into()))
    }

    /// Clear meeting bar state for a ticker.
    ///
    /// Called when a ticker is removed or when bootstrap completes
    /// without any meeting bars to merge.
    fn clear_meeting_bar_state(&mut self, ticker: &str) {
        self.first_ws_tick_ts.remove(ticker);

        // Remove all meeting bar starts for this ticker
        self.meeting_bar_starts.retain(|(t, _), _| t != ticker);

        // Remove all REST partials for this ticker
        self.rest_partials.retain(|(t, _), _| t != ticker);

        // Remove all pending WS meeting bars for this ticker
        self.pending_ws_meeting_bars.retain(|(t, _), _| t != ticker);
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
                self.completed_queue.push_back(cb.clone());
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
            "BarBuilder(tickers={}, timeframes={:?}, late_arrival_ms={}, idle_close_ms={})",
            self.tickers.len(),
            tf_labels,
            self.late_arrival_ms,
            self.idle_close_ms,
        )
    }
}

// ── BarBuilder (internal methods, not exposed to Python) ────────────

impl BarBuilder {
    /// Generate forward-fill continuation bars for a gap between the last
    /// closed bar and a new trade's bar_start.
    ///
    /// Takes all needed data as parameters to avoid borrow conflicts with
    /// the caller (which holds a mutable borrow on self.tickers).
    fn forward_fill_gap(
        ref_price: f64,
        last_closed_end: i64,
        new_bar_start: i64,
        tf: Timeframe,
        ticker: &str,
    ) -> Vec<CompletedBar> {
        let dur = tf.duration_ms();
        let mut filled = Vec::new();

        let mut fill_start = last_closed_end;
        while fill_start < new_bar_start {
            let fill_end = SessionCalendar::bar_end(fill_start, tf);

            let fill_session = SessionCalendar::classify(fill_start);
            if fill_session == Session::Closed {
                fill_start = fill_end;
                continue;
            }

            let bar = BarState {
                open: ref_price,
                high: ref_price,
                low: ref_price,
                close: ref_price,
                volume: 0.0,
                trade_count: 0,
                vwap_numer: 0.0,
                vwap_denom: 0.0,
                bar_start: fill_start,
                bar_end: fill_end,
                session: fill_session,
                seq: 0,
            };
            filled.push(bar.to_completed(ticker, tf));

            fill_start = fill_end;
        }

        filled
    }

    /// Internal: try to merge a completed bar with its REST partial.
    ///
    /// Called from drain_completed() for each completed bar.
    /// Returns:
    ///   MergeResult::Merged(BarState) — successfully merged REST + WS
    ///   MergeResult::NotAMeetingBar — not a meeting bar, pass through
    ///   MergeResult::Deferred — REST not ready, WS bar stored for later
    fn try_merge_meeting_bar(&mut self, ticker: &str, tf: Timeframe, ws_bar: &BarState) -> MergeResult {
        let key = (ticker.to_string(), tf);

        // Check if this is a meeting bar
        let expected_start = match self.meeting_bar_starts.get(&key) {
            Some(&start) => start,
            None => return MergeResult::NotAMeetingBar,
        };

        // Verify the bar_start matches
        if ws_bar.bar_start != expected_start {
            return MergeResult::NotAMeetingBar;
        }

        // Check if we have a REST partial
        match self.rest_partials.remove(&key) {
            Some(rest_bar) => {
                self.meeting_bar_starts.remove(&key);
                let mut merged = rest_bar;
                merged.merge(ws_bar);
                MergeResult::Merged(merged)
            }
            None => {
                // REST partial not ready — defer the WS bar
                self.pending_ws_meeting_bars.insert(key, ws_bar.clone());
                MergeResult::Deferred
            }
        }
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
        let mut bar = BarState::new(
            100.0,
            50.0,
            ts(10, 0, 0),
            ts(10, 1, 0),
            Session::Regular,
            1,
        );
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

    // ── advance (pure-Rust) ─────────────────────────────────────────

    #[test]
    fn test_bar_state_expired_detection() {
        // A 1m bar from 10:00:00 to 10:01:00.
        let bar = BarState::new(
            100.0,
            50.0,
            ts(10, 0, 0),
            ts(10, 1, 0),
            Session::Regular,
            1,
        );
        // At 10:00:30, bar should NOT be expired.
        assert!(ts(10, 0, 30) < bar.bar_end);
        // At 10:01:00 exactly, bar IS expired (now_ms >= bar_end).
        assert!(ts(10, 1, 0) >= bar.bar_end);
        // At 10:01:30, bar IS expired.
        assert!(ts(10, 1, 30) >= bar.bar_end);
    }

    #[test]
    fn test_ticker_bars_expired_drain() {
        // Verify that expired bars can be drained from a TickerBars map.
        let mut tb = TickerBars::new();
        // Insert 1m bar ending at 10:01
        tb.bars.insert(
            Timeframe::Min1,
            BarState::new(
                100.0,
                50.0,
                ts(10, 0, 0),
                ts(10, 1, 0),
                Session::Regular,
                1,
            ),
        );
        // Insert 5m bar ending at 10:05
        tb.bars.insert(
            Timeframe::Min5,
            BarState::new(
                100.0,
                50.0,
                ts(10, 0, 0),
                ts(10, 5, 0),
                Session::Regular,
                2,
            ),
        );

        // At 10:02, only the 1m bar is expired.
        let now = ts(10, 2, 0);
        let expired: Vec<Timeframe> = tb
            .bars
            .iter()
            .filter(|(_, bar)| now >= bar.bar_end)
            .map(|(&tf, _)| tf)
            .collect();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], Timeframe::Min1);

        // At 10:05, both are expired.
        let now2 = ts(10, 5, 0);
        let expired2: Vec<Timeframe> = tb
            .bars
            .iter()
            .filter(|(_, bar)| now2 >= bar.bar_end)
            .map(|(&tf, _)| tf)
            .collect();
        assert_eq!(expired2.len(), 2);
    }
}
