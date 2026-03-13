// rust/src/replayer/mod.rs
//
// `TickDataReplayer` — PyO3 class that embeds the tick-data replay
// engine directly in the Python process.
//
// Replaces the standalone `local_tickdata_replayer` binary + WebSocket
// transport with a zero-copy, in-process callback model.

pub mod config;
pub mod engine;
pub mod loader;
pub mod stats;
pub mod types;

use std::sync::{mpsc, Arc, RwLock};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use chrono_tz::America::New_York;
use log::info;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use config::ReplayConfig;
use engine::{ReplayCommand, Timeline};
use types::DataType;

// ── TickDataReplayer ────────────────────────────────────────────────

/// Tick-level data replayer, embedded in the Python process.
///
/// Reads Parquet files from the data lake, replays quotes and trades
/// at the correct pace using a virtual timeline, and delivers payloads
/// to a Python callback.
///
/// Example (Python)::
///
///     from jerry_trader._rust import TickDataReplayer
///
///     def on_tick(symbol: str, payload: dict):
///         print(symbol, payload["ev"], payload["bp"])
///
///     replayer = TickDataReplayer(
///         replay_date="20251113",
///         lake_data_dir="/mnt/data/lake",
///         data_start_ts_ns=clock.data_start_ts_ns,
///         speed=1.0,
///     )
///     replayer.subscribe("AAPL", ["Q", "T"], on_tick)
#[pyclass]
pub struct TickDataReplayer {
    cmd_tx: mpsc::Sender<ReplayCommand>,
    #[allow(dead_code)]
    engine_handle: Option<std::thread::JoinHandle<()>>,
    timeline: Arc<RwLock<Timeline>>,
}

#[pymethods]
impl TickDataReplayer {
    /// Create a new replayer.
    ///
    /// Args:
    ///     replay_date: Date in YYYYMMDD format.
    ///     lake_data_dir: Path to the data-lake root.
    ///     data_start_ts_ns: Epoch-ns anchor for the virtual clock
    ///         (should match `ReplayClock.data_start_ts_ns`).
    ///     speed: Replay speed multiplier (1.0 = real-time).
    ///     start_time: Optional start time in ``"HH:MM"`` or
    ///         ``"HH:MM:SS"`` (America/New_York).
    ///     max_gap_ms: Threshold for logging large time gaps (ms).
    #[new]
    #[pyo3(signature = (replay_date, lake_data_dir, data_start_ts_ns, speed=1.0, start_time=None, max_gap_ms=None))]
    fn new(
        replay_date: &str,
        lake_data_dir: &str,
        data_start_ts_ns: i64,
        speed: f64,
        start_time: Option<&str>,
        max_gap_ms: Option<u64>,
    ) -> PyResult<Self> {
        let start_timestamp_ns = start_time
            .map(|st| parse_start_time(replay_date, st))
            .transpose()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?;

        let config = ReplayConfig {
            replay_date: replay_date.to_string(),
            start_timestamp_ns,
            lake_data_dir: lake_data_dir.to_string(),
            max_gap_ms,
        };

        let timeline = Arc::new(RwLock::new(Timeline::new(data_start_ts_ns, speed)));
        let (cmd_tx, cmd_rx) = mpsc::channel();

        let tl = timeline.clone();
        let cfg = config.clone();
        let engine_handle = std::thread::Builder::new()
            .name("replay-engine".into())
            .spawn(move || engine::engine_loop(cfg, cmd_rx, tl))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to start engine thread: {}",
                    e
                ))
            })?;

        info!(
            "TickDataReplayer created: date={}, speed={:.1}x, data_start_ts_ns={}",
            replay_date, speed, data_start_ts_ns,
        );

        Ok(Self {
            cmd_tx,
            engine_handle: Some(engine_handle),
            timeline,
        })
    }

    // ── Subscribe / Unsubscribe ─────────────────────────────────

    /// Subscribe a symbol for replay.
    ///
    /// Blocks until Parquet data is loaded; replay starts immediately
    /// after this call returns.
    ///
    /// Args:
    ///     symbol: Ticker symbol (e.g. ``"AAPL"``).
    ///     events: Event types — ``["Q"]``, ``["T"]``, or ``["Q","T"]``.
    ///     callback: ``fn(symbol: str, payload: dict) -> None``
    #[pyo3(signature = (symbol, events, callback))]
    fn subscribe(
        &self,
        py: Python,
        symbol: &str,
        events: Vec<String>,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        let data_types: Vec<DataType> = events
            .iter()
            .filter_map(|e| match e.as_str() {
                "Q" => Some(DataType::Quotes),
                "T" => Some(DataType::Trades),
                _ => None,
            })
            .collect();

        if data_types.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "events must contain 'Q' and/or 'T'",
            ));
        }

        let (ack_tx, ack_rx) = mpsc::channel();

        self.cmd_tx
            .send(ReplayCommand::Subscribe {
                symbol: symbol.to_string(),
                data_types,
                callback,
                ack: ack_tx,
            })
            .map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err("Engine thread is not running")
            })?;

        // Release GIL while waiting for Parquet load.
        // `move` captures ack_rx by value (Receiver is Send but not Sync,
        // so a reference can't cross the thread boundary).
        py.detach(move || {
            ack_rx
                .recv()
                .map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err("Engine thread hung up")
                })?
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Subscribe failed: {}",
                        e
                    ))
                })
        })
    }

    /// Unsubscribe a symbol (stops its replay tasks).
    fn unsubscribe(&self, symbol: &str) -> PyResult<()> {
        self.cmd_tx
            .send(ReplayCommand::Unsubscribe {
                symbol: symbol.to_string(),
            })
            .map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err("Engine thread is not running")
            })
    }

    /// Batch-preload Parquet data for multiple symbols at once.
    ///
    /// Each data-type file is scanned only **once** (with an ``is_in``
    /// filter) instead of once per symbol, then the result is cached
    /// so that subsequent :meth:`subscribe` calls skip I/O entirely.
    ///
    /// Args:
    ///     symbols: List of tickers, e.g. ``["AAPL", "MSFT"]``.
    ///     events:  Data types — ``["Q"]``, ``["T"]``, or ``["Q","T"]``.
    #[pyo3(signature = (symbols, events))]
    fn batch_preload(
        &self,
        py: Python,
        symbols: Vec<String>,
        events: Vec<String>,
    ) -> PyResult<()> {
        let data_types: Vec<DataType> = events
            .iter()
            .filter_map(|e| match e.as_str() {
                "Q" => Some(DataType::Quotes),
                "T" => Some(DataType::Trades),
                _ => None,
            })
            .collect();

        if data_types.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "events must contain 'Q' and/or 'T'",
            ));
        }

        let (ack_tx, ack_rx) = mpsc::channel();

        self.cmd_tx
            .send(ReplayCommand::BatchPreload {
                symbols,
                data_types,
                ack: ack_tx,
            })
            .map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err("Engine thread is not running")
            })?;

        // Release GIL while batch loading.
        py.detach(move || {
            ack_rx
                .recv()
                .map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err("Engine thread hung up")
                })?
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Batch preload failed: {}",
                        e
                    ))
                })
        })
    }

    // ── Transport controls ──────────────────────────────────────
    //
    // State changes (speed / pause / resume) are applied directly to the
    // shared `Arc<RwLock<Timeline>>` so they take effect immediately
    // (the replay tasks read the timeline on every loop iteration).
    // `jump_to` also notifies the engine thread to abort+restart tasks.

    /// Change replay speed.  Re-anchors the timeline so the current
    /// position is preserved.
    fn set_speed(&self, speed: f64) -> PyResult<()> {
        self.timeline.write().unwrap().set_speed(speed);
        Ok(())
    }

    /// Pause playback.
    fn pause(&self) -> PyResult<()> {
        self.timeline.write().unwrap().pause();
        Ok(())
    }

    /// Resume playback.
    fn resume(&self) -> PyResult<()> {
        self.timeline.write().unwrap().resume();
        Ok(())
    }

    /// Jump to a specific data timestamp (epoch ns).
    ///
    /// **Note:** running replay tasks are aborted on jump.  Full
    /// restart-from-new-position is planned for Phase E.
    fn jump_to(&self, target_ts_ns: i64) -> PyResult<()> {
        self.timeline.write().unwrap().jump_to(target_ts_ns);
        // Also notify engine so it can abort & restart replay tasks.
        let _ = self.cmd_tx.send(ReplayCommand::JumpTo(target_ts_ns));
        Ok(())
    }

    // ── Queries ─────────────────────────────────────────────────

    /// Current virtual time as epoch nanoseconds.
    fn now_ns(&self) -> i64 {
        self.timeline.read().unwrap().now_ns()
    }

    /// Current virtual time as epoch milliseconds.
    fn now_ms(&self) -> i64 {
        self.now_ns() / 1_000_000
    }

    /// Whether playback is paused.
    #[getter]
    fn is_paused(&self) -> bool {
        self.timeline.read().unwrap().is_paused()
    }

    /// Current speed multiplier.
    #[getter]
    fn speed(&self) -> f64 {
        self.timeline.read().unwrap().speed()
    }

    /// Current data-start anchor (epoch ns).
    #[getter]
    fn data_start_ts_ns(&self) -> i64 {
        self.timeline.read().unwrap().data_start_ts_ns()
    }

    /// Get replay statistics as a Python dict.
    fn get_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let tl = self.timeline.read().unwrap();
        let dict = PyDict::new(py);
        dict.set_item("now_ns", tl.now_ns())?;
        dict.set_item("speed", tl.speed())?;
        dict.set_item("paused", tl.is_paused())?;
        dict.set_item("data_start_ts_ns", tl.data_start_ts_ns())?;
        Ok(dict)
    }

    // ── Lifecycle ───────────────────────────────────────────────

    /// Shut down the engine thread.  Called automatically on drop.
    fn shutdown(&mut self) -> PyResult<()> {
        let _ = self.cmd_tx.send(ReplayCommand::Shutdown);
        if let Some(handle) = self.engine_handle.take() {
            let _ = handle.join();
        }
        Ok(())
    }

    fn __repr__(&self) -> String {
        let tl = self.timeline.read().unwrap();
        let state = if tl.is_paused() { "paused" } else { "running" };
        format!(
            "TickDataReplayer(now_ms={}, speed={:.1}x, {})",
            tl.now_ns() / 1_000_000,
            tl.speed(),
            state,
        )
    }
}

impl Drop for TickDataReplayer {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(ReplayCommand::Shutdown);
        // Don't join here — could deadlock if called from Python __del__.
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

/// Parse "HH:MM" or "HH:MM:SS" (America/New_York) to epoch nanoseconds.
fn parse_start_time(replay_date: &str, start_time_str: &str) -> anyhow::Result<i64> {
    let year: i32 = replay_date[0..4].parse()?;
    let month: u32 = replay_date[4..6].parse()?;
    let day: u32 = replay_date[6..8].parse()?;

    let parts: Vec<&str> = start_time_str.split(':').collect();
    let hour: u32 = parts[0].parse()?;
    let minute: u32 = parts[1].parse()?;
    let second: u32 = if parts.len() > 2 {
        parts[2].parse()?
    } else {
        0
    };

    let naive_date =
        NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| anyhow::anyhow!("Invalid date"))?;
    let naive_time = NaiveTime::from_hms_opt(hour, minute, second)
        .ok_or_else(|| anyhow::anyhow!("Invalid time"))?;
    let naive_dt = NaiveDateTime::new(naive_date, naive_time);

    let dt = New_York
        .from_local_datetime(&naive_dt)
        .single()
        .ok_or_else(|| anyhow::anyhow!("Ambiguous or invalid datetime"))?;

    let timestamp_ns = dt
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow::anyhow!("Timestamp out of range"))?;

    Ok(timestamp_ns)
}
