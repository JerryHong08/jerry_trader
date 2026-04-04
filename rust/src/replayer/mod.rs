// rust/src/replayer/mod.rs
//
// `TickDataReplayer` — PyO3 class that embeds the tick-data replay
// engine directly in the Python process.
//
// Replaces the standalone `local_tickdata_replayer` binary + WebSocket
// transport with a zero-copy, in-process callback model.
//
// IMPORTANT: The replayer uses a shared time handle from `ReplayClock`
// to ensure all components see the same virtual time. This eliminates
// clock drift between the Python clock and Rust replayer.

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

// Import SharedTimeHandle from clock module
use crate::clock::SharedTimeHandle;

// ── TickDataReplayer ────────────────────────────────────────────────

/// Tick-level data replayer, embedded in the Python process.
///
/// Reads Parquet files from the data lake, replays quotes and trades
/// at the correct pace using a shared virtual timeline, and delivers payloads
/// to a Python callback.
///
/// **Clock Synchronization:**
/// The replayer uses a `SharedTimeHandle` from `ReplayClock` to read the
/// current virtual time. This ensures all components (Python clock, bar builder,
/// factor engine) see the same time without drift.
///
/// Example (Python)::
///
///     from jerry_trader._rust import ReplayClock, TickDataReplayer
///
///     # Create the master clock
///     clock = ReplayClock(data_start_ts_ns=..., speed=1.0)
///
///     # Pass shared time handle to replayer
///     replayer = TickDataReplayer(
///         replay_date="20251113",
///         lake_data_dir="/mnt/data/lake",
///         shared_time=clock.get_shared_time(),
///     )
///     replayer.subscribe("AAPL", ["Q", "T"], on_tick)
#[pyclass]
pub struct TickDataReplayer {
    cmd_tx: mpsc::Sender<ReplayCommand>,
    #[allow(dead_code)]
    engine_handle: Option<std::thread::JoinHandle<()>>,
    /// Legacy timeline for backward compatibility (used when no shared_time)
    timeline: Arc<RwLock<Timeline>>,
    /// Shared time handle from ReplayClock (preferred)
    shared_time: Option<SharedTimeHandle>,
    /// Speed for legacy mode
    speed: f64,
}

#[pymethods]
impl TickDataReplayer {
    /// Create a new replayer.
    ///
    /// **Recommended (shared time mode):**
    ///     replayer = TickDataReplayer(
    ///         replay_date="20251113",
    ///         lake_data_dir="/mnt/data/lake",
    ///         shared_time=clock.get_shared_time(),
    ///     )
    ///
    /// **Legacy (standalone timeline):**
    ///     replayer = TickDataReplayer(
    ///         replay_date="20251113",
    ///         lake_data_dir="/mnt/data/lake",
    ///         data_start_ts_ns=...,
    ///         speed=1.0,
    ///     )
    ///
    /// Args:
    ///     replay_date: Date in YYYYMMDD format.
    ///     lake_data_dir: Path to the data-lake root.
    ///     shared_time: A `SharedTimeHandle` from `ReplayClock.get_shared_time()`.
    ///         When provided, the replayer uses this as the time source (recommended).
    ///     data_start_ts_ns: (Legacy) Epoch-ns anchor for the virtual clock.
    ///         Ignored if `shared_time` is provided.
    ///     speed: (Legacy) Replay speed multiplier. Ignored if `shared_time` is provided.
    ///     start_time: Optional start time in ``"HH:MM"`` or ``"HH:MM:SS"``.
    ///     max_gap_ms: Threshold for logging large time gaps (ms).
    #[new]
    #[pyo3(signature = (replay_date, lake_data_dir, shared_time=None, data_start_ts_ns=0, speed=1.0, start_time=None, max_gap_ms=None))]
    fn new(
        replay_date: &str,
        lake_data_dir: &str,
        shared_time: Option<SharedTimeHandle>,
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

        // Use shared time if provided, otherwise create standalone timeline
        let (timeline, effective_speed) = if let Some(ref handle) = shared_time {
            // Shared time mode - timeline is just a placeholder for pause state
            let tl = Arc::new(RwLock::new(Timeline::new_placeholder()));
            (tl, 1.0) // Speed is controlled by ReplayClock, not us
        } else {
            // Legacy mode - standalone timeline
            let tl = Arc::new(RwLock::new(Timeline::new(data_start_ts_ns, speed)));
            (tl, speed)
        };

        let (cmd_tx, cmd_rx) = mpsc::channel();

        let tl = timeline.clone();
        let st = shared_time.clone();
        let cfg = config.clone();
        let engine_handle = std::thread::Builder::new()
            .name("replay-engine".into())
            .spawn(move || engine::engine_loop(cfg, cmd_rx, tl, st))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to start engine thread: {}",
                    e
                ))
            })?;

        if shared_time.is_some() {
            info!(
                "TickDataReplayer created (shared time mode): date={}",
                replay_date,
            );
        } else {
            info!(
                "TickDataReplayer created (legacy mode): date={}, speed={:.1}x, data_start_ts_ns={}",
                replay_date, speed, data_start_ts_ns,
            );
        }

        Ok(Self {
            cmd_tx,
            engine_handle: Some(engine_handle),
            timeline,
            shared_time,
            speed: effective_speed,
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

    // ── Transport controls (legacy mode only) ───────────────────────
    //
    // In shared time mode, these are no-ops because the ReplayClock
    // controls time. In legacy mode, they update the internal Timeline.

    /// Change replay speed (legacy mode only).
    ///
    /// In shared time mode, this is a no-op. Control speed via `ReplayClock.set_speed()`.
    fn set_speed(&self, speed: f64) -> PyResult<()> {
        if self.shared_time.is_some() {
            log::warn!("set_speed() called in shared time mode - use ReplayClock.set_speed() instead");
            return Ok(());
        }
        self.timeline.write().unwrap().set_speed(speed);
        Ok(())
    }

    /// Pause playback (legacy mode only).
    ///
    /// In shared time mode, this is a no-op. Use `ReplayClock.pause()`.
    fn pause(&self) -> PyResult<()> {
        if self.shared_time.is_some() {
            log::warn!("pause() called in shared time mode - use ReplayClock.pause() instead");
            return Ok(());
        }
        self.timeline.write().unwrap().pause();
        Ok(())
    }

    /// Resume playback (legacy mode only).
    ///
    /// In shared time mode, this is a no-op. Use `ReplayClock.resume()`.
    fn resume(&self) -> PyResult<()> {
        if self.shared_time.is_some() {
            log::warn!("resume() called in shared time mode - use ReplayClock.resume() instead");
            return Ok(());
        }
        self.timeline.write().unwrap().resume();
        Ok(())
    }

    /// Jump to a specific data timestamp (legacy mode only).
    ///
    /// In shared time mode, this is a no-op. Use `ReplayClock.jump_to()`.
    fn jump_to(&self, target_ts_ns: i64) -> PyResult<()> {
        if self.shared_time.is_some() {
            log::warn!("jump_to() called in shared time mode - use ReplayClock.jump_to() instead");
            return Ok(());
        }
        self.timeline.write().unwrap().jump_to(target_ts_ns);
        let _ = self.cmd_tx.send(ReplayCommand::JumpTo(target_ts_ns));
        Ok(())
    }

    // ── Queries ─────────────────────────────────────────────────

    /// Current virtual time as epoch nanoseconds.
    fn now_ns(&self) -> i64 {
        if let Some(ref handle) = self.shared_time {
            handle.now_ns()
        } else {
            self.timeline.read().unwrap().now_ns()
        }
    }

    /// Current virtual time as epoch milliseconds.
    fn now_ms(&self) -> i64 {
        self.now_ns() / 1_000_000
    }

    /// Whether playback is paused.
    #[getter]
    fn is_paused(&self) -> bool {
        if self.shared_time.is_some() {
            // In shared time mode, we don't track pause state here
            // The caller should check ReplayClock.is_paused()
            false
        } else {
            self.timeline.read().unwrap().is_paused()
        }
    }

    /// Current speed multiplier.
    #[getter]
    fn speed(&self) -> f64 {
        if self.shared_time.is_some() {
            // In shared time mode, speed is controlled by ReplayClock
            1.0 // Placeholder - caller should check ReplayClock.speed
        } else {
            self.timeline.read().unwrap().speed()
        }
    }

    /// Current data-start anchor (epoch ns).
    #[getter]
    fn data_start_ts_ns(&self) -> i64 {
        if self.shared_time.is_some() {
            // In shared time mode, this is managed by ReplayClock
            0 // Placeholder
        } else {
            self.timeline.read().unwrap().data_start_ts_ns()
        }
    }

    /// Get replay statistics as a Python dict.
    fn get_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("now_ns", self.now_ns())?;
        dict.set_item("speed", self.speed())?;
        dict.set_item("paused", self.is_paused())?;
        dict.set_item("data_start_ts_ns", self.data_start_ts_ns())?;
        dict.set_item("shared_time_mode", self.shared_time.is_some())?;
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
        let state = if self.is_paused() { "paused" } else { "running" };
        let mode = if self.shared_time.is_some() { "shared" } else { "legacy" };
        format!(
            "TickDataReplayer(now_ms={}, mode={}, {})",
            self.now_ns() / 1_000_000,
            mode,
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
