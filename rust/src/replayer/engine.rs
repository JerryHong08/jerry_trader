// rust/src/replayer/engine.rs
//
// Replay engine: internal timeline, command dispatch, and the
// timing-critical quote / trade replay loops.
//
// Runs on a dedicated OS thread with its own tokio runtime so that
// busy-wait spin-loops never block the caller.
//
// Clock modes:
// - Shared time mode: Uses SharedTimeHandle from ReplayClock (recommended)
// - Legacy mode: Uses internal Timeline (standalone)

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::Result;
use log::{error, info, warn};
use pyo3::prelude::*;
use tokio::task::JoinHandle;

use crate::clock::SharedTimeHandle;
use crate::replayer::config::ReplayConfig;
use crate::replayer::loader::{self, PreloadedData};
use crate::replayer::stats::ReplayStats;
use crate::replayer::types::{DataType, RawQuote, RawTrade};

// ── Timeline (legacy mode) ─────────────────────────────────────────────
//
// In shared time mode, this is a placeholder. Time queries go through
// SharedTimeHandle instead.

pub struct Timeline {
    wall_clock_start: Instant,
    data_start_ts_ns: i64,
    speed: f64,
    paused: bool,
    pause_start: Option<Instant>,
    total_pause_ns: i64,
    is_placeholder: bool,
}

impl Timeline {
    pub fn new(data_start_ts_ns: i64, speed: f64) -> Self {
        Self {
            wall_clock_start: Instant::now(),
            data_start_ts_ns,
            speed,
            paused: false,
            pause_start: None,
            total_pause_ns: 0,
            is_placeholder: false,
        }
    }

    /// Create a placeholder timeline for shared time mode.
    /// Time queries will be handled by SharedTimeHandle.
    pub fn new_placeholder() -> Self {
        Self {
            wall_clock_start: Instant::now(),
            data_start_ts_ns: 0,
            speed: 1.0,
            paused: false,
            pause_start: None,
            total_pause_ns: 0,
            is_placeholder: true,
        }
    }

    /// Current virtual time (epoch nanoseconds).
    pub fn now_ns(&self) -> i64 {
        if self.is_placeholder {
            warn!("Timeline::now_ns() called on placeholder - returning 0");
            return 0;
        }
        let raw = self.raw_elapsed_ns();
        let effective = raw - self.total_pause_ns;
        self.data_start_ts_ns + (effective as f64 * self.speed) as i64
    }

    pub fn set_speed(&mut self, speed: f64) {
        if self.is_placeholder {
            return;
        }
        let current = self.now_ns();
        self.wall_clock_start = Instant::now();
        self.total_pause_ns = 0;
        self.pause_start = None;
        self.paused = false;
        self.data_start_ts_ns = current;
        self.speed = speed;
    }

    pub fn pause(&mut self) {
        if self.is_placeholder {
            return;
        }
        if !self.paused {
            self.paused = true;
            self.pause_start = Some(Instant::now());
        }
    }

    pub fn resume(&mut self) {
        if self.is_placeholder {
            return;
        }
        if self.paused {
            if let Some(ps) = self.pause_start.take() {
                self.total_pause_ns += ps.elapsed().as_nanos() as i64;
            }
            self.paused = false;
        }
    }

    pub fn jump_to(&mut self, target_ts_ns: i64) {
        if self.is_placeholder {
            return;
        }
        let was_paused = self.paused;
        let now = Instant::now();
        self.wall_clock_start = now;
        self.total_pause_ns = 0;
        self.data_start_ts_ns = target_ts_ns;
        if was_paused {
            self.pause_start = Some(now);
        } else {
            self.pause_start = None;
        }
    }

    pub fn speed(&self) -> f64 {
        self.speed
    }

    pub fn is_paused(&self) -> bool {
        self.paused
    }

    pub fn data_start_ts_ns(&self) -> i64 {
        self.data_start_ts_ns
    }

    /// Compute the wall-clock `Instant` at which `target_data_ts_ns`
    /// will be reached.  Returns `None` if the target is already past
    /// or the clock is paused.
    pub fn target_instant_for(&self, target_data_ts_ns: i64) -> Option<Instant> {
        if self.is_placeholder || self.paused {
            return None;
        }
        let current = self.now_ns();
        if target_data_ts_ns <= current {
            return None;
        }
        let data_remaining = target_data_ts_ns - current;
        let wall_remaining_ns = (data_remaining as f64 / self.speed) as u64;
        Instant::now().checked_add(Duration::from_nanos(wall_remaining_ns))
    }

    fn raw_elapsed_ns(&self) -> i64 {
        if self.paused {
            self.pause_start
                .map(|ps| ps.duration_since(self.wall_clock_start).as_nanos() as i64)
                .unwrap_or(0)
        } else {
            self.wall_clock_start.elapsed().as_nanos() as i64
        }
    }
}

// ── Time source abstraction ────────────────────────────────────────────

/// Abstract time source that works in both shared and legacy modes.
pub enum TimeSource {
    Shared(SharedTimeHandle),
    Legacy(Arc<RwLock<Timeline>>),
}

impl TimeSource {
    /// Get current time in nanoseconds.
    pub fn now_ns(&self) -> i64 {
        match self {
            TimeSource::Shared(handle) => handle.now_ns(),
            TimeSource::Legacy(tl) => tl.read().unwrap().now_ns(),
        }
    }

    /// Check if paused (only meaningful in legacy mode).
    pub fn is_paused(&self) -> bool {
        match self {
            TimeSource::Shared(_) => false, // Pause state managed by ReplayClock
            TimeSource::Legacy(tl) => tl.read().unwrap().is_paused(),
        }
    }

    /// Get target instant for a future timestamp.
    /// In shared mode, we can't compute this (speed controlled by ReplayClock).
    pub fn target_instant_for(&self, target_ts_ns: i64) -> Option<Instant> {
        match self {
            TimeSource::Shared(_) => {
                // In shared time mode, we need to poll for time progression
                // since we don't control the speed. Return None to indicate
                // we should use polling instead.
                None
            }
            TimeSource::Legacy(tl) => tl.read().unwrap().target_instant_for(target_ts_ns),
        }
    }

    /// Get speed multiplier (only meaningful in legacy mode).
    pub fn speed(&self) -> f64 {
        match self {
            TimeSource::Shared(_) => 1.0, // Speed controlled by ReplayClock
            TimeSource::Legacy(tl) => tl.read().unwrap().speed(),
        }
    }
}

// ── Commands ────────────────────────────────────────────────────────

pub enum ReplayCommand {
    Subscribe {
        symbol: String,
        data_types: Vec<DataType>,
        callback: Py<PyAny>,
        ack: std::sync::mpsc::Sender<Result<()>>,
    },
    Unsubscribe {
        symbol: String,
    },
    /// Batch-load Parquet data for multiple symbols at once (1 scan per
    /// data type instead of N).  Results are stored in
    /// `preloaded_cache` so subsequent `Subscribe` calls skip I/O.
    BatchPreload {
        symbols: Vec<String>,
        data_types: Vec<DataType>,
        ack: std::sync::mpsc::Sender<Result<()>>,
    },
    SetSpeed(f64),
    Pause,
    Resume,
    JumpTo(i64),
    Shutdown,
}

// ── Engine main loop ────────────────────────────────────────────────

pub fn engine_loop(
    config: ReplayConfig,
    cmd_rx: std::sync::mpsc::Receiver<ReplayCommand>,
    timeline: Arc<RwLock<Timeline>>,
    shared_time: Option<SharedTimeHandle>,
) {
    // Build a dedicated tokio runtime for Parquet I/O + replay tasks.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("Failed to build tokio runtime");

    let mut active_tasks: HashMap<(String, DataType), JoinHandle<()>> = HashMap::new();
    let mut preloaded_cache: HashMap<String, PreloadedData> = HashMap::new();
    let stats = Arc::new(RwLock::new(ReplayStats::new()));

    // Create time source based on mode
    let time_source = if let Some(handle) = shared_time {
        info!("Replay engine started (shared time mode) for date {}", config.replay_date);
        TimeSource::Shared(handle)
    } else {
        info!("Replay engine started (legacy mode) for date {}", config.replay_date);
        TimeSource::Legacy(timeline)
    };

    loop {
        match cmd_rx.recv() {
            // ── Subscribe ───────────────────────────────────────────
            Ok(ReplayCommand::Subscribe {
                symbol,
                data_types,
                callback,
                ack,
            }) => {
                let callback = Arc::new(callback);
                let mut result: Result<()> = Ok(());

                for &dt in &data_types {
                    let cache_key = format!("{}_{:?}", symbol, dt);

                    // Load Parquet if not already cached.
                    if !preloaded_cache.contains_key(&cache_key) {
                        match rt.block_on(loader::load_symbol_data(&config, &symbol, dt)) {
                            Ok(data) => {
                                preloaded_cache.insert(cache_key.clone(), data);
                            }
                            Err(e) => {
                                error!("Failed to load {} {:?}: {}", symbol, dt, e);
                                result = Err(e);
                                continue;
                            }
                        }
                    }

                    // Skip if already replaying this (symbol, data_type).
                    if active_tasks.contains_key(&(symbol.clone(), dt)) {
                        info!("{} {:?} already subscribed, skipping", symbol, dt);
                        continue;
                    }

                    // Spawn the replay task.
                    if let Some(data) = preloaded_cache.get(&cache_key) {
                        let handle = spawn_replay_task(
                            &rt,
                            symbol.clone(),
                            dt,
                            data.clone(),
                            time_source.clone(),
                            callback.clone(),
                            stats.clone(),
                            config.max_gap_ms,
                        );
                        active_tasks.insert((symbol.clone(), dt), handle);
                    }
                }

                let _ = ack.send(result);
            }

            // ── BatchPreload ────────────────────────────────────────
            Ok(ReplayCommand::BatchPreload {
                symbols,
                data_types,
                ack,
            }) => {
                info!(
                    "Batch preloading {} symbols ({:?})",
                    symbols.len(),
                    data_types
                );
                let load_start = Instant::now();

                match rt.block_on(loader::load_multi_symbol_data(
                    &config, &symbols, &data_types,
                )) {
                    Ok(loaded) => {
                        let count = loaded.len();
                        for (key, data) in loaded {
                            preloaded_cache.insert(key, data);
                        }
                        let elapsed = load_start.elapsed();
                        info!(
                            "Batch preload done: {} cache entries in {:.1}s",
                            count,
                            elapsed.as_secs_f64()
                        );
                        let _ = ack.send(Ok(()));
                    }
                    Err(e) => {
                        error!("Batch preload failed: {}", e);
                        let _ = ack.send(Err(e));
                    }
                }
            }

            // ── Unsubscribe ─────────────────────────────────────────
            Ok(ReplayCommand::Unsubscribe { symbol }) => {
                for dt in [DataType::Quotes, DataType::Trades] {
                    if let Some(handle) = active_tasks.remove(&(symbol.clone(), dt)) {
                        handle.abort();
                        info!("Unsubscribed {} {:?}", symbol, dt);
                    }
                }
            }

            // ── Speed / Pause / Resume (legacy mode only) ──────────────
            Ok(ReplayCommand::SetSpeed(_))
            | Ok(ReplayCommand::Pause)
            | Ok(ReplayCommand::Resume) => {
                // These are handled directly by the Timeline in legacy mode
                // or are no-ops in shared time mode
            }

            // ── Jump ────────────────────────────────────────────────
            Ok(ReplayCommand::JumpTo(ts)) => {
                // Abort all running tasks; update timeline.
                for (_, handle) in active_tasks.drain() {
                    handle.abort();
                }
                // Timeline already updated by the Python method
                info!("Jumped to ts={}", ts);
            }

            // ── Shutdown ────────────────────────────────────────────
            Ok(ReplayCommand::Shutdown) | Err(_) => {
                info!("Replay engine shutting down");
                for (_, handle) in active_tasks.drain() {
                    handle.abort();
                }
                break;
            }
        }
    }

    rt.shutdown_timeout(Duration::from_secs(2));
    info!("Replay engine stopped");
}

// Implement Clone for TimeSource (needed for spawning tasks)
impl Clone for TimeSource {
    fn clone(&self) -> Self {
        match self {
            TimeSource::Shared(handle) => TimeSource::Shared(handle.clone()),
            TimeSource::Legacy(tl) => TimeSource::Legacy(tl.clone()),
        }
    }
}

// ── Task spawning ───────────────────────────────────────────────────

fn spawn_replay_task(
    rt: &tokio::runtime::Runtime,
    symbol: String,
    data_type: DataType,
    data: PreloadedData,
    time_source: TimeSource,
    callback: Arc<Py<PyAny>>,
    stats: Arc<RwLock<ReplayStats>>,
    max_gap_ms: Option<u64>,
) -> JoinHandle<()> {
    rt.spawn(async move {
        let result = match data_type {
            DataType::Quotes => {
                if let Some(quotes) = data.quotes {
                    replay_quotes(
                        symbol.clone(),
                        quotes,
                        time_source,
                        callback,
                        stats,
                        max_gap_ms,
                    )
                    .await
                } else {
                    Ok(())
                }
            }
            DataType::Trades => {
                if let Some(trades) = data.trades {
                    replay_trades(symbol.clone(), trades, time_source, callback, stats, max_gap_ms)
                        .await
                } else {
                    Ok(())
                }
            }
        };

        if let Err(e) = result {
            error!("Replay error for {}: {}", symbol, e);
        }
    })
}

// ── Quote replay (polling-based for shared time) ─────────────────────

async fn replay_quotes(
    symbol: String,
    quotes: Vec<RawQuote>,
    time_source: TimeSource,
    callback: Arc<Py<PyAny>>,
    stats: Arc<RwLock<ReplayStats>>,
    max_gap_ms: Option<u64>,
) -> Result<()> {
    if quotes.is_empty() {
        return Ok(());
    }

    // Skip data already in the past relative to the timeline.
    let current_ts = time_source.now_ns();
    let mut idx = quotes.partition_point(|q| q.participant_timestamp < current_ts);

    if idx > 0 {
        info!("{} skipped {} historical quotes", symbol, idx);
        stats.write().unwrap().increment_skipped(&symbol, idx);
    }

    info!(
        "Starting quote replay for {} ({} remaining of {})",
        symbol,
        quotes.len() - idx,
        quotes.len()
    );

    let mut last_log = Instant::now();
    let mut messages_since_log: usize = 0;

    // Determine if we're in shared time mode
    let is_shared_mode = matches!(time_source, TimeSource::Shared(_));

    while idx < quotes.len() {
        // ── Pause gate (poll in shared mode, check in legacy mode) ──
        if is_shared_mode {
            // In shared mode, just poll for time progression
            tokio::time::sleep(Duration::from_millis(1)).await;
        } else {
            // In legacy mode, check pause state
            while time_source.is_paused() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        // ── Batch flush: all quotes up to current virtual time ──
        let target_ts = time_source.now_ns();
        let batch_start = idx;

        while idx < quotes.len() && quotes[idx].participant_timestamp <= target_ts {
            idx += 1;
        }

        // Send the batch to Python in one GIL acquisition.
        if idx > batch_start {
            let batch_count = idx - batch_start;
            messages_since_log += batch_count;

            tokio::task::block_in_place(|| {
                Python::attach(|py| -> PyResult<()> {
                    for i in batch_start..idx {
                        let payload = quotes[i].to_payload(&symbol);
                        let dict = payload.to_py_dict(py)?;
                        callback.call1(py, (&symbol, dict))?;
                    }
                    Ok(())
                })
            })
            .map_err(|e: PyErr| anyhow::anyhow!("Callback error: {}", e))?;

            // Update stats.
            {
                let mut s = stats.write().unwrap();
                for _ in 0..batch_count {
                    s.increment_messages(&symbol);
                }
            }
        }

        // ── Periodic progress log (every 10 s) ─────────────────
        if last_log.elapsed().as_secs() >= 10 {
            let progress = idx as f64 / quotes.len() as f64 * 100.0;
            let rate = messages_since_log as f64 / last_log.elapsed().as_secs_f64();
            info!(
                "{} progress: {}/{} ({:.1}%), rate: {:.0} msg/s",
                symbol,
                idx,
                quotes.len(),
                progress,
                rate,
            );
            last_log = Instant::now();
            messages_since_log = 0;
        }

        // Done?
        if idx >= quotes.len() {
            break;
        }

        // ── Sleep toward next quote ─────────────────────────────
        let next_ts = quotes[idx].participant_timestamp;

        if is_shared_mode {
            // In shared mode, we don't know the speed, so we poll
            // Use a small sleep to avoid busy-waiting
            tokio::time::sleep(Duration::from_millis(10)).await;
        } else {
            // In legacy mode, use precise timing
            let target_instant = time_source.target_instant_for(next_ts);

            if let Some(target) = target_instant {
                // Log large market gaps.
                if let Some(max_gap) = max_gap_ms {
                    let remaining = target.saturating_duration_since(Instant::now());
                    if remaining.as_millis() > max_gap as u128 {
                        info!(
                            "{} waiting for market gap: {:.2}s",
                            symbol,
                            remaining.as_secs_f64(),
                        );
                    }
                }

                // Timing loop — three tiers.
                loop {
                    let now = Instant::now();
                    if now >= target {
                        break;
                    }

                    if time_source.is_paused() {
                        break;
                    }

                    let remaining = target.duration_since(now);
                    let remaining_us = remaining.as_micros() as i64;

                    if remaining_us < 50 {
                        // Tier 1 — < 50 µs: busy-wait for precision.
                        while Instant::now() < target {
                            std::hint::spin_loop();
                        }
                        break;
                    } else if remaining_us < 5_000 {
                        // Tier 2 — 50 µs–5 ms: sleep most, then spin.
                        let sleep_us = (remaining_us - 50).max(0) as u64;
                        if sleep_us > 0 {
                            tokio::time::sleep(Duration::from_micros(sleep_us)).await;
                        }
                    } else {
                        // Tier 3 — > 5 ms: chunked sleep (max 10 ms).
                        let chunk_us = ((remaining_us - 1_000).max(0) as u64).min(10_000);
                        if chunk_us > 0 {
                            tokio::time::sleep(Duration::from_micros(chunk_us)).await;
                        }
                    }
                }
            }
        }
    }

    info!("Completed quote replay for {}", symbol);
    Ok(())
}

// ── Trade replay (polling-based for shared time) ────────────────────

async fn replay_trades(
    symbol: String,
    trades: Vec<RawTrade>,
    time_source: TimeSource,
    callback: Arc<Py<PyAny>>,
    stats: Arc<RwLock<ReplayStats>>,
    _max_gap_ms: Option<u64>,
) -> Result<()> {
    if trades.is_empty() {
        return Ok(());
    }

    let current_ts = time_source.now_ns();
    let mut idx = trades.partition_point(|t| t.participant_timestamp < current_ts);

    if idx > 0 {
        info!("{} skipped {} historical trades", symbol, idx);
        stats.write().unwrap().increment_skipped(&symbol, idx);
    }

    info!(
        "Starting trade replay for {} ({} remaining of {})",
        symbol,
        trades.len() - idx,
        trades.len()
    );

    // Determine if we're in shared time mode
    let is_shared_mode = matches!(time_source, TimeSource::Shared(_));

    while idx < trades.len() {
        // Pause gate.
        if is_shared_mode {
            tokio::time::sleep(Duration::from_millis(1)).await;
        } else {
            while time_source.is_paused() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        let target_ts = time_source.now_ns();
        let batch_start = idx;

        while idx < trades.len() && trades[idx].participant_timestamp <= target_ts {
            idx += 1;
        }

        if idx > batch_start {
            let batch_count = idx - batch_start;

            tokio::task::block_in_place(|| {
                Python::attach(|py| -> PyResult<()> {
                    for i in batch_start..idx {
                        let payload = trades[i].to_payload(&symbol);
                        let dict = payload.to_py_dict(py)?;
                        callback.call1(py, (&symbol, dict))?;
                    }
                    Ok(())
                })
            })
            .map_err(|e: PyErr| anyhow::anyhow!("Callback error: {}", e))?;

            {
                let mut s = stats.write().unwrap();
                for _ in 0..batch_count {
                    s.increment_messages(&symbol);
                }
            }
        }

        if idx >= trades.len() {
            break;
        }

        // Sleep toward next trade
        let next_ts = trades[idx].participant_timestamp;

        if is_shared_mode {
            // In shared mode, poll for time progression
            tokio::time::sleep(Duration::from_millis(10)).await;
        } else {
            let target_instant = time_source.target_instant_for(next_ts);
            if let Some(target) = target_instant {
                let now = Instant::now();
                if now < target {
                    tokio::time::sleep(target.duration_since(now)).await;
                }
            }
        }
    }

    info!("Completed trade replay for {}", symbol);
    Ok(())
}
