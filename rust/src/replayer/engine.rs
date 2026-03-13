// rust/src/replayer/engine.rs
//
// Replay engine: internal timeline, command dispatch, and the
// timing-critical quote / trade replay loops.
//
// Runs on a dedicated OS thread with its own tokio runtime so that
// busy-wait spin-loops never block the caller.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::Result;
use log::{error, info};
use pyo3::prelude::*;
use tokio::task::JoinHandle;

use crate::replayer::config::ReplayConfig;
use crate::replayer::loader::{self, PreloadedData};
use crate::replayer::stats::ReplayStats;
use crate::replayer::types::{DataType, RawQuote, RawTrade};

// ── Timeline ────────────────────────────────────────────────────────
//
// Mirrors `ReplayClock` logic but is a plain Rust struct (no PyO3)
// behind `Arc<RwLock<…>>` so replay tasks can read it lock-free
// relative to each other (readers don't block readers).

pub struct Timeline {
    wall_clock_start: Instant,
    data_start_ts_ns: i64,
    speed: f64,
    paused: bool,
    pause_start: Option<Instant>,
    total_pause_ns: i64,
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
        }
    }

    /// Current virtual time (epoch nanoseconds).
    pub fn now_ns(&self) -> i64 {
        let raw = self.raw_elapsed_ns();
        let effective = raw - self.total_pause_ns;
        self.data_start_ts_ns + (effective as f64 * self.speed) as i64
    }

    pub fn set_speed(&mut self, speed: f64) {
        let current = self.now_ns();
        self.wall_clock_start = Instant::now();
        self.total_pause_ns = 0;
        self.pause_start = None;
        self.paused = false;
        self.data_start_ts_ns = current;
        self.speed = speed;
    }

    pub fn pause(&mut self) {
        if !self.paused {
            self.paused = true;
            self.pause_start = Some(Instant::now());
        }
    }

    pub fn resume(&mut self) {
        if self.paused {
            if let Some(ps) = self.pause_start.take() {
                self.total_pause_ns += ps.elapsed().as_nanos() as i64;
            }
            self.paused = false;
        }
    }

    pub fn jump_to(&mut self, target_ts_ns: i64) {
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
        if self.paused {
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

    info!("Replay engine started for date {}", config.replay_date);

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
                            timeline.clone(),
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

            // ── Speed / Pause / Resume ──────────────────────────────
            // These are applied directly to the shared Timeline by the
            // Python-facing methods — the engine doesn't need to act.
            Ok(ReplayCommand::SetSpeed(_))
            | Ok(ReplayCommand::Pause)
            | Ok(ReplayCommand::Resume) => {}

            // ── Jump ────────────────────────────────────────────────
            // Timeline already updated by the Python method; engine
            // just needs to abort running tasks.
            Ok(ReplayCommand::JumpTo(ts)) => {
                // Abort all running tasks; update timeline.
                for (_, handle) in active_tasks.drain() {
                    handle.abort();
                }
                timeline.write().unwrap().jump_to(ts);
                info!("Jumped to ts={}", ts);
                // Phase E will re-start tasks from the new position.
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

// ── Task spawning ───────────────────────────────────────────────────

fn spawn_replay_task(
    rt: &tokio::runtime::Runtime,
    symbol: String,
    data_type: DataType,
    data: PreloadedData,
    timeline: Arc<RwLock<Timeline>>,
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
                        timeline,
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
                    replay_trades(symbol.clone(), trades, timeline, callback, stats, max_gap_ms)
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

// ── Quote replay (3-tier adaptive sleep) ────────────────────────────

async fn replay_quotes(
    symbol: String,
    quotes: Vec<RawQuote>,
    timeline: Arc<RwLock<Timeline>>,
    callback: Arc<Py<PyAny>>,
    stats: Arc<RwLock<ReplayStats>>,
    max_gap_ms: Option<u64>,
) -> Result<()> {
    if quotes.is_empty() {
        return Ok(());
    }

    // Skip data already in the past relative to the timeline.
    let current_ts = timeline.read().unwrap().now_ns();
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

    while idx < quotes.len() {
        // ── Pause gate ──────────────────────────────────────────
        while timeline.read().unwrap().is_paused() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // ── Batch flush: all quotes up to current virtual time ──
        let target_ts = timeline.read().unwrap().now_ns();
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

        // ── 3-tier adaptive sleep toward next quote ─────────────
        let next_ts = quotes[idx].participant_timestamp;
        let target_instant = { timeline.read().unwrap().target_instant_for(next_ts) };

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

            // Timing loop — three tiers like local_tickdata_replayer.
            loop {
                let now = Instant::now();
                if now >= target {
                    break;
                }

                // Re-check pause (break to outer loop which will gate).
                if timeline.read().unwrap().is_paused() {
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
                    // Re-check at top of loop.
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

    info!("Completed quote replay for {}", symbol);
    Ok(())
}

// ── Trade replay (simpler sleep) ────────────────────────────────────

async fn replay_trades(
    symbol: String,
    trades: Vec<RawTrade>,
    timeline: Arc<RwLock<Timeline>>,
    callback: Arc<Py<PyAny>>,
    stats: Arc<RwLock<ReplayStats>>,
    _max_gap_ms: Option<u64>,
) -> Result<()> {
    if trades.is_empty() {
        return Ok(());
    }

    let current_ts = timeline.read().unwrap().now_ns();
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

    while idx < trades.len() {
        // Pause gate.
        while timeline.read().unwrap().is_paused() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let target_ts = timeline.read().unwrap().now_ns();
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

        // Simple tokio sleep (no spin-loop for trades — lower frequency).
        let next_ts = trades[idx].participant_timestamp;
        let target_instant = timeline.read().unwrap().target_instant_for(next_ts);
        if let Some(target) = target_instant {
            let now = Instant::now();
            if now < target {
                tokio::time::sleep(target.duration_since(now)).await;
            }
        }
    }

    info!("Completed trade replay for {}", symbol);
    Ok(())
}
