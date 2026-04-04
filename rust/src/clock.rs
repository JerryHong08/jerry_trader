// rust/src/clock.rs
//
// High-precision replay clock for the jerry_trader system.
//
// In replay mode this is the **single source of truth** for "what time is it?"
// across every Python module (FactorEngine, StateEngine, BFF, etc.).
//
// Uses `std::time::Instant` (monotonic) for drift-free timekeeping, the same
// approach proven in the local_tickdata_replayer's 3-tier adaptive sleep.
//
// In live mode the Python `clock.py` singleton bypasses this entirely and
// falls back to `time.time()`, so there is zero overhead.
//
// Exposed to Python as `jerry_trader._rust.ReplayClock` via PyO3.

use pyo3::prelude::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

// ── ReplayClock ─────────────────────────────────────────────────────

/// A monotonic, drift-free virtual clock for replay mode.
///
/// Maps wall-clock elapsed time to a data-time range anchored at
/// `data_start_ts_ns`.  Supports pause/resume, speed control, and
/// arbitrary seek (`jump_to`).
///
/// All "now" queries are O(1) with no syscall — just an `Instant`
/// subtraction and a multiply.
///
/// The clock also publishes its current time to a shared `AtomicI64`
/// so that other Rust components (e.g., `TickDataReplayer`) can read
/// the time without GIL contention.
#[pyclass]
#[derive(Debug)]
pub struct ReplayClock {
    /// Monotonic anchor — when the clock was last (re-)started.
    wall_clock_start: Instant,

    /// Market-data epoch nanoseconds corresponding to `wall_clock_start`.
    data_start_ts_ns: i64,

    /// Replay speed multiplier (1.0 = real-time, 2.0 = 2× fast-forward).
    speed: f64,

    /// Whether the clock is paused.
    paused: bool,

    /// The `Instant` at which `pause()` was called (if currently paused).
    pause_start: Option<Instant>,

    /// Accumulated wall-clock nanoseconds spent in paused state.
    /// Subtracted from raw elapsed to get effective elapsed.
    total_pause_ns: i64,

    /// Shared atomic time for zero-GIL reads by other Rust components.
    /// Updated on every `now_ns()` call.
    shared_time_ns: Arc<AtomicI64>,
}

#[pymethods]
impl ReplayClock {
    // ── Constructor ─────────────────────────────────────────────────

    /// Create a new `ReplayClock`.
    ///
    /// Args:
    ///     data_start_ts_ns: The market-data epoch nanosecond timestamp that
    ///         corresponds to "now" (i.e. the earliest tick or the jump target).
    ///     speed: Replay speed multiplier (default 1.0 = real-time).
    #[new]
    #[pyo3(signature = (data_start_ts_ns, speed = 1.0))]
    fn new(data_start_ts_ns: i64, speed: f64) -> Self {
        let shared = Arc::new(AtomicI64::new(data_start_ts_ns));
        Self {
            wall_clock_start: Instant::now(),
            data_start_ts_ns,
            speed,
            paused: false,
            pause_start: None,
            total_pause_ns: 0,
            shared_time_ns: shared,
        }
    }

    // ── Time queries ────────────────────────────────────────────────

    /// Current replay time as epoch **nanoseconds**.
    fn now_ns(&self) -> i64 {
        let raw_elapsed_ns = self.raw_elapsed_ns();
        let effective_ns = raw_elapsed_ns - self.total_pause_ns;
        let result = self.data_start_ts_ns + (effective_ns as f64 * self.speed) as i64;

        // Update shared atomic for zero-GIL reads
        self.shared_time_ns.store(result, Ordering::Relaxed);

        result
    }

    /// Current replay time as epoch **milliseconds**.
    fn now_ms(&self) -> i64 {
        self.now_ns() / 1_000_000
    }

    /// Effective wall-clock nanoseconds elapsed since start (speed-adjusted).
    fn elapsed_ns(&self) -> i64 {
        let raw = self.raw_elapsed_ns();
        let effective = raw - self.total_pause_ns;
        (effective as f64 * self.speed) as i64
    }

    // ── Speed control ───────────────────────────────────────────────

    /// Change replay speed.
    ///
    /// Re-anchors the clock so the current position is preserved:
    /// the new speed takes effect from this instant onward.
    fn set_speed(&mut self, speed: f64) {
        // Snapshot current position before re-anchoring.
        let current = self.now_ns();
        self.wall_clock_start = Instant::now();
        self.total_pause_ns = 0;
        self.pause_start = None;
        self.paused = false;
        self.data_start_ts_ns = current;
        self.speed = speed;

        // Update shared time
        self.shared_time_ns.store(current, Ordering::Relaxed);
    }

    /// Get the current speed multiplier.
    #[getter]
    fn speed(&self) -> f64 {
        self.speed
    }

    // ── Pause / Resume ──────────────────────────────────────────────

    /// Freeze the clock.  Subsequent `now_*()` calls return the frozen time.
    fn pause(&mut self) {
        if !self.paused {
            self.paused = true;
            self.pause_start = Some(Instant::now());
        }
    }

    /// Resume the clock from where it was frozen.
    fn resume(&mut self) {
        if self.paused {
            if let Some(ps) = self.pause_start.take() {
                self.total_pause_ns += ps.elapsed().as_nanos() as i64;
            }
            self.paused = false;
        }
    }

    /// Whether the clock is currently paused.
    #[getter]
    fn is_paused(&self) -> bool {
        self.paused
    }

    // ── Seek ────────────────────────────────────────────────────────

    /// Jump to an arbitrary point in market-data time.
    ///
    /// Re-anchors `wall_clock_start` and `data_start_ts_ns` atomically.
    /// If the clock was paused, it stays paused at the new position.
    fn jump_to(&mut self, target_ts_ns: i64) {
        let was_paused = self.paused;
        // Re-anchor.
        let now = Instant::now();
        self.wall_clock_start = now;
        self.total_pause_ns = 0;
        self.data_start_ts_ns = target_ts_ns;
        // Preserve pause state — use the same Instant so now_ns() == target exactly.
        if was_paused {
            self.pause_start = Some(now);
        } else {
            self.pause_start = None;
        }

        // Update shared time
        self.shared_time_ns.store(target_ts_ns, Ordering::Relaxed);
    }

    /// The current data-start anchor (epoch ns).
    /// Useful for diagnostics / logging.
    #[getter]
    fn data_start_ts_ns(&self) -> i64 {
        self.data_start_ts_ns
    }

    // ── Shared time access ──────────────────────────────────────────

    /// Get a clone of the shared time handle.
    ///
    /// This can be passed to other Rust components (e.g., TickDataReplayer)
    /// so they can read the clock time without GIL contention.
    #[pyo3(name = "get_shared_time")]
    fn py_get_shared_time(&self) -> SharedTimeHandle {
        SharedTimeHandle {
            inner: self.shared_time_ns.clone(),
        }
    }

    // ── Display ─────────────────────────────────────────────────────

    fn __repr__(&self) -> String {
        let state = if self.paused { "paused" } else { "running" };
        format!(
            "ReplayClock(now_ms={}, speed={:.1}x, {})",
            self.now_ms(),
            self.speed,
            state,
        )
    }
}

// ── Internal helpers (not exposed to Python) ────────────────────────

impl ReplayClock {
    /// Raw wall-clock nanoseconds since `wall_clock_start`, accounting for
    /// the fact that if we are *currently* paused we should not count from
    /// `pause_start` to now.
    fn raw_elapsed_ns(&self) -> i64 {
        if self.paused {
            // Frozen: elapsed up to the moment we paused.
            self.pause_start
                .map(|ps| ps.duration_since(self.wall_clock_start).as_nanos() as i64)
                .unwrap_or(0)
        } else {
            self.wall_clock_start.elapsed().as_nanos() as i64
        }
    }
}

// ── SharedTimeHandle ─────────────────────────────────────────────────

/// A handle for reading the ReplayClock time without GIL contention.
///
/// Created by `ReplayClock.get_shared_time()` and passed to other
/// Rust components that need to read the clock.
#[pyclass]
#[derive(Debug, Clone)]
pub struct SharedTimeHandle {
    inner: Arc<AtomicI64>,
}

impl SharedTimeHandle {
    /// Read the current clock time (epoch nanoseconds).
    ///
    /// This is lock-free and GIL-free — safe to call from any thread.
    pub fn now_ns(&self) -> i64 {
        self.inner.load(Ordering::Relaxed)
    }

    /// Read the current clock time (epoch milliseconds).
    pub fn now_ms(&self) -> i64 {
        self.now_ns() / 1_000_000
    }
}

#[pymethods]
impl SharedTimeHandle {
    /// Read the current clock time (epoch nanoseconds).
    #[pyo3(name = "now_ns")]
    fn py_now_ns(&self) -> i64 {
        self.now_ns()
    }

    /// Read the current clock time (epoch milliseconds).
    #[pyo3(name = "now_ms")]
    fn py_now_ms(&self) -> i64 {
        self.now_ms()
    }

    fn __repr__(&self) -> String {
        format!("SharedTimeHandle(now_ns={})", self.now_ns())
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    const START_NS: i64 = 1_700_000_000_000_000_000; // arbitrary epoch ns

    #[test]
    fn test_now_advances() {
        let clock = ReplayClock::new(START_NS, 1.0);
        let t0 = clock.now_ns();
        thread::sleep(Duration::from_millis(10));
        let t1 = clock.now_ns();
        assert!(t1 > t0, "clock should advance: t0={t0}, t1={t1}");
        // At 1× speed, ~10ms of wall time = ~10_000_000 ns of data time.
        let delta = t1 - t0;
        assert!(delta > 5_000_000, "delta should be > 5ms: {delta}");
        assert!(delta < 50_000_000, "delta should be < 50ms: {delta}");
    }

    #[test]
    fn test_now_ms_matches_now_ns() {
        let clock = ReplayClock::new(START_NS, 1.0);
        let ns = clock.now_ns();
        let ms = clock.now_ms();
        assert_eq!(ms, ns / 1_000_000);
    }

    #[test]
    fn test_speed_multiplier() {
        let clock = ReplayClock::new(START_NS, 2.0);
        thread::sleep(Duration::from_millis(50));
        let elapsed_data = clock.now_ns() - START_NS;
        // At 2× speed, ~50ms wall → ~100ms data.
        // Allow generous tolerance for CI.
        assert!(
            elapsed_data > 70_000_000,
            "2× speed: data elapsed should be > 70ms: {elapsed_data}"
        );
        assert!(
            elapsed_data < 200_000_000,
            "2× speed: data elapsed should be < 200ms: {elapsed_data}"
        );
    }

    #[test]
    fn test_pause_resume() {
        let mut clock = ReplayClock::new(START_NS, 1.0);
        thread::sleep(Duration::from_millis(10));

        clock.pause();
        assert!(clock.is_paused());
        let frozen = clock.now_ns();

        // Wait while paused — time should NOT advance.
        thread::sleep(Duration::from_millis(50));
        let still_frozen = clock.now_ns();
        assert_eq!(frozen, still_frozen, "clock should be frozen while paused");

        // Resume.
        clock.resume();
        assert!(!clock.is_paused());
        thread::sleep(Duration::from_millis(10));
        let after_resume = clock.now_ns();
        assert!(
            after_resume > frozen,
            "clock should advance after resume: frozen={frozen}, after={after_resume}"
        );
        // The 50ms we spent paused should NOT count.
        let total_data_elapsed = after_resume - START_NS;
        assert!(
            total_data_elapsed < 80_000_000,
            "paused time should not count: total={total_data_elapsed}"
        );
    }

    #[test]
    fn test_jump_to() {
        let mut clock = ReplayClock::new(START_NS, 1.0);
        thread::sleep(Duration::from_millis(10));

        let jump_target = START_NS + 3_600_000_000_000; // +1 hour
        clock.jump_to(jump_target);

        let after_jump = clock.now_ns();
        // Should be very close to jump_target (within a few ms of wall time).
        let diff = (after_jump - jump_target).abs();
        assert!(
            diff < 5_000_000,
            "after jump should be near target: diff={diff}ns"
        );
    }

    #[test]
    fn test_jump_while_paused() {
        let mut clock = ReplayClock::new(START_NS, 1.0);
        clock.pause();

        let jump_target = START_NS + 7_200_000_000_000; // +2 hours
        clock.jump_to(jump_target);

        // Should still be paused at the new position.
        assert!(clock.is_paused());
        let t = clock.now_ns();
        assert_eq!(t, jump_target, "paused jump should land exactly on target");

        // Wait — should remain frozen.
        thread::sleep(Duration::from_millis(10));
        assert_eq!(clock.now_ns(), jump_target);
    }

    #[test]
    fn test_set_speed_preserves_position() {
        let mut clock = ReplayClock::new(START_NS, 1.0);
        thread::sleep(Duration::from_millis(20));
        let before = clock.now_ns();

        clock.set_speed(5.0);
        let after = clock.now_ns();
        // Position should be ~same (within a few ms of re-anchor).
        let diff = (after - before).abs();
        assert!(
            diff < 5_000_000,
            "set_speed should preserve position: diff={diff}ns"
        );

        // Now time advances 5× faster.
        thread::sleep(Duration::from_millis(20));
        let later = clock.now_ns();
        let fast_delta = later - after;
        assert!(
            fast_delta > 50_000_000,
            "5× speed: data delta should be > 50ms: {fast_delta}"
        );
    }

    #[test]
    fn test_elapsed_ns() {
        let clock = ReplayClock::new(START_NS, 2.0);
        thread::sleep(Duration::from_millis(25));
        let e = clock.elapsed_ns();
        // 25ms wall × 2× speed = ~50ms data elapsed.
        assert!(e > 30_000_000, "elapsed should be > 30ms: {e}");
        assert!(e < 100_000_000, "elapsed should be < 100ms: {e}");
    }

    #[test]
    fn test_repr() {
        let clock = ReplayClock::new(START_NS, 1.0);
        let r = clock.__repr__();
        assert!(r.contains("ReplayClock("));
        assert!(r.contains("1.0x"));
        assert!(r.contains("running"));
    }

    #[test]
    fn test_double_pause_is_idempotent() {
        let mut clock = ReplayClock::new(START_NS, 1.0);
        clock.pause();
        let t1 = clock.now_ns();
        clock.pause(); // second pause should be a no-op
        let t2 = clock.now_ns();
        assert_eq!(t1, t2);
    }

    #[test]
    fn test_double_resume_is_idempotent() {
        let mut clock = ReplayClock::new(START_NS, 1.0);
        clock.pause();
        clock.resume();
        clock.resume(); // second resume should be a no-op
        // Just ensure it doesn't panic.
        let _ = clock.now_ns();
    }

    #[test]
    fn test_shared_time_handle() {
        let clock = ReplayClock::new(START_NS, 1.0);
        let handle = clock.py_get_shared_time();

        // Initial value
        let t0 = handle.now_ns();
        assert!(t0 >= START_NS);

        // After some time
        thread::sleep(Duration::from_millis(10));
        clock.now_ns(); // Update shared time
        let t1 = handle.now_ns();
        assert!(t1 > t0, "shared time should advance: t0={t0}, t1={t1}");
    }

    #[test]
    fn test_shared_time_no_gil() {
        let clock = ReplayClock::new(START_NS, 1.0);
        let handle = clock.py_get_shared_time();

        // Read from another thread without GIL
        let handle_clone = handle.clone();
        let t = thread::spawn(move || {
            handle_clone.now_ns()
        }).join().unwrap();

        assert!(t >= START_NS);
    }
}
