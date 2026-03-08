# Phase 2.5.3.5 — ReplayClock + TickDataReplayer Integration

## Problem Statement

The replay system has **two unsynchronized data streams** and **13+ modules** with no global clock:

| Stream | Engine | Pacing | Drift |
|--------|--------|--------|-------|
| **Tick data** (trades/quotes) | Rust `local_tickdata_replayer` (separate binary) | `Instant`-anchored, 3-tier adaptive sleep | <2ms/hr |
| **Snapshot data** (market movers) | Python `MarketSnapshotReplayer` | `asyncio.sleep(file_gap / speed)` | Cumulative, unbounded |

Modules like `FactorEngine`, `StateEngine`, `BFF`, `StaticDataWorker` call `time.time()` or `datetime.now()` — **completely wrong in replay mode**.

### Modules Requiring Time Awareness

| # | Module | Current Time Source | Severity |
|---|--------|-------------------|----------|
| 1 | FactorEngine | `time.time()` for compute grid + InfluxDB timestamps | **Critical** |
| 2 | StateEngine | `time.time()` for cooldown tracking | **High** |
| 3 | JerryTraderBFF | `datetime.now()` in WS messages to frontend | **High** |
| 4 | ChartDataBFF | `datetime.now().date()` for query range defaults | **Medium** |
| 5 | StaticDataWorker | `datetime.now(ET)` for cached data timestamps | **High** |
| 6 | NewsWorker | Custom `_get_current_time()` (reads stream, fragile) | **Medium** |
| 7 | NewsProcessor | Passes `current_time` from NewsWorker | **Medium** |
| 8 | MarketSnapshotReplayer | File timestamps (correct for its data, but unsynchronized) | **Medium** |
| 9 | BarsBuilder | Event timestamps from ticks (data-driven, low risk) | **Low** |
| 10 | Schema (NewsArticle) | `datetime.now()` fallbacks | **Low** |

---

## Architecture: Rust ReplayClock as Single Source of Truth

### Core Idea

The `local_tickdata_replayer` already has a drift-proof `GlobalTimeline` using `Instant`-anchored absolute targets. **Promote this into a shared `ReplayClock` PyO3 class** that every Python module can query, and **merge the tick replayer into `jerry_trader._rust`** as a PyO3 class reading from the same clock.

### Why the Rust Replayer's Timing Works

Three design choices make it drift-proof:

1. **Absolute-instant targets** — sleep target = `wall_clock_start + Duration::from_nanos(data_offset)`, not `now + delta`. Cumulative sleep error never compounds.

2. **3-tier adaptive sleep**:
   - `>5ms` → tokio sleep in 10ms chunks with recheck loop
   - `50µs–5ms` → tokio sleep then recheck
   - `<50µs` → `std::hint::spin_loop()` busy-wait

3. **Batch flush on wake** — every wake emits *all* overdue messages, self-correcting any oversleep.

### Why Python+Rust Preserves Precision

The timing-critical loop runs on a **Rust-owned OS thread** inside a Rust-owned tokio runtime. Python's GIL is not involved during pacing. The GIL is only acquired at the **delivery boundary** (~100-1000 Hz, not per-tick). Merging actually **reduces** latency by eliminating:

- WebSocket JSON serialization/deserialization (~100-500µs per batch)
- TCP loopback latency (~50-100µs per round-trip)
- `replayer_manager.py` async recv + queue dispatch overhead

### Post-Merge Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                    RUST (jerry_trader._rust)                        │
│                                                                     │
│  ┌──────────────┐   ┌──────────────────┐   ┌──────────────────┐    │
│  │ ReplayClock   │   │ TickDataReplayer  │   │ BarBuilder       │    │
│  │ #[pyclass]    │◀──│ #[pyclass]        │   │ #[pyclass]       │    │
│  │              │   │                  │   │ (existing)       │    │
│  │ now_ns()     │   │ Owns tokio       │   └──────────────────┘    │
│  │ now_ms()     │   │ runtime. Replay  │                           │
│  │ jump_to()    │   │ loop runs on     │                           │
│  │ set_speed()  │   │ Rust thread.     │                           │
│  │ pause()      │   │                  │                           │
│  │ resume()     │   │ Delivers ticks   │                           │
│  └──────┬───────┘   │ via PyO3 callback│                           │
│         │           └────────┬─────────┘                           │
└─────────┼──────────────────┼───────────────────────────────────────┘
          │                  │
          │ PyO3             │ PyO3 callback
          │                  │
┌─────────┼──────────────────┼───────────────────────────────────────┐
│         ▼                  ▼              PYTHON                    │
│  ┌──────────────┐   ┌──────────────────┐                           │
│  │ clock.py      │   │ on_tick()         │                          │
│  │ singleton     │   │  → bars_builder   │                          │
│  │              │   │  → factor_engine  │                          │
│  │ now_ms()     │   │  → tick_server    │                          │
│  │ now_datetime │   └──────────────────┘                           │
│  └──────┬───────┘                                                   │
│         │                                                           │
│    used by every module:                                            │
│    FactorEngine, StateEngine, BFF, ChartDataBFF,                    │
│    StaticDataWorker, NewsWorker, MarketSnapshotReplayer              │
│                                                                     │
│  ┌──────────────────────────────────┐                               │
│  │ RemoteClockFollower              │  (for remote machines)        │
│  │ Redis heartbeat + monotonic      │                               │
│  │ interpolation, <5ms accuracy     │                               │
│  └──────────────────────────────────┘                               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Detailed Component Design

### Component 1: `ReplayClock` — `rust/src/clock.rs`

```rust
#[pyclass]
pub struct ReplayClock {
    wall_clock_start: Instant,       // monotonic anchor
    data_start_ts_ns: i64,           // earliest market timestamp (epoch ns)
    speed: f64,                       // 1.0 = realtime, 2.0 = 2x
    paused: bool,
    pause_start: Option<Instant>,
    total_pause_ns: i64,              // accumulated pause duration
}

#[pymethods]
impl ReplayClock {
    #[new]
    fn new(data_start_ts_ns: i64, speed: f64) -> Self;

    fn now_ns(&self) -> i64;          // current replay time as epoch ns
    fn now_ms(&self) -> i64;          // current replay time as epoch ms
    fn jump_to(&mut self, target_ts_ns: i64);  // re-anchor to arbitrary time
    fn set_speed(&mut self, speed: f64);       // change speed (re-anchors)
    fn pause(&mut self);
    fn resume(&mut self);
    fn elapsed_ns(&self) -> i64;      // speed-adjusted elapsed since start
    fn is_paused(&self) -> bool;
    fn speed(&self) -> f64;
}
```

**Key design: `now_ns()` formula:**

```rust
fn now_ns(&self) -> i64 {
    let raw_elapsed = if self.paused {
        self.pause_start.unwrap().duration_since(self.wall_clock_start).as_nanos() as i64
    } else {
        self.wall_clock_start.elapsed().as_nanos() as i64
    };
    let effective_elapsed = raw_elapsed - self.total_pause_ns;
    self.data_start_ts_ns + (effective_elapsed as f64 * self.speed) as i64
}
```

**`jump_to()` re-anchors both fields atomically:**

```rust
fn jump_to(&mut self, target_ts_ns: i64) {
    self.total_pause_ns = 0;
    self.wall_clock_start = Instant::now();
    self.data_start_ts_ns = target_ts_ns;
}
```

### Component 2: `clock.py` — Python singleton

```python
# jerry_trader/clock.py
from jerry_trader._rust import ReplayClock
import time
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
_clock: ReplayClock | None = None

def init_clock(data_start_ts_ns: int, speed: float = 1.0):
    global _clock
    _clock = ReplayClock(data_start_ts_ns, speed)

def set_live_mode():
    global _clock
    _clock = None

def now_ms() -> int:
    if _clock is None:
        return int(time.time() * 1000)
    return _clock.now_ms()

def now_datetime() -> datetime:
    return datetime.fromtimestamp(now_ms() / 1000, tz=ET)

def jump_to(ts_ns: int):     _clock.jump_to(ts_ns)
def set_speed(s: float):     _clock.set_speed(s)
def pause():                  _clock.pause()
def resume():                 _clock.resume()
```

**Live mode behavior:** When `_clock is None`, `now_ms()` falls back to `time.time()`. Zero overhead, zero behavior change in live mode.

### Component 3: `TickDataReplayer` — `rust/src/replayer/`

Merged from `local_tickdata_replayer/src/` into the `jerry_trader._rust` crate.

**File structure:**

```
rust/src/
├── lib.rs              # PyO3 module root (add ReplayClock + TickDataReplayer)
├── bars.rs             # BarBuilder (existing, unchanged)
├── clock.rs            # ReplayClock #[pyclass]
└── replayer/
    ├── mod.rs           # TickDataReplayer #[pyclass]
    ├── config.rs        # ReplayConfig
    ├── types.rs         # QuotePayload, TradePayload, RawQuote, RawTrade
    ├── engine.rs        # Core replay loop (timing-critical code, unchanged)
    ├── loader.rs        # Parquet loading via polars
    └── stats.rs         # DriftStats, PerfMetrics
```

**Key `#[pyclass]`:**

```rust
#[pyclass]
pub struct TickDataReplayer {
    runtime: Arc<tokio::runtime::Runtime>,    // Rust-owned tokio runtime
    command_tx: mpsc::Sender<ReplayCommand>,  // control channel
    clock: Arc<ReplayClock>,                  // shared with Python clock.py
}

enum ReplayCommand {
    Subscribe { symbols: Vec<String>, events: Vec<String> },
    Unsubscribe { symbols: Vec<String> },
    JumpTo { target_ts_ns: i64 },
    SetSpeed { speed: f64 },
    Pause,
    Resume,
}

#[pymethods]
impl TickDataReplayer {
    #[new]
    fn new(clock: &ReplayClock, config: PyReplayConfig) -> PyResult<Self>;
    fn start(&self) -> PyResult<()>;
    fn set_callback(&self, callback: PyObject) -> PyResult<()>;
    fn subscribe(&self, symbols: Vec<String>, events: Vec<String>) -> PyResult<()>;
    fn unsubscribe(&self, symbols: Vec<String>) -> PyResult<()>;
    fn jump_to(&self, target_ts_ns: i64) -> PyResult<()>;
    fn pause(&self) -> PyResult<()>;
    fn resume(&self) -> PyResult<()>;
    fn set_speed(&self, speed: f64) -> PyResult<()>;
    fn get_stats(&self) -> PyResult<HashMap<String, PyObject>>;
}
```

**Data delivery — PyO3 callback (replaces WebSocket):**

```rust
// Inside the replay loop (Rust thread)
fn deliver_batch(callback: &PyObject, batch: Vec<Payload>) {
    Python::with_gil(|py| {
        for payload in &batch {
            let dict = payload.to_py_dict(py);
            callback.call1(py, (payload.symbol(), payload.event_type(), dict)).ok();
        }
    });
}
```

GIL acquisition frequency: ~100-1000 Hz (once per wake cycle). Negligible overhead.

**Python consumer side:**

```python
def on_tick(symbol: str, event: str, payload: dict):
    """Called from Rust thread with GIL held. Must be fast — just enqueue."""
    if event == "T":
        bars_builder_queue.put_nowait((symbol, payload))
        factor_engine_queue.put_nowait((symbol, payload))
    elif event == "Q":
        tick_data_queue.put_nowait((symbol, payload))

replayer = TickDataReplayer(clock=replay_clock, config=config)
replayer.set_callback(on_tick)
replayer.start()
replayer.subscribe(["AAPL", "TSLA"], ["T", "Q"])
```

### Component 4: Remote Machine Sync — Redis Heartbeat

The **ChartBFF machine** is the **domain master** of the replay clock. It runs
both ChartBFF and `local_tickdata_replayer`, owns the `ReplayClock` Rust
instance, and publishes heartbeats via Redis. Remote machines (e.g. the one
running `MarketSnapshotReplayer`) **follow** the clock via `RemoteClockFollower`.

```
Machine A (ChartBFF + tick replayer    Machine B (snapshot replayer)
         — clock master)
┌─────────────────────┐                ┌─────────────────────────┐
│ ReplayClock          │  Redis SET     │ RemoteClockFollower      │
│ ChartBFF             │──every 100ms──▶│ reads heartbeat          │
│ local_tickdata_      │                │ interpolates between     │
│   replayer           │                │ beats using monotonic_ns │
│ replay_clock:{sid}   │                │                         │
│ {ts_ns, speed,       │                │ MarketSnapshotReplayer   │
│  paused}             │                │ polls follower.now_ns()  │
└─────────────────────┘                └─────────────────────────┘
```

**Rationale:** The ChartBFF is the orchestrator that the frontend controls
(play/pause/seek/speed). Both ChartBFF and the tick replayer run on the same
machine and share the same `ReplayClock` in-process. Control commands from
the UI take effect immediately without cross-machine round-trips.

```python
class RemoteClockFollower:
    """Tracks remote ReplayClock via Redis heartbeat + local interpolation."""

    def __init__(self, redis, session_id, speed=1.0):
        self.speed = speed
        self._last_heartbeat_ts_ns = 0
        self._last_heartbeat_local = time.monotonic_ns()

    def sync(self, heartbeat: dict):
        self._last_heartbeat_ts_ns = heartbeat["ts_ns"]
        self._last_heartbeat_local = time.monotonic_ns()
        self.speed = heartbeat["speed"]

    def now_ns(self) -> int:
        local_elapsed = time.monotonic_ns() - self._last_heartbeat_local
        return self._last_heartbeat_ts_ns + int(local_elapsed * self.speed)
```

Expected accuracy: **<5ms** with 100ms heartbeat interval.

### Component 5: MarketSnapshotReplayer Sync

The Python `MarketSnapshotReplayer` becomes a **slave** to the central clock:

```python
# Before (cumulative drift):
time_diff = (ts[i] - ts[i-1]).total_seconds()
await asyncio.sleep(time_diff / self.speed)

# After (clock-synchronized):
while clock.now_ms() < target_file_ts_ms:
    await asyncio.sleep(0.01)  # poll every 10ms
# Emit — clock says it's time
```

On remote machines, use `RemoteClockFollower.now_ns()` instead.

### Component 6: The "Jump" Feature

Enables strategy pre-locate: jump to any point in the trading day.

```python
# Strategy pre-locator identifies a setup at 10:23 AM on Jan 15
target = datetime(2026, 1, 15, 10, 23, 0, tzinfo=ET)
target_ns = int(target.timestamp() * 1e9)

clock.jump_to(target_ns)
replayer.jump_to(target_ns)  # re-indexes into preloaded data
```

**When `jump_to()` fires, downstream modules must:**

| Module | Action on Jump |
|--------|---------------|
| TickDataReplayer | Stop current tasks, binary-skip to new position in preloaded data, resume |
| MarketSnapshotReplayer | Binary-search sorted file list, resume from target file |
| FactorEngine | Flush all `TickerContext` state, re-bootstrap from new position |
| BarsBuilder | `flush()` current bars, re-query ClickHouse for new position |
| StateEngine | Reset cooldown timers |
| BFF | Send `timeline_jump` event to frontend |
| ChartModule (frontend) | Clear chart, re-fetch bars for new time range |

---

## Module Migration Reference

Replace `time.time()` / `datetime.now()` with `clock.now_ms()` / `clock.now_datetime()`:

| Module | File | Line(s) | Before | After |
|--------|------|---------|--------|-------|
| FactorEngine | `engine.py` | L450 | `_snap_sec(int(time.time() * 1000))` | `_snap_sec(clock.now_ms())` |
| FactorEngine | `engine.py` | L668 | `_snap_sec(int(time.time() * 1000))` | `_snap_sec(clock.now_ms())` |
| StateEngine | `state_engine.py` | L206 | `now = time.time()` | `now_ms = clock.now_ms()` |
| BFF | `bff.py` | L380 | `datetime.now().isoformat()` | `clock.now_datetime().isoformat()` |
| ChartDataBFF | `chart_bff.py` | L463 | `datetime.now().date()` | `clock.now_datetime().date()` |
| StaticDataWorker | `static_data_worker.py` | L244 | `datetime.now(ET)` | `clock.now_datetime()` |
| NewsWorker | `news_worker.py` | L810 | `_get_current_time()` | `clock.now_datetime()` |

---

## Cargo.toml Changes

New dependencies for the replayer merge:

```toml
[dependencies]
# Existing
pyo3 = { version = "0.22", features = ["extension-module"] }
chrono = "0.4"
chrono-tz = "0.10"

# New for TickDataReplayer
tokio = { version = "1.35", features = ["full"] }
polars = { version = "0.43", features = ["parquet", "lazy", "dtype-datetime"] }
arrow = "53.0"
parquet = "53.0"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
anyhow = "1.0"
log = "0.4"
```

Trade-off: polars adds ~20MB to the `.so` and ~30-60s compile time. Acceptable for the gains.

---

## What We Gain vs. Lose

### Gains

| Aspect | Before (separate binary + WS) | After (merged PyO3) |
|--------|-------------------------------|---------------------|
| Clock sync | None (two independent timelines) | **Shared `Arc<ReplayClock>`** — zero drift |
| Jump | Not supported | `replayer.jump_to()` + `clock.jump_to()` atomic |
| Data delivery latency | ~200-600µs (WS + JSON) | ~5-20µs (GIL acquire + dict) |
| Speed control | Not supported | `replayer.set_speed(2.0)` |
| Build | Two crates, two builds | Single `maturin develop` |
| Deploy | Binary + manager.py + WS port | `import jerry_trader._rust` |
| Shared code | None | `SessionCalendar`, `ReplayClock`, timestamp utils |
| Process management | Separate process, may crash independently | In-process, shared lifecycle |

### Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Replayer crash takes down Python | Rust panics caught with `catch_unwind`; replay loop isolated on own thread |
| Increased `.so` binary size (~20MB from polars) | Acceptable trade-off; consider `parquet`+`arrow` direct if needed later |
| tokio runtime inside PyO3 | Well-supported pattern (pyo3-asyncio, robyn, pydantic-core all do this) |
| Full data preload into memory | Existing behavior; mitigate with streaming Parquet reads if needed |

---

## Implementation Phases

### Phase A — `ReplayClock` Rust core + Python singleton ✅

- [x] `rust/src/clock.rs` — `ReplayClock` `#[pyclass]` with `now_ns/ms`, `jump_to`, `set_speed`, `pause/resume` (11 Rust unit tests, 33 total pass)
- [x] Register in `rust/src/lib.rs`
- [x] `python/src/jerry_trader/clock.py` — singleton with live-mode fallback
- [x] Update `_rust.pyi` stubs
- [x] Unit tests: `python/tests/core/test_replay_clock.py` — 46 pytest tests (Rust direct, Python singleton, drift accuracy, monotonicity)
- [x] Wire into `backend_starter.py`: `init_replay()` in replay, `set_live_mode()` in live
- [x] CLI accepts `YYYYMMDD` (defaults 04:00 ET) or `YYYYMMDDHHMMSS` for exact start time

### Phase A.5 — Frontend clock sync ✅

- [x] `GET /api/clock` endpoint on ChartDataBFF (port 5002) — returns `{mode, now_ms, speed, paused, data_start_ts_ns, session_id}`
- [x] `TimelineClock.tsx` — polls `/api/clock` every 1s, interpolates between polls via `performance.now() × speed`
- [x] Visual indicators: orange `REPLAY` badge + pulsing dot, speed multiplier, `⏸ PAUSED` state, red dot on disconnect
- [x] Live mode: unchanged green "Market Time" display

### Phase B — Module migration (local machine) ✅

- [x] FactorEngine: `time.time()` → `clock.now_ms()` (2 lines, highest impact)
- [x] StateEngine: `time.time()` → `clock.now_ms()` / `clock.now_s()` for cooldowns
- [x] ChartDataBFF: `datetime.now()` → `clock.now_datetime()`
- [x] StaticDataWorker: `datetime.now(ET)` → `clock.now_datetime()`
- [x] NewsWorker: `_get_current_time()` → `clock.now_datetime()`
- [x] Verify: live mode unchanged (`_clock is None` → falls through)

### Phase C — Merge TickDataReplayer into `jerry_trader._rust`

- [ ] Port `local_tickdata_replayer/src/` into `rust/src/replayer/`
- [ ] Replace `GlobalTimeline` with shared `Arc<ReplayClock>`
- [ ] Replace WebSocket output with PyO3 callback delivery
- [ ] Add `ReplayCommand` channel (`Subscribe`, `JumpTo`, `SetSpeed`, `Pause`, `Resume`)
- [ ] Register `TickDataReplayer` in `lib.rs`
- [ ] Update `Cargo.toml` with polars/tokio/serde dependencies
- [ ] Python integration: callback → queue distribution to BarsBuilder/FactorEngine
- [ ] Retire `replayer_manager.py` (keep `local_tickdata_replayer/` as archive/reference)

### Phase D — Remote machine sync + snapshot replayer

The **ChartBFF machine** is the clock domain master (also runs
`local_tickdata_replayer` in-process). Remote machines (running
`MarketSnapshotReplayer`) follow via Redis heartbeat.

- [ ] Redis heartbeat publisher in `clock.py` (100ms interval, from ChartBFF machine)
- [ ] `RemoteClockFollower` class (monotonic interpolation between heartbeats)
- [ ] Modify `MarketSnapshotReplayer` to poll `RemoteClockFollower.now_ns()` instead of `asyncio.sleep`
- [ ] Test cross-machine sync (same network + Tailscale)

### Phase E — Jump + control plane

- [ ] `jump_to()` propagation through all modules (flush/reset/re-bootstrap)
- [ ] `set_speed()` propagation (clock + replayer + snapshot replayer)
- [ ] `pause()` / `resume()` propagation
- [ ] Frontend timeline control UI (play/pause/seek bar/speed selector)
- [ ] Strategy pre-locate orchestrator: accepts `(ticker, timestamp)` list, auto-sequences jumps

### Phase F — Historical data backfill for replay mode

- [ ] `localdata_loader/data_loader.py` → ClickHouse backfill (OHLCV bars for replay date)
- [ ] ChartDataBFF reads backfilled bars from ClickHouse (same path as live mode)
- [ ] Frontend chart works identically in both modes
