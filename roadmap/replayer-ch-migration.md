# Replayer CH Migration + Clock Sync

## Context

Current `MarketSnapshotReplayer` reads from `cache/market_mover/YYYY/MM/DD/*_market_snapshot.parquet` files with legacy column name mapping (`todaysChangePerc→changePercent` etc). The goal is to unify the data source to ClickHouse `market_snapshot_collector` table, which already has standardized column names from `build-snapshot`.

Additionally:
- Clock is not paused during bootstrap, causing a 23s gap where clock ticks from 04:00 while bootstrap processes 4:00-9:30
- Replayer only supports `RemoteClockFollower` (cross-machine) or `asyncio.sleep` (local fallback), not the local `ReplayClock`
- Old parquet files need to be migrated to CH before we can deprecate the parquet replayer

## Current Architecture

```
Parquet files (cache/market_mover/)
  → MarketSnapshotReplayer (asyncio loop)
    → Redis INPUT Stream (xadd)
      → SnapshotProcessor (stream_listener)
        → Redis OUTPUT Stream + CH market_snapshot

Clock: ReplayClock initialized at startup (04:00), runs continuously
Bootstrap: HistoricalLoader runs in separate thread, jumps clock to 9:30 at end
Problem: 23s gap where clock is at 04:00:00~04:00:23 while bootstrap runs
```

## Target Architecture

```
ClickHouse market_snapshot_collector (unified data source)
  → CHReplayer (asyncio loop, reads from CH)
    → Redis INPUT Stream (xadd)
      → SnapshotProcessor (stream_listener, unchanged)

Clock lifecycle:
  1. init_replay(04:00) → clock starts
  2. pause() immediately → clock frozen
  3. bootstrap runs (HistoricalLoader, ~23s)
  4. jump_to(9:30) + resume() → clock continues from 9:30
  5. CHReplayer starts, gates on clock time
```

## Decision

Three changes, ordered by dependency:

### Change 1: Clock pause during bootstrap
- `_init_clock()` initializes ReplayClock at 04:00
- `clock.pause()` called immediately after init
- After bootstrap completes, `clock.jump_to(end_ts)` + `clock.resume()`
- CHReplayer (and any clock-dependent component) waits for clock to be running before pushing data
- **Impact**: backend_starter.py only, ~5 lines

### Change 2: CHReplayer replaces Parquet replayer
New class `CHReplayer` (or refactor `MarketSnapshotReplayer`):
- Reads from `market_snapshot_collector` CH table by date + mode
- Columns already standardized (no rename needed)
- Timing driven by local `ReplayClock` via `clock.now_ms()` polling (same pattern as `RemoteClockFollower` but reads from local Rust clock)
- Still supports `RemoteClockFollower` for cross-machine deployment
- `start_from` maps to CH `WHERE timestamp > start_ms`
- Speed control via clock's `set_speed()` (inherent in ReplayClock)

### Change 3: Parquet → CH migration for historical data
- Script to scan `cache/market_mover/*.parquet`, unify schema, insert to CH `market_snapshot_collector`
- One-time operation, can be CLI command under existing data CLI
- After migration, parquet replayer path removed from backend_starter

## Rejected Alternatives

1. **Keep both parquet + CH replayer** — adds complexity, two code paths to maintain. CH is the single source of truth.
2. **Skip Replayer entirely, bootstrap only** — bootstrap covers historical data, but doesn't handle "live replay" from 9:30 onward when you want to watch the market unfold in real-time.
3. **Replayer reads directly from Parquet → Processor without Redis** — breaks the architecture (Processor expects Redis INPUT Stream), and we want unified CH data source.

## Plan

### Step 1: Clock pause during bootstrap (backend_starter.py)
- `_init_clock()`: add `clock.pause()` after `clock.init_replay()`
- `_run_bootstrap()`: add `clock.resume()` after `clock.jump_to()`
- Add `clock.wait_until_running()` helper (or use `is_paused` check in replayer)

### Step 2: CHReplayer (new file or refactor)
- Query CH `market_snapshot_collector` for given date+mode, ordered by timestamp
- Poll `clock.now_ms()` to gate each window (same as remote clock path but local)
- Push to Redis INPUT Stream (same `xadd` as current replayer)
- Support `start_from` parameter (skip already-bootstrapped windows)
- Support `RemoteClockFollower` for cross-machine (keep existing path)

### Step 3: Parquet migration CLI
- `data-cli parquet-to-ch --date YYYYMMDD` command
- Read parquet, unify schema to match `market_snapshot_collector`, insert to CH
- Verify row counts after migration

### Step 4: Update backend_starter config
- Remove parquet-based replayer initialization
- CHReplayer uses CH config from role config
- Add `start_from` auto-detection (query CH for last processed timestamp)

### Step 5: Cleanup
- Remove parquet file reading code from `MarketSnapshotReplayer`
- Update config.yaml examples
- Update ROADMAP.md
