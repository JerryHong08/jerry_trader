# Fix Timing Race: Snapshot Bootstrap → Clock Jump → Subscribe

## Context

User discovered that frontend subscribing immediately after snapshot bootstrap completes can fail with:
```
factor_engine - WARNING - _run_bootstrap - BIAF: no trades available after timeout
```

Root cause: clock has not jumped to bootstrap end time yet when subscribe triggers trades loading.

## Analysis

### Timing Sequence in backend_starter.py

```python
# HistoricalLoader.bootstrap() completes in background thread
result = loader.bootstrap(...)

# THEN clock.jump_to() happens (inside same thread)
clock.jump_to(end_ts_ns)  # Moves clock to bootstrap end (08:10:00 ET)
clock.resume()

# THEN event signals
self._bootstrap_complete_event.set()
```

### Problem Flow

1. Bootstrap thread runs `loader.bootstrap()`
2. Bootstrap returns successfully
3. **Before** `clock.jump_to()` executes, frontend sends subscribe request
4. ChartBFF triggers `BarsBuilder.add_ticker()` → `trades_backfill()`
5. `trades_backfill()` calls `_load_trades()` with `end_ts_ms = clock.now_ms()`
6. Clock is still at initialization time (04:00:00 ET) or paused
7. Trades query uses wrong `end_ts_ms` → returns wrong range or empty
8. FactorEngine waits → timeout

### Related Code

- `backend_starter.py:1016-1059` — `_run_bootstrap()` thread
- `bars_builder_service.py:662` — `end_ts_ms = clock_mod.now_ms()`
- `chart_app/server.py` — subscribe handler triggers bootstrap

## Decision

**Option C: Clock jump happens inside bootstrap thread before event signal**

This ensures clock is always at correct position before any subscriber can proceed.

## Plan

1. Move `clock.jump_to()` and `clock.resume()` inside `_run_bootstrap()` thread
2. Ensure jump happens **before** `self._bootstrap_complete_event.set()`
3. Test with BIAF 20260313 replay — subscribe should work immediately after bootstrap

## Rejected

- **Option A (subscribe waits)**: Adds latency to every subscribe, poor UX
- **Option B (use bootstrap_end)**: Requires coordinator to track end time, extra state
