# Fix Meeting Bar Merge Race Condition

Task: 3.23
Status: todo
Priority: high
Tags: bug, bars-builder, bootstrap, rust

## Problem

When WS meeting bar completes before REST partial is stored by `trades_backfill`, the WS bar is written unmerged and REST partial becomes orphaned.

## Root Cause

1. `_try_merge_meeting_bar()` returns the raw WS bar when REST partial is not ready
2. Comment says "trades_backfill will handle merge when it finishes" but that's incorrect
3. `trades_backfill` only stores REST partial, doesn't check for pending WS bars
4. `trades_backfill` and `_flush_loop` run in different threads (race condition)

## Evidence (2026-03-13 BIAF)

```
FIRST_WS_TICK: 08:05:44.988 ET
MEETING_BAR detected:
  - 10s: bar_start=08:05:40 (boundary=08:05:50, only ~5s away!)
  - 1m:  bar_start=08:05:00 (boundary=08:06:00)
  - 5m:  bar_start=08:05:00 (boundary=08:10:00)
  - 1h/4h/15m/30m: bar_start=08:00:00

trades_backfill: stored REST partials for all 7 TFs
filtered 29 trades after first_ws_ts
```

### Race Condition Timeline

```
Time (real)    Thread           Action
------------------------------------------------------------
23:03:30       ws_loop          FIRST_WS_TICK at 08:05:44.988
23:03:30       ws_loop          Detect meeting bars
23:03:31       trades_backfill  Start loading parquet
23:04:02       _flush_loop      advance() at virtual time ~08:05:45
                                → 10s bar may close (boundary 08:05:50)
                                → _try_merge_meeting_bar() called
                                → REST partial NOT ready
                                → Returns raw WS bar (29 trades)
                                → Write to ClickHouse (UNMERGED!)
23:04:02       trades_backfill  Completes, stores REST partial (641 trades)
                                → REST partial orphaned
```

### Why 10s is Most Affected

- 10s bar boundary (08:05:50) is only ~5 seconds after first WS tick (08:05:44.988)
- In fast replay, virtual time advances quickly
- `_flush_loop` may close the 10s bar before `trades_backfill` completes
- Longer timeframes (5m, 1h) have more buffer time

## Proposed Fix Options

### Option A: Store WS bar for deferred merge (Python-level fix)

```python
# Add new state
self._ws_meeting_bars: Dict[str, Dict[str, Bar]] = {}
self._meeting_lock = threading.Lock()

# In _try_merge_meeting_bar()
if rest_bar:
    return merged
else:
    # Store WS bar instead of returning it
    self._ws_meeting_bars.setdefault(ticker, {})[tf] = bar
    return None  # Signal: don't write yet

# In _flush_loop, after advance()
pending_merged = self._check_pending_ws_meeting_bars(now_et_ms)
```

**Pros**: Minimal changes, stays in Python
**Cons**: More state to manage, lock contention, complex

### Option B: Move meeting bar logic to Rust (recommended)

```rust
// In BarBuilder
pub fn set_first_ws_tick(&mut self, symbol: &str, ts_ms: i64);
pub fn get_meeting_bar_starts(&self, symbol: &str) -> HashMap<String, i64>;

// drain_completed() internally handles merge
// Returns merged bars directly
```

**Pros**:
- No Python-Rust boundary crossing per bar
- No lock needed (single-threaded in Rust)
- Cleaner architecture
- Better performance

**Cons**: Requires Rust changes

### Option C: Defer WS tick processing until bootstrap complete

```python
# Queue WS ticks during bootstrap
self._pending_ws_ticks: Dict[str, List[Tick]] = {}

# After trades_backfill completes
for tick in pending_ticks:
    self.bar_builder.ingest_trade(...)
```

**Pros**: Simplest conceptually, no meeting bar needed
**Cons**: Higher latency for first WS ticks, memory for queue

## Recommendation

**Option B** (move to Rust) is the best long-term solution:

1. Eliminates thread safety concerns
2. Reduces Python-Rust boundary calls
3. Aligns with existing Rust BarBuilder architecture
4. Simpler mental model

## Files

- `python/src/jerry_trader/services/bar_builder/bars_builder_service.py`
- `rust/src/bar_builder.rs` (if Option B)
- `rust/src/bar_builder/meeting_bar.rs` (new file, if Option B)

## Related

- Discovered during replay testing on 2026-04-11
- Affects 10s, 1m timeframes most noticeably (shorter boundaries)
- Similar pattern to VolumeTracker (stateful Rust component)
