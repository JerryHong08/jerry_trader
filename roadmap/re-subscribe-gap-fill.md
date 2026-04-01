# Re-subscribe Gap Fill Design

## Problem

When a ticker is unsubscribed and re-subscribed, there's a gap in data:

```
08:17 Subscribe → trades_backfill generates bars, ClickHouse stores them
08:20 Unsubscribe → cleanup() clears memory state, ClickHouse bars remain
08:30 Re-subscribe → _get_last_bar_start_today() returns value
                    → all_tfs_have_bars = True
                    → skips trades_backfill
                    → 08:20~08:30 trades LOST!
```

**Impact:**
1. BarsBuilder: Gap in bars (missing 10 minutes of data)
2. FactorEngine: Indicator state wrong (didn't receive gap bars)

## Current Behavior Analysis

### BarsBuilderService.add_ticker()
```python
# Check if bars already exist
all_tfs_have_bars = True
for tf in bootstrap_tfs:
    last_start = self._get_last_bar_start_today(symbol, tf)
    if last_start is None:
        all_tfs_have_bars = False
        break
    per_tf_starts[tf] = last_start

if all_tfs_have_bars:
    # BUG: Skips backfill, misses gap!
    self._coordinator.on_bars_ready(symbol, tf)
else:
    # Full backfill from session start
    self.trades_backfill(...)
```

### FactorEngine.add_ticker()
```python
# Check if bars exist
if self._check_bars_exist(symbol, tf):
    # Warmup from existing bars
    self._bootstrap_bars_for_timeframe(...)
else:
    # Wait for BarsBuilder to produce bars
```

## Solution: Gap Fill on Re-subscribe

### Design

Instead of skipping backfill when bars exist, do incremental gap fill:

1. Get last bar timestamp from ClickHouse
2. Fetch trades from last_bar_end to now
3. Build only the gap bars
4. Merge with existing data

### Implementation

**BarsBuilderService.add_ticker():**
```python
# Always check last bar start
per_tf_starts: Dict[str, int] = {}
for tf in bootstrap_tfs:
    last_start = self._get_last_bar_start_today(symbol, tf)
    if last_start:
        per_tf_starts[tf] = last_start

if per_tf_starts:
    # Gap fill: fetch trades from last bar to now
    # Use max(per_tf_starts.values()) as from_ms
    from_ms = max(per_tf_starts.values())
    self.trades_backfill(symbol, bootstrap_tfs, from_ms, per_tf_starts)
else:
    # Full backfill from session start
    self.trades_backfill(symbol, bootstrap_tfs, None, {})
```

**trades_backfill() modification:**
- When `from_ms` is provided, only fetch trades >= from_ms
- When `per_tf_starts` provided, skip bars before that timestamp

### Edge Cases

| Scenario | Action |
|----------|--------|
| First subscribe (no bars) | Full backfill from session start |
| Re-subscribe with gap | Gap fill from last bar to now |
| Re-subscribe no gap (< 1s) | Skip backfill, mark ready immediately |
| Bars exist but stale (previous day) | Treat as first subscribe |

### FactorEngine Warmup

FactorEngine needs to warmup from the gap bars:

```python
def _bootstrap_bars_for_timeframe(...):
    # Query bars from last_factor_ts or last_bar_ts
    # Not just last N bars
    bars = query_ohlcv_bars(symbol, timeframe, from_ts=last_processed_ts)
    for bar in bars:
        for indicator in indicators:
            indicator.on_bar(bar)
```

## Implementation Tasks

### Phase 1: BarsBuilder Gap Fill
- [x] Modify `add_ticker()` to always run trades_backfill
- [x] Pass `per_tf_starts` for dedup on re-subscribe
- [x] Handle new timeframes with `per_tf_starts_new`

### Phase 2: FactorEngine Gap Warmup
- [x] No change needed - warmup from ClickHouse includes gap bars

### Phase 3: Testing
- [ ] Test first subscribe (no bars)
- [ ] Test re-subscribe with gap
- [ ] Test re-subscribe immediately after unsubscribe
- [ ] Test cross-day scenario

## Alternative: Simpler Approach

If gap fill is too complex, consider:

**Option: Always Full Backfill**
- On re-subscribe, always do full backfill
- Use `per_tf_starts` to skip already-existing bars (dedup)
- Pros: Simple, always correct
- Cons: More I/O, slower

This is what `per_tf_starts` was designed for but isn't being used correctly.
