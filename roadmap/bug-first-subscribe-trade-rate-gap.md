# Bug: First-subscribe trade_rate Gap (~17 Seconds)

**Created:** 2026-04-05
**Status:** Fixed (Option D: WS tick buffering)
**Severity:** Medium
**Component:** Backend — BarsBuilderService + FactorEngine

## Symptoms

On first-time subscription of a ticker:
- Bar indicators warm up smoothly from historical bars
- trade_rate shows a **visible gap** between the last warmup value and the first real-time update
- Gap is approximately 10-20 seconds (observed ~17s in logs)
- The gap manifests as a flat line or missing data point in the trade_rate chart

## Observed Behavior (from logs)

```
# Bootstrap trades cover up to ~13:40:06 UTC
trade_rate warmup: last value at 13:40:06 UTC

# Real-time WS ticks start flowing at ~13:40:23 UTC
# Gap: ~17 seconds where trade_rate has no data

first_ws_ts = 13:40:23 UTC  (first real-time trade from WS)
bootstrap trades filtered to < first_ws_ts = < 13:40:23 UTC
last bootstrap trade actually at ~13:40:06 UTC  (17s before first_ws_ts)
```

## Root Cause

In `bars_builder_service.py` (~line 740), `trades_backfill()` filters trades for bar building:

```python
# Filter trades to before first_ws_ts for dedup
trades = [t for t in trades if t["timestamp"] < first_ws_ts]
```

This filtering is **correct for bar building** — it prevents duplicate bars between backfill and real-time WS stream. But the same filtered trades are used for **tick indicator warmup** (trade_rate). The filtering removes trades between the last bootstrap trade (~13:40:06) and `first_ws_ts` (~13:40:23), creating a gap.

The gap exists because:
1. `first_ws_ts` is captured when the first WS trade arrives
2. Bootstrap trades are fetched from Redis/session data
3. There's an inherent latency between "last trade in Redis" and "first WS trade"
4. The filter `< first_ws_ts` is strict, losing any trades close to the boundary

## Affected Files

- `python/src/jerry_trader/services/bar_builder/bars_builder_service.py` — trades_backfill filter
- `python/src/jerry_trader/services/factor/factor_engine.py` — tick warmup consumes filtered trades
- `python/src/jerry_trader/services/orchestration/bootstrap_coordinator.py` — trade storage

## Fix Options

### Option A: Store Unfiltered Trades for Tick Warmup

Store two sets of trades in the coordinator:
- `_bar_trades[symbol]` — filtered trades (before first_ws_ts) for bar building
- `_tick_trades[symbol]` — all trades for tick indicator warmup

```python
# coordinator stores both
self._bar_trades[symbol] = filtered_trades   # < first_ws_ts
self._tick_trades[symbol] = all_trades        # unfiltered

# FactorEngine fetches from _tick_trades for trade_rate warmup
trades = coordinator.get_tick_trades(symbol)
```

### Option B: Buffer WS Ticks During Bootstrap

During the bootstrap window (after first_ws_ts captured, before warmup complete), buffer incoming WS ticks. After tick warmup completes, replay the buffered ticks:

```python
# In FactorEngine
if bootstrap_in_progress:
    self._ws_tick_buffer[symbol].append(tick)
else:
    self._on_tick(tick)

# After warmup completes
for tick in self._ws_tick_buffer.pop(symbol, []):
    self._on_tick(tick)
```

### Option C: Use <= first_ws_ts for Tick Trades (Simple)

For tick indicators, use `<= first_ws_ts` instead of `< first_ws_ts`:

```python
bar_trades = [t for t in trades if t["timestamp"] < first_ws_ts]
tick_trades = [t for t in trades if t["timestamp"] <= first_ws_ts]
```

This reduces the gap but doesn't fully eliminate it. Simpler than Option A/B.

## Recommended

Option A is the cleanest: separate trade streams for bar vs tick warmup. Option C is a quick patch that reduces the gap but may still have a few seconds missing.

## Related

- `roadmap/bug-resubscribe-trade-rate-not-warmed.md` — re-subscribe version of this bug
- `roadmap/re-subscribe-gap-fill.md` — overall gap fill design
