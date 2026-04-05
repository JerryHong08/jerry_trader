# Bug: Re-subscribe trade_rate Not Warmed Up

**Created:** 2026-04-05
**Status:** Fixed
**Severity:** High
**Component:** Backend — FactorEngine

## Symptoms

On re-subscribe (when ClickHouse already has bars for all timeframes):
- Bar-based indicators (EMA) warm up correctly from ClickHouse bars
- Tick-based indicators (trade_rate) show **no data** or flat zero
- trade_rate panel remains empty until enough real-time ticks accumulate
- The coordinator is falsely notified that tick warmup completed

## Root Cause

In `factor_engine.py` (~line 564), the "immediate warmup" path (`if all_tfs_have_bars`) only performs **bar warmup**:

```python
if all_tfs_have_bars:
    # 1. Warms up bar indicators from ClickHouse bars ✅
    # 2. Marks tick bootstrap as done WITHOUT actually doing tick warmup ❌
    # 3. Reports "tick_warmup" done to coordinator (a lie) ❌
```

The path skips the normal bootstrap flow where:
1. `BarsBuilder.trades_backfill()` collects historical trades
2. Trades are stored in the coordinator
3. `FactorEngine._bootstrap_tick_indicators()` consumes them from coordinator
4. Tick indicators (trade_rate) get warmup values

On re-subscribe, the immediate warmup path is taken because ClickHouse has bars. But it never feeds historical trades to tick indicators. The current fix added `coordinator.report_done()` calls which is worse — it falsely signals completion.

## Affected Files

- `python/src/jerry_trader/services/factor/factor_engine.py` — immediate warmup path (~line 564)
- `python/src/jerry_trader/services/orchestration/bootstrap_coordinator.py` — trade storage
- `python/src/jerry_trader/services/bar_builder/bars_builder_service.py` — trades_backfill

## Fix Plan

### Option A: Get Trades from Coordinator in Immediate Warmup

After bar warmup in the immediate path, also get trades from the coordinator and feed to tick indicators:

```python
if all_tfs_have_bars:
    # Bar warmup (existing)
    for tf in tfs_to_process:
        self._bootstrap_bars_for_timeframe(symbol, tf, ...)

    # Tick warmup (NEW)
    tick_key = f"{symbol}:tick"
    trades = self._coordinator.get_bootstrap_trades(symbol)
    if trades:
        self._bootstrap_tick_indicators(symbol, trades)
    self._bootstrap_done.add(tick_key)
```

Requires: coordinator to have stored trades from `trades_backfill()` even when bars already exist.

### Option B: Always Run trades_backfill on Re-subscribe

Don't take the immediate warmup shortcut. Always run the full bootstrap flow:
1. BarsBuilder.trades_backfill() (gap-fill mode, uses per_tf_starts)
2. FactorEngine consumes trades from coordinator for tick warmup
3. FactorEngine warms up bar indicators from ClickHouse (as before)

This is simpler but trades_backfill must run even when bars exist (which it does now with per_tf_starts).

### Option C: Separate Tick Warmup Bar Builder

Create a lightweight tick-only warmup that queries recent trades from ClickHouse directly, bypassing the coordinator entirely.

## Related

- `roadmap/re-subscribe-gap-fill.md` — re-subscribe flow design
- `roadmap/bug-first-subscribe-trade-rate-gap.md` — related gap issue
