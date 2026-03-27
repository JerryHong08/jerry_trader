# Bar Backfill Observer for Factor Bootstrap

## Context

FactorEngine needs to bootstrap bar-based indicators (EMA20) with historical bars. Currently:

| Component | Trigger | Bars Source | Scope |
|-----------|---------|-------------|-------|
| `trades_backfill` | Redis stream | Ticks → Bars | 10s, 1m, 5m intraday |
| `custom_bar_backfill` | ChartBFF direct call | Polygon/local | 1m~1M multi-day |

**Problem:** `custom_bar_backfill` runs asynchronously in ChartBFF's thread pool. FactorEngine only waits for `trades_backfill`. For 4h bars (only from `custom_bar_backfill`), if FactorEngine queries before backfill completes → EMA warmup fails.

## Analysis

### Existing Observer Pattern

BarsBuilder already has trade observers:
```python
# BarsBuilderService
self._trade_observers: list = []

def register_trade_observer(self, callback):
    self._trade_observers.append(callback)

# Called after trades_backfill
for observer in self._trade_observers:
    observer(symbol, trades)
```

### Options Considered

**Option A: Extend Observer Pattern (In-Process)**
- Add `_bar_backfill_observers` to shared location
- `custom_bar_backfill` notifies after each TF completes
- FactorEngine registers callback

**Option B: Redis Pub/Sub**
- Publish `bar_backfill:{session}` events
- FactorEngine subscribes

**Option C: Unified BarBackfillService**
- Refactor both backfill mechanisms into one service

### Decision

**Option A** - Extend Observer Pattern:
1. ChartBFF and FactorEngine run in same process (same machine role)
2. Matches existing `_trade_observers` pattern
3. No Redis overhead for in-process communication
4. Quick to implement

## Implementation Plan

1. Add `_bar_backfill_observers` list to `ClickHouseClient` (shared by ChartBFF)
2. Add `register_bar_backfill_observer(callback)` method
3. `custom_bar_backfill` notifies observers after each TF completes
4. FactorEngine registers `_on_bar_backfill` callback during startup
5. FactorEngine retries bootstrap for that TF when notified

## Code Locations

| File | Change |
|------|--------|
| `services/bar_builder/bar_query_service.py` | Add observer list + notification |
| `apps/chart_app/server.py` | Wire observer registration |
| `services/factor/factor_engine.py` | Register callback, handle notification |

## Related Tasks

- 3.3 Multi-timeframe FactorEngine
- 6.1 Decouple trades_backfill from timeframe switch
- 6.3 Factor bootstrap sync to trades_bootstrap
