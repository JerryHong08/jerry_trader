# Event Bus Architecture for Service Communication

## Context

Current inter-service communication uses direct callbacks, creating tight coupling:

```
ChartBFF.custom_bar_backfill()
    └── _bar_backfill_observers (callback list)
        └── FactorEngine._on_bar_backfill()

BarsBuilderService.trades_backfill()
    └── _trade_observers (callback list)
        └── FactorEngine._on_bootstrap_trades()
```

**Problems:**
1. Scattered state tracking: 5+ in-memory sets/dicts across services
2. Cross-service dependencies via direct method calls
3. Hard to trace event flow (who emitted what, who consumed what)
4. Complex race conditions (who sets state first?)
5. Testing requires mocking multiple services

**Affected Services:**
- `ChartBFF` - publishes bar backfill completion
- `BarsBuilderService` - publishes trades backfill completion, bar closed events
- `FactorEngine` - subscribes to bar/tick events for bootstrap and computation

## Analysis

### Current Callback Pattern

| Service | Callback List | Purpose |
|---------|---------------|---------|
| `BarsBuilderService` | `_trade_observers` | Notify when trades_backfill completes |
| `ClickHouseClient` | `_bar_backfill_observers` | Notify when custom_bar_backfill completes |
| `FactorEngine` | (consumer only) | React to backfill events |

### State Tracking (Scattered)

| Service | State Variable | Purpose |
|---------|----------------|---------|
| `ChartBFF` | `_backfill_done` | Track which tickers have been backfilled |
| `ChartBFF` | `_backfill_in_progress` | Prevent duplicate backfill |
| `BarsBuilderService` | `_bootstrap_events` | Track bootstrap completion per symbol |
| `FactorEngine` | `_bootstrap_events` | Track bar bootstrap per symbol |
| `FactorEngine` | `_tick_bootstrap_events` | Track tick bootstrap per symbol |
| `FactorEngine` | `_failed_bootstrap_timeframes` | Track TFs that need retry |

### Event Flow (Desired)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        EVENT BUS (Redis Streams)                             │
│                        events:{session_id}                                   │
└─────────────────────────────────────────────────────────────────────────────┘

BarBackfillCompleted ──┬──▶ FactorEngine.bootstrap_retry()
                       └──▶ (future: other subscribers)

TradesBackfillCompleted ──▶ FactorEngine.tick_bootstrap()

BarClosed ─────────────────▶ FactorEngine.on_bar() (real-time computation)
```

## Decision

Implement Redis Streams-based EventBus to replace callback pattern:

1. **Single event stream** per session: `events:{session_id}`
2. **Typed events** with consistent schema
3. **Publish/Subscribe** pattern - services decoupled via message bus
4. **Single source of truth** - ClickHouse for bar state, not in-memory sets

## Event Schema

```python
class EventType:
    # Bar events
    BAR_BACKFILL_COMPLETED = "bar_backfill_completed"
    BAR_CLOSED = "bar_closed"

    # Tick events
    TRADES_BACKFILL_COMPLETED = "trades_backfill_completed"

    # Future: more event types
    TICK_SNAPSHOT = "tick_snapshot"  # For tick-based factor computation

@dataclass
class Event:
    type: str
    symbol: str
    timeframe: str | None
    timestamp_ns: int
    data: dict
```

### Event Examples

**BarBackfillCompleted:**
```json
{
  "type": "bar_backfill_completed",
  "symbol": "AAPL",
  "timeframe": "5m",
  "timestamp_ns": 1234567890000000000,
  "data": {"bar_count": 1588, "source": "local"}
}
```

**TradesBackfillCompleted:**
```json
{
  "type": "trades_backfill_completed",
  "symbol": "AAPL",
  "timeframe": null,
  "timestamp_ns": 1234567890000000000,
  "data": {"trade_count": 50000}
}
```

**BarClosed (real-time):**
```json
{
  "type": "bar_closed",
  "symbol": "AAPL",
  "timeframe": "5m",
  "timestamp_ns": 1234567890000000000,
  "data": {"open": 150.0, "high": 151.0, "low": 149.5, "close": 150.5, "volume": 10000}
}
```

## Rejected Alternatives

1. **Keep callback pattern** - Already showing maintenance issues
2. **In-process signal/slot** - Doesn't work across machines
3. **Redis Pub/Sub only** - No persistence, consumers might miss events
4. **ClickHouse polling** - Adds latency, more queries

## Implementation Plan

### Phase 1: EventBus Core
- [ ] Create `platform/event_bus.py` with Redis Streams backend
- [ ] Define `Event` dataclass and `EventType` enum
- [ ] Implement `publish()`, `subscribe()`, `start_consumer()`
- [ ] Thread-safe consumer loop with graceful shutdown

### Phase 2: Migrate Publishers
- [ ] BarsBuilderService publish `TradesBackfillCompleted`
- [ ] BarsBuilderService publish `BarClosed` (replace Redis pub/sub for bars)
- [ ] ChartBFF publish `BarBackfillCompleted`

### Phase 3: Migrate Subscribers
- [ ] FactorEngine subscribe to `BarBackfillCompleted`
- [ ] FactorEngine subscribe to `TradesBackfillCompleted`
- [ ] FactorEngine subscribe to `BarClosed`

### Phase 4: Cleanup
- [ ] Remove `_trade_observers` from BarsBuilderService
- [ ] Remove `_bar_backfill_observers` from ClickHouseClient
- [ ] Remove `_failed_bootstrap_timeframes` from FactorEngine (use retry loop instead)
- [ ] Simplify bootstrap logic in FactorEngine

### Phase 5: Testing
- [ ] Unit tests for EventBus
- [ ] Integration test: ChartBFF → EventBus → FactorEngine
- [ ] Integration test: BarsBuilder → EventBus → FactorEngine

## Code Locations

| File | Change |
|------|--------|
| `platform/event_bus.py` | NEW - EventBus implementation |
| `services/bar_builder/bars_builder_service.py` | Publish events, remove observers |
| `services/bar_builder/bar_query_service.py` | Publish events, remove observers |
| `services/factor/factor_engine.py` | Subscribe to events, remove callback handlers |
| `apps/chart_app/server.py` | Publish BarBackfillCompleted |
| `runtime/backend_starter.py` | Wire EventBus to all services |

## Related Tasks

- 7.1: Redis streams to event_bus migration (AI Agent Layer)
- Design Concern: Event bus architecture for Stage 4
- 3.14: Bar backfill observer (ARCHIVED - will be replaced)
