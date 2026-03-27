# Deep Dive: Real-Time Tick Data & Factor Computation Flow

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                       │
│  Polygon WS    ThetaData WS    Local Parquet (Replay)                       │
└───────┬──────────────┬───────────────┬──────────────────────────────────────┘
        │              │               │
        └──────────────┼───────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    UnifiedTickManager (async fan-out)                        │
│  • Single WebSocket connection to data source                               │
│  • Fan-out to multiple consumer queues per (client, stream_key)             │
└───────┬──────────────────────┬──────────────────────┬───────────────────────┘
        │                      │                      │
        ▼                      ▼                      ▼
┌──────────────┐    ┌─────────────────┐    ┌──────────────────────┐
│ ChartBFF     │    │ BarsBuilderSvc  │    │ FactorEngine         │
│ (WS→Frontend)│    │ (Rust BarBuilder│    │ (hybrid compute)     │
│              │    │  → ClickHouse)  │    │                      │
└──────────────┘    └────────┬────────┘    └──────────┬───────────┘
                             │                        │
              ┌──────────────┘                        │
              │ (Redis pub/sub: bars:{sym}:{tf})      │
              ▼                                       │
     FactorEngine.on_bar() ──────────────────────────┤
     • EMA20 computed from bar close                 │
                                                      │
     FactorEngine._on_tick() ◄───────────────────────┘
     • TradeRate computed from tick stream
                                                      │
     FactorEngine._compute_loop() ───────────────────┘
     • 1s interval: compute tick indicators
     • Write to ClickHouse + Redis pub/sub
                                                      │
                                                      ▼
                                            Redis pub/sub
                                            channel: factors:{symbol}
                                                      │
                                                      ▼
                                            ChartBFF WebSocket
                                            (factor_update msg)
                                                      │
                                                      ▼
                                            Frontend FactorChartModule
```

## Current Implementation Status

### ✅ Working

1. **FactorEngine** (`services/factor/factor_engine.py`)
   - Hybrid architecture: bar-based (EMA) + tick-based (TradeRate) indicators
   - Bar indicators updated from Redis pub/sub (bars from BarsBuilder)
   - Tick indicators updated from direct UnifiedTickManager consumption
   - 1-second compute loop for tick indicators
   - Writes to ClickHouse via FactorStorage
   - Publishes real-time updates to Redis pub/sub (`factors:{symbol}`)

2. **BarsBuilderService**
   - Rust BarBuilder integration with watermark-based close
   - Trades backfill from Polygon/Parquet for bootstrap
   - Meeting bar merge (REST prefix + WS suffix)
   - Publishes completed bars to Redis pub/sub

3. **Frontend FactorChartModule**
   - Historical factor fetch via REST API (`/api/factors/{ticker}`)
   - WebSocket subscription for real-time updates
   - Dual Y-axis support for different factor scales

### ❌ Broken/Missing

1. **Critical Bug: moduleId Mismatch in Frontend**
   - `tickDataStore.ts:184` hardcodes `'default'` as moduleId when forwarding factor updates
   - `FactorChartModule.tsx` uses actual `moduleId` prop as key
   - **Result**: Real-time factor updates never match stored data

2. ~~**Factor Bootstrap Timing**~~ ✅ FIXED
   - ~~FactorEngine waits for BarsBuilder bootstrap~~
   - ~~But factor bootstrap doesn't wait for trades_backfill~~
   - ~~Race condition: tick indicators may miss historical trades~~
   - **Fix**: Added `_tick_bootstrap_events` for tick-based factors
   - `_run_bootstrap` now waits for `_on_bootstrap_trades` callback
   - REST API and WebSocket consumer wait for both bar and tick bootstrap

3. **Sub-Follower Chart Mode**
   - Factor chart should auto-sync timeframe with main chart
   - Currently manual timeframe selector
   - Switching main chart timeframe doesn't update factor chart

4. ~~**Trades Backfill Triggering**~~ ✅ FIXED
   - ~~Every timeframe switch triggers trades_backfill~~
   - ~~Should only trigger on symbol subscribe/unsubscribe~~
   - ~~Wastes resources and causes unnecessary data fetches~~
   - **Fix**: Changed `active_tickers` to `_active_timeframes: Dict[str, set]`
   - `_add_ticker` only triggers `trades_backfill` for NEW tickers
   - `_remove_ticker` only fully unsubscribes when ALL timeframes are removed

## Critical Path to Working Real-Time Factors

### Immediate Fixes (High Priority)

1. **Fix moduleId Mismatch (Task 5.1)**
   ```typescript
   // tickDataStore.ts current (broken):
   factorStore.updateFactors('default', symbol, timestamp_ns, factors);

   // Should be: Need to track which modules are subscribed to which symbols
   // Option A: Broadcast to all modules (simpler)
   // Option B: Track per-symbol subscriptions (more efficient)
   ```

2. **Factor Chart Sub-Follower Mode (Task 5.2)**
   - Remove manual timeframe selector from FactorChartModule
   - Listen to sync group timeframe changes
   - Auto-fetch factors when main chart timeframe changes

### Backend Improvements (Medium Priority)

3. **Decouple Trades Backfill from Timeframe (Task 6.1)**
   - Move backfill trigger from `chart_data_service.py` to ticker subscription
   - Only backfill once per ticker session, not per timeframe

4. **Factor Bootstrap Sync (Task 6.3)**
   - Ensure FactorEngine.tick_bootstrap waits for BarsBuilder.trades_backfill
   - Currently races: tick indicators may start before historical trades fed

## Code Locations

### Backend

| Component | File |
|-----------|------|
| FactorEngine | `python/src/jerry_trader/services/factor/factor_engine.py` |
| BarsBuilder | `python/src/jerry_trader/services/bar_builder/bars_builder_service.py` |
| ChartBFF | `python/src/jerry_trader/apps/chart_app/server.py` |
| FactorStorage | `python/src/jerry_trader/services/factor/factor_storage.py` |
| Indicators | `python/src/jerry_trader/services/factor/indicators/` |

### Frontend

| Component | File |
|-----------|------|
| FactorChartModule | `frontend/src/components/FactorChartModule.tsx` |
| Factor Data Store | `frontend/src/stores/factorDataStore.ts` |
| Tick Data Store | `frontend/src/stores/tickDataStore.ts` |

## Data Flow Details

### Factor Bootstrap (Historical)

```
1. Frontend: FactorChartModule mounts with symbol
2. HTTP GET /api/factors/{ticker}?from_ms=X&to_ms=Y
3. ChartBFF.get_factors():
   a. Wait for FactorEngine.bootstrap (if running)
   b. Query ClickHouse for stored factors
   c. Return {factors: {ema_20: [{time, value}, ...], trade_rate: [...]}}
4. Frontend: factorDataStore populates state
5. Chart renders with historical data
```

### Factor Real-Time (Live)

```
1. Frontend: WebSocket send {action: "subscribe_factors", symbols: ["AAPL"]}
2. ChartBFF._handle_factor_subscribe():
   a. Creates Redis pub/sub consumer task
   b. Listens on channel "factors:AAPL"
3. FactorEngine._write_factors():
   a. Writes to ClickHouse
   b. Publishes to Redis pub/sub
4. ChartBFF._consume_factor_stream():
   a. Receives Redis message
   b. WebSocket send {type: "factor_update", data: {...}}
5. Frontend tickDataStore.onmessage:
   a. Detects type === "factor_update"
   b. Calls factorDataStore.updateFactors()
   *** BUG: uses 'default' as moduleId ***
6. Frontend FactorChartModule:
   a. Should re-render with new factor point
   *** BROKEN: moduleId mismatch ***
```

## Testing Checklist

- [ ] Subscribe to AAPL factors, verify EMA20 updates on each bar close
- [ ] Subscribe to AAPL factors, verify TradeRate updates every 1s
- [ ] Switch main chart timeframe, factor chart auto-syncs
- [ ] Unsubscribe AAPL, subscribe TSLA, verify no AAPL updates received
- [ ] Check browser DevTools WebSocket messages for factor_update
- [ ] Check Redis pub/sub: `SUBSCRIBE factors:AAPL`

## Related Tasks

- 5.1: Fix factor real-time update moduleId mismatch bug
- 5.2: Factor chart sub-follower mode
- 6.1: Decouple trades_backfill from timeframe switch
- 6.3: Factor bootstrap sync to trades_bootstrap
