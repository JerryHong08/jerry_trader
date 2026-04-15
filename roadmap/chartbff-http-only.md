# ChartBFF Python → Rust Separation

## Status

**In Progress** - 2026-04-16

## Background

ROADMAP 13.10: ChartBFF should only keep HTTP API for historical queries, while real-time WebSocket streaming moves to Rust ComputeBox's WSPublisher.

## Current Architecture

```
ChartBFF (Python, Port 8000)
├── WebSocket: /ws/tickdata
│   ├── subscribe/unsubscribe handlers
│   ├── tick streaming from UnifiedTickManager
│   ├── factor streaming from Redis pub/sub
│   └── BootstrapCoordinator integration
│
└── HTTP API:
│   ├── /api/chart/bars/{ticker} - historical bars from CH
│   ├── /api/factors/{ticker} - historical factors from CH
│   ├── /api/factors/specs - factor metadata
│   └── /api/clock - clock state for frontend
```

## Target Architecture

```
Rust ComputeBox (Port 8000)
├── WebSocket Server (WSPublisher)
│   ├── subscribe/unsubscribe handlers
│   ├── bar streaming from bar_stream channel
│   ├── factor streaming from factor_stream channel
│   └── BootstrapStatus tracking
│
└── Data Flow (internal Rust):
│   trades → bars → factors → ClickHouse → WebSocket
│
Python ChartBFF (Port 8001 or merged into Rust WS)
├── HTTP API (only):
│   ├── /api/chart/bars/{ticker} - SELECT CH for history
│   ├── /api/factors/{ticker} - SELECT CH for history
│   ├── /api/factors/specs - factor metadata
│   └── /api/clock - clock state
```

## API Separation Details

### HTTP APIs that stay in Python

| Endpoint | Purpose | Implementation |
|----------|---------|----------------|
| `/api/chart/bars/{ticker}` | Historical bars query | ClickHouseClient._query_bars_clickhouse() |
| `/api/factors/{ticker}` | Historical factors query | FactorStorage.query_factors() |
| `/api/factors/specs` | Factor specs for frontend | FactorRegistry.get_specs_for_api() |
| `/api/clock` | Clock state for TimelineClock | clock.now_ms(), is_replay(), etc. |

These are read-only queries against ClickHouse. No real-time streaming.

### WebSocket functionality that moves to Rust

| Function | Current | Target |
|----------|---------|--------|
| Tick streaming | UnifiedTickManager → WebSocket | BarBuilderEngine → bar_stream → WSPublisher |
| Bar streaming | Redis pub/sub → WebSocket | BarBuilderEngine → bar_stream → WSPublisher |
| Factor streaming | Redis pub/sub → WebSocket | FactorEngineCore → factor_stream → WSPublisher |
| Subscribe/unsubscribe | ChartBFF._handle_subscribe() | WSPublisher.handle_subscribe() |
| Bootstrap wait | BootstrapCoordinator | DataLayer.BootstrapStatus |

## Dependencies

ChartBFF currently depends on:
- **UnifiedTickManager**: Shared with BarsBuilderService/FactorEngine
- **BootstrapCoordinator**: Unified bootstrap orchestration
- **BarsBuilderService**: `_bars_builder` reference for partial bars
- **FactorEngine**: `_factor_engine` reference for bootstrap wait
- **FactorStorage**: ClickHouse factor queries
- **ClickHouseClient**: Bar queries + custom_bar_backfill

## Transition Plan

### Phase 1: Keep HTTP API, Remove WebSocket (this task)

1. Document separation (this file)
2. Identify remaining HTTP-only components
3. Note: Python ChartBFF still runs with WebSocket for now
   - Rust ComputeBox integration not complete

### Phase 2: Rust WSPublisher Integration (requires 13.11 blocked)

1. BackendStarter uses Rust ComputeBox instead of Python services
2. Python ChartBFF only starts HTTP API server
3. Rust WSPublisher handles WebSocket on port 8000
4. Frontend connects to Rust WS for real-time, Python HTTP for history

### Phase 3: Python ChartBFF Simplification (future)

Option A: Merge HTTP API into Rust WS server (Rust handles both)
Option B: Keep Python HTTP API on separate port (8001)

## Blocking Issues

13.10 cannot fully remove WebSocket from ChartBFF until:
- [ ] Rust WSPublisher is production-ready
- [ ] BackendStarter integrates Rust ComputeBox
- [ ] BootstrapCoordinator works with Rust services
- [ ] Backtest system uses Rust ComputeBox

These are tracked in 13.11 and 13.12.

## Current Status

**Documentation complete. Implementation blocked by 13.11.**

For now, ChartBFF keeps both HTTP and WebSocket. When 13.11 completes, Python WebSocket can be removed.
