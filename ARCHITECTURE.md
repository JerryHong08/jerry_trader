# Jerry Trader — Architecture Documentation

> **Status: STAGE 3 IN PROGRESS**
> Domain Layer populated, Factor Engine operational, Panel Chart System complete.
> ATC-R Agent Layer design finalized, implementation starting.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Machine Topology](#2-machine-topology)
3. [Current Python Layout](#3-current-python-layout)
4. [Layer Definitions](#4-layer-definitions)
5. [Bootstrap Coordinator V2](#5-bootstrap-coordinator-v2)
6. [Rust Extension Layout](#6-rust-extension-layout)
7. [Data Flow](#7-data-flow)
8. [ATC-R Agent Layer](#8-atc-r-agent-layer)
9. [Development Guidelines](#9-development-guidelines)

---

## 1. Project Overview

**Jerry Trader** is a personal, multi-machine, real-time US pre-market momentum trading system.

The core strategy targets **short-term pre-market momentum** (gap-ups, float awareness, relative volume, catalyst news). The system is designed to:

- Collect, process and replay real-time tick data (Polygon.io / ThetaData / local Parquet)
- Build OHLCV bars in real-time using a Rust-accelerated bar builder
- Process market snapshots (Top Gainers, rank, relative volume, static fundamentals)
- Fetch and LLM-classify financial news catalysts
- Execute orders via IBKR TWS/Gateway
- Serve all of this to a React/TradingView frontend over WebSocket
- Support **full replay mode** with a Rust-backed distributed clock for cross-machine time sync

**Current Stage:** Stage 3 (Domain Layer populated, Factor Engine operational)

**Next Stage:** ATC-R Agent Layer (Rule Engine + Agent Think + Backtest)

---

## 2. Machine Topology

```
┌─────────────────────────────────────────────────────────────────┐
│  Machine A  (WSL2 / primary)                                    │
│                                                                 │
│  • ChartBFF          (WebSocket, bar serving — port 8000)       │
│  • BarsBuilder       (Rust BarBuilder → ClickHouse)             │
│  • FactorEngine      (Multi-timeframe indicators)               │
│  • Rule Engine       (ATC-R Strategy DSL matching)              │
│  • OrderRuntime      (IBKR adapter, FastAPI — port 8888)        │
│  • GlobalClock       (ReplayClock master + Redis heartbeat)     │
│  • Frontend (pnpm dev / built static)                           │
│                                                                 │
│  Redis A  │  ClickHouse A  │  Postgres A                        │
└────────────────────────┬────────────────────────────────────────┘
                         │ Tailscale / LAN
        ┌────────────────┴───────────────────┐
        │                                    │
┌───────▼──────────────┐        ┌────────────▼─────────────┐
│  Machine B  (oldman) │        │  Machine C  (mibuntu)    │
│                      │        │                          │
│  • JerryTraderBFF    │        │  • NewsWorker            │
│    (market_bff)      │        │  • NewsProcessor (LLM)   │
│    (port 5001)       │        │  • Agent Reasoner        │
│  • SnapshotProcessor │        │    (ATC-R LLM thinking)  │
│  • StaticDataWorker  │        │  • AgentBFF (port 5003)  │
│  • Collector         │        │  • Backtest Engine       │
│  • Replayer          │        │                          │
│                      │        │  Redis B (shared w/ B)   │
│  Redis B  │ ClickHouse│       │  Postgres B              │
└──────────────────────┘        └──────────────────────────┘
```

**Redis A** — tick data streams, bar streams, ChartBFF pub/sub, clock heartbeats, factor streams
**Redis B** — snapshot streams, news cache, static data, order/portfolio pub/sub
**ClickHouse** — OHLCV bars (persistent), market snapshot history, factor snapshots
**Postgres** — order history, news articles, LLM results
**Parquet (local lake)** — raw tick data for replay (Machine A path)

---

## 3. Current Python Layout

```
python/
├── src/
│   └── jerry_trader/
│       │
│       ├── __init__.py
│       ├── _rust.pyi                    # Rust extension stubs
│       ├── clock.py                     # Global clock singleton
│       ├── schema.py                    # Shared Pydantic/dataclass schemas
│       │
│       ├── platform/                    # ── INFRASTRUCTURE LAYER ──────────────────
│       │   ├── config/
│       │   │   ├── config.py            # Config loader, lake_data_dir, env resolution
│       │   │   └── session.py           # make_session_id, session parsing
│       │   ├── storage/
│       │   │   ├── clickhouse.py        # Pure ClickHouse connect/query client
│       │   │   └── ohlcv_writer.py      # ClickHouse OHLCV write helper
│       │   ├── messaging/
│       │   │   ├── redis_streams.py     # Redis Streams helpers
│       │   │   └── rpc/                 # [Stage4] Redis-based RPC stubs (empty)
│       │   └── event_bus/               # [Stage4] in-process event bus (empty)
│       │
│       ├── shared/                      # ── CROSS-CUTTING UTILITIES ───────────────
│       │   ├── ids/
│       │   │   └── redis_keys.py        # Centralized Redis key / channel names
│       │   ├── time/
│       │   │   ├── timezone.py          # ET ZoneInfo constant, helpers
│       │   │   └── remote_clock.py      # RemoteClockFollower
│       │   ├── logging/
│       │   │   └── logger.py            # setup_logger
│       │   └── utils/
│       │       ├── async_helpers.py
│       │       ├── data_utils.py
│       │       ├── parse.py
│       │       ├── paths.py
│       │       └── momo_token.py
│       │
│       ├── domain/                      # ── DOMAIN LAYER (pure business logic) ────
│       │   ├── market/                  # Bar, BarPeriod, Tick, Snapshot value objects
│       │   │   ├── bar.py               # Bar value object with OHLCV fields
│       │   │   └── bar_period.py        # BarPeriod with to_timedelta(), from_str()
│       │   ├── order/                   # Order, Fill, Contract domain models
│       │   │   ├── order.py             # Order, OrderState value objects
│       │   │   └── contract.py          # Contract domain model
│       │   ├── strategy/                # [Stage3] Signal, Risk domain models
│       │   └── factor/                  # FactorSnapshot value object
│       │       └── factor_snapshot.py   # FactorSnapshot with symbol, timeframe, factors
│       │
│       ├── services/                    # ── SERVICE / USE-CASE LAYER ──────────────
│       │   │                            #    Stateful workers, no HTTP/WS here
│       │   ├── orchestration/           # BootstrapCoordinator (V2)
│       │   │   └── bootstrap_coordinator.py
│       │   ├── bar_builder/
│       │   │   ├── bars_builder_service.py    # Main bar builder service
│       │   │   ├── chart_data_service.py      # Chart data management
│       │   │   └── bar_query_service.py       # Bar query helpers
│       │   ├── market_snapshot/
│       │   │   ├── processor.py               # SnapshotProcessor (Rust VolumeTracker)
│       │   │   └── overview_chart_data_manager.py  # Overview chart data
│       │   ├── market_data/
│       │   │   ├── feeds/
│       │   │   │   ├── unified_tick_manager.py      # Unified tick data manager
│       │   │   │   ├── polygon_manager.py           # Polygon WebSocket feed
│       │   │   │   ├── thetadata_manager.py         # ThetaData feed
│       │   │   │   ├── replayer_manager.py          # Replay manager
│       │   │   │   └── synced_replayer_manager.py   # Synced replay manager
│       │   │   ├── bootstrap/
│       │   │   │   ├── data_loader.py         # Local data loader (Parquet)
│       │   │   │   ├── polygon_fetcher.py     # Polygon REST API fetcher
│       │   │   │   ├── benchmark_loader.py    # Benchmark data loader
│       │   │   │   ├── date_utils.py          # Date utilities
│       │   │   │   ├── path_loader.py         # Path resolution
│       │   │   │   └── ticker_utils.py        # Ticker utilities
│       │   │   └── static/
│       │   │       ├── static_data_worker.py  # Static data worker
│       │   │       ├── fundamentals_fetch.py  # Fundamentals fetcher
│       │   │       └── borrow_fee_fetch.py    # Borrow fee fetcher
│       │   ├── news/
│       │   │   ├── news_worker.py       # News fetcher (momo/benzinga/fmp)
│       │   │   ├── news_fetch.py        # News fetch helpers
│       │   │   └── processor.py         # LLM news classifier
│       │   ├── factor/                  # [Stage3] Factor computation
│       │   │   └── factor_engine.py     # FactorManager (Python, to be rewritten in Rust)
│       │   └── strategy/                # [Stage3] Strategy execution
│       │       └── state_engine.py      # StateEngine (Python, to be rewritten in Rust)
│       │
│       ├── apps/                        # ── APPLICATION / INTERFACE LAYER ─────────
│       │   │                            #    HTTP, WebSocket, process entry-points
│       │   ├── chart_app/               # Machine A — tick data + bar serving
│       │   │   └── server.py            # ChartBFF FastAPI/WebSocket app
│       │   ├── order_app/               # Machine A — IBKR order management
│       │   │   ├── main.py
│       │   │   ├── api/
│       │   │   │   ├── routes_orders.py
│       │   │   │   ├── routes_portfolio.py
│       │   │   │   └── server.py
│       │   │   ├── adapter/
│       │   │   │   ├── ib_gateway.py
│       │   │   │   ├── ibkr_client.py
│       │   │   │   ├── ibkr_wrapper.py
│       │   │   │   └── event_bus.py
│       │   │   ├── models/
│       │   │   │   ├── order_models.py
│       │   │   │   ├── portfolio_models.py
│       │   │   │   ├── event_models.py
│       │   │   │   ├── order.py
│       │   │   │   └── contract.py
│       │   │   ├── persistence/
│       │   │   │   ├── db.py
│       │   │   │   └── models.py
│       │   │   └── services/
│       │   │       ├── order_service.py
│       │   │       ├── portfolio_service.py
│       │   │       └── database_service.py
│       │   ├── snapshot_app/            # Machine B — snapshot collect / replay
│       │   │   ├── collector.py
│       │   │   └── replayer.py
│       │   ├── market_bff/              # Machine B — JerryTrader BFF (snapshot/top-gainers)
│       │   │   └── server.py            # JerryTraderBFF FastAPI/WebSocket app
│       │   ├── news_app/                # Machine C — news BFF
│       │   │   └── server.py            # AgentBFF FastAPI/WebSocket app
│       │   └── agent_app/               # Machine C — [Stage4] agent runtime (empty)
│       │
│       └── runtime/                     # ── PROCESS ORCHESTRATION ─────────────────
│           ├── __main__.py              # `python -m jerry_trader.runtime --machine wsl2`
│           ├── backend_starter.py       # Main orchestrator (900+ lines)
│           └── ml/                      # ML pipeline
│               ├── dataset.py
│               ├── model.py
│               ├── train.py
│               ├── evaluate.py
│               ├── mock_data.py
│               └── saved_models/
│
├── tests/
│   ├── core/                            # Unit tests (pure, no I/O)
│   │   ├── test_bar_builder.py
│   │   ├── test_replay_clock.py
│   │   ├── test_snapshot_compute.py
│   │   └── test_bridge.py
│   └── integration/                     # Tests requiring live infra (Redis, CH etc)
│       └── test_bars_clickhouse.py
```

---

## 4. Layer Definitions

The codebase follows a **layered architecture** with strict dependency rules:

```
┌──────────────────────────────────────────┐
│           apps/  +  runtime/             │  ← Entry points, HTTP/WS, CLI
├──────────────────────────────────────────┤
│               services/                  │  ← Stateful workers, use-cases
├──────────────────────────────────────────┤
│               domain/                    │  ← Pure value objects, NO I/O
├───────────────────┬──────────────────────┤
│    platform/      │       shared/        │  ← Infra clients  |  Utils
└───────────────────┴──────────────────────┘
```

**Dependency Rules:**
- `domain/` → imports NOTHING from this project (only stdlib + Pydantic)
- `services/` → imports `domain/`, `platform/`, `shared/`. Never imports `apps/`
- `apps/` → imports `services/`, `domain/`, `platform/`, `shared/`
- `runtime/` → imports everything; it is the composition root
- `platform/` → imports `shared/` only. **Never imports `services/` or `apps/`.**
- `shared/` → imports nothing from this project

**Current Status:**
- ✅ All services moved from `packages/` to `services/`
- ✅ `backend_starter.py` moved to `runtime/`
- ✅ `shared/logging/logger.py` moved from `shared/utils/`
- ✅ Platform layer clean (no service imports)
- ✅ **Bootstrap Coordinator V2** implemented (direct service calls, no Redis Streams)
- ✅ **Domain Layer** populated with Bar, BarPeriod, Order, OrderState, FactorSnapshot
- ✅ `config_builder.py` moved from `platform/config/` to `runtime/` (only consumer is backend_starter)

---

## 5. Bootstrap Coordinator V2

The **Bootstrap Coordinator** orchestrates the ticker subscription flow to ensure bars and factors are warmed up before serving data to the frontend.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        ChartBFF (WebSocket)                      │
│                          apps/chart_app/                         │
└──────────────────────────────┬──────────────────────────────────┘
                               │ start_bootstrap(symbol, timeframes)
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BootstrapCoordinator                          │
│              services/orchestration/                             │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Per-symbol, per-timeframe state tracking                │   │
│  │  - TimeframeState: PENDING → READY → DONE                │   │
│  │  - TradesBootstrapState: NOT_NEEDED → FETCHING → READY   │   │
│  └──────────────────────────────────────────────────────────┘   │
└────────────┬───────────────────────────────┬────────────────────┘
             │                               │
   add_ticker()│                    add_ticker()│
             ▼                               ▼
┌─────────────────────────┐    ┌─────────────────────────┐
│   BarsBuilderService    │    │     FactorEngine        │
│   services/bar_builder/ │    │   services/factor/      │
│                         │    │                         │
│ • Fetch trades (Polygon │    │ • Register consumers    │
│   or parquet)           │    │ • Wait for trades       │
│ • Build bars (Rust)     │    │ • Tick warmup           │
│ • Store to ClickHouse   │    │ • Wait for bars         │
│ • Notify coordinator    │    │ • Bar warmup            │
│   (on_bars_ready)       │    │ • Notify coordinator    │
└───────────┬─────────────┘    └───────────┬─────────────┘
            │                              │
            ▼                              ▼
     ┌──────────────┐             ┌──────────────┐
     │  ClickHouse  │             │  ClickHouse  │
     │  ohlcv_bars  │             │   factors    │
     └──────────────┘             └──────────────┘
```

### BootstrapableService Protocol

Services that participate in bootstrap implement the `BootstrapableService` protocol:

```python
class BootstrapableService(Protocol):
    def add_ticker(self, symbol: str, timeframes: list[str]) -> bool:
        """Add ticker for processing. Check ClickHouse first — if bars exist,
        notify coordinator immediately."""
        ...

    def remove_ticker(self, symbol: str) -> bool:
        """Remove ticker from processing."""
        ...

    def is_ready(self, symbol: str) -> bool:
        """Check if ticker bootstrap is complete."""
        ...
```

**Implementations:**
- `BarsBuilderService` — fetches trades, builds bars, stores to ClickHouse
- `FactorEngine` — waits for trades/bars, warms up indicators

### Per-Timeframe State Tracking

The coordinator tracks bootstrap state per symbol and timeframe:

```python
TimeframeState:
  PENDING → FETCHING → READY → WARMUP → DONE
              ↑           ↑
              └─ BarsBuilder notifies
                          └─ FactorEngine notifies

TradesBootstrapState (shared across timeframes):
  NOT_NEEDED → PENDING → FETCHING → READY → CONSUMING → DONE
```

### Key Design Decisions

1. **Direct Method Calls** — Services register with coordinator; coordinator calls `add_ticker()` directly instead of using Redis Streams (more reliable, better timing control)

2. **Incremental Bootstrap** — When switching timeframes on an existing ticker, only the new timeframes are bootstrapped (not a full re-bootstrap)

3. **ClickHouse First Check** — Services check if bars already exist in ClickHouse before fetching; if they exist, bootstrap is skipped for that timeframe

4. **Thread-Safe State** — All coordinator state changes are protected by `threading.Lock()`

### Flow: New Ticker Subscription

1. Frontend sends WebSocket subscribe request to ChartBFF
2. ChartBFF calls `coordinator.start_bootstrap(symbol, timeframes)` in background thread
3. Coordinator analyzes timeframes, creates `TickerBootstrap` state
4. Coordinator calls `BarsBuilderService.add_ticker()` and `FactorEngine.add_ticker()` in background threads
5. BarsBuilder checks ClickHouse — if no bars, fetches trades and builds bars
6. BarsBuilder stores bars to ClickHouse, calls `coordinator.on_bars_ready(symbol, tf)`
7. FactorEngine registers as consumer, waits for trades/bars via coordinator
8. FactorEngine warms up indicators from historical data
9. FactorEngine calls `coordinator.report_done(symbol, phase, consumer_id)`
10. When all phases complete, coordinator sets ready event
11. ChartBFF waits on `coordinator.wait_for_ticker_ready()` (30s timeout)
12. ChartBFF serves bars from ClickHouse to frontend

### Flow: Timeframe Switching

When switching from 1m to 5m on an existing ticker:

1. ChartBFF calls `coordinator.start_bootstrap(symbol, ["5m"])`
2. Coordinator detects existing bootstrap, adds "5m" to timeframe states
3. Coordinator calls `BarsBuilderService.add_ticker()` with just `["5m"]`
4. BarsBuilder detects existing ticker, only bootstraps new timeframe
5. Same for FactorEngine — only new timeframe is processed
6. Existing 1m tick stream and bars continue uninterrupted

---

## 6. Rust Extension Layout

```
rust/
├── Cargo.toml
├── Cargo.lock
└── src/
    ├── lib.rs           # PyO3 module root, exports all public types
    ├── bars.rs          # BarBuilder — watermark-based OHLCV bar builder
    ├── clock.rs         # ReplayClock — monotonic replay clock
    ├── snapshot.rs      # VolumeTracker — snapshot compute
    ├── factors.rs       # z_score, price_accel, factor helpers
    └── replayer/
        ├── mod.rs       # TickDataReplayer
        └── loader.rs    # load_trades_from_parquet (Parquet lake reader)
```

**Key Features:**
- **BarBuilder**: Watermark-based close with late-arrival window, min-heap scheduling for boundary-driven expiry
- **ReplayClock**: Monotonic replay clock with Redis heartbeat (100ms interval)
- **VolumeTracker**: High-performance snapshot processing
- **Factors**: z_score, price_accel computation

**Stage 3 Additions (Planned):**
- `state_engine.rs` — rewrite of Python `StateEngine` (high-frequency tick state)
- `factor_engine.rs` — rewrite of Python `FactorManager`

---

## 7. Data Flow

### Live Mode (Machine A) — Bootstrap Coordinator V2

```
Polygon WebSocket
      │
      ▼
UnifiedTickManager  ──fan-out──►  BarsBuilderService  ──► ClickHouse (OHLCV)
      │                    │             │
      │                    │             └──► BootstrapCoordinator
      │                    │                      ▲
      │                    │    on_bars_ready()   │
      │                    │                      │
      │                    └──► FactorEngine ─────┘
      │                         (bar warmup)   report_done()
      │
      └──► ChartBFF ◄────────── wait_for_ready()
                │
                ▼
            Frontend (WebSocket)
```

**Bootstrap Flow:**
1. ChartBFF receives subscribe request from frontend
2. ChartBFF calls `coordinator.start_bootstrap(symbol, timeframes)`
3. Coordinator calls `BarsBuilderService.add_ticker()` and `FactorEngine.add_ticker()` directly
4. BarsBuilder fetches trades → builds bars → stores in ClickHouse → `coordinator.on_bars_ready()`
5. FactorEngine waits for bars → warms up indicators → `coordinator.report_done()`
6. Coordinator signals "ready" → ChartBFF serves data

**Key Design:** Direct method calls via `BootstrapableService` protocol, not Redis Streams.

### Live Mode (Machine B)
### Live Mode (Machine B)

```
Polygon REST (snapshot)
      │
      ▼
MarketSnapshotCollector ──► Redis B (raw snapshot)
                                  │
                                  ▼
                         SnapshotProcessor (Rust VolumeTracker)
                                  │
                     ┌────────────┴────────────┐
                     ▼                         ▼
              ClickHouse (snapshot)      Redis B (processed)
                                               │
                                               ▼
                                       JerryTraderBFF ──► Frontend
```

### Replay Mode

```
Parquet Lake (Machine A)
      │
      ▼
TickDataReplayer (Rust)          ReplayClock (Rust)
      │                                │
      │                                └──► Redis A heartbeat (100ms)
      ▼                                            │
UnifiedTickManager                                 ▼
   (SyncedReplayerManager)               Machine B: RemoteClockFollower
      │                                            │
      └──► same as live from here ──►    MarketSnapshotReplayer
```

**Clock Synchronization:**
- ChartBFF machine is the clock domain master (runs `ReplayClock` in-process)
- Remote machines follow via Redis heartbeat using `RemoteClockFollower`
- Monotonic interpolation between heartbeats for sub-100ms accuracy

### News Pipeline (Machine C)

```
NewsWorker (poll: momo/benzinga/fmp)
      │
      ▼
Redis B (raw news queue)  ──► Postgres (articles)
      │
      ▼
NewsProcessor (LLM: DeepSeek / Kimi)
      │
      ▼
Postgres (classified results)  ──► Redis B (news events)
      │
      ▼
AgentBFF ──► Frontend / [Stage4] AgentRuntime
```

---

## 8. ATC-R Agent Layer

ATC-R (Agent Trading Copilot - Rule-driven) is inspired by ACT-R cognitive architecture. The key insight: **Agent doesn't poll raw data — a Rule Engine monitors factors and activates Agent only when conditions match.**

### Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                 ATC-R Agent System                  │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │ Layer 1: Rule Engine (always-on, low-lat)   │   │
│  │                                             │   │
│  │   Strategy DSL Rules (backtest optimized)   │   │
│  │   ├─ Factor threshold triggers              │   │
│  │   ├─ News classifier triggers               │   │
│  │   └─ Pattern: IF condition THEN activate    │   │
│  │                                             │   │
│  │   Conflict Resolution: highest utility wins │   │
│  │                                             │   │
│  └─────────────────────────────────────────────┘   │
│                        ↓                           │
│                   Activation Event                 │
│                   {rule_id, symbol, context}       │
│                        ↓                           │
│  ┌─────────────────────────────────────────────┐   │
│  │ Layer 2: Agent Think (on-demand, LLM)       │   │
│  │                                             │   │
│  │   State: idle → thinking → done             │   │
│  │                                             │   │
│  │   1. Context Aggregator                     │   │
│  │      - Gather factors, news, trades         │   │
│  │      - Time window: last 5min               │   │
│  │                                             │   │
│  │   2. LLM Reasoning                          │   │
│  │      - Per-rule prompt template             │   │
│  │      - Deep analysis → action               │   │
│  │                                             │   │
│  └─────────────────────────────────────────────┘   │
│                        ↓                           │
│  ┌─────────────────────────────────────────────┐   │
│  │ Layer 3: Action                              │   │
│  │                                             │   │
│  │   - Notify (Telegram/Discord/Webhook)       │   │
│  │   - Generate Widget (template-based)        │   │
│  │   - Trigger downstream rule                 │   │
│  │                                             │   │
│  └─────────────────────────────────────────────┘   │
│                                                     │
│  ═══════════════════════════════════════════════   │
│  Offline: Backtest Engine                          │
│  - Optimize rule thresholds                        │
│  - Validate prompt templates                       │
│  - Simulate agent decisions                        │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### Rule Schema (Strategy DSL)

```yaml
# Example: Volume Spike + EMA Crossover Strategy
id: vol_spike_ema
name: "Volume spike with EMA crossover"

# Trigger conditions (backtest optimized thresholds)
trigger:
  type: AND
  conditions:
    - factor: volume_ratio_5m
      op: gt
      value: 3.0        # Optimized via backtest
    - factor: price
      op: cross_above
      target: ema_20
  window: 60s          # Conditions must occur within 60s

# Utility score (learned from backtest)
utility: 0.85

# Context to gather when triggered
context:
  factors: [ema_20, volume_ratio_5m, trade_rate]
  factor_history: 5m
  recent_news: 3
  recent_trades: 20

# Pre-set prompt template for this rule
prompt_template: |
  You are analyzing a momentum signal for {{symbol}}.
  Trigger: Volume spike ({{volume_ratio_5m}}x) + price crossed above EMA20.
  Analyze: Is this sustained? Key levels? Recommended action?
  Output as JSON: {signal_quality, action, message, key_levels}

# Allowed actions
actions:
  - notify
  - create_widget
  - set_alert
```

### Implementation Structure

```
python/src/jerry_trader/agent/
  rules/
    schema.py           # Rule Pydantic models
    parser.py           # YAML/JSON parser
    storage.py          # Rule DB storage
  engine/
    matcher.py          # Pattern matching
    resolver.py         # Conflict resolution
    executor.py         # Trigger execution
  context/
    aggregator.py       # Context gathering
  reasoner/
    llm_client.py       # DeepSeek/Kimi API
    prompts.py          # Template rendering
    parser.py           # Output parsing
  backtest/
    simulator.py        # Rule simulation
    optimizer.py        # Threshold optimization
    evaluator.py        # Decision quality metrics
  bff/
    main.py             # FastAPI app
    websocket.py        # WS handler
```

### Why This Design

| Concern | Solution |
|---------|----------|
| Agent latency | Rule Engine runs continuously, catches signals fast |
| LLM cost | Agent only activated on significant events |
| Strategy trust | Rules are backtest-optimized, not ad-hoc |
| Explainability | Each activation has clear trigger + prompt |
| Scalability | Multiple rules monitor multiple symbols |

See [roadmap/atcr-agent-system.md](roadmap/atcr-agent-system.md) for detailed task breakdown.

---

## 9. Development Guidelines

### Adding New Services

1. Create service in `services/<domain>/`
2. Import only from `domain/`, `platform/`, `shared/`
3. Never import from `apps/` or `runtime/`
4. Add service wiring to `runtime/backend_starter.py`

### Adding New Apps

1. Create app in `apps/<app_name>/`
2. Create `server.py` with FastAPI/WebSocket setup
3. Import services from `services/`
4. Add app startup to `runtime/backend_starter.py`

### Modifying Rust Code

1. Edit files in `rust/src/`
2. Rebuild: `poetry run maturin develop`
3. For release builds: `poetry run maturin develop --release`
4. Update `_rust.pyi` stubs if adding new exports

### Testing

- Unit tests (pure logic) → `python/tests/core/`
- Integration tests (requires infra) → `python/tests/integration/`
- Always test after bar builder or clock changes

### Configuration

- Machine roles: `config.yaml` (copy from `config.yaml.example`)
- Data paths: `basic_config.yaml`
- Environment variables: `.env`

### Known Issues

- ✅ **FIXED (Bootstrap Coordinator V2):** Timeframe switching no longer causes 30s blocking timeout. The coordinator now supports incremental bootstrap for new timeframes on existing tickers.

- When switching frontend chart timespan [10s, 1m], newest bar may cover last bar's timespan duration (open/high/low)
  - Not fixed to avoid disrupting current tickdata orchestration
