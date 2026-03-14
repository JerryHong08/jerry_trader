# Jerry Trader — Architecture & Restructuring Proposal

> **Status: DRAFT — agreed, migration in progress**
> This document describes the proposed restructured layout for the Python codebase before entering Stage 3 & 4.
> Current state is partially restructured; this doc captures the target end-state.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Machine Topology](#2-machine-topology)
3. [Current Structure Problems](#3-current-structure-problems)
4. [Proposed Python Layout](#4-proposed-python-layout)
5. [Layer Definitions](#5-layer-definitions)
6. [Domain Package Map](#6-domain-package-map)
7. [Rust Extension Layout](#7-rust-extension-layout)
8. [Non-Python Layout](#8-non-python-layout)
9. [Data Flow](#9-data-flow)
10. [Stage 3 & 4 Fit](#10-stage-3--4-fit)
11. [Migration Notes](#11-migration-notes)
12. [Work Division](#12-work-division)

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

Future goals (Stage 3/4): strategy engine, risk management, ML pipeline, and an AI agent layer for monitoring, analysis, and autonomous execution.

---

## 2. Machine Topology

```
┌─────────────────────────────────────────────────────────────────┐
│  Machine A  (WSL2 / primary)                                    │
│                                                                 │
│  • ChartBFF          (WebSocket, bar serving — port 8000)       │
│  • BarsBuilder       (Rust BarBuilder → ClickHouse)             │
│  • OrderRuntime      (IBKR adapter, FastAPI — port 8888)        │
│  • GlobalClock       (ReplayClock master + Redis heartbeat)     │
│  • [Stage3] FactorEngine                                        │
│  • [Stage3] StateEngine                                         │
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
│    (port 5001)       │        │  • AgentBFF (port 5003)  │
│  • SnapshotProcessor │        │  • [Stage4] AgentRuntime │
│  • StaticDataWorker  │        │                          │
│  • Collector         │        │  Redis B (shared w/ B)   │
│  • Replayer          │        │  Postgres B              │
│                      │        └──────────────────────────┘
│  Redis B  │ ClickHouse│
└──────────────────────┘
```

**Redis A** — tick data streams, bar streams, ChartBFF pub/sub, clock heartbeats
**Redis B** — snapshot streams, news cache, static data, order/portfolio pub/sub
**ClickHouse** — OHLCV bars (persistent), market snapshot history
**Postgres** — order history, news articles, LLM results
**Parquet (local lake)** — raw tick data for replay (Machine A path)

---

## 3. Current Structure Problems

Before proposing the new layout, here are the pain points in the current structure:

| # | Problem | Location |
|---|---------|----------|
| 1 | **Mixed abstraction levels in `packages/`** — `bar_builder` is a service, `analytics` has domain models, `market_data` has infra feeds, all side by side | `packages/` |
| 2 | **Duplicate / competing layout** — `packages/market_data/bootstrap/` AND `packages/market_data/infra/bootstrap/` both exist | `packages/market_data/` |
| 3 | **Typo in path** — `applilcation` (3 l's) | `packages/market_data/applilcation/` |
| 4 | **`shared/utils/` is a junk drawer** — `momo_token.py`, `session.py`, `paths.py`, `config_builder.py` all live there without clear grouping | `shared/utils/` |
| 5 | **`platform/` is nearly empty** — `event_bus/` and `messaging/rpc/` are empty dirs; `storage/` only has ClickHouse | `platform/` |
| 6 | **`apps/market_runtime/` owns `backend_starter.py`** — the starter is the composition root for the whole system, yet it's buried inside one app's folder | `apps/market_runtime/` |
| 7 | **`apps/research_runtime/` is empty** | `apps/research_runtime/` |
| 8 | **`tools/` is empty** | `tools/` |
| 9 | **`analytics/` is structurally incomplete** — `application/` is empty, `domain/` has only two files, no clear boundary | `packages/analytics/` |
| 10 | **No `strategy/` domain** — strategy logic has no home yet, will be needed in Stage 3 | — |
| 11 | **`skills/` sits at repo root** — intentional, kept here (see §8) | `skills/` |
| 12 | **`backend_starter` is a monolith** — 900 lines, handles clock init, service wiring, signal handling, watchdog, all in one class | — |
| 13 | **`platform/storage/clickhouse_client.py` imports `packages/bar_builder/chart_data_service.py`** — a platform-layer file importing a service-layer file, breaking the dependency rule | `platform/storage/` |
| 14 | **Tests have stale import paths** — `jerry_trader.core._bridge`, `jerry_trader.data_supply.bootstrap_data_supply`, `jerry_trader.core.snapshot.compute` reference a previous layout | `python/tests/` |

---

## 4. Proposed Python Layout

```
python/
├── src/
│   └── jerry_trader/
│       │
│       ├── __init__.py
│       ├── _rust.pyi                    # Rust extension stubs (auto/manual)
│       ├── clock.py                     # Global clock singleton (keep as-is, top-level)
│       ├── schema.py                    # Shared Pydantic/dataclass schemas
│       │
│       ├── platform/                    # ── INFRASTRUCTURE LAYER ──────────────────
│       │   ├── config/
│       │   │   ├── config.py            # Config loader, lake_data_dir, env resolution
│       │   │   └── session.py           # make_session_id (moved from shared/utils)
│       │   ├── storage/
│       │   │   ├── clickhouse.py        # Pure ClickHouse connect/query client (NO service imports)
│       │   │   ├── postgres.py          # SQLAlchemy engine factory
│       │   │   ├── redis.py             # Redis client factory
│       │   │   └── ohlcv_writer.py      # ClickHouse OHLCV write helper
│       │   ├── messaging/
│       │   │   ├── redis_streams.py     # Redis Streams helpers
│       │   │   ├── redis_pubsub.py      # Redis pub/sub helpers
│       │   │   └── rpc/                 # [Stage4] Redis-based RPC stubs
│       │   └── event_bus/
│       │       └── bus.py               # [Stage4] in-process event bus
│       │
│       ├── shared/                      # ── CROSS-CUTTING UTILITIES ───────────────
│       │   ├── ids/
│       │   │   └── redis_keys.py        # Centralised Redis key / channel names
│       │   ├── time/
│       │   │   ├── timezone.py          # ET ZoneInfo constant, helpers
│       │   │   └── remote_clock.py      # RemoteClockFollower
│       │   ├── logging/
│       │   │   └── logger.py            # setup_logger (moved from shared/utils)
│       │   └── utils/
│       │       ├── async_helpers.py
│       │       ├── data_utils.py
│       │       ├── parse.py
│       │       └── paths.py
│       │
│       ├── domain/                      # ── DOMAIN LAYER (pure business logic) ────
│       │   ├── market/
│       │   │   ├── tick.py              # Tick / Trade value objects
│       │   │   ├── bar.py               # Bar / OHLCV value objects
│       │   │   └── snapshot.py          # Snapshot value objects (top-gainer row etc)
│       │   ├── order/
│       │   │   ├── order.py             # Order, Fill domain models
│       │   │   └── contract.py          # Contract domain model
│       │   ├── strategy/                # [Stage3] — placeholder, keep empty for now
│       │   │   ├── signal.py            # Signal value object
│       │   │   └── risk.py              # Risk limits domain model
│       │   └── factor/                  # [Stage3] — Factor domain
│       │       └── factor.py            # Factor value object
│       │
│       ├── services/                    # ── SERVICE / USE-CASE LAYER ──────────────
│       │   │                            #    Stateful workers, no HTTP/WS here
│       │   ├── bar_builder/
│       │   │   ├── bars_builder_service.py
│       │   │   ├── chart_data_service.py
│       │   │   └── clickhouse_bar_client.py  # Moved from platform/storage/clickhouse_client.py
│       │   │   #   (was importing chart_data_service — belongs here, not in platform)
│       │   ├── market_snapshot/
│       │   │   ├── processor.py         # SnapshotProcessor (Rust VolumeTracker)
│       │   │   └── overview_chart_data_manager.py
│       │   ├── market_data/
│       │   │   ├── feeds/
│       │   │   │   ├── unified_tick_manager.py
│       │   │   │   ├── polygon_manager.py
│       │   │   │   ├── thetadata_manager.py
│       │   │   │   ├── replayer_manager.py
│       │   │   │   └── synced_replayer_manager.py
│       │   │   ├── bootstrap/
│       │   │   │   ├── local_data_loader.py   # (merged from localdata_loader/)
│       │   │   │   ├── polygon_fetcher.py
│       │   │   │   ├── benchmark_loader.py
│       │   │   │   ├── date_utils.py
│       │   │   │   ├── path_loader.py
│       │   │   │   └── ticker_utils.py
│       │   │   └── static/
│       │   │       ├── static_data_worker.py  # (moved from market_data/applilcation — note typo fixed)
│       │   │       ├── fundamentals_fetch.py
│       │   │       └── borrow_fee_fetch.py
│       │   ├── news/
│       │   │   ├── news_worker.py
│       │   │   ├── news_fetch.py        # (moved from news/infra)
│       │   │   └── processor.py         # LLM news classifier
│       │   ├── factor/                  # [Stage3]
│       │   │   └── factor_engine.py     # FactorManager (moved from analytics/domain)
│       │   └── strategy/                # [Stage3]
│       │       ├── state_engine.py      # StateEngine (moved from analytics/domain)
│       │       └── risk_engine.py       # RiskEngine (new)
│       │
│       ├── apps/                        # ── APPLICATION / INTERFACE LAYER ─────────
│       │   │                            #    HTTP, WebSocket, process entry-points
│       │   ├── chart_app/               # Machine A — tick data + bar serving
│       │   │   ├── __init__.py
│       │   │   └── server.py            # ChartBFF FastAPI/WebSocket app
│       │   ├── order_app/               # Machine A — IBKR order management
│       │   │   ├── __init__.py
│       │   │   ├── main.py
│       │   │   ├── api/
│       │   │   │   ├── routes_orders.py
│       │   │   │   ├── routes_portfolio.py
│       │   │   │   └── server.py
│       │   │   ├── adapter/
│       │   │   │   ├── ib_gateway.py
│       │   │   │   ├── ibkr_client.py
│       │   │   │   └── ibkr_wrapper.py
│       │   │   ├── models/
│       │   │   │   ├── order_models.py
│       │   │   │   ├── portfolio_models.py
│       │   │   │   ├── event_models.py
│       │   │   │   └── contract.py
│       │   │   ├── persistence/
│       │   │   │   ├── db.py
│       │   │   │   └── models.py
│       │   │   └── services/
│       │   │       ├── order_service.py
│       │   │       ├── portfolio_service.py
│       │   │       └── database_service.py
│       │   ├── snapshot_app/            # Machine B — snapshot collect / replay
│       │   │   ├── __init__.py
│       │   │   ├── collector.py
│       │   │   └── replayer.py
│       │   ├── market_bff/              # Machine B — JerryTrader BFF (snapshot/top-gainers)
│       │   │   ├── __init__.py
│       │   │   └── server.py            # JerryTraderBFF FastAPI/WebSocket app
│       │   ├── news_app/                # Machine C — news BFF + AgentBFF
│       │   │   ├── __init__.py
│       │   │   └── server.py            # AgentBFF FastAPI/WebSocket app
│       │   └── agent_app/               # Machine C — [Stage4] agent runtime
│       │       ├── __init__.py
│       │       └── server.py            # AgentBFF HTTP + WS interface
│       │
│       └── runtime/                     # ── PROCESS ORCHESTRATION ─────────────────
│           ├── __main__.py              # `python -m jerry_trader --machine wsl2` entry point
│           ├── backend_starter.py       # Moved here from apps/market_runtime/backend_starter.py
│           └── ml/                      # ML pipeline (was packages/ml_learning)
│               ├── dataset.py
│               ├── model.py
│               ├── train.py
│               ├── evaluate.py
│               └── saved_models/
│
├── tests/
│   ├── __init__.py
│   ├── test_config.py
│   ├── core/                            # Unit tests (pure, no I/O)
│   │   ├── test_bar_builder.py
│   │   ├── test_chart_data_service.py
│   │   ├── test_data_loader.py
│   │   ├── test_factors_compute.py
│   │   ├── test_replay_clock.py
│   │   ├── test_snapshot_compute.py
│   │   ├── test_synced_replayer.py
│   │   └── test_bridge.py
│   └── integration/                     # Tests requiring live infra (Redis, CH etc)
│       └── test_bars_clickhouse.py
```

---

## 5. Layer Definitions

The proposed layout follows a **layered architecture** with strict dependency rules:

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

**Rules:**
- `domain/` → imports NOTHING from this project (only stdlib + Pydantic)
- `services/` → imports `domain/`, `platform/`, `shared/`. Never imports `apps/`
- `apps/` → imports `services/`, `domain/`, `platform/`, `shared/`
- `runtime/` → imports everything; it is the composition root
- `platform/` → imports `shared/` only. **Never imports `services/` or `apps/`.**
- `shared/` → imports nothing from this project

> ⚠️ **Known violation to fix:** `platform/storage/clickhouse_client.py` currently imports
> `packages/bar_builder/chart_data_service.py`. This file will be moved to
> `services/bar_builder/clickhouse_bar_client.py` during Phase 2.

---

## 6. Domain Package Map

Mapping of every current file to its new home:

### `platform/`

| Current | Proposed |
|---------|----------|
| `platform/config/config.py` | `platform/config/config.py` ✓ |
| `shared/utils/session.py` | `platform/config/session.py` |
| `platform/storage/clickhouse_client.py` | ⚠️ **NOT** `platform/storage/clickhouse.py` — see violation note above. Moves to `services/bar_builder/clickhouse_bar_client.py` |
| `platform/storage/ohlcv_writer.py` | `platform/storage/ohlcv_writer.py` ✓ |
| `platform/messaging/redis/redis_streams.py` | `platform/messaging/redis_streams.py` |
| `platform/messaging/rpc/` (empty) | `platform/messaging/rpc/` (keep, Stage4) |
| `platform/event_bus/` (empty) | `platform/event_bus/bus.py` (keep, Stage4) |
| _(new)_ | `platform/storage/clickhouse.py` — pure connect-only ClickHouse client (no service deps) |

### `shared/`

| Current | Proposed |
|---------|----------|
| `shared/ids/redis_keys.py` | `shared/ids/redis_keys.py` ✓ |
| `shared/time/remote_clock.py` | `shared/time/remote_clock.py` ✓ |
| `shared/time/timezone.py` | `shared/time/timezone.py` ✓ |
| `shared/utils/logger.py` | `shared/logging/logger.py` |
| `shared/utils/async_helpers.py` | `shared/utils/async_helpers.py` ✓ |
| `shared/utils/data_utils.py` | `shared/utils/data_utils.py` ✓ |
| `shared/utils/parse.py` | `shared/utils/parse.py` ✓ |
| `shared/utils/paths.py` | `shared/utils/paths.py` ✓ |
| `shared/utils/config_builder.py` | `platform/config/config.py` (merge) |
| `shared/utils/momo_token.py` | `shared/utils/momo_token.py` ✓ |

### `services/`

| Current | Proposed |
|---------|----------|
| `packages/bar_builder/bars_builder_service.py` | `services/bar_builder/bars_builder_service.py` |
| `packages/bar_builder/chart_data_service.py` | `services/bar_builder/chart_data_service.py` |
| `packages/market_snapshot/processor.py` | `services/market_snapshot/processor.py` |
| `packages/market_snapshot/overview_chart_data_manager.py` | `services/market_snapshot/overview_chart_data_manager.py` |
| `packages/market_data/feeds/*.py` | `services/market_data/feeds/*.py` |
| `packages/market_data/bootstrap/localdata_loader/*.py` | `services/market_data/bootstrap/*.py` |
| `packages/market_data/infra/bootstrap/polygon_fetcher.py` | `services/market_data/bootstrap/polygon_fetcher.py` |
| `packages/market_data/infra/feeds/static/*.py` | `services/market_data/static/*.py` |
| `packages/market_data/applilcation/static_data_worker.py` | `services/market_data/static/static_data_worker.py` (note: `applilcation` typo fixed) |
| `packages/news/news_worker.py` | `services/news/news_worker.py` |
| `packages/news/infra/news_fetch.py` | `services/news/news_fetch.py` |
| `packages/news/application/processor.py` | `services/news/processor.py` |
| `packages/analytics/domain/engine.py` | `services/factor/factor_engine.py` |
| `packages/analytics/domain/state_engine.py` | `services/strategy/state_engine.py` |
| `packages/ml_learning/*.py` | `runtime/ml/*.py` |

### `apps/`

| Current | Proposed |
|---------|----------|
| `apps/market_runtime/interfaces/chart_bff.py` | `apps/chart_app/server.py` |
| `apps/market_runtime/interfaces/bff.py` | `apps/market_bff/server.py` |
| `apps/market_runtime/interfaces/news_bff.py` | `apps/news_app/server.py` |
| `apps/order_runtime/` | `apps/order_app/` ✓ (structure already good) |
| `apps/snapshot_runtime/collector.py` | `apps/snapshot_app/collector.py` |
| `apps/snapshot_runtime/replayer.py` | `apps/snapshot_app/replayer.py` |
| `apps/agent_runtime/` (empty) | `apps/agent_app/` (Stage4) |
| `apps/research_runtime/` (empty) | **DELETE** (research lives in `runtime/ml/`) |
| `apps/market_runtime/` (folder) | **DELETE** after all contents moved |

### `runtime/`

| Current | Proposed |
|---------|----------|
| `apps/market_runtime/backend_starter.py` | `runtime/backend_starter.py` |
| _(none)_ | `runtime/__main__.py` |

---

## 7. Rust Extension Layout

The Rust extension layout is already clean. Minimal changes proposed:

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

**Stage 3 additions (Rust):**
- `state_engine.rs` — rewrite of Python `StateEngine` (high-frequency tick state)
- `factor_engine.rs` — rewrite of Python `FactorManager`

These will be exported through `lib.rs` exactly as the existing types are.

---

## 8. Non-Python Layout

```
jerry_trader/
├── rust/                    # Rust extension (PyO3 / maturin)
├── frontend/                # React + TypeScript + TradingView
│   └── src/
│       ├── components/
│       ├── stores/          # Zustand stores
│       ├── services/        # WebSocket client services
│       ├── hooks/
│       ├── types/
│       └── utils/
├── python/                  # Python source (described above)
├── sql/                     # ClickHouse + Postgres DDL
│   ├── clickhouse_ohlcv.sql
│   └── clickhouse_market_snapshot.sql
├── alembic/                 # Postgres migrations (order/news schema)
├── scripts/                 # One-off / ops scripts
│   └── tickdata_extract.py
├── docs/                    # LaTeX PDF + source
├── assets/                  # Screenshot images for README
├── skills/                  # Agent skill definitions (markdown)  ← KEEP HERE
│   ├── analysis/
│   └── trade/
├── data/                    # Local Parquet lake (gitignored)
├── logs/                    # Runtime logs (gitignored)
├── config.yaml              # Machine role config
├── config.yaml.example
├── basic_config.yaml
├── alembic.ini
├── pyproject.toml
├── README.md
├── ARCHITECTURE.md
└── CHANGELOG.md
```

> `skills/` stays at the repo root intentionally — it is documentation for the future Agent layer (Stage 4) and should be co-located with the system config rather than buried inside the Python package.

---

## 9. Data Flow

### Live Mode (Machine A)

```
Polygon WebSocket
      │
      ▼
UnifiedTickManager  ──fan-out──►  BarsBuilderService  ──► ClickHouse (OHLCV)
      │                                  │
      │                                  └──► Redis A (bar stream)
      │                                            │
      ▼                                            ▼
  ChartBFF  ◄──────────────────────────── Frontend (WebSocket)
      │
      └──► Redis A (tick stream)
```

### Live Mode (Machine B)

```
Polygon REST (snapshot)
      │
      ▼
MarketsnapshotCollector ──► Redis B (raw snapshot)
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

## 10. Stage 3 & 4 Fit

### Stage 3 — Strategy Engine

The new `services/strategy/` and `services/factor/` packages are placeholder homes for:

- `FactorEngine` (Python → Rust rewrite lands in `rust/src/factor_engine.rs`, Python wrapper in `services/factor/factor_engine.py`)
- `StateEngine` (Python → Rust rewrite lands in `rust/src/state_engine.rs`, Python wrapper in `services/strategy/state_engine.py`)
- `RiskEngine` — new, lives in `services/strategy/risk_engine.py`
- Signal objects live in `domain/strategy/signal.py`

The `backend_starter.py` composition root will wire these in via the same config-driven role pattern as today.

### Stage 4 — Agent Layer

```
apps/agent_app/          ← AgentBFF HTTP + WebSocket interface
services/agent/          ← (new) agent loop, tool dispatch, memory
    ├── agent.py         ← core agent loop
    ├── tools.py         ← tool registry (wraps existing services)
    ├── memory.py        ← short + long-term memory store
    └── planner.py       ← [future] planning module
platform/messaging/rpc/  ← Redis-based RPC for cross-service tool calls
skills/                  ← Markdown skill instruction files (already at root)
```

The Agent will have read access to:
- Redis A/B (real-time data)
- ClickHouse (OHLCV history, snapshot history)
- Postgres (orders, news)

And write access to:
- `order_app` via RPC / REST (for autonomous execution)
- Redis A (for publishing signals / overrides)

---

## 11. Migration Notes

This is a **big refactor** that touches import paths across the whole project. Suggested migration order to minimize breakage:

### Phase 1 — Low-risk moves (no logic changes, no cross-file import updates needed)

**You do:**
1. Fix typo: rename `packages/market_data/applilcation/` → `packages/market_data/application/` and update its one internal import in `backend_starter.py`
2. Delete empty dirs: `apps/research_runtime/`, `tools/`, `packages/market_data/domain/`, `packages/analytics/application/`
3. Move `shared/utils/logger.py` → `shared/logging/logger.py` and do the grep+replace for all `from jerry_trader.shared.utils.logger import` across the codebase (mechanical, ~30 files)

**I do:**
4. Move `backend_starter.py` from `apps/market_runtime/` → `runtime/backend_starter.py`
5. Add `runtime/__main__.py` as thin entry point wrapper
6. Add `domain/` skeleton directories with empty `__init__.py` stubs (market, order, strategy, factor)

---

### Phase 2 — `packages/` → `services/` (the big rename)

**I do** (all of this is mechanical path changes + import rewrites):
7. Move `packages/bar_builder/` → `services/bar_builder/`
8. Move `platform/storage/clickhouse_client.py` → `services/bar_builder/clickhouse_bar_client.py` (fixes the layer violation — it imports `chart_data_service`, so it belongs in services)
9. Move `packages/market_snapshot/` → `services/market_snapshot/`
10. Move `packages/market_data/feeds/` → `services/market_data/feeds/`
11. Move `packages/market_data/bootstrap/localdata_loader/` → `services/market_data/bootstrap/` (flatten the extra dir level)
12. Move `packages/market_data/infra/bootstrap/polygon_fetcher.py` → `services/market_data/bootstrap/polygon_fetcher.py`
13. Move `packages/market_data/infra/feeds/static/` → `services/market_data/static/`
14. Move `packages/market_data/application/static_data_worker.py` → `services/market_data/static/static_data_worker.py`
15. Move `packages/news/news_worker.py` + `packages/news/infra/news_fetch.py` + `packages/news/application/processor.py` → `services/news/` (flatten)
16. Move `packages/analytics/domain/engine.py` → `services/factor/factor_engine.py`
17. Move `packages/analytics/domain/state_engine.py` → `services/strategy/state_engine.py`
18. Move `packages/ml_learning/` → `runtime/ml/`
19. Update all imports project-wide for all the above moves

---

### Phase 3 — `apps/` rename

**I do:**
20. Move `apps/market_runtime/interfaces/chart_bff.py` → `apps/chart_app/server.py`
21. Move `apps/market_runtime/interfaces/bff.py` → `apps/market_bff/server.py`
22. Move `apps/market_runtime/interfaces/news_bff.py` → `apps/news_app/server.py`
23. Move `apps/snapshot_runtime/` → `apps/snapshot_app/`
24. Move `apps/order_runtime/` → `apps/order_app/`
25. Delete `apps/market_runtime/` (now empty)
26. Update all imports for all the above

---

### Phase 4 — Platform consolidation

**You do** (you know the config internals best):
27. Merge `shared/utils/config_builder.py` into `platform/config/config.py` — decide what to keep, split or absorb
28. Move `shared/utils/session.py` → `platform/config/session.py` — update imports

**I do:**
29. Flatten `platform/messaging/redis/redis_streams.py` → `platform/messaging/redis_streams.py`
30. Add `platform/storage/clickhouse.py` — pure connection-only client (no service imports)
31. Fix stale test imports (`jerry_trader.core._bridge`, `jerry_trader.data_supply.*`, `jerry_trader.core.snapshot.compute`)

---

### What NOT to change
- `clock.py` — stays at `jerry_trader/clock.py` (top-level singleton, imported everywhere)
- `schema.py` — stays at `jerry_trader/schema.py`
- `_rust.pyi` — auto-managed by maturin
- `rust/` — no structural changes until Stage 3 rewrites begin
- `frontend/` — out of scope for this restructure
- `alembic/` — out of scope
- `sql/` — out of scope
- `skills/` — stays at repo root (agent documentation, not source)

---

## 12. Work Division

### Summary

| Phase | Owner | Work |
|-------|-------|------|
| Phase 1 — step 1 | **You** | Fix `applilcation` typo + update its import in `backend_starter.py` |
| Phase 1 — step 2 | **You** | Delete the 4 empty dirs |
| Phase 1 — step 3 | **You** | `logger.py` move + grep-replace all `shared.utils.logger` imports |
| Phase 1 — steps 4–6 | **Me** | `runtime/` scaffold + `domain/` stubs |
| Phase 2 — steps 7–19 | **Me** | All `packages/` → `services/` moves + import rewrites |
| Phase 3 — steps 20–26 | **Me** | All `apps/` renames + import rewrites |
| Phase 4 — steps 27–28 | **You** | Config/session consolidation (you know the internals) |
| Phase 4 — steps 29–31 | **Me** | Messaging flatten, pure clickhouse client, test import fixes |

### Why this split?

- The **three things assigned to you** are either judgment calls (config merging) or purely mechanical grep-replaces on a single import string — they're small, safe, and take 10–15 minutes each.
- Everything else involves moving multiple files with interdependent imports all at once — doing that in one scripted pass is much less error-prone than doing it incrementally by hand. I handle those.
- **Start with Phase 1 (your steps 1–3) first**, then signal me and I'll immediately run Phase 1 steps 4–6, then we proceed in order. This way both of us are never touching the same files simultaneously.

### Your checklist (do in order)

- [ ] **Step 1** — `git mv python/src/jerry_trader/packages/market_data/applilcation python/src/jerry_trader/packages/market_data/application` and update the one line in `backend_starter.py` that imports `from jerry_trader.packages.market_data.applilcation.static_data_worker`
- [ ] **Step 2** — Delete empty dirs: `apps/research_runtime/`, `tools/`, `packages/market_data/domain/`, `packages/analytics/application/`
- [ ] **Step 3** — `git mv python/src/jerry_trader/shared/utils/logger.py python/src/jerry_trader/shared/logging/logger.py` then grep-replace every `from jerry_trader.shared.utils.logger import` → `from jerry_trader.shared.logging.logger import` across the whole project (including tests). About 30 files.
- [ ] **Step 27** (after Phase 3 is done) — Decide how to merge `shared/utils/config_builder.py` into `platform/config/config.py`
- [ ] **Step 28** (after Phase 3 is done) — `git mv shared/utils/session.py platform/config/session.py` + update imports

---

## Summary Table

| Layer | Directory | Responsibility |
|-------|-----------|---------------|
| Infrastructure | `platform/` | DB clients, Redis, messaging primitives |
| Cross-cutting | `shared/` | Logging, time, IDs, pure utils |
| Domain | `domain/` | Value objects, NO I/O, NO dependencies |
| Services | `services/` | Stateful workers, use-cases, data pipelines |
| Apps | `apps/` | HTTP/WS servers, IBKR adapter, process interfaces |
| Orchestration | `runtime/` | `backend_starter`, `__main__`, ML pipeline |
| Rust core | `rust/` | Performance-critical: bars, clock, snapshot, factors |
