# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Jerry Trader is a multi-machine, real-time US pre-market momentum trading system targeting short-term gap-ups with float awareness, relative volume tracking, and catalyst news classification. The system supports both live trading and full historical replay with distributed clock synchronization.

## Development Commands

### Environment Setup
```bash
# Install dependencies
poetry install

# Build Rust extension (required after any Rust code changes)
poetry run maturin develop

# For release-optimized builds
poetry run maturin develop --release

# Verify Rust extension
poetry run python -c "from jerry_trader._rust import sum_as_string; print(sum_as_string(1, 2))"
```

### Database Setup
```bash
# Run Alembic migrations (requires DATABASE_URL in .env)
poetry run alembic upgrade head

# Ensure Redis is running
redis-server  # or via Docker
```

### Running the System
```bash
# Start backend services for a machine profile (defined in config.yaml)
poetry run python -m jerry_trader.runtime.backend_starter --machine wsl2

# Dry-run to see what would start
poetry run python -m jerry_trader.runtime.backend_starter --machine wsl2 --dry-run

# With replay mode (historical date)
poetry run python -m jerry_trader.runtime.backend_starter --machine wsl2 --defaults.replay_date 20260115

# IBKR order management backend
uvicorn jerry_trader.apps.order_app.main:app --reload --port 8888

# Frontend
cd frontend && pnpm install && pnpm dev
```

### Testing
```bash
# Run all tests
poetry run pytest

# Run specific test file
poetry run pytest python/tests/core/test_bar_builder.py

# Run with verbose output
poetry run pytest -v
```

### Code Formatting
```bash
# Format Python code
poetry run black python/
poetry run isort python/

# Check formatting without changes
poetry run black --check python/
```

## Architecture

For detailed architecture documentation, see [ARCHITECTURE.md](ARCHITECTURE.md).

For Stage 3 implementation recommendations, see [STAGE3_RECOMMENDATIONS.md](STAGE3_RECOMMENDATIONS.md).

### Layered Structure

The codebase follows strict layered architecture with dependency rules:

```
apps/ + runtime/     ← Entry points, HTTP/WS, CLI
       ↓
   services/         ← Stateful workers, use-cases
       ↓
    domain/          ← Pure value objects, NO I/O
       ↓
platform/ + shared/  ← Infra clients | Utils
```

**Dependency Rules:**
- `domain/` imports NOTHING from this project (only stdlib + Pydantic)
- `services/` imports `domain/`, `platform/`, `shared/`. Never imports `apps/`
- `apps/` imports `services/`, `domain/`, `platform/`, `shared/`
- `runtime/` imports everything (composition root)
- `platform/` imports `shared/` only. Never imports `services/` or `apps/`
- `shared/` imports nothing from this project

### Key Directories

- `python/src/jerry_trader/apps/` - Application entry points (FastAPI servers, WebSocket handlers)
- `python/src/jerry_trader/services/` - Business logic services (bar builder, market data feeds, news processing)
- `python/src/jerry_trader/domain/` - Pure domain models (market, order, strategy, factor)
- `python/src/jerry_trader/platform/` - Infrastructure (config, storage, messaging)
- `python/src/jerry_trader/shared/` - Cross-cutting utilities (logging, time, IDs)
- `python/src/jerry_trader/runtime/` - Process orchestration (backend_starter, ML pipeline)
- `rust/src/` - Performance-critical components (BarBuilder, ReplayClock, VolumeTracker, factors)

### Multi-Machine Topology

The system is designed to run across multiple machines connected via LAN or Tailscale:

**Machine A (primary):**
- ChartBFF (WebSocket, bar serving - port 8000)
- BarsBuilder (Rust BarBuilder → ClickHouse)
- OrderRuntime (IBKR adapter - port 8888)
- GlobalClock (ReplayClock master + Redis heartbeat)
- Frontend (React + TradingView)
- Redis A, ClickHouse A, Postgres A

**Machine B (data processing):**
- JerryTraderBFF (market snapshot BFF - port 5001)
- SnapshotProcessor (Rust VolumeTracker)
- StaticDataWorker
- Collector/Replayer
- Redis B, ClickHouse B

**Machine C (news & NLP):**
- NewsWorker (momo/benzinga/fmp polling)
- NewsProcessor (LLM classification via DeepSeek/Kimi)
- AgentBFF (port 5003)
- Redis B (shared with Machine B), Postgres B

Machine roles are configured in `config.yaml`. See `config.yaml.example` for the full schema.

## Important Notes

### Bar Builder
- Uses watermark-based close with late-arrival window
- Configured via `configure_watermark(late_arrival_ms, idle_close_ms)`
- Completed bars drained from Rust via `drain_completed()` in flush loop
- Min-heap scheduling for boundary-driven expiry (not full map sweep)

### Clock Synchronization
- In replay mode, ChartBFF machine runs `ReplayClock` and publishes heartbeat to Redis (100ms interval)
- Remote machines use `RemoteClockFollower` for monotonic interpolation between heartbeats
- Critical for cross-machine time accuracy in replay scenarios

### Testing
- Unit tests in `python/tests/core/` - pure logic, no I/O
- Integration tests in `python/tests/integration/` - require live infrastructure (Redis, ClickHouse)
- Always run tests after significant changes to bar builder or clock logic

### Code Style
- Python: Black + isort (line length 88)
- Follow PEP 8 naming conventions
- Type hints preferred for public APIs
- Rust: Standard rustfmt conventions


## Stage 3 & 4 (Planned)

**Stage 3 - Strategy Engine:**
- Rewrite StateEngine and FactorEngine in Rust
- Real-time risk management engine
- ML pipeline for breakout-compute-analyze context model

**Stage 4 - AI Agent Layer:**
- Event-driven system with Redis streams
- Agent loop with tool dispatch and memory
- RPC for cross-service tool calls
- Skills defined in `skills/` directory (markdown format)
