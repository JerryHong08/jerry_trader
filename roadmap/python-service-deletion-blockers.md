# Python BarsBuilderService/FactorEngine Deletion Plan

## Status

**Blocked** - 2026-04-16

## Background

ROADMAP 13.11: Delete Python BarsBuilderService and FactorEngine when Rust ComputeBox replaces them.

## Current Dependencies

### BarsBuilderService Usage

```
BarsBuilderService imported by:
├── runtime/backend_starter.py - Service orchestration
├── services/backtest/batch_engine.py - Backtest pipeline
├── services/orchestration/bootstrap_coordinator.py - Bootstrap wiring
└── apps/chart_app/server.py - ChartBFF partial bar reference
```

### FactorEngine Usage

```
FactorEngine imported by:
├── runtime/backend_starter.py - Service orchestration
├── services/backtest/runner.py - Backtest factor computation
├── apps/chart_app/server.py - ChartBFF bootstrap wait
├── services/orchestration/bootstrap_coordinator.py - Bootstrap wiring
├── services/signal/engine.py - Signal generation from factors
├── services/strategy/state_engine.py - State transitions from factors
├── services/factor/factor_registry.py - Indicator creation
└── services/bar_builder/bar_query_service.py - BarsBuilder wiring
```

## What Rust ComputeBox Replaces

| Python Component | Rust Equivalent | Status |
|------------------|-----------------|--------|
| BarBuilder (FFI) | BarBuilderEngine | ✅ 13.6 done |
| BarsBuilderService orchestration | ComputeBox wrapper | ❌ Not implemented |
| FactorEngine.compute() | FactorEngineCore | ✅ 13.8 done |
| Redis pub/sub bars | broadcast::channel bar_stream | ✅ 13.7 done |
| Redis pub/sub factors | broadcast::channel factor_stream | ✅ 13.9 done |
| ClickHouse writes | CHWriter | ✅ 13.6 done |
| WebSocket streaming | WSPublisher | ✅ 13.3-13.5 done |
| BootstrapStatus | DataLayer.BootstrapStatus | ✅ 13.2 done |

## What Rust ComputeBox Does NOT Replace (Yet)

| Python Component | Needed for | Rust Status |
|------------------|------------|-------------|
| UnifiedTickManager | Polygon/Databento WS connection | ❌ Rust HTTP client not ready |
| trades_backfill | Bootstrap from historical data | ❌ 13.12 pending |
| BootstrapCoordinator | Multi-service bootstrap orchestration | ❌ Python-only |
| FactorRegistry | Indicator factory | ❌ Python-only (factors.yaml) |
| custom_bar_backfill | Polygon day/week/month bars | ❌ Python-only |
| EventBus | Inter-service messaging | ❌ Python-only |

## Integration Points Needed Before Deletion

### 1. BackendStarter Integration

Current Python code:
```python
# backend_starter.py
if "BarsBuilder" in self.roles:
    self.bars_builder = BarsBuilderService(...)
if "FactorEngine" in self.roles:
    self.factor_engine = FactorEngine(...)
```

Target Rust integration:
```python
# backend_starter.py (future)
if "ComputeBox" in self.roles:
    from jerry_trader._rust import ComputeBox
    self.compute_box = ComputeBox(
        ch_config=config["clickhouse"],
        ws_port=8000,
        factors_config=load_factors_yaml(),
    )
    self.compute_box.start()  # Rust tokio runtime
```

**Blocking**: Python wrapper for Rust ComputeBox not implemented.

### 2. Bootstrap Flow

Current: BootstrapCoordinator calls Python services
```python
coordinator.register_service("bars_builder", self.bars_builder)
coordinator.register_service("factor_engine", self.factor_engine)
coordinator.start_bootstrap(symbol, timeframes)
# → bars_builder.trades_backfill()
# → factor_engine._process_bootstrap_trades()
```

Target: Python coordinator calls Rust via FFI
```python
coordinator.register_rust_service("compute_box", self.compute_box)
coordinator.start_bootstrap(symbol, timeframes)
# → compute_box.bootstrap_from_trades(symbol, from_ms, to_ms)
# → Rust loads trades, builds bars, computes factors, writes CH
```

**Blocking**: Rust bootstrap API not exposed to Python.

### 3. Backtest Integration

Current: Backtest uses Python BarsBuilderService/FactorEngine
```python
# batch_engine.py
bars_builder = BarsBuilderService(session_id=session_id, ...)
factor_engine = FactorEngine(session_id=session_id, ...)
```

Target: Backtest uses Rust ComputeBox
```python
compute_box = ComputeBox(session_id=session_id, ...)
compute_box.load_historical_trades(symbol, from_ms, to_ms)
bars = compute_box.get_bars(symbol, timeframe)
factors = compute_box.get_factors(symbol, name)
```

**Blocking**: Backtest Rust integration not implemented.

### 4. Signal/State Engine Integration

Current: SignalEngine consumes factors from FactorEngine
```python
# signal/engine.py
factor_engine.add_ticker(symbol)
factors = factor_engine.get_current_factors(symbol)
```

Target: SignalEngine consumes from Rust stream
```python
compute_box.subscribe_factors(symbol, callback)
# or
factors = compute_box.get_latest_factors(symbol)
```

**Blocking**: Signal Engine Rust integration not designed.

## Minimum Requirements for 13.11

1. **Python wrapper for ComputeBox** - Expose Rust ComputeBox as Python class
2. **Bootstrap FFI** - `compute_box.bootstrap_from_trades(symbol, from_ms)`
3. **BackendStarter integration** - Replace BarsBuilder/FactorEngine with ComputeBox
4. **Backtest integration** - Or keep Python backtest using legacy path temporarily
5. **Factor config loading** - Rust reads factors.yaml or Python passes config

## Recommendation

**Keep Python services for now. Focus on 13.12 (Rust HTTP client) next.**

The Rust ComputeBox core is complete (13.1-13.9). What's missing:
- Python ↔ Rust integration layer (FFI wrapper)
- Bootstrap coordination from Python → Rust
- Live mode data source (Polygon trades) in Rust

13.12 addresses live mode trades. After that, we can build the FFI wrapper and integration.

## Prerequisites

- [ ] 13.12 Rust HTTP client for Polygon trades
- [ ] Python ComputeBox wrapper (`_rust.pyi` additions)
- [ ] Bootstrap FFI: `compute_box.bootstrap_from_trades()`
- [ ] BackendStarter ComputeBox role integration
- [ ] Backtest ComputeBox integration (or legacy path)

These should be added to ROADMAP as new tasks under Section 13 or Section 2.
