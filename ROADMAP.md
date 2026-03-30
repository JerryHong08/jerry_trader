# Roadmap

## 1. Domain Layer

Foundation pure business logic value objects.

- [ ] 1.1 Populate domain/market/ with Tick, Bar, Snapshot models
- [ ] 1.2 Populate domain/order/ with Order, Fill, Contract models
- [ ] 1.3 Populate domain/strategy/ with Signal, Risk models
- [ ] 1.4 Populate domain/factor/ with Factor value objects

## 2. Rust Core

Performance-critical components rewritten in Rust.

- [-] 2.1 StateEngine rewrite in Rust
- [ ] 2.2 FactorEngine rewrite in Rust

## 3. Services Layer

Stateful workers and use-case implementations.

- [-] 3.2 StateEngine Python wrappers and integration
- [~] [3.15](roadmap/event-bus-architecture.md) ~~EventBus for service communication (replace callbacks)~~ — superseded by 3.7 Bootstrap Coordinator
- [-] [3.7](roadmap/bootstrap-coordinator.md) Bootstrap Coordinator — unified orchestration for ticker subscribe pipeline
- [ ] 3.4 Real-time risk management engine with position limits
- [ ] 3.5 Risk management rules and drawdown checks
- [ ] 3.6 Risk engine integration with order execution

## 4. ML Pipeline

Machine learning for breakout-compute-analyze context model.

- [ ] 4.1 Expand runtime/ml/ with training/evaluation workflows
- [ ] 4.2 Historical context model for breakout detection
- [ ] 4.3 Integrate ML pipeline with FactorEngine
- [ ] 4.4 Simulate market_snapshot replay using historical trade&quote bulk file

## 5. Frontend

React/TradingView UI modules and UX improvements.

- [ ] 5.4 Unify bar chart and factor chart styles
- [ ] 5.5 Frame group feature
- [ ] 5.6 Better UX improvements

## 6. Orchestration

System-wide coordination and backtest infrastructure.

- [ ] 6.2 Configurable timeframe switch and bootstrap computation
- [ ] 6.4 Tickers pre-location fitting strategy/conditions
- [ ] 6.5 Strategy pre-locate orchestrator with auto-sequenced jumps
- [ ] 6.6 Pre-located tickers pipeline backtest visualization and validation

## 7. AI Agent Layer

Event-driven AI agent system with generative UI capabilities.

**Core Infrastructure:**
- [ ] 7.1 Redis streams to event_bus migration
- [ ] 7.2 Redis-based RPC for cross-service tool calls
- [ ] [7.3](roadmap/chatbox-module.md) Agent BFF - WebSocket + HTTP interface

**Agent Runtime:**
- [ ] 7.4 Tool registry - wrap services as callable tools
- [ ] 7.5 Agent core loop - LLM reasoning + tool dispatch
- [ ] 7.6 Context manager - session state + memory

**Generative Widget System:**
- [ ] [7.7](roadmap/chatbox-module.md) Widget sandbox - iframe + security model
- [ ] [7.8](roadmap/chatbox-module.md) Widget registry - CRUD + lifecycle
- [ ] [7.9](roadmap/chatbox-module.md) Widget generation - template → LLM
- [ ] [7.10](roadmap/chatbox-module.md) Human-agent interaction - context, right-click, focus

**Validation & CLI:**
- [ ] 7.11 txt to tool-calling validation
- [ ] 7.12 Strategy pipeline agent validation
- [ ] 7.13 ML pipeline agent validation
- [ ] 7.14 Basic CLI tools and skills instruction

## 8. Optional Features

Enhancements and additional modules.

- [ ] 8.1 Historical orders analysis module

## Design Concerns

Architectural decisions needing discussion.

- [ ] config_builder.py merge with config.py in platform/config/
- [ ] Domain layer empty placeholders strategy

## Open Issues

Known issues and bugs requiring attention.

_Inbox empty - add new issues as they arise_

- [ ] Maybe we should constrain what timeframes of each bar based factor need. e.g. for ema20 it may only need 10s and 1m bar.
