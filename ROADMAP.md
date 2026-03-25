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

- [ ] [6.1](roadmap/factor-realtime-analysis.md) Decouple trades_backfill from timeframe switch events
- [ ] 6.2 Configurable timeframe switch and bootstrap computation
- [ ] [6.3](roadmap/factor-realtime-analysis.md) Factor bootstrap sync to trades_bootstrap
- [ ] 6.4 Tickers pre-location fitting strategy/conditions
- [ ] 6.5 Strategy pre-locate orchestrator with auto-sequenced jumps
- [ ] 6.6 Pre-located tickers pipeline backtest visualization and validation

## 7. AI Agent Layer

Event-driven AI agent system (Stage 4).

- [ ] 7.1 Redis streams to event_bus migration
- [ ] 7.2 Redis-based RPC for cross-service tool calls
- [ ] 7.3 AgentBFF HTTP + WebSocket interface
- [ ] 7.4 Agent core loop with tool dispatch
- [ ] 7.5 Tool registry wrapping existing services
- [ ] 7.6 Short and long-term memory system
- [ ] 7.7 txt to tool-calling validation
- [ ] 7.8 Strategy pipeline agent validation
- [ ] 7.9 ML pipeline agent validation
- [ ] 7.10 Basic CLI tools and skills instruction

## 8. Optional Features

Enhancements and additional modules.

- [ ] 8.1 Historical orders analysis module
- [ ] [8.3](roadmap/chatbox-module.md) Chatbox module

## Design Concerns

Architectural decisions needing discussion.

- [ ] config_builder.py merge with config.py in platform/config/
- [ ] Event bus architecture for Stage 4
- [ ] Domain layer empty placeholders strategy

## Open Issues

Known issues and bugs requiring attention.

_Inbox empty - add new issues as they arise_
