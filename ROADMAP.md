# Roadmap

## 1. Domain Layer

Foundation pure business logic value objects.

- [ ] 1.3 Populate domain/strategy/ with Signal, Risk models
- [ ] 1.4 Decide strategy for domain layer empty placeholders

## 2. Rust Core

Performance-critical components rewritten in Rust.

- [-] 2.1 StateEngine rewrite in Rust
- [-] 2.2 FactorEngine rewrite in Rust

## 3. Services Layer

Stateful workers and use-case implementations.

- [-] 3.2 StateEngine Python wrappers and integration
- [ ] 3.4 Real-time risk management engine with position limits
- [ ] 3.5 Risk management rules and drawdown checks
- [ ] 3.6 Risk engine integration with order execution

- [~] 3.20 智能默认 - Factor-Timeframe 映射配置
- [x] 3.21 Move config_builder.py from platform/config/ to runtime/ (only consumer is backend_starter)
## 4. ML Pipeline

Machine learning for breakout-compute-analyze context model.

- [ ] 4.1 Expand runtime/ml/ with training/evaluation workflows
- [ ] 4.2 Historical context model for breakout detection
- [ ] 4.3 Integrate ML pipeline with FactorEngine
- [ ] 4.4 Simulate market_snapshot replay using historical trade&quote bulk file

## 5. Frontend

React/TradingView UI modules and UX improvements.

- [ ] 5.6 Better UX improvements

- [x] 5.15 Fix tick factor real-time update - compute and publish on tick arrival
- [x] [5.16](roadmap/factor-subscription-scenarios.md) Fix factor subscription lifecycle - timeframe switch, unsubscribe/re-subscribe, multi-chart scenarios
- [x] 5.17 Cheat method: cleanup ticker on unsubscribe - clear runtime state only, preserve ClickHouse history
- [x] 5.18 Batch factor updates in factorDataStore -- updateFactors calls set() N times for N factors, causing N React re-renders per tick. Refactor to single set() call with all factors merged at once.
## 6. Orchestration

System-wide coordination and backtest infrastructure.

- [ ] 6.4 Tickers pre-location fitting strategy/conditions
- [ ] 6.5 Strategy pre-locate orchestrator with auto-sequenced jumps
- [ ] 6.6 Pre-located tickers pipeline backtest visualization and validation

## 7. AI Agent Layer

[ATC-R Agent System](roadmap/atcr-agent-system.md) — Two-phase design: near-term Agent as automated factor researcher, future ACT-R real-time decision support.

**Phase A: Signal System + Agent Factor Mining (Near-term)**
- [ ] [7.1](roadmap/atcr-agent-system.md) Signal Engine — subscribe to factor streams, evaluate DSL rules, trigger on top-20 entry
- [ ] [7.2](roadmap/atcr-agent-system.md) Strategy DSL — YAML rule schema, factor conditions, parser/validator
- [ ] [7.3](roadmap/atcr-agent-system.md) Agent Factor Mining — orchestrator, skills (define_factor, run_backtest, evaluate, optimize), prompts
- [ ] [7.4](roadmap/atcr-agent-system.md) Backtest Infrastructure — pre-filter candidates from snapshots, factor history cursor, result store

**Phase B: ACT-R Real-Time Agent (Future)**
- [ ] [7.5](roadmap/atcr-agent-system.md) News Pre-filter + Conflict Resolution
- [ ] [7.6](roadmap/atcr-agent-system.md) Agent Think — BFF, Context Aggregator, LLM Reasoner, Tool Registry
- [ ] [7.7](roadmap/atcr-agent-system.md) Agent Decision Simulation + Prompt Validation
- [ ] [7.8](roadmap/atcr-agent-system.md) Output & UX — Notification, Dynamic Widget, Chat Interface

**Infrastructure:**
- [ ] [7.9](roadmap/factor-stream-rust-migration.md) Factor stream Rust migration - migrate from Python asyncio to Rust FactorBroadcaster when Signal Engine is introduced
## 8. Optional Features

Enhancements and additional modules.

- [ ] 8.1 Historical orders analysis module

## Design Concerns

Architectural decisions needing discussion.

(none currently)

## Open Issues

Known issues and bugs requiring attention.

(none currently)
