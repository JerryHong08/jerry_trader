# Roadmap

## 1. Domain Layer

Foundation pure business logic value objects.

- [ ] 1.3 Populate domain/strategy/ with Signal, Risk models

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

- [ ] 3.20 智能默认 - Factor-Timeframe 映射配置
## 4. ML Pipeline

Machine learning for breakout-compute-analyze context model.

- [ ] 4.1 Expand runtime/ml/ with training/evaluation workflows
- [ ] 4.2 Historical context model for breakout detection
- [ ] 4.3 Integrate ML pipeline with FactorEngine
- [ ] 4.4 Simulate market_snapshot replay using historical trade&quote bulk file

## 5. Frontend

React/TradingView UI modules and UX improvements.

- [ ] 5.6 Better UX improvements

- [-] 5.15 Fix tick factor real-time update - compute and publish on tick arrival
- [ ] [5.16](roadmap/factor-subscription-scenarios.md) Fix factor subscription lifecycle - timeframe switch, unsubscribe/re-subscribe, multi-chart scenarios
- [ ] 5.17 Cheat method: cleanup ticker on unsubscribe - clear runtime state only, preserve ClickHouse history
## 6. Orchestration

System-wide coordination and backtest infrastructure.

- [ ] 6.4 Tickers pre-location fitting strategy/conditions
- [ ] 6.5 Strategy pre-locate orchestrator with auto-sequenced jumps
- [ ] 6.6 Pre-located tickers pipeline backtest visualization and validation

## 7. AI Agent Layer

ATC-R Agent System - ACT-R inspired architecture with production rules (Strategy DSL) as first-class citizens.

**Phase 1: Rule Engine (Activation Layer)**
- [ ] [7.1](roadmap/atcr-agent-system.md) Strategy DSL - rule definition, real-time matching, backtest optimization
- [ ] [7.2](roadmap/atcr-agent-system.md) News Pre-filter - LLM threshold filter for news activation
- [ ] [7.3](roadmap/atcr-agent-system.md) Rule Engine - factor/news trigger matching with conflict resolution

**Phase 2: Agent Think (Reasoning Layer)**
- [ ] [7.4](roadmap/atcr-agent-system.md) Agent BFF - WebSocket + HTTP interface for agent communication
- [ ] [7.5](roadmap/atcr-agent-system.md) Context Aggregator - gather factors, news, trades when triggered
- [ ] [7.6](roadmap/atcr-agent-system.md) Agent Reasoner - LLM with per-rule prompt templates
- [ ] [7.7](roadmap/atcr-agent-system.md) Tool Registry - wrap services as callable tools

**Phase 3: Backtest Integration**
- [ ] [7.8](roadmap/atcr-agent-system.md) Strategy Backtest - validate rules on historical data
- [ ] [7.9](roadmap/atcr-agent-system.md) Threshold Optimization - grid search for optimal trigger values
- [ ] [7.10](roadmap/atcr-agent-system.md) Agent Decision Simulation - simulate agent decisions in backtest mode

**Phase 4: Output & UX**
- [ ] [7.11](roadmap/atcr-agent-system.md) Notification System - Telegram/Discord/Webhook
- [ ] [7.12](roadmap/atcr-agent-system.md) Dynamic Widget - template-based widget generation
- [ ] [7.13](roadmap/atcr-agent-system.md) Chat Interface - simple natural language interaction

**Deprioritized:**

**Infrastructure:**
- [ ] [7.18](roadmap/factor-stream-rust-migration.md) Factor stream Rust migration - migrate from Python asyncio to Rust FactorBroadcaster when Signal Engine is introduced
## 8. Optional Features

Enhancements and additional modules.

- [ ] 8.1 Historical orders analysis module

## Design Concerns

Architectural decisions needing discussion.

- [ ] config_builder.py merge with config.py in platform/config/
- [ ] Domain layer empty placeholders strategy

## Open Issues

Known issues and bugs requiring attention.

- [ ] Maybe we should constrain what timeframes of each bar based factor need. e.g. for ema20 it may only need 10s and 1m bar.
