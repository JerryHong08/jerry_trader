# Roadmap

## 1. Domain Layer

Foundation pure business logic value objects.

- [x] 1.1 Populate domain/market/ with Bar, BarPeriod models (Tick, Snapshot pending)
- [x] 1.2 Populate domain/order/ with Order, OrderState, Fill models
- [ ] 1.3 Populate domain/strategy/ with Signal, Risk models
- [x] 1.4 Populate domain/factor/ with FactorSnapshot value object

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

- [x] 3.17 Factor Registry - 预配置 Factor 系统（启动时加载）
- [x] 3.18 FactorEngine 重构 - 使用 Registry 创建 indicators
- [ ] 3.19 增量更新 - Timeframe 切换时历史数据 merge
- [ ] 3.20 智能默认 - Factor-Timeframe 映射配置
## 4. ML Pipeline

Machine learning for breakout-compute-analyze context model.

- [ ] 4.1 Expand runtime/ml/ with training/evaluation workflows
- [ ] 4.2 Historical context model for breakout detection
- [ ] 4.3 Integrate ML pipeline with FactorEngine
- [ ] 4.4 Simulate market_snapshot replay using historical trade&quote bulk file

## 5. Frontend

React/TradingView UI modules and UX improvements.

- [x] 5.4 Unify bar chart and factor chart styles
- [ ] 5.5 Frame group feature
- [ ] 5.6 Better UX improvements

- [x] [5.14](roadmap/panel-chart-system.md) TradingView-style panel chart system
  - [x] 5.14.1 Real-time price chart updates from trade ticks
  - [x] 5.14.2 TradeRate canvas stability (single fitContent)
  - [x] 5.14.3 Overlay factor rendering timing fix
## 6. Orchestration

System-wide coordination and backtest infrastructure.

- [x] 6.2 Configurable timeframe switch and bootstrap computation
  - [x] 6.2.1 简化 Bootstrap 架构：删除 Redis Stream，使用 Coordinator 直接管理服务生命周期
  - [x] 6.2.1.1 Phase 2: 修改 BootstrapCoordinator - 添加服务注册接口
  - [x] 6.2.1.2 Phase 3: 修改 BarsBuilderService - 实现 BootstrapableService 接口
  - [x] 6.2.1.3 Phase 4: 修改 FactorEngine - 实现 BootstrapableService 接口
  - [x] 6.2.1.4 Phase 5: 修改 ChartBFF - 删除 XADD 逻辑
  - [x] 6.2.1.5 Phase 6: 清理 - 删除 Redis Stream 相关代码
- [ ] 6.4 Tickers pre-location fitting strategy/conditions
- [ ] 6.5 Strategy pre-locate orchestrator with auto-sequenced jumps
- [ ] 6.6 Pre-located tickers pipeline backtest visualization and validation

- [x] 6.7 Fix timeframe switching blocking - coordinator should add new timeframes to existing bootstrap instead of returning early
  - [x] 6.7.1 Fix FactorEngine and BarsBuilderService to bootstrap new timeframes on existing tickers
- [x] 6.8 Cleanup flow - coordinator.cleanup() notifies all services to stop tracking ticker
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

- [ ] Maybe we should constrain what timeframes of each bar based factor need. e.g. for ema20 it may only need 10s and 1m bar.
