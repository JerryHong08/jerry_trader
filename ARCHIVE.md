# Archive

## 1. Domain Layer

Foundation pure business logic value objects.

- [x] 1.1 Populate domain/market/ with Bar, BarPeriod models (Tick, Snapshot pending)
- [x] 1.2 Populate domain/order/ with Order, OrderState, Fill models
- [x] 1.4 Populate domain/factor/ with FactorSnapshot value object

## 2. Rust Core
Performance-critical components rewritten in Rust.

- [x] 2.1 BarBuilder with watermark-based close and late-arrival window
- [x] 2.2 ReplayClock with Redis heartbeat for cross-machine sync
- [x] 2.3 VolumeTracker for snapshot processing
- [x] 2.4 Factor helpers (z_score, price_accel)
- [x] 2.6 TickDataReplayer with Parquet lake reader


## 3. Services Layer
Stateful workers and use-case implementations.

- [x] 3.9 Preload parquet load method optimization

- [x] 3.1 FactorEngine Python implementation (hybrid bar + tick based)
- [x] [3.3](roadmap/factor-behavior-design.md) Multi-timeframe FactorEngine (EMA per timeframe: 10s, 1m, 5m, etc.)
- [x] [3.7](roadmap/factor-timeframe-schema.md) Add timeframe column to ClickHouse factors schema
- [x] [3.8](roadmap/factor-ema-computation.md) Fix EMA20 computation for multi-timeframe (10s, 1m, 5m)
- [x] 3.9 Fix FactorEngine subscribing to wrong timeframes (tick subscription defaults)
- [x] 3.10 Fix factor bootstrap race condition (wait_for_bootstrap vs pre_register)
- [x] 3.11 Fix ClickHouse client config for ChartBFF (custom_bar_backfill)

- [x] 3.12 Fix concurrent ClickHouse queries in bar_query_service (per-thread clients)
- [x] 3.13 Fix tick-based factor bootstrap sent before ready (tick_bootstrap_events)

- [x] [3.14](roadmap/bar-backfill-observer.md) Add bar backfill observer for FactorEngine bootstrap

- [~] [3.15](roadmap/event-bus-architecture.md) ~~EventBus for service communication (replace callbacks)~~ — superseded by 3.7 Bootstrap Coordinator
- [x] [3.7](roadmap/bootstrap-coordinator-v2.md) Bootstrap Coordinator V2 — per-timeframe orchestration with trades sharing

- [x] 3.16 Bootstrap Coordinator V2 集成测试：10s/1m bar + 10s factor 完整流程

- [x] 3.17 Factor Registry - 预配置 Factor 系统（启动时加载）
- [x] 3.18 FactorEngine 重构 - 使用 Registry 创建 indicators
- [~] 3.19 前端增量更新 - Dropped（前端缓存复杂度高，性价比低）

## 5. Frontend
React/TradingView UI modules and UX improvements.

- [x] 5.3 Keyboard shortcuts

- [x] [5.1](roadmap/factor-realtime-analysis.md) Fix factor real-time update moduleId mismatch bug
- [x] [5.2](roadmap/factor-realtime-analysis.md) Factor chart sub-follower mode (sync timeframe with main chart)
- [x] 5.7 Fix FactorChart position reset on real-time updates
- [x] 5.8 Fix chart timespan switch: newest bar covers last bar's open/high/low
- [~] 5.3 Abstract all search boxes into unified component

- [x] 5.9 HelpPanel 组件风格统一 - Shortcuts/Layouts/Settings 三栏风格统一
- [x] 5.10 Layout Templates Tab 样式优化 - 分割线、选中状态绿色文字、白色指示器
- [x] 5.11 Settings Tab 重构 - Export/Import Layout 水平并列，Storage 区块上移
- [x] 5.12 Demo 模式首次访问自动弹出 Layout Panel
- [x] 5.13 页面刷新后默认全局视图 (focus-to-fit)

- [x] 5.4 Unify bar chart and factor chart styles
- [~] 5.5 Frame group feature (dropped - low ROI)
- [x] [5.14](roadmap/panel-chart-system.md) TradingView-style panel chart system
  - [x] 5.14.1 Real-time price chart updates from trade ticks
  - [x] 5.14.2 TradeRate canvas stability (single fitContent)
  - [x] 5.14.3 Overlay factor rendering timing fix

## 6. Orchestration

System-wide coordination and backtest infrastructure.

- [x] [6.3](roadmap/factor-realtime-analysis.md) Factor bootstrap sync to trades_bootstrap

- [x] [6.1](roadmap/factor-realtime-analysis.md) Decouple trades_backfill from timeframe switch events

- [x] 6.2 Configurable timeframe switch and bootstrap computation
  - [x] 6.2.1 简化 Bootstrap 架构：删除 Redis Stream，使用 Coordinator 直接管理服务生命周期
  - [x] 6.2.1.1 Phase 2: 修改 BootstrapCoordinator - 添加服务注册接口
  - [x] 6.2.1.2 Phase 3: 修改 BarsBuilderService - 实现 BootstrapableService 接口
  - [x] 6.2.1.3 Phase 4: 修改 FactorEngine - 实现 BootstrapableService 接口
  - [x] 6.2.1.4 Phase 5: 修改 ChartBFF - 删除 XADD 逻辑
  - [x] 6.2.1.5 Phase 6: 清理 - 删除 Redis Stream 相关代码
- [x] 6.7 Fix timeframe switching blocking - coordinator should add new timeframes to existing bootstrap instead of returning early
  - [x] 6.7.1 Fix FactorEngine and BarsBuilderService to bootstrap new timeframes on existing tickers
- [x] 6.8 Cleanup flow - coordinator.cleanup() notifies all services to stop tracking ticker
- [x] [6.9](roadmap/re-subscribe-gap-fill.md) Fix re-subscribe gap fill - use per_tf_starts for dedup
- [x] 6.10 Fix clock synchronization - ReplayClock and TickDataReplayer shared time
- [x] 6.11 Fix factor query - add session filter and remove FINAL

## 7. AI Agent Layer

ATC-R Agent System - ACT-R inspired architecture with production rules (Strategy DSL) as first-class citizens.

**Phase 1: Rule Engine (Activation Layer)**
- [~] 7.14 Redis streams migration (not needed - rules listen directly to factor stream)
- [~] 7.15 Redis RPC (over-engineered - direct function calls sufficient)
- [~] 7.16 Widget sandbox (Phase 4, simplified)
- [~] 7.17 Right-click interaction (over-engineered)

## 8. Optional Features
Enhancements and additional modules.
- [x] 8.1 Multi-machine SSH deployment configured in config.yaml
- [x] 8.2 News engine output from log to JSON log
