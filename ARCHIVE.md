# Archive

## 1. Domain Layer

Foundation pure business logic value objects.

- [x] 1.1 Populate domain/market/ with Bar, BarPeriod models (Tick, Snapshot pending)
- [x] 1.2 Populate domain/order/ with Order, OrderState, Fill models
- [x] 1.4 Populate domain/factor/ with FactorSnapshot value object


## 10. Backtest Data Acquisition & Validation

- [x] 10.1 Polygon downloader — data/downloader.py: S3 download with resume (adapted from quant101)
- [x] 10.2 CSV.gz → Parquet converter — data/converter.py: Polars streaming conversion
- [x] 10.3 Data checker — data/checker.py: validate trades/quotes/snapshot completeness
- [x] 10.4 Snapshot builder — data/snapshot_builder.py: two-stage streaming pipeline (sink_parquet + chunked rank) for 150M+ trades
- [x] 10.5 Data CLI — data/cli.py: unified CLI (download, convert, check, build-snapshot, prepare)

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

- [~] 3.20 智能默认 - Factor-Timeframe 映射配置
- [x] 3.21 Move config_builder.py from platform/config/ to runtime/ (only consumer is backend_starter)

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

- [x] 5.15 Fix tick factor real-time update - compute and publish on tick arrival
- [x] [5.16](roadmap/factor-subscription-scenarios.md) Fix factor subscription lifecycle - timeframe switch, unsubscribe/re-subscribe, multi-chart scenarios
- [x] 5.17 Cheat method: cleanup ticker on unsubscribe - clear runtime state only, preserve ClickHouse history
- [x] 5.18 Batch factor updates in factorDataStore -- updateFactors calls set() N times for N factors, causing N React re-renders per tick. Refactor to single set() call with all factors merged at once.
- [x] 5.20 RankList double-click for group sync - double-click adds symbol to global subscription then triggers group sync
- [x] 5.25 Remove legacy FactorChartModule - removed component, registry entry, type, template reference, and stale comments
- [x] 5.26 Fix localStorage unbounded growth - size limits, pruning, throttling implemented in useWebSocket.ts
- [x] 5.27 Fix TradingView setData loop - seriesInitializedRef tracks init state in OverviewChartModule.tsx
- [x] 5.28 Fix Zustand Map recreation - only create new Map on actual changes in marketDataStore.ts
- [x] 5.29 Fix event listener leaks - cleanup in useEffect return in OverviewChartModule.tsx

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

- [x] [7.1](roadmap/atcr-agent-system.md) Signal Engine — subscribe to factor streams, evaluate DSL rules, trigger on top-20 entry
- [x] [7.2](roadmap/atcr-agent-system.md) Strategy DSL — YAML rule schema, factor conditions, parser/validator
- [x] 7.10 Signal event persistence — store trigger events to ClickHouse (rule_id, symbol, trigger_time, factors). Returns computed offline via SQL JOIN with ohlcv bars. Includes SQL schema + Python storage writer.

## 8. Optional Features
Enhancements and additional modules.
- [x] 8.1 Multi-machine SSH deployment configured in config.yaml
- [x] 8.2 News engine output from log to JSON log

## 9. Backtest Pipeline

- [x] [9.1](roadmap/backtest-pipeline-design.md) Backtest data models — domain/backtest/types.py + services/backtest/config.py
- [x] [9.2](roadmap/backtest-prefilter-findings.md) Candidate pre-filter — true entry detection from market_snapshot
- [x] 9.3 Data loader — data_loader.py: Rust loaders for trades/quotes Parquet
- [x] [9.4](roadmap/backtest-pipeline-design.md) Batch engine — batch_engine.py: FactorEngineBatchAdapter, reuses live FactorRegistry indicators (replaces factor_batch.py)
- [x] 9.5 Signal evaluator — evaluator.py: evaluate DSL rules against factor timeseries, reuse evaluate_condition/trigger
- [x] 9.6 Return & metrics — metrics.py: slippage (ask+buffer), returns at multi-horizon, MFE/MAE, time-to-peak
- [x] 9.7 Output — output.py: console summary table + ClickHouse backtest_results table persistence
- [x] 9.8 Runner + CLI — runner.py orchestrator + cli.py entry point (--date, --date-range, --rules, --gain-threshold)
- [x] 9.9 snapshot_builder_v2: 分层输出 ranked_data (Pre-filter) + collector_format (Replay)
- [x] 9.10 split 预处理
- [x] 9.11 HistoricalLoader — CH bootstrap for replay mode
- [x] 9.14 Parquet Snapshot Validator
