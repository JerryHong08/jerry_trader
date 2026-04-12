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

- [x] 10.1.0 Data CLI — download, convert, check, build-snapshot, prepare commands
- [x] 10.2.0 CSV.gz → Parquet converter with skip-if-exists
- [x] 10.3.0 Data checker — verify parquet files + CH snapshot readiness


## 11. Backtest Mining & Experiment Framework

[Agent Mining Phase](roadmap/agent-mining-phase.md) — Factor implementation, threshold optimization, multi-condition rules.
[Experiment Framework](roadmap/experiment-framework.md) — Mathematical validation, reproducibility, knowledge transfer.

**Infrastructure:**

- [x] [11.1](roadmap/experiment-framework.md) Design structured experiment log schema — hypothesis, design, results, statistical validation, reproducibility checklist
- [x] [11.21](roadmap/experiment-framework.md) Experiment CLI query tool — Python query functions built into experiment_logger.py: find_experiments_by_hypothesis(), get_all_lessons(), get_ticker_insights()
- [x] [11.22](roadmap/experiment-framework.md) Document experiment log usage in SKILL.md — how to read logs, query knowledge_base, learn from past experiments, use validation gates. Agent guide.

## 2. Rust Core
Performance-critical components rewritten in Rust.

- [x] 2.1 BarBuilder with watermark-based close and late-arrival window
- [x] 2.2 ReplayClock with Redis heartbeat for cross-machine sync
- [x] 2.3 VolumeTracker for snapshot processing
- [x] 2.4 Factor helpers (z_score, price_accel)
- [x] 2.6 TickDataReplayer with Parquet lake reader

- [x] 2.3.0 Add ClickHouse data source to Rust replayer — use `clickhouse` crate to query trades/quotes from CH as primary source (Parquet fallback), unify backtest data path, config-driven (date + ticker list)


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

- [x] [3.23](roadmap/fix-meeting-bar-merge-race-condition.md) Fix meeting bar merge race condition in bars_builder
- [x] [3.24](roadmap/forward-fill-null-bars-from-first-trade-in-barbuilder.md) Forward-fill null bars from first trade in BarBuilder

- [x] [3.22](roadmap/live-trf-filtering-apply-delay-threshold-filter-across-real-time-pipeline-components-collector-unified-ticker-manager-bars-builder-factor-engine.md) Live TRF filtering — apply delay threshold filter across real-time pipeline components (collector, unified_ticker_manager, bars_builder, factor_engine)


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

- [x] 5.21 Factor panel real-time value display - last value shown in panel header with factor color
- [x] 5.24 RankList virtual scroll - use @tanstack/virtual for RankList rendering, avoid DOM bloat with 100+ tickers
- [x] 5.30 Fix overview chart incremental update bug — existing ticker lines stop updating after first render
- [x] 5.31 Notebooks — exploration & visualization for snapshot/backtest data, 01-snapshot-price-vwap as template


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

- [x] 6.13 Add mining CLI entry point in backtest/cli.py
- [x] [6.16](roadmap/mining-framework-clear-rules-dir-before-each-test.md) Mining framework: clear rules dir before each test

- [x] 6.20 Fix SignalEvaluator to accept Rule list directly

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

- [x] [7.4](roadmap/atcr-agent-system.md) Backtest Infrastructure — pre-filter candidates from snapshots, factor history cursor, result store


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
- [x] 9.1.0 Data CLI — download, convert, check, build-snapshot, prepare pipeline
- [x] 9.2.0 CSV.gz → Parquet converter (Polars streaming, skip-if-exists)
- [x] 9.3.0 Snapshot builder — trades → CH market_snapshot_collector + market_snapshot (ranked)
- [x] 9.4.0 HistoricalLoader batch bootstrap — bulk CH read/write, in-memory subscription accumulation, last_df filling, clock jump on completion
- [x] [9.5](roadmap/replayer-ch-migration.md) CHReplayer — 读 CH market_snapshot_collector，推送 INPUT Stream，本地 clock 支持
  - [x] 9.5.1 Clock pause during bootstrap — init 后 pause，bootstrap 完成后 jump_to + resume
  - [x] 9.5.2 CHReplayer 实现 — 读 CH、poll local clock、xadd INPUT Stream
  - [x] 9.5.3 Parquet → CH migration CLI — 历史数据一次性迁入
  - [x] 9.5.4 backend_starter 集成 — 替换旧 parquet replayer，auto-detect start_from
- [x] 9.6.0 Split adjustment — adjustment_factor = split_from / split_to，应用于 prev_close
- [x] [9.7](roadmap/replayer-ch-migration.md) Clock-based Bootstrap Synchronization
- [x] 9.9.0 Parquet snapshot data integrity checker
- [x] 9.10.0 Parquet → CH Migration Script
- [~] 9.11.0 Snapshot bootstrap Rust 加速
- [x] 9.12 Fix volume semantics in replay mode — snapshot_builder outputs per-window incremental volume but VolumeTracker expects cumulative
- [x] 9.13 Implement relativeVolumeDaily calculation — currently never computed, just passed through as 0/1
- [x] 9.15 snapshot_builder: compute prev_volume from day_aggs instead of hardcoding 0.0
- [x] 9.16 Fix VWAP — use cumulative turnover/volume instead of per-window VWAP
- [x] 9.17 Round volume to integer in snapshot_builder to avoid float precision display
- [x] 9.18 SignalEvaluator cooldown/dedup — skip same-rule triggers within configurable window (default 60s)
- [x] 9.19 Backtest grouped statistics — per-rule and per-ticker breakdown in console output
- [x] 9.20 Backtest results visualization notebook — 02-backtest-results.ipynb with return distribution, MFE/MAE scatter, timeline
- [x] 9.21 Backtest pre-market time filter — restrict pipeline to 4:00-9:30 AM ET, ignore out-of-hours trades/signals
- [x] [9.24](roadmap/filter-trf-exch-4-trades-remove-stale-finra-delayed-reports-from-dataloader-design-doc.md) Filter TRF (exch=4) trades — remove stale FINRA delayed reports from DataLoader, design doc
- [x] 9.25 Fix backtest pre-filter: remove silent fallback to market_snapshot_collector, require market_snapshot data
- [x] 9.26 Build-snapshot unified pipeline: produce both market_snapshot_collector (raw) and market_snapshot (processed, subscribed tickers only) in one step, auto-process-from-collector
- [x] 9.27 Align market_snapshot_collector schema with live collector: remove rank/change columns, add CREATE DATABASE, use jerry_trader namespace
- [x] 9.28 Delete duplicate schemas copy.py, rename enrich→process terminology (CLI + code)
- [x] 9.22 DataLoader bulk query — single CH query for all tickers instead of 82 roundtrips (10s→<1s)
- [x] 9.23 Backtest date range batch run — support --date-range START END, reuse CH connection and rule loading across dates
- [x] 9.29 build-snapshot --force flag — unified rerun switch to overwrite collector + snapshot data
- [x] 9.30 process-snapshot vectorized rank — replace 3960-window Python loop with Polars over() rank, 16s→<1s
- [x] 9.31 Pipeline progress bars — add tqdm to process-snapshot window loop, DataLoader ticker loading, backtest runner factor computation
- [x] [9.32](roadmap/merge-build-process-snapshot.md) Merge build-snapshot + process-snapshot — remove process-snapshot subcommand, build-snapshot --force handles both tables
- [x] [9.33](roadmap/check-verbose.md) check --verbose — show ticker-level gaps, time range issues, not just READY/MISSING
- [x] [9.34](roadmap/date-range-parallel.md) Date range parallel execution — run multiple dates in parallel with ProcessPoolExecutor
- [x] [9.35](roadmap/richer-signal-output.md) Richer signal output — show rule details, factor values at trigger time, not just aggregate stats
- [x] [9.36](roadmap/dry-run-candidates-only.md) dry-run mode: preview pre-filter candidates without running factor/signal pipeline
- [x] [9.37](roadmap/prefilter-subscription-logic.md) PreFilter use subscription logic — match live processor behavior (subscription rank vs snapshot rank)
