# Roadmap

## 1. Domain Layer

Foundation pure business logic value objects.

- [ ] 1.3 Populate domain/strategy/ with Signal, Risk models

## 2. Rust Core

Performance-critical components rewritten in Rust.

- [-] 2.1 StateEngine rewrite in Rust
- [-] 2.2 FactorEngine rewrite in Rust
- [x] 2.3 Add ClickHouse data source to Rust replayer — use `clickhouse` crate to query trades/quotes from CH as primary source (Parquet fallback), unify backtest data path, config-driven (date + ticker list)

## 3. Services Layer

Stateful workers and use-case implementations.

- [-] 3.2 StateEngine Python wrappers and integration
- [ ] 3.4 Real-time risk management engine with position limits
- [ ] 3.5 Risk management rules and drawdown checks
- [ ] 3.6 Risk engine integration with order execution

- [ ] [3.22](roadmap/live-trf-filtering-apply-delay-threshold-filter-across-real-time-pipeline-components-collector-unified-ticker-manager-bars-builder-factor-engine.md) Live TRF filtering — apply delay threshold filter across real-time pipeline components (collector, unified_ticker_manager, bars_builder, factor_engine)
- [x] [3.23](roadmap/fix-meeting-bar-merge-race-condition.md) Fix meeting bar merge race condition in bars_builder
- [x] [3.24](roadmap/forward-fill-null-bars-from-first-trade-in-barbuilder.md) Forward-fill null bars from first trade in BarBuilder
## 4. ML Pipeline

Machine learning for breakout-compute-analyze context model.

- [ ] 4.1 Expand runtime/ml/ with training/evaluation workflows
- [ ] 4.2 Historical context model for breakout detection
- [ ] 4.3 Integrate ML pipeline with FactorEngine
- [ ] 4.4 Simulate market_snapshot replay using historical trade&quote bulk file

## 5. Frontend

React/TradingView UI modules and UX improvements.

- [ ] 5.6 Better UX improvements

- [ ] 5.19 Signal visualization module - toast/notification bar for real-time signal alerts, click to jump to ticker chart
- [x] 5.21 Factor panel real-time value display - last value shown in panel header with factor color
- [ ] 5.22 News → Chart markers - show news event markers on bar chart at corresponding timestamps
- [ ] 5.23 Replay timeline scrubber - draggable timeline/progress bar in replay mode, jump to any time point via replay clock pause/resume API
- [x] 5.24 RankList virtual scroll - use @tanstack/virtual for RankList rendering, avoid DOM bloat with 100+ tickers

  staticProfileCache, staticNewsCache, versionCache 从不清理，导致 localStorage 超限和性能下降。需要实现 LRU 淘汰策略或定期清理机制。

  line 538 的 seriesInitializedRef.current.clear() 导致高频重初始化，引发 setData 循环触发。需要优化初始化逻辑，避免不必要的 clear() 调用。

  每次 patch 都创建新 Map，高频更新时 GC 压力大。需要优化为增量更新，避免每次都创建新 Map 对象。

  ResizeObserver 和 DOM 事件监听器可能未正确清理，导致内存泄漏。需要在 useEffect cleanup 中确保所有监听器被移除。
- [x] 5.30 Fix overview chart incremental update bug — existing ticker lines stop updating after first render
- [x] 5.31 Notebooks — exploration & visualization for snapshot/backtest data, 01-snapshot-price-vwap as template
## 6. Orchestration

System-wide coordination and backtest infrastructure.

- [ ] 6.4 Tickers pre-location fitting strategy/conditions
- [ ] 6.5 Strategy pre-locate orchestrator with auto-sequenced jumps
- [ ] 6.6 Pre-located tickers pipeline backtest visualization and validation

## 7. AI Agent Layer

[ATC-R Agent System](roadmap/atcr-agent-system.md) — Two-phase design: near-term Agent as automated factor researcher, future ACT-R real-time decision support.

**Phase A: Signal System + Agent Factor Mining (Near-term)**
- [ ] [7.3](roadmap/atcr-agent-system.md) Agent Factor Mining — orchestrator, skills (define_factor, run_backtest, evaluate, optimize), prompts
- [x] [7.4](roadmap/atcr-agent-system.md) Backtest Infrastructure — pre-filter candidates from snapshots, factor history cursor, result store

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

## 9. Backtest Pipeline

- [x] 9.1 Data CLI — download, convert, check, build-snapshot, prepare pipeline
- [x] 9.2 CSV.gz → Parquet converter (Polars streaming, skip-if-exists)
- [x] 9.3 Snapshot builder — trades → CH market_snapshot_collector + market_snapshot (ranked)
- [x] 9.4 HistoricalLoader batch bootstrap — bulk CH read/write, in-memory subscription accumulation, last_df filling, clock jump on completion
- [x] [9.5](roadmap/replayer-ch-migration.md) CHReplayer — 读 CH market_snapshot_collector，推送 INPUT Stream，本地 clock 支持
  - [x] 9.5.1 Clock pause during bootstrap — init 后 pause，bootstrap 完成后 jump_to + resume
  - [x] 9.5.2 CHReplayer 实现 — 读 CH、poll local clock、xadd INPUT Stream
  - [x] 9.5.3 Parquet → CH migration CLI — 历史数据一次性迁入
  - [x] 9.5.4 backend_starter 集成 — 替换旧 parquet replayer，auto-detect start_from

- [x] 9.6 Split adjustment — adjustment_factor = split_from / split_to，应用于 prev_close

- [x] [9.7](roadmap/replayer-ch-migration.md) Clock-based Bootstrap Synchronization

  已实现: bootstrap 完成后 clock.jump_to(bootstrap_end_ts) 跳转到结束时间点
  剩余: bootstrap 期间暂停时钟、其他 clock-dependent 组件等待 bootstrap 完成后再启动

- [x] 9.9 Parquet snapshot data integrity checker

  迁移前检查 Parquet snapshot 数据完整性，筛选有问题的日期：
- 时间范围检查：首尾 timestamp 是否覆盖完整交易时段
- 间隔分析：计算相邻 snapshot 间隔，识别异常（如 >10s gap、interval 统计）
- Schema 验证：列名、类型、必需字段是否存在
- 行数统计：每个 snapshot 的 ticker 数量是否合理（如 <100 或 >10000 异常）
- 输出报告：列出有问题的日期及其异常类型

- [x] 9.10 Parquet → CH Migration Script

  将历史 Parquet 数据迁移到 ClickHouse：
- 扫描 cache/market_mover/*.parquet
- 统一 schema：去掉 rank (如果有)
- 写入 CH market_snapshot_raw 表
- 验证迁移完整性（行数、时间范围）
- 保留 Parquet 作为备份

- [~] 9.11 Snapshot bootstrap Rust 加速

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
## 10. Backtest Data Acquisition & Validation

- [x] 10.1 Data CLI — download, convert, check, build-snapshot, prepare commands
- [x] 10.2 CSV.gz → Parquet converter with skip-if-exists
- [x] 10.3 Data checker — verify parquet files + CH snapshot readiness


## Design Concerns

Architectural decisions needing discussion.

(none currently)

## Open Issues

Known issues and bugs requiring attention.

- StateEngine (state_engine.py) still writes signal_events to InfluxDB — needs migration to ClickHouse
- processor.py Rust import error: `compute_derived_metrics` not exported from `_rust` (pre-existing, needs `maturin develop`)
