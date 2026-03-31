# Archive

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

## 6. Orchestration

System-wide coordination and backtest infrastructure.

- [x] [6.3](roadmap/factor-realtime-analysis.md) Factor bootstrap sync to trades_bootstrap

- [x] [6.1](roadmap/factor-realtime-analysis.md) Decouple trades_backfill from timeframe switch events


## 8. Optional Features
Enhancements and additional modules.
- [x] 8.1 Multi-machine SSH deployment configured in config.yaml
- [x] 8.2 News engine output from log to JSON log
