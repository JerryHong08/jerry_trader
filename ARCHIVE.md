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

- [x] [11.1.0](roadmap/experiment-framework.md) Design structured experiment log schema — hypothesis, design, results, statistical validation, reproducibility checklist
- [x] [11.21.0](roadmap/experiment-framework.md) Experiment CLI query tool — Python query functions built into experiment_logger.py: find_experiments_by_hypothesis(), get_all_lessons(), get_ticker_insights()
- [x] [11.22.0](roadmap/experiment-framework.md) Document experiment log usage in SKILL.md — how to read logs, query knowledge_base, learn from past experiments, use validation gates. Agent guide.

- [x] [11.9.0](roadmap/agent-mining-phase.md) Implement relative_volume factor — core pre-market signal, volume vs 20-period avg, register in factors.yaml
- [x] [11.10.0](roadmap/agent-mining-phase.md) Implement price_direction factor — distinguish buy vs sell pressure, avoid passive sell traps
- [x] [11.11.0](roadmap/agent-mining-phase.md) Implement gap_percent factor — entry position filter, avoid chasing overextended stocks >15% gap
- [x] [11.14.0](roadmap/biaf-vs-kidz-analysis.md) Analyze BIAF vs KIDZ signal quality — why BIAF succeeded (62% win) while KIDZ failed (0% win)
- [x] [11.15.0](roadmap/agent-mining-phase.md) Threshold sweep mining — trade_rate [100,150,200,250] × relative_volume [2,3,5]
- [x] [11.16.0](roadmap/agent-mining-phase.md) Multi-condition rule composition test — trade_rate + price_direction + relative_volume + gap_percent combinations
- [x] [11.17.0](roadmap/agent-mining-phase.md) Multi-date validation workflow — establish standard: 10+ days validation before deployment
- [x] [11.24.0](roadmap/mining-batch-optimization.md) Mining batch optimization — shared preprocessing (PreFilter + DataLoader + FactorEngine once), 90+ min → ~50s
- [x] [11.20.0](roadmap/ticker-stratification.md) Ticker-specific adaptive thresholds — different parameters for high-float vs low-float stocks
- [x] [11.23.0](roadmap/config-driven-mining.md) Config-driven mining search space - move thresholds and factor combinations to YAML
- [x] [11.25.0](roadmap/backtest-paradigm-analysis.md) Validate dynamic exit timing — analyze MFE/MAE data to test if factor-based exit beats fixed time exit
  - [x] 11.20.1 Validate stratification effectiveness — batch classify all candidates, compare win rate between momentum_candidate vs avoid, multi-date >10
- [x] 11.26.0 Correct KIDZ analysis — actual win rate 62.5% not 0%, update all documentation
- [x] 11.28.0 Mining → backtest_results persistence
- [x] 11.32.0 Event Framework Validation — 验证 Event Selection vs Factor Ranking，发现 Factor Ranking 无效
- [x] [11.33.0](roadmap/event-framework-implementation.md) Event Definition Module — 实现 Boolean Event Selection 框架
  - [x] 11.33.1 Event 数据结构 — name, conditions (Boolean filter), semantic
  - [x] 11.33.2 EventEvaluator — match_signal_to_event(), reject/accept
  - [x] 11.33.3 events.yaml 配置 — reversal_entry, dip_buy, anti_patterns 定义
- [x] [11.34.0](roadmap/mining-refactor.md) Mining 流程重构 — 改用 Avg Return + Win Rate 验证
  - [x] 11.34.1 验证指标调整 — 删除 IC 验证，改用 Avg Return > 2%, Win Rate > 45%
  - [x] 11.34.2 Multi-date 稳定性 — 各日期 Avg Return > 0，方差 < 50% of mean
  - [x] 11.34.3 Boolean Mining — Event-based selection，不做 factor ranking
- [x] [11.35.0](roadmap/mining-program.md) Autoresearch-style Research Loop — 基于karpathy/autoresearch方法论
  - [x] 11.35.1 mining-program.md — 研究指南文档（假设驱动、评估规则）
  - [x] 11.35.2 results.tsv — 实验结果追踪机制
  - [x] 11.35.3 mining-lessons.md — 失败/成功经验记录
  - [x] 11.35.4 CLI --record 支持 — 自动记录实验结果
- [x] 11.36.0 扩展样本量验证 — 10天数据验证完成，发现 2026-03-09 异常值导致结果偏差
- [~] 11.39.0 Regime Detection 研究 — 识别导致 2026-03-09 异常收益的市场条件
- [x] [11.41.0](roadmap/backtest-realtime-alignment.md) 回测-实盘对齐：修复 Snapshot 时序选股问题 — 当前回测使用未来数据（ticker 全 session 计算），需改为模拟实盘的时序选股（从首次进入 top20 时间开始计算）
- [x] [11.42.0](roadmap/event-trigger-architecture.md) 重构 Event 架构：从条件筛选改为事件触发模式 — TriggerType enum (FIRST_ENTRY/CONTINUOUS), evaluate_ticker 支持触发式评估，first_entry_map 集成到 SharedBacktestData
- [x] 11.43.0 统一 Backtest Pipeline — CLI 和 App 共用 BacktestPipeline，Pipeline 负责 ClickHouse 写入，App/CLI 只做显示层

- [~] 11.44.0 Backtest App 支持跨多日期运行 — BacktestRequest.date 改为 dates: list[str]，Runner 循环调用 pipeline.run()，前端支持日期范围选择

- [x] 11.12.0 Quote data integration for backtest — extend DataLoader to load quotes, compute quote-based factors
  - [x] 11.12.1 Add fetch_polygon_quotes for live-mode quote backfill
- [x] [11.13.0](roadmap/agent-mining-phase.md) Implement bid_ask_spread factor — liquidity quality signal, requires quote stream

- [x] [11.45.0](roadmap/momo-filter-experiment.md) Document MOMO filter experiment methodology and findings

- [~] [11.2.0](roadmap/experiment-framework.md) Establish statistical validation standards — sample size >100, multi-date >10, significance tests, confidence intervals
- [~] [11.3.0](roadmap/experiment-framework.md) Implement experiment reproducibility framework — rule YAML versioning, parameter snapshots, data version tracking
- [~] [11.4.0](roadmap/experiment-framework.md) Design agent knowledge transfer format — queryable experiment logs, successful/failed patterns, ticker insights
- [~] [11.5.0](roadmap/experiment-framework.md) Define stepwise validation workflow — Quick Check → Thorough → Paper → Production gates with statistical rigor
- [~] [11.6.0](roadmap/experiment-framework.md) Create backtest-to-production deployment pipeline — statistical gates, paper trading, risk limits, monitoring
- [~] [11.7.0](roadmap/experiment-framework.md) Define bottleneck-to-architecture-upgrade workflow — detect slow factors, propose Rust migration, document implementation path
- [~] [11.8.0](roadmap/experiment-framework.md) Document mathematical mindset in SKILL.md — hypothesis-first, statistical rigor, common pitfalls, validation best practices
- [~] 11.46.0 Update evaluation framework: tiered retention (30/50/100/200%+), distribution shift, lost-MOMO profiling

## 12. Factor Engine Unified Architecture

- [x] [12.1](roadmap/factor-trait-design.md) Design Rust unified Factor trait interface — compute_batch + update methods

- [x] [12.2](roadmap/factor-plugin-architecture.md) Implement RelativeVolume unified interface — validate design works for batch + incremental
- [x] [12.3](roadmap/factor-plugin-architecture.md) Migrate existing factors to unified trait — EMA, TradeRate, VWAPDeviation
- [x] 12.5 Enable quote indicators in live FactorEngine — replace hardcoded quote_indicators=[] with registry creation
- [x] 12.6 Implement VolAccelFactor incremental update — add ring buffer for live mode
- [x] 12.7 Unify Python-Rust factor name mapping — remove hardcoded _RUST_NAME dict from batch_engine.py
- [x] 12.8 Rust factor defaults from config — read config/factors.yaml instead of hardcoded defaults
- [x] 12.9 Rename Tick→Trade throughout Rust+Python codebase — unify naming to match YAML type:trade
- [x] 12.10 Add quote backfill (_process_bootstrap_quotes) — symmetric with existing trades backfill
- [x] 12.11 Fix trade_rate missing from real-time — bootstrap_done key `tick` vs `trade` in _run_bootstrap and _compute_loop
- [x] 12.12 Fix quote_rate bootstrap 86 pairs — Rust compute_batch data-driven → grid-driven matching bootstrap_trade_rate
- [x] 12.13 Fix quote_rate no continuous updates — stream key routing Q.SYMBOL vs .Q. mismatch + _write_factors Redis publish gated on CH write

- [~] [12.4](roadmap/factor-plugin-architecture.md) Eliminate Python Indicator classes — state moves to Rust, Python only configures


## 14. ML-Driven Strategy Discovery

- [x] [14.1](roadmap/ml-strategy-discovery.md) 探索性分析工具 — 分析因子与收益关系、找到高收益区域、建议策略阈值
  - [x] 14.1.1 分析因子-收益相关性矩阵
  - [x] 14.1.2 高收益信号的因子分布分析
  - [x] 14.1.3 因子重要性排名
  - [x] 14.1.4 建议的因子阈值范围
- [x] [14.2](roadmap/ml-strategy-discovery.md) ML 模型层 — 收益预测模型、因子重要性分析、SHAP 解释
  - [x] 14.2.1 GradientBoostingRegressor 模型训练
  - [x] 14.2.2 多日数据训练（至少30交易日）
  - [x] 14.2.3 SHAP 因子解释
  - [x] 14.2.4 模型持久化和版本管理
- [x] [14.3](roadmap/ml-strategy-discovery.md) 动态策略层 — 用 ML 模型替代硬编码阈值，期望收益 > 阈值时入场
  - [x] 14.3.1 MLStrategy 类实现 — predict + confidence
  - [x] 14.3.2 与 Stage 架构结合 — WATCH + ML ENTRY
  - [x] 14.3.3 简化 events.yaml — 只保留 WATCH 条件
  - [x] 14.3.4 SignalEngine 实时预测集成
- [x] [14.4](roadmap/ml-strategy-discovery.md) 多日期验证 — 确保策略稳定性，交叉验证，限制模型复杂度
  - [x] 14.4.1 StrategyValidator — 多日表现验证
  - [x] 14.4.2 交叉验证防止过拟合
  - [x] 14.4.3 稳定性评分指标
  - [x] 14.4.4 模型复杂度限制
- [x] 14.5 混合架构设计 — Event model 字段规范、EventEvaluator ML 调用接口、SignalEngine 集成点
  - [x] [14.5.1](roadmap/ml-event-architecture.md) Event model 字段支持 — events.yaml 解析 model 配置、Event 数据模型增加 model 字段
  - [x] [14.5.2](roadmap/ml-event-architecture.md) EventEvaluator ML 调用 — 加载 ML 模型、predict 替代布尔判断、返回 expected_return + confidence
  - [x] [14.5.3](roadmap/ml-event-architecture.md) SignalEngine 实时集成 — 加载模型到内存、实时 factor → predict 流程、性能优化
  - [x] [14.5.4](roadmap/ml-event-architecture.md) hard_constraints 支持 — ENTRY 阶段硬约束作为风控、ML 预测 + 硬约束组合判断
  - [x] [14.5.5](roadmap/ml-event-architecture.md) 模型版本管理 — models/ 目录结构、模型热加载、A/B 测试支持
- [x] [14.6](roadmap/ml-event-architecture.md) 回测验证 ML 策略 — 用历史数据验证 ML entry vs 布尔 entry、对比 win_rate 和 expected_return
- [x] [14.7](roadmap/ml-event-architecture.md) Add ML event definition to events.yaml
- [x] 14.8 Collect 30+ days data with full 8 factors
- [x] 14.9 Retrain ML model with regularization


## 15. Big Winner Strategy Discovery

- [x] [15.1](roadmap/big-winner-definition-and-analysis-framework.md) Big Winner Definition and Analysis Framework

  Define what constitutes a 'big winner' (gain threshold, volume threshold, time window) and establish analysis methodology.

- [x] [15.2](roadmap/trajectory-pattern-analysis.md) Trajectory Pattern Analysis

  Analyze price/volume trajectory patterns of big winners: identify common shapes (sustained rise, late breakout, step-wise), volume distribution patterns, and key inflection points.

- [x] [15.3](roadmap/stratified-analysis-by-gain-level.md) Stratified Analysis by Gain Level

  Analyze big winners by gain tiers: Super Winners (>100%), Big Winners (40-100%), Medium Winners (20-40%). Identify features that distinguish each tier.

- [x] 15.10 识别并清理已废弃的探索脚本 — window_impact_analyzer, window_strategy_tester, entry_metrics_*_analyzer 等

- [x] [15.9](roadmap/winner-loser-analysis.md) 分析大赢家vs直接亏损的入场因子差异 — 只用入场时刻信息，找出区分特征
- [x] [15.11](roadmap/entry-gain-analysis.md) 验证vol_accel_momentum信号的入场涨幅分布 — 理解涨幅与结果的关系，不找最优阈值
- [x] [15.12](roadmap/refined-entry-validation.md) 基于发现设计新的入场条件：accel_sustainability > 5.7 AND vol_accel_15to30 > 2.3 AND gain_pct_at_entry < 2%
- [x] [15.13](roadmap/refined-entry-full-validation.md) 验证新入场条件在更多数据上的效果

- [x] 15.16 大赢家覆盖分析 — 每日赢家分布(50%+, 20-50%, 10-20%)、策略覆盖率对比、遗漏原因诊断（入场过滤过严？时机不对？）
- [~] 15.17 放宽入场条件：session_phase 允许 early+mid, rel_vol 降至 1.5, 重新评估覆盖率

- [x] [15.18](roadmap/dense-signal-hypothesis-validation.md) 建立统计验证框架：基于dense_signal_collector全集采样，划分探索日期和验证日期，形成假设→实验→统计检验的工作流
- [x] 15.19 基于探索结果设计宽松版入场条件（early+mid，rel_vol放宽），在验证集上对比标准版，显著性检验
- [x] 15.20 定义策略评估准则：多日期胜率、平均收益、信号数量、precision/recall、in-sample vs out-of-sample显著性检验
- [x] [15.21](roadmap/dense-signal-hypothesis-validation.md) 创建dense_signal_collector多日期批量采集脚本 — 对所有可用日期运行collect_signals()，输出合并的factor→return数据集parquet，支持增量更新
- [x] [15.22](roadmap/dense-signal-hypothesis-validation.md) 单因子区分力分析 — 每个因子按winner/loser分组统计（均值/中位数/分布），KS检验，单因子AUC。回答：哪些因子真正区分赢家？当前阈值是否合理？
- [x] [15.23](roadmap/dense-signal-hypothesis-validation.md) 条件概率矩阵与Pareto前沿 — 枚举因子阈值组合（gap/rel_vol/vol_accel），计算每个组合的precision和recall，绘制Pareto前沿找coverage vs quality最优trade-off
- [x] [15.24](roadmap/dense-signal-hypothesis-validation.md) 探索集/验证集划分与冷验证 — EXPLORE_DATES发现规律→冻结阈值→VALIDATE_DATES单次验证。对比in-sample vs out-of-sample的precision/recall/avg_return
- [x] 15.26 Streaming engine bar构建bug修复 — ingest_trade one-by-one只产生25%的bar，导致假阳性结果（27信号/63%胜率/+3.5%EV）。修复为使用ingest_trades_batch对齐batch模式。修复后真实结果：所有策略负EV。
- [x] 15.27 cold_validation precision与真实EV鸿沟诊断 — precision=19.9%对应EV=-2.30%。tp15_sl20下赢+15%/输-20%不对称性没有被cold_validation纳入。需重新设计评估指标
- [x] 15.28 策略全负EV根因分析 — 根因：(1) momentum_entry过于宽松(rel_vol>2)导致低质量信号，(2) tp15_sl20 exit次优，(3) March10单日暴跌拖累全局。修复：volume_strong_10+trade_rate>10+tpsl10_15 → +3.81%改善
- [x] 15.29 出场策略架构评估 — 多日期测试(38信号/5日)：tp10_sl15=-0.10%EV最优(+2.20pp vs tp15_sl20的-2.30%)。Trailing stop在单日过拟合(+6.56%)但多日期崩溃(-1.88%)。Partial exits略优但不显著。默认exit改为tp10_sl15。
- [x] 15.30 WATCH两阶段架构价值验证 — WATCH(gap_up_watch) 100%通过率，与single-stage结果完全相同。设计目的为temporal timing gate(记录first_entry_ms启动30min窗口)而非过滤器。PreFilter负责ticker筛选,WATCH负责时间戳锚定。架构合理，无需改动。

- [~] [15.4](roadmap/early-signal-feature-engineering.md) Early Signal Feature Engineering
- [~] [15.5](roadmap/ml-model-for-winner-prediction.md) ML Model for Winner Prediction
- [~] [15.6](roadmap/real-time-strategy-simulation.md) Real-time Strategy Simulation
- [~] [15.7](roadmap/volume-acceleration-analysis.md) Volume Acceleration Analysis
- [~] [15.8](roadmap/medium-vs-big-differentiation.md) Medium vs Big Differentiation
- [~] 15.14 将新入场条件集成到events.yaml配置中
- [~] 15.15 分析14个直接亏损案例，寻找额外过滤条件
- [~] [15.25](roadmap/dense-signal-hypothesis-validation.md) 核心假说验证报告 — 正式陈述"盘前跳空+成交量加速→预测后续涨幅"，汇总15.22-15.24证据，判断假说成立/不成立/需修正，记录发现和下一步
- [~] 15.31 市场状态过滤器 — March10单日EV=-10.03%（6信号/1赢），其他6日EV=+2.61%（37信号/67.6%胜率）。需添加市场宽度/SPY方向过滤器，避免broad selloff日的假信号。
- [~] 15.32 回测默认配置固化 — volume_strong_10(rel_vol>5+trade_rate>10)+tp10_sl15已设为默认(DEFAULT_EVENTS+get_default_exit_strategy)。需运行完整回测验证(10+日期)，写入validation notes。

- [x] 15.37 Build manual MOMO labeling CSV pipeline — generate multi-date candidate CSV with key diagnostics (trade count, spread, ignition delay, peak) for human review
- [x] 15.38 Create labeling module in frontend — ticker list navigation (CSV/CH), premarket price+factor chart (reuse ChartPanelSystem), labeling controls, keyboard shortcuts

- [~] 15.33 Reposition Layer 1 goal to 100% MOMO capture — implement permissive baseline with minimal false-negative rate
- [~] 15.36 Implement purification Layer 2+ on top of permissive Layer 1 — stricter btd/range thresholds to filter false positives while keeping all MOMO

## 16. Test Suite

- [x] 16.1 test domain/market/bar.py — OHLC validation, __post_init__ invariants
- [x] 16.2 test domain/session.py — trading session phases, time boundaries
- [x] 16.3 test domain/market/tick.py — tick data validation, edge cases
- [x] 16.4 test domain/order/order.py — order state machine, position sizing
- [x] 16.5 test shared/time/timezone.py — timezone conversions, DST boundaries
- [x] 16.6 test shared/utils/data_utils.py — general utility functions
- [x] 16.7 test domain/backtest/types.py — Candidate, Signal dataclass serialization
- [x] 16.8 test services/backtest/exit_strategy.py — take-profit/stop-loss logic
- [x] 16.9 test services/backtest/event_validator.py — event validation metrics
- [x] 16.10 test services/factor/factor_registry.py — auto-registration, reflection
- [x] 16.11 test services/backtest/config.py — config parsing and defaults


## 17. Frontend Polish

- [x] 17.1 删除 styles/globals.css — 与 index.css 完全重复的未引用文件
- [x] 17.2 去重 checkCollision — App.tsx 和 layoutUtils.ts 有相同的碰撞检测函数，App.tsx 应导入使用
- [x] 17.3 添加 ErrorBoundary — 包裹所有模块，防止单个模块崩溃导致整个页面白屏
- [x] 17.4 添加 Vitest + store 测试 — 7 个 Zustand store 的单元测试（目前 13,500 行代码零测试）
- [x] 17.5 补充 guidelines/Guidelines.md — 填入真实编码规范（dark-only theme、Zinc color palette、store 模式等）
- [x] 17.6 拆分超大 chart 组件 — OverviewChartModule(1215行)、BacktestChartModule(1163行)、ChartPanelSystem(1010行) 提取 hooks
- [x] 17.7 排查 SettingsMenu.tsx 是否为死代码 — HelpPanel 的 Settings tab 可能已取代它

- [x] 17.8 React.memo on GridItem/ChartPanelHeader/SymbolSearch — 这三个组件在拖拽和数据更新时高频重渲染但未用 memo
- [x] 17.9 layoutUtils.ts 单元测试 — SpatialHashGrid 碰撞检测、collision check 纯几何逻辑适合单测
- [x] 17.10 替换 19 处 any 类型为精确类型 — 主要涉及 RankList、ChartPanelSystem、BacktestChartModule、marketDataStore


## 2. Rust Core
Performance-critical components rewritten in Rust.

- [x] 2.1 BarBuilder with watermark-based close and late-arrival window
- [x] 2.2 ReplayClock with Redis heartbeat for cross-machine sync
- [x] 2.3 VolumeTracker for snapshot processing
- [x] 2.4 Factor helpers (z_score, price_accel)
- [x] 2.6 TickDataReplayer with Parquet lake reader

- [x] 2.3.0 Add ClickHouse data source to Rust replayer — use `clickhouse` crate to query trades/quotes from CH as primary source (Parquet fallback), unify backtest data path, config-driven (date + ticker list)

- [x] 2.10 Initialize Rust `env_logger` or pyo3 logging bridge so Rust `log::info!` / `log::warn!` messages appear in Python logs instead of being silently dropped

- [~] 2.8 StateEngine rewrite in Rust
- [~] 2.9 FactorEngine rewrite in Rust


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

- [~] 3.2 StateEngine Python wrappers and integration


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

- [x] [5.32](roadmap/backtest-visualization-architecture.md) BacktestChartModule fix — 修复 setMarkers API 错误，使用 lightweight-charts v5 正确的 marker API
  - [x] 5.32.1 BacktestConfigModule — 日期范围、事件选择、参数配置面板
  - [x] 5.32.2 BacktestProgressModule — WebSocket 接收实时进度、进度条、日志输出


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

- [x] [6.19](roadmap/factor-plugin-architecture.md) Add factor unit test framework
- [x] 6.21 Prepare 10+ trading days data for backtest validation
- [x] [6.23](roadmap/fix-timing-race-snapshot-bootstrap-clock-jump-subscribe.md) Fix timing race: snapshot bootstrap → clock jump → subscribe

- [x] 6.14 Add parallel execution for mining parameter sweep
- [x] 6.15 Add multi-date mining with aggregate report
- [x] [6.17](roadmap/factor-registration-v4.md) Implement auto-registration for indicator classes
- [x] [6.24](roadmap/backtest-visualization-architecture.md) Backtest Backend API — FastAPI service for backtest visualization
  - [x] 6.24.1 ClickHouse backtest_signals table — 存储回测信号结果（不重复存储 bar 数据）
  - [x] 6.24.2 BacktestRunner async task — PreFilter → DataLoader → FactorEngine → EventEvaluator，增量写入 CH
  - [x] 6.24.3 WebSocket progress push — 实时推送回测进度、信号到前端
  - [x] 6.24.4 API endpoints — POST /start, GET /results/{exp_id}, GET /chart/{exp_id}/{ticker}
  - [x] 6.24.5 Signal diagnostics — 价格路径分析（max_price/min_price/time_to_max）、亏损归因、因子相关性
- [x] 6.26 Backtest Visualization UX 优化 — Results Browser scrollbar、BacktestChart ResizeObserver zoom fix、Factors display panel

- [x] 6.28 信号详情面板增强 — 点击 entry/exit marker 显示完整 factor 信息，tooltip 交互
- [x] 6.31 prepare --tickers 精简模式 — 只准备指定 ticker 的 trades/quotes/snapshot 数据
- [x] 6.32 缺失数据检测 — 检测哪些日期 trades/quotes 不完整，在 prepare 前预警

- [x] 6.33 结果导出 — CSV/Excel 导出实验结果，JSON 导出完整信号+factors
- [x] [6.35](roadmap/factors-时间线可视化-存储-backtest-过程中每个-bar-的-factors-到-clickhouse-支持完整时间线-overlay-当前只存储-entry-signal-的-factors.md) Factors 时间线可视化 — 存储 backtest 过程中每个 bar 的 factors 到 ClickHouse，支持完整时间线 overlay（当前只存储 entry signal 的 factors）


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

- [x] [9.1.0](roadmap/backtest-pipeline-design.md) Backtest data models — domain/backtest/types.py + services/backtest/config.py
- [x] [9.2.0](roadmap/backtest-prefilter-findings.md) Candidate pre-filter — true entry detection from market_snapshot
- [x] 9.3.0 Data loader — data_loader.py: Rust loaders for trades/quotes Parquet
- [x] [9.4.0](roadmap/backtest-pipeline-design.md) Batch engine — batch_engine.py: FactorEngineBatchAdapter, reuses live FactorRegistry indicators (replaces factor_batch.py)
- [x] 9.5.0 Signal evaluator — evaluator.py: evaluate DSL rules against factor timeseries, reuse evaluate_condition/trigger
- [x] 9.6.0 Return & metrics — metrics.py: slippage (ask+buffer), returns at multi-horizon, MFE/MAE, time-to-peak
- [x] 9.7.0 Output — output.py: console summary table + ClickHouse backtest_results table persistence
- [x] 9.8.0 Runner + CLI — runner.py orchestrator + cli.py entry point (--date, --date-range, --rules, --gain-threshold)
- [x] 9.9.0 snapshot_builder_v2: 分层输出 ranked_data (Pre-filter) + collector_format (Replay)
- [x] 9.10.0 split 预处理
- [x] 9.11.0 HistoricalLoader — CH bootstrap for replay mode
- [x] 9.14.0 Parquet Snapshot Validator
- [x] 9.1.0 Data CLI — download, convert, check, build-snapshot, prepare pipeline
- [x] 9.2.0 CSV.gz → Parquet converter (Polars streaming, skip-if-exists)
- [x] 9.3.0 Snapshot builder — trades → CH market_snapshot_collector + market_snapshot (ranked)
- [x] 9.4.0 HistoricalLoader batch bootstrap — bulk CH read/write, in-memory subscription accumulation, last_df filling, clock jump on completion
- [x] [9.5.0](roadmap/replayer-ch-migration.md) CHReplayer — 读 CH market_snapshot_collector，推送 INPUT Stream，本地 clock 支持
  - [x] 9.5.1 Clock pause during bootstrap — init 后 pause，bootstrap 完成后 jump_to + resume
  - [x] 9.5.2 CHReplayer 实现 — 读 CH、poll local clock、xadd INPUT Stream
  - [x] 9.5.3 Parquet → CH migration CLI — 历史数据一次性迁入
  - [x] 9.5.4 backend_starter 集成 — 替换旧 parquet replayer，auto-detect start_from
- [x] 9.6.0 Split adjustment — adjustment_factor = split_from / split_to，应用于 prev_close
- [x] [9.7.0](roadmap/replayer-ch-migration.md) Clock-based Bootstrap Synchronization
- [x] 9.9.0 Parquet snapshot data integrity checker
- [x] 9.10.0 Parquet → CH Migration Script
- [~] 9.11.0 Snapshot bootstrap Rust 加速
- [x] 9.12.0 Fix volume semantics in replay mode — snapshot_builder outputs per-window incremental volume but VolumeTracker expects cumulative
- [x] 9.13.0 Implement relativeVolumeDaily calculation — currently never computed, just passed through as 0/1
- [x] 9.15.0 snapshot_builder: compute prev_volume from day_aggs instead of hardcoding 0.0
- [x] 9.16.0 Fix VWAP — use cumulative turnover/volume instead of per-window VWAP
- [x] 9.17.0 Round volume to integer in snapshot_builder to avoid float precision display
- [x] 9.18.0 SignalEvaluator cooldown/dedup — skip same-rule triggers within configurable window (default 60s)
- [x] 9.19.0 Backtest grouped statistics — per-rule and per-ticker breakdown in console output
- [x] 9.20.0 Backtest results visualization notebook — 02-backtest-results.ipynb with return distribution, MFE/MAE scatter, timeline
- [x] 9.21.0 Backtest pre-market time filter — restrict pipeline to 4:00-9:30 AM ET, ignore out-of-hours trades/signals
- [x] [9.24.0](roadmap/filter-trf-exch-4-trades-remove-stale-finra-delayed-reports-from-dataloader-design-doc.md) Filter TRF (exch=4) trades — remove stale FINRA delayed reports from DataLoader, design doc
- [x] 9.25.0 Fix backtest pre-filter: remove silent fallback to market_snapshot_collector, require market_snapshot data
- [x] 9.26.0 Build-snapshot unified pipeline: produce both market_snapshot_collector (raw) and market_snapshot (processed, subscribed tickers only) in one step, auto-process-from-collector
- [x] 9.27.0 Align market_snapshot_collector schema with live collector: remove rank/change columns, add CREATE DATABASE, use jerry_trader namespace
- [x] 9.28.0 Delete duplicate schemas copy.py, rename enrich→process terminology (CLI + code)
- [x] 9.22.0 DataLoader bulk query — single CH query for all tickers instead of 82 roundtrips (10s→<1s)
- [x] 9.23.0 Backtest date range batch run — support --date-range START END, reuse CH connection and rule loading across dates
- [x] 9.29.0 build-snapshot --force flag — unified rerun switch to overwrite collector + snapshot data
- [x] 9.30.0 process-snapshot vectorized rank — replace 3960-window Python loop with Polars over() rank, 16s→<1s
- [x] 9.31.0 Pipeline progress bars — add tqdm to process-snapshot window loop, DataLoader ticker loading, backtest runner factor computation
- [x] [9.32.0](roadmap/merge-build-process-snapshot.md) Merge build-snapshot + process-snapshot — remove process-snapshot subcommand, build-snapshot --force handles both tables
- [x] [9.33.0](roadmap/check-verbose.md) check --verbose — show ticker-level gaps, time range issues, not just READY/MISSING
- [x] [9.34.0](roadmap/date-range-parallel.md) Date range parallel execution — run multiple dates in parallel with ProcessPoolExecutor
- [x] [9.35.0](roadmap/richer-signal-output.md) Richer signal output — show rule details, factor values at trigger time, not just aggregate stats
- [x] [9.36.0](roadmap/dry-run-candidates-only.md) dry-run mode: preview pre-filter candidates without running factor/signal pipeline
- [x] [9.37.0](roadmap/prefilter-subscription-logic.md) PreFilter use subscription logic — match live processor behavior (subscription rank vs snapshot rank)


## Open Issues

Known issues and bugs requiring attention.

- [x] StateEngine (state_engine.py) still writes signal_events to InfluxDB — needs migration to ClickHouse
