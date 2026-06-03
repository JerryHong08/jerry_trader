# Roadmap

## 1. Domain Layer

Foundation pure business logic value objects.

- [ ] 1.3 Populate domain/strategy/ with Signal, Risk models

## 2. Rust Core

Performance-critical components rewritten in Rust.

- [ ] [2.11](roadmap/rust-compute-box-architecture.md) Optimize BarsBuilder bootstrap - move load+ingest to Rust

## 3. Services Layer

Stateful workers and use-case implementations.

- [ ] 3.4 Real-time risk management engine with position limits
- [ ] 3.5 Risk management rules and drawdown checks
- [ ] 3.6 Risk engine integration with order execution

## 4. ML Pipeline

Machine learning for breakout detection and prediction.

- [ ] 4.1 Expand runtime/ml/ with training/evaluation workflows
- [ ] 4.2 Historical context model for breakout detection
- [ ] 4.3 Integrate ML pipeline with FactorEngine
- [ ] 4.4 Simulate market_snapshot replay using historical trade&quote bulk file

## 5. Frontend

React/TradingView UI modules and UX improvements.

- [ ] 5.6 Better UX improvements
- [ ] 5.19 Signal visualization module - toast/notification bar for real-time signal alerts, click to jump to ticker chart
- [ ] 5.22 News → Chart markers - show news event markers on bar chart at corresponding timestamps
- [ ] 5.23 Replay timeline scrubber - draggable timeline/progress bar in replay mode, jump to any time point via replay clock pause/resume API
- [ ] [5.33](roadmap/multi-pane-time-sync.md) Multi-pane chart time axis sync

## 6. Orchestration

System-wide coordination and backtest infrastructure.

- [ ] 6.4 Tickers pre-location fitting strategy/conditions
- [ ] 6.5 Strategy pre-locate orchestrator with auto-sequenced jumps
- [ ] 6.6 Pre-located tickers pipeline backtest visualization and validation
- [ ] 6.12 Add --date-range support to prepare command
- [ ] [6.18](roadmap/factor-plugin-architecture.md) Document new factor validation SOP
- [ ] [6.22](roadmap/snapshot-filter-criteria.md) Unify SnapshotFilterCriteria abstraction across backtest and runtime
  - [ ] 6.22.1 Runtime replay load rank snapshot to Redis stream when data exists

- [ ] 6.25 Backtest App multi-date support
- [ ] 6.27 Factors caching — store computed factors in ClickHouse to avoid recomputation
- [ ] 6.29 Timeline scrubber — multi-signal timeline visualization with drag-to-jump
- [ ] 6.30 Backtest vs live comparison — validate backtest signals against historical live signals
- [ ] 6.34 Batch experiment comparison panel — A/B test different parameter combinations side-by-side

## 7. AI Agent Layer

[ATC-R Agent System](roadmap/atcr-agent-system.md) — Two-phase design: near-term Agent as automated factor researcher, future ACT-R real-time decision support.

- [ ] [7.3](roadmap/atcr-agent-system.md) Agent Factor Mining — orchestrator, skills (define_factor, run_backtest, evaluate, optimize), prompts
- [ ] [7.5](roadmap/atcr-agent-system.md) News Pre-filter + Conflict Resolution
- [ ] [7.6](roadmap/atcr-agent-system.md) Agent Think — BFF, Context Aggregator, LLM Reasoner, Tool Registry
- [ ] [7.7](roadmap/atcr-agent-system.md) Agent Decision Simulation + Prompt Validation
- [ ] [7.8](roadmap/atcr-agent-system.md) Output & UX — Notification, Dynamic Widget, Chat Interface
- [ ] [7.9](roadmap/factor-stream-rust-migration.md) Factor stream Rust migration - migrate from Python asyncio to Rust FactorBroadcaster when Signal Engine is introduced

## 8. Backtest Mining & Experiment Framework

- [ ] [8.18](roadmap/agent-mining-phase.md) Implement large_trade_ratio factor — institutional participation signal, tick-based analysis
- [ ] [8.19](roadmap/agent-mining-phase.md) News sentiment factor integration — connect NewsWorker to backtest, catalyst-backed momentum filter
- [ ] 8.37 Execution cost analysis — slippage/spread/opportunity cost impact on avg return
- [ ] 8.38 Paper Trading validation — Event framework live mode effectiveness
- [ ] 8.40 SignalEngine unify events.yaml consumption — replace DSL rules, share event definition across backtest/live
  - [ ] 8.40.1 Unify Event and Rule data models
  - [ ] 8.40.2 Refactor SignalEngine to load events.yaml instead of rules/*.yaml
  - [ ] 8.40.3 SignalEngine use EventEvaluator.evaluate_ticker for real-time evaluation
  - [ ] 8.40.4 Remove config/rules/ directory and Rule domain model
  - [ ] 8.40.5 Verify backtest and live SignalEngine produce identical results

## 9. Rust Compute Box Architecture

- [ ] [9.1](roadmap/rust-compute-box-architecture.md) DataLayer - trades/bars_buffer internal storage, get_recent_bars() interface
- [ ] [9.2](roadmap/rust-compute-box-architecture.md) BootstrapStatus - state management (NotStarted/InProgress/Ready/Failed)
- [ ] [9.3](roadmap/rust-compute-box-architecture.md) WS Server - tokio async, Port 8000
- [ ] [9.4](roadmap/rust-compute-box-architecture.md) Subscribe/Unsubscribe handlers
- [ ] [9.5](roadmap/rust-compute-box-architecture.md) Bar + Factor message push, SubscribeAck/BootstrapReady protocol
- [ ] [9.6](roadmap/rust-compute-box-architecture.md) BarBuilderEngine - bars write directly to buffer + CH
- [ ] [9.7](roadmap/rust-compute-box-architecture.md) Bar broadcast channel → FactorEngineCore
- [ ] [9.8](roadmap/rust-compute-box-architecture.md) FactorEngineCore - subscribe bar stream, compute factors
- [ ] [9.9](roadmap/rust-compute-box-architecture.md) Factors write directly to CH + push WS
- [ ] [9.10](roadmap/rust-compute-box-architecture.md) ChartBFF - keep only HTTP API (historical queries)
- [ ] [9.11](roadmap/rust-compute-box-architecture.md) Delete BarsBuilderService/FactorEngine Python versions
- [ ] [9.12](roadmap/rust-compute-box-architecture.md) Rust HTTP client - fetch Polygon trades or CH unified data source

## 10. Big Winner Strategy Discovery

- [ ] [10.34](roadmap/momo-filter-experiment.md) Design activation-anchored pipeline — detect ignition events as decision anchor instead of PreFilter entry moment
- [ ] 10.35 Investigate VCIG (+322.8%) trade profile — reconstruct activation moment and verify which features would have captured it

## 11. Breakout Capture Strategy

Focus on capturing the explosive breakout moment itself — short time window (1-5 min), microstructure-level precision.

- [ ] 11.1 Adaptive ignition scoring — replace fixed thresholds with per-ticker relative deviation from pre-entry baseline
- [ ] 11.2 Microstructure breakout signals — trade size distribution shifts, aggressor ratio acceleration, spread collapse velocity
- [ ] 11.3 Fused ignition intensity — continuous composite score from 5 methods instead of boolean outputs
- [ ] 11.4 Short-window capture backtest — evaluate profit potential of 1-5min post-ignition entries
