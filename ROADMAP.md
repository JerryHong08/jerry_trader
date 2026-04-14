# Roadmap

## 1. Domain Layer

Foundation pure business logic value objects.

- [ ] 1.3 Populate domain/strategy/ with Signal, Risk models

## 2. Rust Core

Performance-critical components rewritten in Rust.

- [-] 2.8 StateEngine rewrite in Rust
- [-] 2.9 FactorEngine rewrite in Rust
- [ ] 2.10 Initialize Rust `env_logger` or pyo3 logging bridge so Rust `log::info!` / `log::warn!` messages appear in Python logs instead of being silently dropped


## 3. Services Layer

Stateful workers and use-case implementations.

- [-] 3.2 StateEngine Python wrappers and integration
- [ ] 3.4 Real-time risk management engine with position limits
- [ ] 3.5 Risk management rules and drawdown checks
- [ ] 3.6 Risk engine integration with order execution


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
- [ ] 5.22 News → Chart markers - show news event markers on bar chart at corresponding timestamps
- [ ] 5.23 Replay timeline scrubber - draggable timeline/progress bar in replay mode, jump to any time point via replay clock pause/resume API

## 6. Orchestration

System-wide coordination and backtest infrastructure.

- [ ] 6.4 Tickers pre-location fitting strategy/conditions
- [ ] 6.5 Strategy pre-locate orchestrator with auto-sequenced jumps
- [ ] 6.6 Pre-located tickers pipeline backtest visualization and validation

- [ ] 6.12 Add --date-range support to prepare command


- [ ] 6.14 Add parallel execution for mining parameter sweep

- [ ] 6.15 Add multi-date mining with aggregate report


- [ ] [6.17](roadmap/factor-plugin-architecture.md) Implement auto-registration for indicator classes

- [ ] [6.18](roadmap/factor-plugin-architecture.md) Document new factor validation SOP

- [x] [6.19](roadmap/factor-plugin-architecture.md) Add factor unit test framework


- [-] 6.21 Prepare 10+ trading days data for backtest validation

  Prepare data from 2026-03-02 to 2026-03-12 (10 trading days). 03-13 already exists. Run: prepare --date 2026-03-01 --end-date 2026-03-15

## 7. AI Agent Layer

[ATC-R Agent System](roadmap/atcr-agent-system.md) — Two-phase design: near-term Agent as automated factor researcher, future ACT-R real-time decision support.

**Phase A: Signal System + Agent Factor Mining (Near-term)**

- [ ] [7.3](roadmap/atcr-agent-system.md) Agent Factor Mining — orchestrator, skills (define_factor, run_backtest, evaluate, optimize), prompts

**Phase B: ACT-R Real-Time Agent (Future)**

- [ ] [7.5](roadmap/atcr-agent-system.md) News Pre-filter + Conflict Resolution
- [ ] [7.6](roadmap/atcr-agent-system.md) Agent Think — BFF, Context Aggregator, LLM Reasoner, Tool Registry
- [ ] [7.7](roadmap/atcr-agent-system.md) Agent Decision Simulation + Prompt Validation
- [ ] [7.8](roadmap/atcr-agent-system.md) Output & UX — Notification, Dynamic Widget, Chat Interface

**Infrastructure:**

- [ ] [7.9](roadmap/factor-stream-rust-migration.md) Factor stream Rust migration - migrate from Python asyncio to Rust FactorBroadcaster when Signal Engine is introduced


## 8. Optional Features

Enhancements and additional modules.

- [ ] 8.3 Historical orders analysis module

## 11. Backtest Mining & Experiment Framework

[Agent Mining Phase](roadmap/agent-mining-phase.md) — Factor implementation, threshold optimization, multi-condition rules.
[Experiment Framework](roadmap/experiment-framework.md) — Mathematical validation, reproducibility, knowledge transfer.

**Infrastructure:**

- [ ] [11.2](roadmap/experiment-framework.md) Establish statistical validation standards — sample size >100, multi-date >10, significance tests, confidence intervals
- [ ] [11.3](roadmap/experiment-framework.md) Implement experiment reproducibility framework — rule YAML versioning, parameter snapshots, data version tracking
- [ ] [11.4](roadmap/experiment-framework.md) Design agent knowledge transfer format — queryable experiment logs, successful/failed patterns, ticker insights
- [ ] [11.5](roadmap/experiment-framework.md) Define stepwise validation workflow — Quick Check → Thorough → Paper → Production gates with statistical rigor
- [ ] [11.6](roadmap/experiment-framework.md) Create backtest-to-production deployment pipeline — statistical gates, paper trading, risk limits, monitoring
- [ ] [11.7](roadmap/experiment-framework.md) Define bottleneck-to-architecture-upgrade workflow — detect slow factors, propose Rust migration, document implementation path
- [ ] [11.8](roadmap/experiment-framework.md) Document mathematical mindset in SKILL.md — hypothesis-first, statistical rigor, common pitfalls, validation best practices

**Factor Implementation:**

- [ ] [11.9](roadmap/agent-mining-phase.md) Implement relative_volume factor — core pre-market signal, volume vs 20-period avg, register in factors.yaml
- [ ] [11.10](roadmap/agent-mining-phase.md) Implement price_direction factor — distinguish buy vs sell pressure, avoid passive sell traps
- [ ] [11.11](roadmap/agent-mining-phase.md) Implement gap_percent factor — entry position filter, avoid chasing overextended stocks >15% gap
- [ ] 11.12 Quote data integration for backtest — extend DataLoader to load quotes, compute quote-based factors
- [ ] [11.13](roadmap/agent-mining-phase.md) Implement bid_ask_spread factor — liquidity quality signal, requires quote stream

**Mining & Analysis:**

- [ ] [11.14](roadmap/agent-mining-phase.md) Analyze BIAF vs KIDZ signal quality — why BIAF succeeded (62% win) while KIDZ failed (0% win)
- [ ] [11.15](roadmap/agent-mining-phase.md) Threshold sweep mining — trade_rate [100,150,200,250] × relative_volume [2,3,5]
- [ ] [11.16](roadmap/agent-mining-phase.md) Multi-condition rule composition test — trade_rate + price_direction + relative_volume + gap_percent combinations
- [ ] [11.17](roadmap/agent-mining-phase.md) Multi-date validation workflow — establish standard: 10+ days validation before deployment
- [ ] [11.18](roadmap/agent-mining-phase.md) Implement large_trade_ratio factor — institutional participation signal, tick-based analysis
- [ ] [11.19](roadmap/agent-mining-phase.md) News sentiment factor integration — connect NewsWorker to backtest, catalyst-backed momentum filter
- [ ] [11.20](roadmap/agent-mining-phase.md) Ticker-specific adaptive thresholds — different parameters for high-float vs low-float stocks



## 12. Factor Engine Unified Architecture

- [ ] [12.1](roadmap/factor-plugin-architecture.md) Design Rust unified Factor trait interface — compute_batch + update methods

- [ ] [12.2](roadmap/factor-plugin-architecture.md) Implement RelativeVolume unified interface — validate design works for batch + incremental

- [ ] [12.3](roadmap/factor-plugin-architecture.md) Migrate existing factors to unified trait — EMA, TradeRate, VWAPDeviation

- [ ] [12.4](roadmap/factor-plugin-architecture.md) Eliminate Python Indicator classes — state moves to Rust, Python only configures

## Design Concerns

Architectural decisions needing discussion.

## Open Issues

Known issues and bugs requiring attention.

- [ ] StateEngine (state_engine.py) still writes signal_events to InfluxDB — needs migration to ClickHouse
