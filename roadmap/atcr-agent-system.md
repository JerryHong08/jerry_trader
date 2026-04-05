# ATC-R Agent System

**Created:** 2026-04-05
**Updated:** 2026-04-06
**Status:** Design Phase
**Inspiration:** ACT-R Cognitive Architecture (adapted for trading)

## Two-Phase Architecture

This system has two distinct phases with different goals and timelines:

### Phase A: Quantitative Signal System + Agent Factor Mining (Near-term)

Traditional quant layer for real-time signal detection, with an AI Agent as an **offline automated researcher** that discovers and optimizes factors/DSL rules through backtesting.

### Phase B: ACT-R Real-Time Agent (Future)

ACT-R inspired real-time agent that assists trading decisions when signals fire — context aggregation, LLM reasoning, and action recommendations.

---

## Phase A: Signal System + Agent Factor Mining

### Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Real-Time Layer (Live)                    │
│                                                             │
│  TickData Manager → FactorEngine → Signal Engine            │
│                          ↓              ↓                   │
│                    Factor Storage    DSL Rule Matching       │
│                    (ClickHouse)      (declaretive rules)     │
│                                         ↓                   │
│                                   Signal Trigger             │
│                                   (进入top20 + 涨幅达标)      │
│                                         ↓                   │
│                                   记录 / 观察 / 通知          │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                Agent Research Layer (Offline)                │
│                                                             │
│  Agent (AI Researcher)                                      │
│  ├─ 1. Pre-filter candidates from historical snapshots      │
│  │     SQL: 盘前进入top20的股票 + 首次进入时间 + 涨幅          │
│  │     → 生成 candidate 表 (symbol, entry_time, filters)    │
│  │                                                          │
│  ├─ 2. Factor Mining Loop                                   │
│  │     hypothesis → backtest → evaluate → refine → repeat   │
│  │                                                          │
│  ├─ 3. Backtest Execution                                   │
│  │     ClickHouse factors → Rule Engine → triggers → returns │
│  │     (不需要全量 replay，直接读 factor history)              │
│  │     (新因子需要 full replay: trades → FactorEngine)        │
│  │                                                          │
│  ├─ 4. Output                                               │
│  │     有效 DSL rules → 部署到 Signal Engine                  │
│  │     因子表现报告                                           │
│  │                                                          │
│  └─ 5. Future: Auto ML                                      │
│        Agent 定义的因子 → ML features                         │
│        回测结果 → training labels                             │
│        自动训练模型 → 替代/增强手工 DSL                        │
└─────────────────────────────────────────────────────────────┘
```

### Focus: US Pre-Market Top 20 Momentum

**Initial scope:**
- Monitor stocks that **suddenly enter pre-market top 20** with a minimum price increase
- This is a narrow, focused signal — traditional quant, no LLM involved
- DSL rules define factor conditions (e.g., trade_rate > X, volume_ratio > Y)
- When triggered: record observation, track returns

### Backtest with Pre-filtering

Instead of brute-force replaying all stocks, use historical market snapshots:

```
全市场 snapshot (e.g., 20260306)
    ↓ SQL 预筛选
盘前进入 top20 的股票 + 首次进入时间 + 涨幅等条件
    ↓ 生成 candidate 表
只对这些股票从对应时间点开始跑 factor backtest
```

**Benefits:**
- Massively reduced search space (20-50 stocks vs thousands)
- Can parameterize pre-filter conditions (top N, min gain threshold)
- Agent can iterate quickly on factor/rule discovery

### Backtest Data Flow

**Existing factors (fast path):**
```
ClickHouse factors table
    → filter by date range + candidate symbols
    → feed to Rule Engine evaluate
    → count triggers, compute returns
```

**New factors (full replay):**
```
ClickHouse raw trades
    → FactorEngine (compute new factor)
    → new factor history to ClickHouse
    → then same fast path as above
```

### Agent Skills (Factor Mining)

| Skill | Description |
|-------|-------------|
| `pre_filter_candidates` | SQL query on snapshot data to find candidate stocks |
| `define_factor` | Define a new factor (Rust code or compose existing) |
| `run_backtest` | Run rule evaluation on historical factor data |
| `evaluate_results` | Compute returns, win rate, max drawdown, Sharpe |
| `optimize_threshold` | Grid search for optimal rule parameters |
| `deploy_rule` | Promote validated rule to Signal Engine config |

### Agent Orchestrator

The agent needs orchestration to manage the factor mining loop:

```
Agent Orchestrator
├─ Session Manager — track which dates, which factors tested
├─ Backtest Runner — execute backtests, collect results
├─ Factor Registry — track available factors, their specs
├─ Result Store — persist backtest results for comparison
└─ Scheduler — queue backtest runs, manage concurrency
```

### Evolution Path

```
手工 DSL → Agent 自动挖 DSL → ML 自动化
   (现在)      (Phase A 目标)     (Phase A 远期 + Section 4 ML Pipeline)
```

---

## Phase B: ACT-R Real-Time Agent (Future)

**When Phase A's Signal Engine is mature and producing reliable signals.**

ACT-R inspired architecture for real-time trading decision support.

### Architecture

```
┌─────────────────────────────────────────────────────┐
│                 ATC-R Agent System                  │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │ Layer 1: Rule Engine (永远在线，低延迟)       │   │
│  │                                             │   │
│  │   Strategy DSL Rules (backtest optimized)   │   │
│  │   ├─ Factor threshold triggers              │   │
│  │   ├─ News classifier triggers               │   │
│  │   └─ Pattern: IF condition THEN activate    │   │
│  │                                             │   │
│  │   Conflict Resolution: select highest utility│   │
│  │                                             │   │
│  └─────────────────────────────────────────────┘   │
│                        ↓                           │
│                   Activation Event                 │
│                   {rule_id, symbol, context}       │
│                        ↓                           │
│  ┌─────────────────────────────────────────────┐   │
│  │ Layer 2: Agent Think (被激活才运行)           │   │
│  │                                             │   │
│  │   State Machine: idle → thinking → done     │   │
│  │                                             │   │
│  │   1. Context Aggregator                     │   │
│  │      - Gather factors, news, trades         │   │
│  │      - Time window: last 5min (configurable)│   │
│  │                                             │   │
│  │   2. LLM Reasoning                          │   │
│  │      - Pre-set prompt template (per rule)   │   │
│  │      - Input: context + trigger reason      │   │
│  │      - Output: analysis + action            │   │
│  │                                             │   │
│  └─────────────────────────────────────────────┘   │
│                        ↓                           │
│  ┌─────────────────────────────────────────────┐   │
│  │ Layer 3: Action                              │   │
│  │                                             │   │
│  │   - Notify (Telegram/Discord/Webhook)       │   │
│  │   - Generate Widget (template-based)        │   │
│  │   - Trigger downstream rule (optional)      │   │
│  │                                             │   │
│  └─────────────────────────────────────────────┘   │
│                                                     │
│  ═══════════════════════════════════════════════   │
│  Offline: Backtest Engine                          │
│  - Optimize rule thresholds                        │
│  - Validate prompt templates                       │
│  - Simulate agent decisions                        │
│  - Compute rule utility scores                     │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### ACT-R Concept Mapping

| ACT-R Concept | ATC-R Implementation | Notes |
|---------------|---------------------|-------|
| Production Rules | Strategy DSL Rules | IF-THEN patterns, backtest optimized |
| Pattern Matching | Rule Engine Trigger | Match current state → select rule |
| Declarative Memory | Context Aggregator | Retrieve relevant info (simplified: time window) |
| Goal Module | State Machine | Track: idle → thinking → done |
| Imaginal Module | LLM Reasoning | Problem solving with pre-set prompt |
| Learning | Backtest Optimization | Adjust thresholds, update utility |

### Rule Schema (Strategy DSL)

```yaml
# Example: Volume Spike + EMA Crossover Strategy
id: vol_spike_ema
name: "Volume spike with EMA crossover"
version: 1.0

# Trigger conditions (backtest optimized thresholds)
trigger:
  type: AND
  conditions:
    - factor: volume_ratio_5m
      op: gt
      value: 3.0        # Optimized via backtest

    - factor: price
      op: cross_above
      target: ema_20

  window: 60s          # Conditions must occur within 60s

# Utility score (learned from backtest)
utility: 0.85

# Context to gather when triggered
context:
  factors: [ema_20, volume_ratio_5m, trade_rate]
  factor_history: 5m   # Last 5 minutes
  recent_news: 3       # Top 3 relevant news
  recent_trades: 20    # Last 20 trades

# Pre-set prompt template for this rule
prompt_template: |
  You are analyzing a momentum signal for {{symbol}}.

  Trigger: Volume spike ({{volume_ratio_5m}}x) + price crossed above EMA20 ({{ema_20}})

  Recent context:
  - Price: {{current_price}}
  - Volume trend: {{volume_history}}
  - Recent news: {{news_summary}}

  Analyze:
  1. Is this a sustained breakout or false signal?
  2. Key levels to watch
  3. Recommended action (entry/wait/avoid)

  Be concise and actionable. Output as JSON:
  {
    "signal_quality": "strong|moderate|weak",
    "action": "notify|watch|ignore",
    "message": "...",
    "key_levels": {"support": ..., "resistance": ...}
  }

# Allowed actions
actions:
  - notify             # Send notification
  - create_widget      # Generate visualization widget
  - set_alert          # Set price/volume alert
  - trigger_rule       # Trigger another rule (chain)
```

---

## Task Breakdown

### Phase A: Signal System + Agent Factor Mining

**7.1 Signal Engine Core**
- [ ] Signal Engine service — subscribe to FactorEngine Redis channels, evaluate DSL rules
- [ ] Signal trigger — detect stocks entering top 20 with gain conditions
- [ ] Signal recording — log triggers and track subsequent returns

**7.2 Strategy DSL**
- [ ] Strategy DSL schema — YAML rule definition, factor conditions, thresholds
- [ ] DSL parser and validator — parse rules, validate factor references

**7.3 Agent Factor Mining**
- [ ] Agent orchestrator — manage factor mining loop sessions
- [ ] Factor mining skills — define_factor, run_backtest, evaluate_results, optimize_threshold
- [ ] Agent prompts — system prompts for factor hypothesis generation and evaluation

**7.4 Backtest Infrastructure**
- [ ] Pre-filter pipeline — SQL-based candidate stock selection from historical snapshots
- [ ] Backtest runner — evaluate rules on ClickHouse factor history
- [ ] New factor full-replay pipeline — trades → FactorEngine → ClickHouse
- [ ] Result store — persist and compare backtest outcomes

### Phase B: ACT-R Real-Time Agent (Future)

**7.5 News Pre-filter + Conflict Resolution**
- [ ] News Pre-filter - LLM threshold filter for news activation
- [ ] Conflict Resolution - select highest utility rule when multiple fire

**7.6 Agent Think**
- [ ] Agent BFF - WebSocket + HTTP interface for agent communication
- [ ] Context Aggregator - gather factors, news, trades when triggered
- [ ] Agent Reasoner - LLM with per-rule prompt templates
- [ ] Tool Registry - wrap services as callable tools

**7.7 Agent Decision Simulation + Prompt Validation**
- [ ] Agent Decision Simulation - simulate agent decisions in backtest mode
- [ ] Prompt Template Validation - evaluate LLM response quality in backtest

**7.8 Output & UX**
- [ ] Notification System - Telegram/Discord/Webhook
- [ ] Dynamic Widget - template-based widget generation
- [ ] Chat Interface - simple natural language interaction

### Infrastructure
- [ ] 7.9 Factor stream Rust migration - migrate from Python asyncio to Rust FactorBroadcaster when Signal Engine is introduced

---

## File Structure

```
python/src/jerry_trader/
  signal/                          # Phase A: Signal Engine
    __init__.py
    engine.py                      # Signal Engine core — factor listener, rule matching
    dsl/
      __init__.py
      schema.py                    # Rule Pydantic models (shared by Phase A & B)
      parser.py                    # YAML/JSON rule parser
      storage.py                   # Rule DB storage
    backtest/
      __init__.py
      runner.py                    # Evaluate rules on historical factor data
      pre_filter.py                # SQL candidate stock selection
      result_store.py              # Persist backtest results

  agent/                           # Phase A: Agent Factor Mining + Phase B: ACT-R
    __init__.py
    orchestrator.py                # Session management, scheduling
    skills/
      __init__.py
      define_factor.py             # Define new factors
      run_backtest.py              # Execute backtest
      evaluate.py                  # Compute metrics
      optimize.py                  # Threshold optimization
    prompts/                       # Agent system prompts
      __init__.py
      factor_mining.py             # Factor hypothesis generation prompts
    # Phase B additions (future):
    context/
      __init__.py
      aggregator.py                # Context gathering
    reasoner/
      __init__.py
      llm_client.py                # DeepSeek/Kimi API
      prompts.py                   # Template rendering
      parser.py                    # Output parsing
    bff/
      __init__.py
      main.py                      # FastAPI app
      websocket.py                 # WS handler
      routes.py                    # HTTP routes

frontend/src/
  agent/
    ChatInterface.tsx              # Phase B
    RuleManager.tsx                # View/edit DSL rules
    TriggerHistory.tsx             # Recent triggers
    BacktestResults.tsx            # Backtest result visualization
```

## Success Metrics

### Phase A
- Signal detection latency: < 100ms (real-time)
- Backtest speed: > 1000 rule evaluations per second (cached factors)
- Factor discovery: agent finds rules with positive expected returns
- Pre-filter accuracy: > 90% of actual top-20 entries captured

### Phase B
- Rule trigger latency: < 100ms
- False positive rate: < 20% (after optimization)
- LLM call frequency: < 10 per hour per symbol
- User engagement: > 5 meaningful triggers per session
