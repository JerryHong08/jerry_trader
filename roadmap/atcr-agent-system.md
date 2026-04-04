# ATC-R Agent System

**Created:** 2026-04-05
**Status:** Design Phase
**Inspiration:** ACT-R Cognitive Architecture (adapted for trading)

## Core Insight

Agent 不适合直接 polling raw data。需要一个 **Rule Engine** 作为激活层，使用 **Strategy DSL**（backtest 优化过的规则）进行 real-time 监听，只有触发时 Agent 才介入进行深度思考。

## Architecture Overview

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

## ACT-R Concept Mapping

| ACT-R Concept | ATC-R Implementation | Notes |
|---------------|---------------------|-------|
| Production Rules | Strategy DSL Rules | IF-THEN patterns, backtest optimized |
| Pattern Matching | Rule Engine Trigger | Match current state → select rule |
| Declarative Memory | Context Aggregator | Retrieve relevant info (simplified: time window) |
| Goal Module | State Machine | Track: idle → thinking → done |
| Imaginal Module | LLM Reasoning | Problem solving with pre-set prompt |
| Learning | Backtest Optimization | Adjust thresholds, update utility |

**Simplifications from ACT-R:**
- No complex activation value computation → use simple time window
- No procedural memory buffer → use Tool Registry directly
- No multi-module synchronization → event-driven flow

## Rule Schema (Strategy DSL)

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

## Task Breakdown

### Phase 1: Rule Engine (7.1-7.3)

**7.1 Strategy DSL**
- Define rule schema (YAML/JSON format)
- Implement rule parser and validator
- Create rule storage (DB + file)
- Support rule CRUD operations

**7.2 News Pre-filter**
- Low-cost LLM classifier (DeepSeek)
- Filter criteria: relevance score > threshold
- Categories: earnings, product, management, regulatory
- Output: relevance_score, category, summary (no deep analysis)

**7.3 Rule Engine**
- Real-time factor/news listener
- Pattern matching: check conditions against current state
- Conflict resolution: select rule with highest utility
- Emit activation events

### Phase 2: Agent Think (7.4-7.7)

**7.4 Agent BFF**
- WebSocket endpoint for agent communication
- HTTP API for rule management
- Event streaming to frontend

**7.5 Context Aggregator**
- Collect factors at trigger time
- Retrieve recent news (filtered)
- Get recent trades
- Format as prompt inputs

**7.6 Agent Reasoner**
- LLM API integration (DeepSeek/Kimi)
- Prompt template rendering
- Parse LLM output → structured decision
- Handle errors/retries

**7.7 Tool Registry**
- Wrap services as callable tools:
  - `get_factors(symbol, timeframe)`
  - `get_news(symbol, limit)`
  - `get_trades(symbol, limit)`
  - `run_backtest(rule_id, date_range)`
  - `send_notification(message, channels)`
- Permission model per tool

### Phase 3: Backtest Integration (7.8-7.10)

**7.8 Strategy Backtest**
- Replay historical data through Rule Engine
- Count trigger events per rule
- Validate condition matching

**7.9 Threshold Optimization**
- Grid search for optimal trigger values
- Metric: signal quality vs false positive rate
- Output: recommended threshold adjustments

**7.10 Agent Decision Simulation**
- For each trigger in backtest, simulate agent decision
- Options:
  - Fast mode: cached/simulated response
  - Full mode: real LLM call (expensive)
- Evaluate decision quality

### Phase 4: Output & UX (7.11-7.13)

**7.11 Notification System**
- Telegram bot integration
- Discord webhook
- Generic webhook support
- Rate limiting

**7.12 Dynamic Widget**
- Template-based widget generation
- Widget types: chart, table, alert panel
- Mount to frontend Panel System

**7.13 Chat Interface**
- Simple chat UI
- Natural language → rule creation (optional)
- View active rules and triggers

## Data Flow Example

```
1. Real-time: Factor Engine computes volume_ratio_5m = 3.5

2. Rule Engine checks conditions:
   - volume_ratio_5m > 3.0 ✓
   - price > ema_20 ✓
   - Both within 60s ✓

3. Rule "vol_spike_ema" matched, utility=0.85

4. Activation Event emitted:
   {
     rule_id: "vol_spike_ema",
     symbol: "PRSO",
     timestamp: 1709...,
     trigger_values: {volume_ratio_5m: 3.5, ...}
   }

5. Agent Think:
   - Context Aggregator: fetch factors, news, trades
   - Render prompt with context
   - LLM generates analysis

6. LLM Output:
   {
     signal_quality: "moderate",
     action: "notify",
     message: "PRSO volume spike 3.5x, price $45.20 above EMA20 $44.80.
               Watch for sustained volume. Entry above $45.50.",
     key_levels: {support: 44.80, resistance: 45.50}
   }

7. Actions executed:
   - Send Telegram notification
   - Optionally: create widget with key levels
```

## Why This Design

1. **Rule as First Citizen** — Rules are testable, optimizable, verifiable
2. **Cost Efficient** — LLM only called on significant events, not every tick
3. **Low Latency Perception** — Rule Engine runs continuously, catches signals fast
4. **Explainable** — Each activation has clear trigger reason + prompt template
5. **Backtest Driven** — Rules validated on historical data before production use
6. **Scalable** — Multiple rules can monitor multiple symbols simultaneously

## Open Questions

1. **Rule Priority** — How to handle multiple rules triggering simultaneously?
2. **Prompt Template Evolution** — Can templates be improved via backtest feedback?
3. **Agent State Persistence** — Should agent remember previous analyses?
4. **Cross-symbol Rules** — Rules that compare multiple symbols?
5. **Rule Chaining** — One rule triggers another? Loop prevention?

## File Structure

```
python/src/jerry_trader/agent/
  __init__.py
  rules/
    __init__.py
    schema.py           # Rule Pydantic models
    parser.py           # YAML/JSON parser
    storage.py          # Rule DB storage
  engine/
    __init__.py
    matcher.py          # Pattern matching
    resolver.py         # Conflict resolution
    executor.py         # Trigger execution
  context/
    __init__.py
    aggregator.py       # Context gathering
  reasoner/
    __init__.py
    llm_client.py       # DeepSeek/Kimi API
    prompts.py          # Template rendering
    parser.py           # Output parsing
  backtest/
    __init__.py
    simulator.py        # Rule simulation
    optimizer.py        # Threshold optimization
    evaluator.py        # Decision quality metrics
  bff/
    __init__.py
    main.py             # FastAPI app
    websocket.py        # WS handler
    routes.py           # HTTP routes

frontend/src/
  agent/
    ChatInterface.tsx
    RuleManager.tsx     # View/edit rules
    TriggerHistory.tsx  # Recent triggers
```

## Success Metrics

- Rule trigger latency: < 100ms
- False positive rate: < 20% (after optimization)
- LLM call frequency: < 10 per hour per symbol
- Backtest speed: > 1000 triggers per second (cached mode)
- User engagement: > 5 meaningful triggers per session
