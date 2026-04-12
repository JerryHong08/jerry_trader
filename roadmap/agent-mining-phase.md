# Agent Strategy Mining Phase

**Status: ACTIVE**

## Overview

Systematic exploration of factor-based trading strategies for pre-market momentum.

Current state: Single-factor (trade_rate), limited depth.
Target state: Multi-factor composition, validated across dates, optimized per-ticker.

## Mining Dimensions

### 1. Factor Expansion

| Factor | Priority | Description | Implementation |
|--------|----------|-------------|----------------|
| relative_volume | P0 | Core signal — volume vs avg | BarIndicator |
| price_direction | P0 | Buy/sell pressure | TickIndicator |
| gap_percent | P1 | Entry position filter | From snapshot |
| bid_ask_spread | P1 | Liquidity quality | QuoteIndicator |
| large_trade_ratio | P2 | Institutional activity | TickIndicator |

### 2. Data Source Integration

| Source | Current | Target | Mining Value |
|--------|---------|--------|--------------|
| Trades | ✓ (trade_rate) | Full tick analysis | Direction, clustering |
| Quotes | ✗ | Quote factors | Spread, depth |
| News | ✗ | News factors | Catalyst type |
| Bars | Partial | Bar patterns | Structure, momentum |

### 3. Rule Composition

Evolution path:

```
Phase 1: trade_rate > threshold
         ↓
Phase 2: trade_rate + relative_volume
         ↓
Phase 3: trade_rate + rel_vol + price_direction
         ↓
Phase 4: trade_rate + rel_vol + price_dir + gap_percent + spread
         ↓
Phase 5: Multi-rule strategies (entry + exit + filter)
```

### 4. Parameter Optimization

Systematic sweeps:

```python
# Example grid search
trade_rate: [100, 150, 200, 250]
relative_volume: [2.0, 3.0, 5.0, 8.0]
gap_percent: [(3, 10), (5, 15), (10, 20)]
price_direction: [0.3, 0.5, 0.7]
```

Output: Optimal parameter matrix per ticker type.

### 5. Validation Standard

- **Minimum**: 10 trading days cross-validation
- **Metrics**: win_rate > 50%, avg_return > 0%, profit_factor > 1.0
- **Stability**: Parameter drift < 20% across dates

## Current Findings

### trade_rate threshold analysis (2026-03-13)

| Threshold | Signals | Win Rate | Avg Return | Profit Factor |
|-----------|---------|----------|------------|---------------|
| 200 | 4 | 25% | -7.03% | 0.02 |
| 100 | 25 | 36% | -3.49% | 0.39 |

**Insight**: Lower threshold = more signals, better metrics. But signal quality varies.

### Per-ticker performance

| Ticker | Signals | Win Rate | Avg Return | Key Observation |
|--------|---------|----------|------------|------------------|
| BIAF | 13 | 62% | +0.65% | Success — persistent momentum |
| KIDZ | 8 | 0% | -6.82% | Failure — no follow-through |
| EDHL | 2 | 50% | -1.68% | Neutral |
| FRSX | 1 | 0% | -18.35% | Failure |
| ACXP | 1 | 0% | -19.46% | Failure |

**Insight**: Ticker-specific behavior. BIAF has real buyers, KIDZ may be passive selling.

## Task Roadmap

### Phase 1: Core Factors (Week 1)

- [ ] #37 Implement relative_volume factor
- [ ] #46 Implement price_direction factor
- [ ] #43 Implement gap_percent factor
- [ ] #47 Quote data integration for backtest
- [ ] #38 Implement bid_ask_spread factor

### Phase 2: Mining & Validation (Week 2)

- [ ] #41 Threshold sweep mining
- [ ] #42 Multi-condition rule composition
- [ ] #48 BIAF vs KIDZ analysis
- [ ] #39 Multi-date validation workflow

### Phase 3: Advanced Integration (Week 3)

- [ ] #44 Implement large_trade_ratio factor
- [ ] #45 News sentiment factor integration
- [ ] #40 Ticker-specific adaptive thresholds

## Success Metrics

| Stage | Win Rate | Avg Return | PF | Signals/Day |
|-------|----------|------------|-----|-------------|
| Current | 36% | -3.49% | 0.39 | 25 |
| Target Phase 1 | 45% | +1.0% | 1.0 | 20 |
| Target Phase 2 | 55% | +2.5% | 1.5 | 15 |
| Target Phase 3 | 60% | +4.0% | 2.0 | 10 |

## Mining Workflow

Agent executes daily:

1. **Define** — Create/update rule YAML
2. **Run** — `poetry run python -m jerry_trader.services.backtest.cli --date YYYY-MM-DD --record-experiment --hypothesis "..."`
3. **Evaluate** — Check exp_XXX.yaml results
4. **Query** — `get_all_lessons()` to learn from past experiments
5. **Sweep** — Parameter grid search
6. **Validate** — Multi-date parallel run
7. **Iterate** — Next parameter set

## Notes

- Experiment logs auto-generated with `--record-experiment` (~38 lines per file)
- Knowledge accumulated in `experiments/knowledge.yaml`
- Query via Python: `find_experiments_by_hypothesis()`, `get_all_lessons()`, `get_ticker_insights()`
- BIAF/KIDZ comparison is key case study for success/failure distinction

---

Created: 2026-04-12
Related: ROADMAP.md 7.3 (Agent Factor Mining)

## Infrastructure: Experiment Framework (Simplified)

详见 [experiment-framework.md](experiment-framework.md)

### 核心设计

简单有效的实验记录：

```
experiments/
├── 2026-04-13/exp_001.yaml   # ~38行
├── knowledge.yaml            # 经验积累
└── schema.md                 # 字段定义
```

### 使用方式

```bash
# 记录实验
poetry run python -m jerry_trader.services.backtest.cli \
    --date 2026-03-13 --record-experiment \
    --hypothesis "Lower threshold increases signals"

# 查询经验
from jerry_trader.services.backtest.experiment_logger import get_all_lessons
lessons = get_all_lessons()
```

### 验证门槛

| Stage | Requirements |
|-------|-------------|
| quick_check | signals > 5, win_rate > 30% |
| thorough | dates > 10, signals > 100 |
| paper | 5+ live days |
| production | Sustained profitability |
