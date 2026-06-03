# ML-Driven Strategy Discovery

## Context

Current strategy definition uses hardcoded thresholds in `events.yaml`:
```yaml
- factor: rel_vol_20
  op: gt
  value: 2.5
```

**Problems:**
1. Hard boundaries are arbitrary (2.49 vs 2.51)
2. Factor independence assumption is wrong
3. Monotonicity assumption is wrong (from reverse mining: trade_rate negatively correlated with return)
4. No data-driven discovery process

**Reverse Mining Findings (2026-03-13):**
- entry_gap_pct: +0.35 correlation (higher gap = better return)
- trade_rate: -0.58 correlation (lower trade rate = better return)
- rel_vol_20: -0.30 correlation (lower relative volume = better return)
- price_direction: -0.14 correlation (dip entry is better)

## Decision

Transition from **Boolean filter** to **ML-based prediction**:

```
Current: Signal = f(x₁, x₂, ...) ∈ {0, 1}  (hard threshold)
New:     Expected_Return = g(x₁, x₂, ...) ∈ ℝ  (continuous prediction)
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    ML-Driven Strategy Discovery              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐              │
│  │  数据层   │ →  │  探索层   │ →  │  模型层   │              │
│  │          │    │          │    │          │              │
│  │ PreFilter│    │ Explorer │    │ ML Model │              │
│  │ DataLoader│   │ 分析工具  │    │ 收益预测  │              │
│  │ FactorEng│    │ 可视化   │    │ 因子重要性│              │
│  └──────────┘    └──────────┘    └──────────┘              │
│       ↓              ↓              ↓                       │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐              │
│  │  回测层   │    │  策略层   │    │  部署层   │              │
│  │          │    │          │    │          │              │
│  │ Pipeline │    │ Strategy │    │ SignalEng│              │
│  │ 收益计算  │    │ 动态条件  │    │ 实时预测  │              │
│  │ 存储     │    │ 置信度   │    │ 风控     │              │
│  └──────────┘    └──────────┘    └──────────┘              │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Phase 1: Exploratory Analysis Tool (1-2 days)

**Goal:** Data → Insights pipeline

**Implementation:**
```python
# services/backtest/explorer.py

class BacktestExplorer:
    def analyze_factor_return_relation(self, date: str) -> dict:
        """Analyze factor-return correlation"""
        pass

    def find_profitable_regions(self, date: str) -> list:
        """Find high-return factor combinations using decision tree"""
        pass
```

**Output:**
```
Factor-Return Correlation:
  entry_gap_pct: +0.35
  trade_rate: -0.58
  rel_vol_20: -0.30

High Return Regions (return > 5%):
  Region 1: entry_gap_pct > 10%, trade_rate < 30
    → 120 signals, avg_return = 8.2%, win_rate = 65%

Suggested Strategy:
  WATCH: entry_gap_pct > 8%
  ENTRY: trade_rate < 30, rel_vol_20 < 5
```

## Phase 2: ML Model Layer (2-3 days)

**Goal:** Predict expected return

**Model Choice:** GradientBoostingRegressor
- Good for tabular data
- Interpretable (feature importance, SHAP)
- No deep learning (insufficient data, overfitting risk)

**Implementation:**
```python
# services/backtest/ml_model.py

class ReturnPredictor:
    def train(self, dates: list[str]) -> None:
        pass

    def predict(self, factors: dict) -> float:
        """Return expected return"""
        pass

    def get_feature_importance(self) -> dict:
        pass

    def explain(self, factors: dict) -> dict:
        """SHAP explanation"""
        pass
```

## Phase 3: Dynamic Strategy Layer (1-2 days)

**Goal:** Replace hardcoded thresholds with ML predictions

**Implementation:**
```python
# services/backtest/strategy.py

class MLStrategy:
    def should_enter(self, factors: dict) -> tuple[bool, float]:
        expected_return = self.predictor.predict(factors)
        return expected_return > self.min_expected_return, expected_return
```

**Simplified events.yaml:**
```yaml
events:
  - name: gap_up_watch
    stage: watch
    trigger: first_entry
    conditions:
      - factor: entry_gap_pct
        op: gt
        value: 4.0  # Keep simple WATCH condition

  - name: ml_entry
    stage: entry
    trigger: continuous
    pre_condition: gap_up_watch
    # No hardcoded conditions - use ML model
    model: return_predictor_v1
    min_expected_return: 0.02
```

## Phase 4: Multi-Date Validation (1 day)

**Goal:** Ensure strategy stability

**Implementation:**
```python
# services/backtest/validator.py

class StrategyValidator:
    def validate(self, strategy, dates: list[str]) -> dict:
        return {
            'avg_return': ...,
            'return_std': ...,
            'positive_days': ...,
            'stability_score': ...,
        }
```

## Files to Create

```
services/backtest/
├── explorer.py         # NEW: Exploratory analysis
├── ml_model.py         # NEW: ML model
├── strategy.py         # NEW: Dynamic strategy
├── validator.py        # NEW: Multi-date validation
```

## Files to Modify

```
config/events.yaml      # Simplify, remove hardcoded ENTRY conditions
services/signal/engine.py  # Integrate ML prediction
```

## Timeline

| Phase | Content | Time | Priority |
|-------|---------|------|----------|
| 1 | Exploratory analysis tool | 1-2 days | High |
| 2 | ML model layer | 2-3 days | High |
| 3 | Dynamic strategy layer | 1-2 days | Medium |
| 4 | Multi-date validation | 1 day | Medium |

**Total:** 5-8 days

## Risks

1. **Overfitting** - Mitigate with cross-validation, limit model complexity
2. **Data insufficiency** - Need at least 30 trading days
3. **Concept drift** - Model may need retraining over time
4. **Latency** - ML prediction adds latency to real-time signals

## Rejected Alternatives

1. **Deep Learning** - Insufficient data, overfitting risk
2. **Reinforcement Learning** - Too complex for current stage
3. **Online Learning** - Adds complexity, start with batch training
