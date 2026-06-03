# ML-Driven Event Architecture Upgrade

## Context

### Problem Statement

Current event-based strategy uses hardcoded Boolean thresholds:

```yaml
conditions:
  - factor: rel_vol_20
    op: gt
    value: 2.5
```

**Fundamental Issues:**

1. **Linear Assumption**: Each condition is evaluated independently
2. **Threshold Sensitivity**: 2.49 vs 2.51 are treated completely differently
3. **No Interaction**: Non-linear relationships between factors are ignored
4. **Arbitrary Boundaries**: Thresholds are manually chosen without data support

### Data Analysis Findings (2026-03-13)

```
Factor-Return Correlations:
- rel_vol_20: -0.158 (negative correlation)
- trade_rate: -0.152 (negative correlation)

High Return Signals (>5%):
- rel_vol_20 mean: 2.43 (optimal range, not simply "lower is better")
- trade_rate mean: 19.81

Low Return Signals (<-5%):
- rel_vol_20 mean: 3.88
- trade_rate mean: 21.19
```

**Key Insight**: There exist optimal ranges, not simple threshold judgments.

### Current Strategy Assumption is WRONG

```yaml
# Current momentum_entry
conditions:
  - factor: rel_vol_20
    op: gt        # WRONG: data shows negative correlation
    value: 2.5
```

Data shows `rel_vol_20` is negatively correlated with returns, meaning lower values are better. The current strategy assumes the opposite.

---

## Decision

**Upgrade to Hybrid Architecture**: Boolean rules for WATCH stage + ML prediction for ENTRY stage.

```
┌─────────────────────────────────────────────────────────────┐
│                    Hybrid Architecture                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  WATCH Stage (选股)         ENTRY Stage (入场决策)          │
│  ┌─────────────────┐       ┌─────────────────────────┐      │
│  │ Boolean Rules   │  ──►  │ ML Prediction           │      │
│  │ (Fast Filter)   │       │ (Precise Decision)      │      │
│  │                 │       │                         │      │
│  │ entry_gap_pct   │       │ expected_return = f(x)  │      │
│  │ > 4%            │       │ if expected > threshold │      │
│  │                 │       │   → ENTER               │      │
│  └─────────────────┘       └─────────────────────────┘      │
│                                     +                       │
│                             ┌─────────────────┐             │
│                             │ Hard Constraints│             │
│                             │ (Risk Control)  │             │
│                             │ session_phase   │             │
│                             │ = mid           │             │
│                             └─────────────────┘             │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Design Rationale

### Why Hybrid Instead of Pure ML?

| Aspect | Pure Boolean | Pure ML | Hybrid |
|--------|-------------|---------|--------|
| Interpretability | High | Low | Medium |
| Speed | Fast | Slower | Fast (WATCH) + Slower (ENTRY) |
| Accuracy | Low | High | High |
| Risk Control | Manual | Implicit | Explicit (hard_constraints) |
| Maintenance | Easy | Hard | Medium |

**Hybrid Advantages:**

1. **Interpretability**: WATCH stage still uses simple rules (easy to understand)
2. **Performance**: ML only runs on pre-filtered candidates
3. **Safety**: hard_constraints as explicit risk control
4. **Gradual Migration**: No need to rewrite all events at once

### Why Not Just Fix Thresholds?

Fixing thresholds still assumes:
- Linear relationships
- Independent factors
- Single optimal point

ML captures:
- Non-linear relationships
- Factor interactions
- Continuous prediction

---

## Architecture Changes

### 1. Event Model Extension

```yaml
# Before
- name: momentum_entry
  stage: entry
  conditions:
    - factor: rel_vol_20
      op: gt
      value: 2.5

# After
- name: ml_entry
  stage: entry
  pre_condition: gap_up_watch

  # ML prediction instead of conditions
  model:
    name: return_predictor_v1
    min_expected_return: 0.02
    min_confidence: 0.5

  # Hard constraints as risk control
  hard_constraints:
    - factor: session_phase
      op: eq
      value: mid
```

### 2. EventEvaluator Changes

```python
# Before
def evaluate(self, factors: dict) -> bool:
    for condition in self.conditions:
        if not self._check_condition(condition, factors):
            return False
    return True

# After
def evaluate(self, factors: dict) -> tuple[bool, float, float]:
    """Returns (should_enter, expected_return, confidence)"""

    # Check hard constraints first
    for constraint in self.hard_constraints:
        if not self._check_condition(constraint, factors):
            return False, 0.0, 0.0

    # ML prediction
    expected_return = self.model.predict(factors)
    confidence = self._compute_confidence(factors, expected_return)

    should_enter = (
        expected_return >= self.min_expected_return
        and confidence >= self.min_confidence
    )

    return should_enter, expected_return, confidence
```

### 3. SignalEngine Integration

```python
class SignalEngine:
    def __init__(self):
        self.model_registry = ModelRegistry()
        self.model_registry.load("return_predictor_v1")

    def evaluate_entry(self, ticker: str, factors: dict) -> EntryDecision:
        event = self.get_entry_event(ticker)

        if event.model:
            model = self.model_registry.get(event.model.name)
            expected_return = model.predict(factors)
            # ...
        else:
            # Fallback to boolean evaluation
            # ...
```

---

## Implementation Tasks

### 14.5.1 Event Model Field Support
- Extend `Event` dataclass with `model` and `hard_constraints` fields
- Update `events.yaml` parser to handle new fields
- Backward compatible with existing boolean conditions

### 14.5.2 EventEvaluator ML Integration
- Load ML model from model registry
- Replace boolean evaluation with ML prediction
- Return `(should_enter, expected_return, confidence)` tuple

### 14.5.3 SignalEngine Real-time Integration
- Model registry with lazy loading
- Real-time factor → prediction pipeline
- Performance optimization (model caching)

### 14.5.4 Hard Constraints Support
- Evaluate hard constraints before ML prediction
- Combine ML prediction + hard constraints
- Explicit risk control layer

### 14.5.5 Model Version Management
- `models/` directory structure
- Model hot-reload without restart
- A/B testing support (future)

### 14.6 Backtest Validation
- Compare ML entry vs boolean entry
- Metrics: win_rate, expected_return, stability
- Validate on multiple dates

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| ML model latency | Model caching, batch prediction |
| Model drift | Periodic retraining, monitoring |
| Overfitting | Cross-validation, limit model complexity |
| Data insufficiency | Start with 10 days, collect more over time |

---

## Rejected Alternatives

1. **Pure ML for all stages**: Too complex, loses interpretability
2. **Complex DSL for conditions**: Still assumes linear relationships
3. **Deep Learning**: Insufficient data, overfitting risk

---

## References

- `roadmap/ml-strategy-discovery.md` - Original ML strategy design
- `roadmap/ml-progress.md` - Implementation progress
- `python/src/jerry_trader/services/backtest/explorer.py` - Factor analysis
- `python/src/jerry_trader/services/backtest/ml_model.py` - ML model
- `python/src/jerry_trader/services/backtest/strategy.py` - ML strategy
- `python/src/jerry_trader/services/backtest/validator.py` - Validation
