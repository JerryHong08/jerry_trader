# ML Strategy Discovery Progress

## 2026-05-01 - Architecture Review & Fixes

### Task 14.7: Add ML Event Definition - DONE

Added `ml_momentum_entry` event to `config/events.yaml`:
```yaml
- name: ml_momentum_entry
  stage: entry
  model:
    name: predictor_v1
    min_expected_return: 0.02
    min_confidence: 0.3
  hard_constraints:
    - factor: session_phase
      op: eq
      value: mid
    - factor: trade_rate
      op: lt
      value: 100
    - factor: rel_vol_20
      op: between
      value: [1.5, 10.0]
```

**Verified:** ML event parsing works correctly with `EventEvaluator.evaluate_ml_event()`.

### Task 14.8: Collect 30+ Days Data - BLOCKED

**Current data availability:**
- 10 trading days (2026-03-02 to 2026-03-13)
- Only 4 factor columns in backtest_results (rel_vol_20, trade_rate, price_direction, gap_pct)
- Missing: bid_ask_spread, entry_gap_pct, order_imbalance, quote_rate

**Action required:**
1. Run backtest pipeline to collect more historical data
2. Ensure all 8 factors are computed and stored

### Task 14.9: Retrain ML Model - PENDING

Waiting for task 14.8 completion.

### Task 14.10: A/B Testing Framework - PENDING

Future work after model improvement.

---

## 2026-05-01 - All Phases Complete

### Phase 1: Exploratory Analysis (Tasks 14.1.1-14.1.4) - DONE

Created `explorer.py` with:
- `BacktestExplorer` class for factor-return analysis
- `ExplorationResult` dataclass with correlations, distributions, thresholds
- `format_exploration_report()` for human-readable output
- `_compare_distributions()` for high/low return comparison with effect size

### Phase 2: ML Model Layer (Tasks 14.2.1-14.2.4) - DONE

Created `ml_model.py` with:
- `ReturnPredictor` class using GradientBoostingRegressor
- `TrainingResult` with cross-validation metrics
- `explain()` and `explain_batch()` methods with SHAP support (fallback when SHAP unavailable)
- `save()` and `load()` for model persistence (JSON config + joblib for sklearn model)

### Phase 3: Dynamic Strategy Layer (Tasks 14.3.1-14.3.4) - DONE

Created `strategy.py` with:
- `MLStrategy` class for ML-based entry decisions
- `StrategyDecision` with should_enter, expected_return, confidence, contributions
- `_compute_confidence()` for confidence scoring based on distance from threshold
- `batch_decide()` for batch processing

### Phase 4: Multi-date Validation (Tasks 14.4.1-14.4.4) - DONE

Created `validator.py` with:
- `StrategyValidator` for cross-date validation
- `ValidationReport` with stability metrics
- `DateMetrics` for per-date performance
- `_compute_stability_score()` combining positive ratio, return consistency, win rate consistency

### Files Created

| File | Purpose |
|------|---------|
| `explorer.py` | Factor-return analysis, correlation matrix, distribution comparison |
| `ml_model.py` | GradientBoostingRegressor training, prediction, SHAP explanation |
| `strategy.py` | ML-based entry decisions with confidence scoring |
| `validator.py` | Multi-date validation with stability scoring |

### Key Findings from 2026-03-13 Analysis

```
Factor-Return Correlations:
- rel_vol_20: -0.158 (most predictive, lower = better)
- trade_rate: -0.152 (lower = better)
- entry_gap_pct: -0.066
- gap_pct: -0.065

CRITICAL: Current strategy (momentum_entry) requires trade_rate > 30,
but data shows LOWER trade_rate correlates with BETTER returns.
This is a fundamental assumption error in the current strategy.
```

### Data Availability Note

- Available data: 10 trading days (2026-03-02 to 2026-03-13)
- Ideal minimum: 30 trading days for robust ML training
- Recommendation: Collect more historical data before production deployment

### Skipped Task

- **14.1.5**: Visualization output - skipped per user request for manual confirmation

---

## 2026-05-01 - Hybrid Architecture Implementation (Tasks 14.5.x)

### Task 14.5.1: Event Model Field Support - DONE

Extended `domain/event.py`:
- Added `ModelConfig` dataclass for ML model configuration
- Added `model` field to `Event` dataclass
- Added `hard_constraints` field for explicit risk control
- Added `matches_hard_constraints()` and `uses_ml()` methods

Updated `event_evaluator.py`:
- `_parse_event_yaml()` now parses `model` and `hard_constraints` from YAML
- Backward compatible with existing boolean-only events

### Task 14.5.2: EventEvaluator ML Integration - DONE

Created `services/model_registry.py`:
- `ModelRegistry` class for ML model management
- Lazy loading: models loaded on first access
- Caching: loaded models stay in memory
- Auto-discovery of models in `models/` directory

Updated `event_evaluator.py`:
- Added `MLEvaluationResult` dataclass
- Added `evaluate_ml_event()` method for ML-based events
- Added `match_signal_with_ml()` for hybrid evaluation
- Returns `(should_enter, expected_return, confidence)` tuple

### Task 14.5.3: SignalEngine Real-time Integration - DONE

Updated `services/signal/engine.py`:
- Added `MLEvaluationResult` import
- Updated `_evaluate_events()` to use `match_signal_with_ml()`
- Updated `_on_trigger()` to log ML prediction results
- Supports both boolean and ML-based events in real-time

### Task 14.5.4: hard_constraints Support - DONE

Integrated in Task 14.5.2:
- `evaluate_ml_event()` checks `hard_constraints` before ML prediction
- Explicit risk control layer that always runs
- Returns early if hard constraints not satisfied

### Task 14.5.5: Model Version Management - DONE

Created `models/` directory structure:
- `models/README.md` - Documentation for model management
- `models/v1/metadata.yaml` - Placeholder for first model version
- Directory structure supports versioning (v1, v2, etc.)
- Auto-discovery by `ModelRegistry`

### Design Document

Created `roadmap/ml-event-architecture.md`:
- Problem statement: linear assumptions, threshold sensitivity, no factor interactions
- Decision: Hybrid architecture (Boolean WATCH + ML ENTRY)
- Design rationale, architecture diagram, implementation tasks
- Risks and mitigations

---

## Task 14.6: Backtest Validation - DONE

### ML vs Boolean Strategy Comparison

Trained model on 55,527 signals (7 dates: 2026-03-02 to 2026-03-10),
tested on 25,588 signals (3 dates: 2026-03-11 to 2026-03-13).

**Data limitation**: Only 4 factor columns available in `backtest_results`
(rel_vol_20, trade_rate, price_direction, gap_pct). Missing: bid_ask_spread,
entry_gap_pct, order_imbalance, quote_rate.

**Model Performance**:
- R² on test set: -0.112 (poor — model doesn't generalize well)
- Feature importance: trade_rate (0.35), rel_vol_20 (0.29), gap_pct (0.22), price_direction (0.15)

**Strategy Comparison (test set)**:

| Strategy | Entry Rate | Avg Return | Win Rate |
|----------|-----------|------------|----------|
| Boolean (rel_vol>2.5, pd>0.7) | 24.2% | +1.28% | 42.2% |
| ML (expected>2%) | 5.1% | -2.79% | 22.7% |
| ML (expected>0%) | 18.3% | -2.05% | 31.8% |

**Key Findings**:
1. **Boolean strategy outperforms ML** on current data — ML model doesn't generalize
2. **Limited factor set** (4 vs 8) reduces ML prediction quality
3. **Negative R²** on test set indicates overfitting on training data
4. **trade_rate is most important** — confirms exploration finding (negative correlation)

**Recommendations**:
1. Collect more data (30+ days) before production ML deployment
2. Add missing factor columns to backtest_results (bid_ask_spread, entry_gap_pct, etc.)
3. Use `backtest_signals` table (80 signals with full factor set) for future training
4. Consider simpler models (linear regression) given limited data
5. Boolean strategy remains viable as baseline while ML improves

**Model saved to**: `models/v1/predictor.json` + `models/v1/predictor.joblib`

---

## Summary

| Phase | Status | Tasks |
|-------|--------|-------|
| 14.1 | DONE | 4/5 (14.1.5 skipped) |
| 14.2 | DONE | 4/4 |
| 14.3 | DONE | 4/4 |
| 14.4 | DONE | 4/4 |
| 14.5 | DONE | 5/5 |
| 14.6 | DONE | 1/1 |

**Total: 22/23 tasks complete (96%)**

**Remaining: 14.1.5 (visualization) — skipped per user request**

### Next Steps (Recommended)

1. Install SHAP for accurate explanation: `pip install shap`
2. Collect more historical data (target: 30+ trading days)
3. Run full training with multi-date validation
4. Integrate MLStrategy into SignalEngine for real-time prediction
5. Compare ML strategy performance vs current hardcoded thresholds

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    ML-Driven Strategy Discovery              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  explorer.py ──→ ml_model.py ──→ strategy.py ──→ validator │
│                                                             │
│  Factor-Return    Return         ML Entry      Stability    │
│  Correlation      Prediction     Decision      Validation   │
│                                                             │
│       ↓              ↓              ↓             ↓          │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌────────┐ │
│  │ PreFilter│    │ ML Model │    │ Signal   │    │ Multi  │ │
│  │ DataLoader│   │ Training │    │ Engine   │    │ Date   │ │
│  │ FactorEng│    │ Predict  │    │ Entry    │    │ Test   │ │
│  └──────────┘    └──────────┘    └──────────┘    └────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```
