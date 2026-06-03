# Backtest & Mining Architecture

## Overview

The backtest system validates trading strategies against historical data. It has evolved from a **Rule-based** system to an **Event-based** system based on validation findings (see roadmap/event-framework-validation.md).

**Key Finding**: Factor ranking (IC-based) is ineffective. Boolean Event selection (Avg Return + Win Rate) works.

---

## Current Architecture (Post Refactor)

### Module Map

```
services/backtest/
├── runner.py (414 lines)          # Single-date backtest pipeline (Rule-based)
├── config.py (177 lines)          # BacktestConfig, TickerData, PreFilterConfig
├── batch_engine.py (246 lines)    # FactorEngineBatchAdapter - compute factors
├── evaluator.py (226 lines)       # SignalEvaluator - evaluate Rule DSL
├── metrics.py (303 lines)         # Return/MFE/MAE computation
├── data_loader.py (350 lines)     # Load trades/quotes/bars from Parquet/CH
├── pre_filter.py (332 lines)      # Candidate selection from market_snapshot
├── output.py (362 lines)          # Persist to ClickHouse, print summary
├── experiment_logger.py (355 lines) # Experiment logging
├── mining.py (520 lines)          # [NEW] EventMiner - Boolean Event mining
├── event_evaluator.py (~250 lines) # [NEW] EventEvaluator - Boolean filter
├── event_validator.py (211 lines) # [NEW] EventValidator - Avg Return validation
└── data/                          # Data preparation utilities
    ├── snapshot_builder.py        # Build market snapshot
    ├── trading_calendar.py        # Trading day calendar
    └── ...

domain/
├── event.py (187 lines)           # [NEW] Event, Condition - Boolean signal definition
├── session.py (66 lines)          # [NEW] SessionPhase - time window classification
├── strategy/
│   ├── rule.py                    # [KEEP] Rule DSL - used by runner.py
│   └ rule_parser.py               # [KEEP] Load rules from YAML
└── backtest/
    └── types.py                   # [KEEP] Candidate, SignalResult, BacktestResult
```

### Line Count Comparison

| Component | Before | After | Reduction |
|-----------|--------|-------|-----------|
| mining.py | 1377 lines | 520 lines | **-857 lines (62%)** |
| event_evaluator.py | 199 lines | ~250 lines | +50 lines |
| **Total** | ~1576 lines | ~770 lines | **-800 lines** |

---

## Data Flow

### Event Mining Pipeline (mining.py)

```
ClickHouse (market_snapshot, trades)
         │
         ▼
    ┌─────────────────┐
    │    PreFilter    │  → Select candidates (top N, min gain)
    └─────────────────┘
         │
         ▼
    ┌─────────────────┐
    │    DataLoader   │  → Load trades/quotes/bars per ticker
    └─────────────────┘
         │
         ▼
    ┌──────────────────┐
    │ FactorEngineBatch │  → Compute factor timeseries
    └──────────────────┘
         │
         ▼
    ┌─────────────────┐
    │ EventEvaluator  │  → Boolean Event selection (accept/reject)
    └─────────────────┘
         │
         ▼
    ┌─────────────────┐
    │ Compute Returns │  → Entry price, horizon returns
    └─────────────────┘
         │
         ▼
    ┌─────────────────┐
    │  EventValidator │  → Validate with Avg Return + Win Rate
    └─────────────────┘
```

### runner.py (Single-date Rule Backtest)

```
Same pipeline but uses SignalEvaluator + Rule DSL instead of EventEvaluator
```

---

## Deleted Code (Rule-based)

The following components were removed during Phase 2 refactoring:

| Deleted | Lines | Purpose |
|---------|-------|---------|
| `MiningConfig` | ~20 | Rule-based configuration |
| `FactorSearchSpace` | ~15 | Factor threshold grid |
| `FactorCombination` | ~5 | Factor combination config |
| `MiningYamlConfig` | ~50 | YAML config parser |
| `MiningResult` | ~20 | Rule result dataclass |
| `AggregateMiningResult` | ~20 | Multi-date Rule aggregate |
| `generate_candidates_from_config()` | ~70 | Generate Rule candidates |
| `load_yaml_config()` | ~5 | Load YAML mining config |
| `mine_batch()` | ~100 | Batch Rule evaluation |
| `mine_batch_multi_date()` | ~80 | Multi-date Rule mining |
| `_evaluate_rule_with_shared_data()` | ~100 | Rule signal evaluation |
| `mine_from_config()` | ~40 | Config-driven mining |
| `validate_strategy()` | ~40 | Multi-date Rule validation |
| `aggregate_results()` | ~90 | Rule aggregation |
| `report_aggregate()` | ~50 | Rule report generation |
| `_record_mining_experiment()` | ~80 | Experiment logging |
| `_extract_lessons()` | ~40 | Lesson extraction |
| `_update_knowledge()` | ~40 | Knowledge update |
| `report()` | ~40 | Rule summary |
| CLI Rule-based options | ~50 | --config, --parallel |

**Total deleted**: ~857 lines

---

## Key Components (Event-based)

### EventMiner (mining.py)

```python
class EventMiner:
    """Event-based mining using Boolean selection."""

    def __init__(self, top_n=20, gain_threshold=2.0, slippage=0.001):
        self.top_n = top_n
        self.gain_threshold = gain_threshold
        self.slippage = slippage

    def _prepare_shared_data(self, date) -> SharedBacktestData:
        """One-time preprocessing for all events."""

    def mine_events(self, dates, events) -> dict[str, EventValidationResult]:
        """Evaluate Events and validate with Avg Return + Win Rate."""

    def _compute_event_returns(self, signals, shared_data) -> list[dict]:
        """Compute entry price and horizon returns."""
```

### EventEvaluator (event_evaluator.py)

```python
class EventEvaluator:
    """Boolean Event selection - accept/reject, no ranking."""

    def evaluate_ticker(self, event, symbol, factor_ts, ...):
        """Evaluate Event against factor timeseries."""
        # For each timestamp:
        #   1. Compute session_phase
        #   2. Check anti-patterns → reject
        #   3. Check valid events → accept

    def match_signal(self, signal) -> Event | None:
        """Match signal to Event."""
```

### EventValidator (event_validator.py)

```python
class EventValidator:
    """Validate with Avg Return + Win Rate, not IC."""

    def validate(self, event, signals_by_date) -> EventValidationResult:
        """Compute metrics across dates."""
        # Metrics: avg_return, win_rate, positive_dates_ratio, return_std

    def compare_events(self, results) -> str:
        """Print comparison table."""
```

---

## CLI

```bash
# Event Mining (only mode)
poetry run python -m jerry_trader.services.backtest.mining \
  --date-range 2026-03-02 2026-03-06 \
  --top-n 20 \
  --gain-threshold 2.0

# Single date
poetry run python -m jerry_trader.services.backtest.mining --date 2026-03-02
```

---

## Validation Results

After Phase 1 fix, event mining works:

| Event | Signals | Avg Return | Win Rate | Positive Dates |
|-------|---------|------------|----------|----------------|
| reversal_entry_neutral_gap | 228 (5 dates) | -0.97% | 33.18% | 40% |
| dip_buy_neutral_gap | 319 (5 dates) | -0.32% | 41.23% | 40% |

**Status**: Events generate signals correctly. Results show Event effectiveness needs improvement (per roadmap findings).

---

## Dependencies

```
mining.py (EventMiner)
    ├── SharedBacktestData (in-file)
    ├── EventEvaluator (event_evaluator.py)
    │   └── Event, Condition (domain/event.py)
    │   └── SessionPhase (domain/session.py)
    ├── EventValidator (event_validator.py)
    ├── PreFilter (pre_filter.py)
    ├── DataLoader (data_loader.py)
    ├── FactorEngineBatchAdapter (batch_engine.py)
    └── BacktestConfig (config.py)

runner.py (unchanged - Rule-based single backtest)
    ├── BacktestConfig (config.py)
    ├── SignalEvaluator (evaluator.py)
    │   └── Rule (domain/strategy/rule.py)
    ├── PreFilter, DataLoader, FactorEngine, Metrics
```

---

*Generated: 2026-04-22*
*Refactored: Phase 1 (fix mine_events), Phase 2 (delete ~857 lines Rule-based code)*

### Data Flow

#### Standard Backtest Pipeline (runner.py)

```
Raw Data (Parquet/ClickHouse)
         │
         ▼
    ┌─────────────┐
    │  PreFilter  │  → Select candidates from market_snapshot
    └─────────────┘
         │
         ▼
    ┌─────────────┐
    │  DataLoader │  → Load trades/quotes/bars per ticker
    └─────────────┘
         │
         ▼
    ┌──────────────────┐
    │ FactorEngineBatch │  → Compute factor timeseries
    └──────────────────┘
         │
         ▼
    ┌─────────────────┐
    │  SignalEvaluator│  → Evaluate rules, find triggers
    └─────────────────┘
         │
         ▼
    ┌─────────────┐
    │   Metrics   │  → Compute returns/MFE/MAE
    └─────────────┘
         │
         ▼
    ┌─────────────┐
    │   Output    │  → Persist to ClickHouse
    └─────────────┘
```

#### Current Event Mining (broken)

```
ClickHouse backtest_results  ← [PROBLEM: depends on old pipeline]
         │
         ▼
    ┌─────────────────┐
    │  EventEvaluator │  → Classify signals by Event
    └─────────────────┘
         │
         ▼
    ┌─────────────────┐
    │  EventValidator │  → Validate with Avg Return
    └─────────────────┘
```

---

## Problem Analysis

### 1. `mine_events()` Logic Error

```python
# Current implementation (WRONG):
SELECT ... FROM backtest_results WHERE date = %(date)s
# Problem: No signals exist - old Rule-based pipeline was removed
```

**Correct flow should be**:
```python
# Prepare data (reuse runner pipeline)
shared_data = _prepare_shared_data(date)

# Evaluate Events directly (like SignalEvaluator)
for event in events:
    for ticker, factor_ts in shared_data.factor_ts_map.items():
        signals = event_evaluator.evaluate_ticker(event, ticker, factor_ts)

# Validate
result = event_validator.validate(event, signals_by_date)
```

### 2. Code Duplication

| Old (Rule-based) | New (Event-based) | Overlap |
|------------------|-------------------|---------|
| `MiningResult` | `EventValidationResult` | Same metrics |
| `mine_batch()` | `mine_events()` | Same preprocessing |
| `_evaluate_rule_with_shared_data()` | (missing) | Should reuse |

### 3. Old Code Residue (~500 lines)

```python
# In mining.py:
class MiningConfig          # Rule-based config - DELETE
class FactorSearchSpace     # Rule search space - DELETE
class FactorCombination     # Rule combination - DELETE
class MiningResult          # Rule result - DELETE
class AggregateMiningResult # Rule aggregate - DELETE

generate_candidates_from_config()  # Generate Rules - DELETE
mine_batch()                       # Evaluate Rules - DELETE
mine_batch_multi_date()            # Multi-date Rules - DELETE
mine_from_config()                 # Config-based mining - DELETE
validate_strategy()                # Rule validation - DELETE
aggregate_results()                # Rule aggregation - DELETE
report_aggregate()                 # Rule report - DELETE
```

---

## Refactoring Plan

### Phase 1: Fix Event Mining (Critical)

**Goal**: Make `mine_events()` work without depending on old pipeline.

**Changes**:

1. **Add `EventEvaluator.evaluate_ticker()`**
   ```python
   def evaluate_ticker(
       self,
       event: Event,
       symbol: str,
       factor_ts: FactorTimeseries,
       session_start_ms: int,
       session_end_ms: int,
   ) -> list[dict]:
       """Evaluate single Event against ticker's factor timeseries."""
       signals = []
       for ts_ms, factors in factor_ts.items():
           if not session_start_ms <= ts_ms < session_end_ms:
               continue

           signal = {
               "trigger_time_ns": ts_ms * 1_000_000,
               **factors,
           }

           if event.matches(signal):
               signals.append(signal)

       return signals
   ```

2. **Rewrite `mine_events()`**
   ```python
   def mine_events(
       self,
       dates: list[str],
       events: list[Event] | None = None,
   ) -> dict[str, EventValidationResult]:

       if events is None:
           events = VALIDATED_EVENTS

       evaluator = EventEvaluator(events=events)
       validator = EventValidator()

       # Collect signals per event per date
       event_signals: dict[str, dict[str, list[dict]]] = {}

       for date in dates:
           # Prepare shared data (reuse existing optimization)
           shared_data = self._prepare_shared_data(date)

           for event in events:
               date_signals = []

               for symbol, factor_ts in shared_data.factor_ts_map.items():
                   sigs = evaluator.evaluate_ticker(
                       event, symbol, factor_ts,
                       shared_data.session_start_ms,
                       shared_data.session_end_ms,
                   )
                   date_signals.extend(sigs)

               # Compute returns for signals
               signals_with_returns = self._compute_returns(
                   date_signals, shared_data
               )

               event_signals[event.name][date] = signals_with_returns

       # Validate each event
       results = {}
       for event in events:
           result = validator.validate(event, event_signals[event.name])
           results[event.name] = result

       return results
   ```

3. **Add `_compute_returns()` helper**
   - Reuse `compute_batch_metrics()` from metrics.py
   - Adapt to work with dict signals instead of TriggerPoint

**Estimated lines**: ~150 new/modified, reuse existing ~300 lines.

### Phase 2: Delete Old Code

**Files to modify**:
- `mining.py`: Delete ~500 lines of Rule-based code
- `cli.py`: Remove `--config`, `--parallel` options for Rule mining

**Keep**:
- `SharedBacktestData` - data preprocessing optimization
- `_prepare_shared_data()` - one-time preprocessing
- `get_trading_dates()` - trading calendar helper

**Estimated reduction**: mining.py from 1377 → ~600 lines.

### Phase 3: Simplify Data Structures

**Current duplication**:
```python
# Rule-based (OLD)
MiningResult: rule_id, total_signals, win_rate_5m, avg_return_5m, profit_factor
AggregateMiningResult: rule_id, dates_tested, avg_win_rate_5m, avg_profit_factor

# Event-based (NEW)
EventValidationResult: event_name, total_signals, avg_return, win_rate, positive_dates_ratio
```

**Proposed unified structure**:
```python
@dataclass
class MiningResult:
    """Unified result for Event mining."""
    name: str  # Event name
    dates_tested: int
    total_signals: int
    avg_return: float  # Primary metric
    win_rate: float
    positive_dates_ratio: float  # Stability
    return_std: float
    per_date: dict[str, DateResult]

@dataclass
class DateResult:
    """Single date result."""
    date: str
    signals: int
    avg_return: float
    win_rate: float
```

### Phase 4: CLI Simplification

**Current CLI** (complex):
```bash
# Rule-based (multiple modes)
--config mining.yaml --date-range ... --parallel 4
--date ... --record-experiment

# Event-based
--events --date-range ...
```

**Proposed CLI** (simple):
```bash
# Single mode: Event mining
poetry run python -m jerry_trader.services.backtest.mining \
  --date-range 2026-03-02 2026-03-13

# Options
--events config/events.yaml  # Custom events config
--output experiments/        # Save results
```

---

## Dependency Graph (After Refactor)

```
mining.py
    ├── SharedBacktestData (config.py)
    ├── EventEvaluator (event_evaluator.py)
    │   └── Event, Condition (domain/event.py)
    │   └── SessionPhase (domain/session.py)
    ├── EventValidator (event_validator.py)
    │   └── Event (domain/event.py)
    ├── FactorEngineBatchAdapter (batch_engine.py)
    ├── DataLoader (data_loader.py)
    ├── PreFilter (pre_filter.py)
    ├── metrics.compute_batch_metrics (metrics.py)
    └── trading_calendar (data/trading_calendar.py)

runner.py (unchanged - for single-date backtest)
    ├── BacktestConfig (config.py)
    ├── SignalEvaluator (evaluator.py)
    │   └── Rule (domain/strategy/rule.py)
    ├── ... (same as current)
```

---

## File Changes Summary

| File | Before | After | Action |
|------|--------|-------|--------|
| `mining.py` | 1377 lines | ~600 lines | Delete old code, rewrite `mine_events()` |
| `event_evaluator.py` | 199 lines | ~250 lines | Add `evaluate_ticker()` |
| `cli.py` | 716 lines | ~400 lines | Remove Rule-based options |
| `domain/strategy/rule.py` | Keep | Keep | Still used by `runner.py` |
| `domain/event.py` | 187 lines | Keep | Core Event definition |
| `config/mining.yaml` | Delete | Delete | Replaced by `events.yaml` |

**Net reduction**: ~1300 lines deleted, ~150 lines added.

---

## Implementation Order

1. **Phase 1** (Priority: Critical)
   - Add `EventEvaluator.evaluate_ticker()`
   - Rewrite `mine_events()` to generate signals directly
   - Add `_compute_returns()` helper
   - Test with 3 dates to verify it works

2. **Phase 2** (Priority: High)
   - Delete Rule-based classes and functions from `mining.py`
   - Remove Rule-based CLI options from `cli.py`
   - Delete `config/mining.yaml`

3. **Phase 3** (Priority: Medium)
   - Unify `MiningResult` and `EventValidationResult`
   - Simplify CLI to single entry point

4. **Phase 4** (Priority: Low)
   - Consider removing `domain/strategy/rule.py` if not needed by runner
   - Or keep for backward compatibility with runner.py

---

## Testing Strategy

After Phase 1:
```bash
# Test event mining
poetry run python -m jerry_trader.services.backtest.mining \
  --events \
  --date-range 2026-03-02 2026-03-04

# Expected output:
# reversal_entry_neutral_gap: 150 signals, avg_return=0.03, win_rate=48%
# dip_buy_neutral_gap: 80 signals, avg_return=0.01, win_rate=42%
```

---

*Generated: 2026-04-22*
*Related: roadmap/event-framework-validation.md, roadmap/mining-refactor.md*
