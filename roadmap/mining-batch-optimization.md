# Mining Batch Optimization (ROADMAP 11.24)

## Problem

Mining runs 90+ minutes for 272 candidates because `_evaluate_candidate()` repeats full backtest pipeline for EACH candidate:

```
for rule in 272_candidates:
    _evaluate_candidate(rule, date)
    → BacktestRunner.run()
      → PreFilter.find()      ← Repeated 272 times! (5-10s each)
      → DataLoader.load()     ← Repeated 272 times! (5-10s each)
      → FactorEngine.compute() ← Repeated 272 times! (5-10s each)
      → SignalEvaluator       ← Only part that differs per rule (~0.1s)
```

Total: 272 × 20s ≈ **90 minutes** (mostly wasted on redundant work)

## Solution: Shared Data + Batch Signal Evaluation

### Architecture

```python
# Phase 1: One-time preprocessing (shared by all candidates)
shared_data = _prepare_shared_data(date)
  → PreFilter.find()          # Once: 5-10s
  → DataLoader.load()         # Once: 5-10s
  → FactorEngine.compute()    # Once: 5-10s per ticker (parallelizable)
  → SharedBacktestData object

# Phase 2: Batch signal evaluation (fast per rule)
for rule in candidates:
    evaluate_signals(rule, shared_data.factor_ts_map)  # ~0.1s
    compute_metrics(signals, shared_data.ticker_data_map)
```

**Expected performance**: 90+ min → **~50 seconds**

### Implementation

#### `SharedBacktestData` dataclass

Contains all precomputed data:
- `candidates`: PreFilter result (subscription set)
- `ticker_data_map`: Loaded trades/quotes/bars
- `factor_ts_map`: Computed factor timeseries
- `session_start_ms`, `session_end_ms`: Session range
- `horizons_ms`, `slippage_buffer`: Metrics config

#### New methods in `StrategyMiner`

| Method | Purpose |
|--------|---------|
| `_prepare_shared_data(date)` | One-time preprocessing |
| `mine_batch(date, rules, shared_data)` | Batch evaluation using shared data |
| `_evaluate_rule_with_shared_data(rule, shared_data)` | Fast signal evaluation |

#### CLI changes

```bash
# Batch mode (default, fast)
poetry run python -m jerry_trader.services.backtest.mining --date 2026-03-13 --config config/mining.yaml

# Legacy mode (for debugging, slow)
poetry run python -m jerry_trader.services.backtest.mining --date 2026-03-13 --config config/mining.yaml --no-batch
```

## Future: Rust Optimization (Phase 2)

Potential Rust migrations for additional speedup:

| Component | Current | Rust Benefit | Estimated |
|-----------|---------|--------------|-----------|
| PreFilter.find() | Polars window iteration (5-10s) | Single-pass HashMap | 1-2s |
| SignalEvaluator | Python loop (~0.1s per rule) | Batch threshold check | ~0.01s |

Total Rust savings: ~30 seconds additional

**Phase 1 first** because:
1. Python optimization solves 90% of problem
2. Rust migration requires more effort (FFI, testing)
3. Phase 1 validates architecture before Rust investment

## Related

- [ROADMAP 11.23](config-driven-mining.md) — YAML search space
- [ROADMAP 13.x](rust-compute-box-architecture.md) — Long-term Rust Compute Box
- Memory: `mining-performance-issue.md` — Root cause analysis
