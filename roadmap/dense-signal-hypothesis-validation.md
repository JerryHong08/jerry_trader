# Dense Signal Hypothesis Validation

## Context

Core hypothesis: **"盘前跳空 + 成交量加速 → 能预测后续涨幅"**

This hypothesis has NOT been statistically validated. All current work (event definitions, factor thresholds, backtest) assumes it's true and optimizes parameters within that assumption. We need to validate (or refute) the hypothesis itself before further optimization.

## Why dense_signal_collector

Current backtest is a filter chain: `PreFilter → EventEvaluator → signals`. Each stage removes samples, creating survivorship bias. We only see tickers that passed all filters, so we can't measure whether filtering actually improves signal quality.

`dense_signal_collector` samples ALL tickers every 2 minutes during MID session, recording factors + 30-min forward returns. This creates an **unbiased full-population dataset** where we can measure the actual predictive power of each factor.

| Question | Current | Dense |
|----------|---------|-------|
| Does rel_vol>2 actually separate winners? | Unknown | Compare distributions directly |
| What if we relax gap to 3%? | Unknown | Compute P(winner\|gap>3%) |
| Are our filters redundant? | Unknown | Conditional probability decomposition |

## Date Split

Critical discipline: **exploration dates ≠ validation dates**. This is the single most important guard against data snooping.

```python
EXPLORE_DATES = ["2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06"]
VALIDATE_DATES = ["2026-03-09", "2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13"]
```

Rule: ALL pattern discovery, threshold tuning, and condition design happens on EXPLORE_DATES. VALIDATE_DATES is touched exactly ONCE for final evaluation. No going back to adjust after seeing validation results.

## Task Breakdown

### 15.21: Multi-Date Batch Collection Script

**File**: `python/src/jerry_trader/scripts/dense_signal_batch.py`

**Function**:
```python
def collect_all_dates(dates: list[str], output_path: str = None) -> pd.DataFrame:
    """Run collect_signals() across all dates, return concatenated DataFrame."""
    all_dfs = []
    for date in dates:
        df = collect_signals(date)
        if not df.empty:
            df['date'] = date
            all_dfs.append(df)
    combined = pd.concat(all_dfs, ignore_index=True)
    if output_path:
        combined.to_parquet(output_path)
    return combined
```

**Output**: `data/dense_signals/all_dates.parquet` with columns:
- ticker, entry_time_ms, return_pct, max_return_pct
- All 8 factors: rel_vol_20, trade_rate, price_direction, gap_pct, bid_ask_spread, entry_gap_pct, order_imbalance, quote_rate
- session_phase, entry_price, exit_price, max_price

**CLI**:
```bash
poetry run python -m jerry_trader.scripts.dense_signal_batch --dates 2026-03-02 2026-03-13 --output data/dense_signals/all.parquet
```

### 15.22: Factor Discriminative Power Analysis

**File**: `python/src/jerry_trader/scripts/factor_discriminative_analysis.py`

**Analysis for each factor**:

1. **Winner tier classification**: Split samples by max_return_pct into tiers:
   - loser: <0%, flat: 0-10%, small: 10-20%, medium: 20-50%, big: 50%+

2. **Distribution comparison**: For each factor, compute:
   - Mean, median, std by tier
   - Box plot data (q25, q50, q75)
   - KS test: does factor distribution differ between big winners and losers?

3. **Single-factor AUC**: How well does each factor alone classify big winner vs not?

4. **Threshold sensitivity**: For each factor, sweep thresholds and compute:
   - P(big_winner | factor > threshold)
   - P(loser | factor > threshold)

**Output**:
```
Factor          | KS stat | AUC   | Big_mean | Loser_mean | Best_thresh
rel_vol_20      | 0.32    | 0.71  | 3.2      | 1.8        | 2.0
vol_accel_5_15  | 0.28    | 0.68  | 1.5      | 0.9        | 1.0
gap_pct         | 0.41    | 0.78  | 8.2      | 3.1        | 4.0
...
```

### 15.23: Conditional Probability Matrix

**File**: in same script as 15.22 or separate `scripts/condition_probability_matrix.py`

**Method**:

Enumerate factor threshold combinations:
```python
gap_thresholds = [2, 3, 4, 5, 6]
rel_vol_thresholds = [1.0, 1.5, 2.0, 2.5, 3.0]
vol_accel_thresholds = [0.6, 0.8, 1.0, 1.2, 1.5]

for gap_t in gap_thresholds:
    for rv_t in rel_vol_thresholds:
        for va_t in vol_accel_thresholds:
            subset = df[(df.gap_pct > gap_t) &
                       (df.rel_vol_20 > rv_t) &
                       (df.vol_accel_5_15 > va_t)]

            precision = (subset.max_return_pct > 20).mean()  # % signals that win
            recall = len(subset[subset.max_return_pct > 20]) / total_winners  # % winners captured
            avg_return = subset.max_return_pct.mean()
            n_signals = len(subset)
```

**Output**:
- CSV table of all combinations with precision, recall, avg_return, n_signals
- Pareto frontier plot: precision vs recall, annotated with threshold combinations
- Current thresholds highlighted on the frontier

**Key question**: Is our current threshold (gap>4%, rel_vol>2.0, vol_accel>1.0) on the Pareto frontier, or is there a strictly better combination?

### 15.24: Train/Test Cold Validation

**File**: `python/src/jerry_trader/scripts/cold_validation.py`

**Protocol**:

Phase 1 — Exploration (on EXPLORE_DATES only):
1. Run 15.21-15.23 on EXPLORE_DATES
2. Identify best threshold combination(s)
3. Document all findings and rationale

Phase 2 — Cold Validation (run ONCE on VALIDATE_DATES):
1. Load pre-frozen thresholds from Phase 1
2. Compute precision, recall, avg_return on VALIDATE_DATES
3. Statistical comparison: in-sample vs out-of-sample

**Metrics for comparison**:
```
                    | In-Sample (Explore) | Out-of-Sample (Validate) | Δ
Precision (winner%) | 48%                 | 42%                      | -6%
Recall (coverage)   | 35%                 | 31%                      | -4%
Avg return          | 15.2%               | 12.8%                    | -2.4%
Signals/day         | 8.4                 | 7.1                      | -1.3
```

**Statistical significance**:
- Paired t-test across dates for avg_return
- McNemar's test for win/loss consistency

### 15.25: Validation Report

**Output document**: `reports/hypothesis_validation_YYYY-MM-DD.md`

**Contents**:
1. **Hypothesis statement**: Formal statement of what we're testing
2. **Methodology**: Data sources, date split, metrics
3. **Factor analysis**: Which factors matter, which don't
4. **Conditional probability**: Key findings from Pareto analysis
5. **Cold validation**: In-sample vs out-of-sample comparison
6. **Conclusion**: Supported / Refuted / Needs Refinement
7. **Next steps**: If supported → integrate to events.yaml; if refuted → what new hypothesis; if needs refinement → specific changes

## Deliverables

| Task | Output | Type |
|------|--------|------|
| 15.21 | `scripts/dense_signal_batch.py` + `data/dense_signals/*.parquet` | Script + Data |
| 15.22 | `scripts/factor_discriminative_analysis.py` + report tables | Script + Report |
| 15.23 | Condition matrix CSV + Pareto chart | Data + Visualization |
| 15.24 | Cold validation results + significance tests | Report |
| 15.25 | `reports/hypothesis_validation_*.md` | Document |

## Dependencies

```
15.21 (batch collection)
  ↓
15.22 (factor analysis) + 15.23 (condition matrix)
  ↓
15.24 (cold validation)
  ↓
15.25 (final report)
  ↓
15.19 (relaxed conditions → events.yaml)
```
