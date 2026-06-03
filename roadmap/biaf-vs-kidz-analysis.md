# BIAF vs KIDZ Signal Quality Analysis

**Task 11.14** — Analyze BIAF vs KIDZ signal characteristics.

## ⚠️ CORRECTION (2026-04-20)

**Earlier analysis contained incorrect data.**

Original claim: "BIAF 62% win vs KIDZ 0% win"

**Actual data (from backtest_results):**

| Ticker | Signals | Win Rate | Avg Return | MFE | MAE |
|--------|---------|----------|------------|-----|-----|
| BIAF | 13 | **61.5%** | +1.89% | +21.49% | -8.55% |
| KIDZ | 8 | **62.5%** | -0.34% | +6.37% | -20.29% |

**Win rates are nearly identical.**

The difference is in **risk/reward profile**, not win rate:
- BIAF: Higher MFE (+21%), Lower MAE (-8%) → Better upside/downside ratio
- KIDZ: Lower MFE (+6%), Higher MAE (-20%) → Worse upside/downside ratio

**Stratification hypothesis updated:**
- Float/TRF predicts **MFE/MAE (risk/reward)**, NOT win rate
- Low float + contaminated TRF → worse MFE/MAE profile

---

## Statistical Validation (2026-04-21)

**Welch's t-test results:**

| Metric | momentum_candidate | avoid | p-value | Significance |
|--------|--------------------|-------|---------|--------------|
| MFE | 21.49% ± 11.66% (n=13) | 6.69% ± 5.52% (n=12) | **0.0007** | *** |
| MAE | -8.55% ± 4.25% (n=13) | -22.29% ± 7.14% (n=12) | **0.0000** | *** |

**Stratification significantly predicts MFE/MAE.**

**LIMITATION:**
- momentum_candidate = BIAF only (1 ticker)
- avoid = KIDZ + EDHL + FRSX + ACXP (4 tickers)
- Need more ticker diversity to generalize stratification effectiveness

---

## Original Analysis (CONTAINS ERRORS)

The following sections contain incorrect conclusions based on wrong data.

## Executive Summary

**Root cause identified**: Ticker characteristics fundamentally differ, making threshold-based strategy discovery ineffective across heterogeneous tickers.

| Factor | BIAF | KIDZ | Impact |
|--------|------|------|---------|
| **Float Shares** | 4.3M | 143K (30x smaller) | Low float = high price volatility |
| **TRF Delayed Trades** | 12 (0.003%) | 26,444 (61% of TRF) | Massive stale data contamination |
| **Avg MFE** | +21.59% | +5.68% | Momentum persistence difference |
| **Avg MAE** | -8.56% | -21.02% | Downside risk difference |
| **Signal Duration** | 70 min avg | 14.7 min avg | Trend sustainability |

## Signal Performance Comparison

### BIAF (15 signals, 2026-03-13)

| Metric | Value | Interpretation |
|--------|-------|---------------|
| Win Rate | 62% | Majority profitable |
| Avg Return 5m | +1.82% | Positive expected return |
| Avg MFE | +21.59% | Strong upside potential |
| Avg MAE | -8.56% | Limited downside |
| Avg Duration | 70 min | Sustained momentum |

**Pattern**: Signals triggered at 12:05-13:15 ET (mid-day), with sustained momentum lasting 18-60 minutes. High MFE indicates real buyers creating lasting price movement.

### KIDZ (9 signals, 2026-03-13)

| Metric | Value | Interpretation |
|--------|-------|---------------|
| Win Rate | 0% | All signals failed |
| Avg Return 5m | -1.93% | Negative expected return |
| Avg MFE | +5.68% | Minimal upside |
| Avg MAE | -21.02% | Severe downside |
| Avg Duration | 14.7 min | Short-lived spikes |

**Pattern**: Signals triggered at 11:30-11:44 ET, with immediate reversal (0-6.8 min duration). High MAE indicates no real buying support — price spikes are artificial.

## Root Cause: TRF Delayed Trades

### TRF Statistics (2026-03-13)

| Ticker | Total Trades | TRF Trades | TRF % | Delayed TRF (>10s) | Avg Delay |
|--------|-------------|------------|-------|--------------------|-----------|
| BIAF | 862,725 | 441,739 | 51% | **12** | 0.4s |
| KIDZ | 157,789 | 43,211 | 27% | **26,444** | 15 min |

**Key insight**: KIDZ has 61% of its TRF trades as delayed (stale reports from hours earlier), while BIAF has only 0.003% delayed.

### Delayed TRF Arrival Time

| Ticker | Arrival Time (UTC) | Arrival Time (ET) | Count |
|--------|--------------------|--------------------|-------|
| BIAF | Hour 20 | 16:00 (after close) | 12 |
| KIDZ | Hour 20-21 | 16:00-17:00 (after close) | 26,341 |

Delayed TRF trades arrive after market close (16:00 ET), but their `participant_timestamp` indicates executions from hours earlier. In backtest, these stale trades inflate `trade_rate` artificially.

### Impact on Signal Quality

**KIDZ's 26K delayed TRF trades**:
- Represent trades executed 30-90 minutes earlier
- Arrive in bulk at 16:00 ET (after signals triggered at 11:30 ET)
- **Not directly contaminating signal triggers** (signals at 11:30 ET, delayed TRF at 16:00 ET)

**But the question remains**: Why do KIDZ signals fail despite no direct delayed TRF contamination at trigger time?

## Root Cause: Float Size Difference

### Fundamental Characteristics

| Ticker | Float Shares | Outstanding | Free Float % | Interpretation |
|--------|-------------|-------------|--------------|---------------|
| BIAF | 4,346,538 | 4,498,708 | 96.6% | Normal small-cap, liquid |
| KIDZ | 143,577 | 361,669 | 39.7% | **Ultra low float**, illiquid |

**30x float size difference** is the critical factor.

### Why Low Float = Poor Signal Quality

1. **Price volatility amplification**
   - 143K float: 10K shares traded = 7% of float moved
   - 4.3M float: 10K shares traded = 0.2% of float moved
   - Small trades create large price swings in KIDZ

2. **TRF distortion multiplier**
   - 26K delayed TRF trades in KIDZ = 18% of total trades
   - In BIAF, same 26K would be only 3% of total trades
   - Relative impact is 6x higher in low float stocks

3. **No real momentum possible**
   - With 143K float, "momentum" is just small position adjustments
   - No institutional buying can sustain a trend
   - Price spikes are noise, not signal

### Price Statistics (2026-03-13)

| Ticker | Price Range | Trades | Price Swing |
|--------|-------------|--------|-------------|
| BIAF | $1.03 - $2.73 | 862K | 165% (with high volume) |
| KIDZ | $2.55 - $4.03 | 158K | 58% (with low volume) |

BIAF's 165% swing is backed by 862K trades (real demand). KIDZ's 58% swing has only 158K trades (thin market).

## Paradigm Implications

### Threshold-Based Strategy Discovery Fails

Current paradigm assumes:
```
trade_rate > threshold → signal → profit
```

But:
- For BIAF-type (large float, clean TRF): threshold works
- For KIDZ-type (small float, contaminated TRF): any threshold fails

**Same threshold produces opposite results**:
- BIAF: trade_rate > 150 → 62% win
- KIDZ: trade_rate > 150 → 0% win

### Implication for Mining

Without ticker stratification, mining:
1. Tests hundreds of threshold combinations
2. Aggregates results across heterogeneous tickers
3. Gets mixed statistics (some BIAF wins, some KIDZ losses)
4. Cannot distinguish which threshold works for which ticker type

**Solution**: Stratify tickers first, then apply thresholds.

## Stratification Criteria

### Proposed Classifier

```yaml
ticker_classification:
  low_float_threshold: 500_000  # <500K float = low float

  trf_quality_threshold: 0.10   # >10% delayed TRF = contaminated

  categories:
    - name: momentum_candidate
      criteria:
        - float_shares > 500_000
        - delayed_trf_ratio < 0.10
      strategy: threshold-based mining

    - name: avoid
      criteria:
        - float_shares < 500_000
        - delayed_trf_ratio > 0.10
      strategy: exclude from strategy discovery
```

### Validation

| Ticker | Float | Delayed TRF Ratio | Category | Strategy Outcome |
|--------|-------|-------------------|----------|-----------------|
| BIAF | 4.3M | 0.003% | momentum_candidate | threshold works |
| KIDZ | 143K | 61% | avoid | threshold fails |
| ISPC | ? | ? | needs classification | validate |
| EONR | ? | ? | needs classification | validate |

## Next Steps

1. **11.20**: Implement ticker stratification in mining pipeline
   - Add float_shares and delayed_trf_ratio as pre-filter criteria
   - Classify tickers before threshold sweep
   - Run mining only on `momentum_candidate` tickers

2. **11.25**: Validate dynamic exit timing
   - BIAF's MFE +21.59% vs return +1.82% suggests better exit possible
   - Factor-based exit (exit when trade_rate drops) may capture more profit

3. **Multi-date validation**
   - Test stratification across 10+ dates
   - Validate that `momentum_candidate` category consistently profitable

## Lessons for Agent Workflow

When mining strategies:
1. **First check ticker stratification** — classify by float + TRF quality
2. **Run threshold sweep only on candidates** — avoid wasting time on avoid-category tickers
3. **Analyze MFE/MAE before thresholds** — momentum persistence predicts success
4. **Separate entry and exit analysis** — MFE > return suggests exit timing matters

## References

- ROADMAP 11.14 — This analysis
- ROADMAP 11.20 — Ticker-specific adaptive thresholds (next)
- ROADMAP 11.25 — Dynamic exit validation (next)
- `roadmap/trf-trade-filtering.md` — TRF delay analysis
- `roadmap/backtest-paradigm-analysis.md` — Paradigm limitations

## Data Sources

- Float shares: `/mnt/blackdisk/quant_data/polygon_data/raw/us_stocks_sip/float_shares/`
- Signal performance: ClickHouse `backtest_results` table
- TRF statistics: ClickHouse `trades` table
