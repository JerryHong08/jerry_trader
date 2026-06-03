# Ticker Stratification Implementation

**Task 11.20** — Implement ticker-specific adaptive thresholds via stratification.

## Implementation

### New Module: `ticker_stratification.py`

Classifies tickers based on fundamental characteristics:

```python
from jerry_trader.services.backtest.ticker_stratification import TickerClassifier

classifier = TickerClassifier()
classification = classifier.classify('BIAF', '2026-03-13')
# → Category: momentum_candidate
# → Float: 4,346,538 (>500K threshold)
# → Delayed TRF: 0.0% (<10% threshold)
```

### Stratification Criteria

| Criterion | Threshold | Pass | Fail |
|-----------|-----------|------|------|
| Float shares | >500,000 | Liquid, suitable | Low float, manipulable |
| Delayed TRF ratio | <10% | Clean data | Contaminated, distorted |

### Classification Categories

| Category | Criteria | Mining Action |
|----------|----------|---------------|
| `momentum_candidate` | Float > threshold AND TRF clean | Include in threshold sweep |
| `avoid` | Either criterion fails | Exclude from mining |
| `unknown` | Missing data | Include (assume OK) |

### Integration with Mining Pipeline

Stratification applied in `_prepare_shared_data()`:

```python
# Step 1: PreFilter → candidates
# Step 2: Stratification → filter candidates (NEW)
# Step 3: DataLoader → ticker_data
# Step 4: FactorEngine → factor_ts
```

### Config: `config/mining.yaml`

```yaml
stratification:
  low_float_threshold: 500_000
  high_delayed_trf_threshold: 0.10
  require_both: false  # Avoid if EITHER fails
```

## Validation Test

| Ticker | Float | Delayed TRF | Category | Expected |
|--------|-------|-------------|----------|----------|
| BIAF | 4.3M | 0.0% | momentum_candidate | ✅ Correct |
| KIDZ | 143K | 61.2% | avoid | ✅ Correct |

## Impact

- Mining now filters out KIDZ-type tickers automatically
- Threshold sweep focuses on tickers where strategy can succeed
- Saves computation time on unsuitable candidates
- Prevents false negative results (threshold fails on wrong ticker type)

## Data Sources

- **Float shares**: `/mnt/blackdisk/quant_data/polygon_data/raw/us_stocks_sip/float_shares/*.parquet`
- **Delayed TRF ratio**: ClickHouse `trades` table (computed on-demand)

## Related

- ROADMAP 11.14 — BIAF vs KIDZ analysis (root cause)
- ROADMAP 11.20 — This implementation
- `roadmap/biaf-vs-kidz-analysis.md` — Analysis details
