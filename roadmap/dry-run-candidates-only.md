# Dry-Run Mode: --candidates-only

## Problem

User wants to preview pre-filter candidates without running full pipeline:
- What tickers would be selected?
- What are their entry gains?
- How many candidates per date?

Currently must run full backtest to see candidates.

## Proposed Solution

Add `--candidates-only` flag:

```bash
poetry run python -m jerry_trader.services.backtest.cli --date 2026-03-13 --candidates-only
```

Output:
```
Pre-filter candidates for 2026-03-13:
  82 tickers (new_entry_only=True, min_gain=2.0%)

  Ticker    Entry Time    Gain%    Price    RelVol
  ─────────────────────────────────────────────────
  KIDZ      11:36:56      4.2%     $3.98    2.1
  FRSX      11:47:16      3.8%     $2.99    1.8
  BIAF      12:21:10      2.5%     $1.75    3.2
  ...

  Summary:
    Avg gain: 3.1%
    Avg relvol: 2.4
    Time range: 11:36 - 14:22 ET
```

## Use Cases

1. Tune pre-filter parameters before full run
2. Verify data quality (expected tickers present)
3. Estimate backtest scope (how many tickers to load)

## Implementation

Short-circuit after Step 1 in `BacktestRunner.run()`:

```python
candidates = self._step_prefilter(config)
if config.candidates_only:
    print_candidates(candidates, config)
    return BacktestResult(date=config.date, total_signals=0, signals=[])
```

## Files to Modify

- `python/src/jerry_trader/services/backtest/config.py` — add `candidates_only: bool`
- `python/src/jerry_trader/services/backtest/cli.py` — add CLI flag
- `python/src/jerry_trader/services/backtest/runner.py` — short-circuit logic
