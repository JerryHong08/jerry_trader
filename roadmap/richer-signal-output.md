# Richer Signal Output

**Status: COMPLETED (2026-04-12)**

## Problem

Backtest output shows aggregate stats only:
```
TOTAL SIGNALS: 4
WIN RATES:     10m: 25%
AVG RETURNS:   10m: -7.03%
```

Missing:
- Which rule triggered?
- Factor values at trigger time
- Trigger conditions that matched
- Entry/exit analysis

## Solution Implemented

Added `--detailed` flag for per-signal detail view:

```bash
poetry run python -m jerry_trader.services.backtest.cli --date 2026-03-13 --detailed
```

Output now includes per-signal factor values:
```
Signal #1: KIDZ @ 11:36:56
  Entry:    $3.99 (slip 0.35%)
  Trigger:  $3.95
  Factors:
    momentum_spike:         3.42
    volume_acceleration:    823.1
    relative_volume:        2.1
  Returns:  30s:+0.15% 1m:-0.5% 5m:-2.1%
  MFE:      +0.15%
  MAE:      -26.1%
```

## Changes Made

1. Added `--detailed` CLI flag
2. Added `detailed` field to BacktestConfig
3. Modified `print_summary()` to accept `detailed` parameter
4. Modified `_print_rule_summary()` to show factor values in detailed mode
5. Updated SKILL.md documentation
