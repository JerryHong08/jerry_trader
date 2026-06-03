# Config-Driven Mining Architecture

**Status: DRAFT**

## Problem

Current `generate_multifactor_candidates()` hardcodes:
- Threshold values: `[100.0, 150.0, 200.0]`
- Factor combinations: which factors to combine
- Combination logic: single → two → three → four factor

Every experiment requires code changes.

## Solution

Move all search parameters to YAML configuration.

### Config Schema

```yaml
# config/mining.yaml
search_space:
  # Factor thresholds to explore
  trade_rate:
    op: GT
    values: [100, 150, 200, 250]
    timeframe: trade

  relative_volume:
    op: GT
    values: [1.5, 2.0, 3.0, 5.0]
    timeframe: 1m
    warmup: 20  # bars needed before valid

  price_direction:
    op: GT
    values: [0.5, 0.7, 0.8]
    timeframe: 1m

  gap_percent:
    op: LT  # Filter: gap < threshold
    values: [10, 15, 20]
    timeframe: 1m

# Factor combinations to test
combinations:
  # Baseline
  - factors: [trade_rate]
    name: "baseline"

  # Two-factor
  - factors: [trade_rate, relative_volume]
    name: "volume_confirmed"

  - factors: [trade_rate, price_direction]
    name: "direction_confirmed"

  # Three-factor
  - factors: [trade_rate, relative_volume, price_direction]
    name: "full_momentum"

  - factors: [trade_rate, relative_volume, gap_percent]
    name: "gap_filtered"

  # Four-factor
  - factors: [trade_rate, relative_volume, price_direction, gap_percent]
    name: "strict_entry"

# Validation gates
validation:
  min_dates: 10
  min_signals: 30
  min_win_rate: 0.50
  min_profit_factor: 1.0
  max_pf_drift: 0.20

# Execution
execution:
  parallel: true
  workers: 4
  record_experiment: true
```

### Implementation

```python
@dataclass
class MiningSearchSpace:
    """Search space for a single factor."""
    factor: str
    op: ComparisonOp
    values: list[float]
    timeframe: str
    warmup: int = 0

@dataclass
class MiningCombination:
    """Factor combination to test."""
    factors: list[str]
    name: str

class ConfigDrivenMiner:
    """Generate candidates from YAML config."""

    def __init__(self, config_path: str = "config/mining.yaml"):
        self.config = self._load_config(config_path)

    def generate_candidates(self) -> list[Rule]:
        """Generate all rule candidates from config."""
        search_spaces = self._parse_search_spaces()
        combinations = self._parse_combinations()

        rules = []
        for combo in combinations:
            # Generate cartesian product of thresholds
            for threshold_set in self._product(combo, search_spaces):
                rule = self._build_rule(combo, threshold_set)
                rules.append(rule)

        return rules
```

### Benefits

| Before | After |
|--------|-------|
| Change code to test new threshold | Edit YAML |
| Hardcoded factor combinations | Configurable combinations |
| Single search strategy | Multiple named strategies |
| No experiment versioning | Config can be saved with results |

### Usage

```bash
# Run with default config
poetry run python -m jerry_trader.services.backtest.mining --config config/mining.yaml

# Run with custom config (different search space)
poetry run python -m jerry_trader.services.backtest.mining --config experiments/exp_001/search.yaml
```

### Experiment Reproducibility

Each experiment saves its config snapshot:

```yaml
# experiments/2026-04-20/exp_001.yaml
id: exp_001
config_snapshot:
  search_space: {...}
  combinations: [...}
  validation: {...}
results:
  - rule_id: tr_150_rv_2
    profit_factor: 1.2
    ...
```

This enables:
- Exact reproduction of past experiments
- A/B comparison of search strategies
- Knowledge transfer: "this config found profitable strategies"

## Implementation Plan

1. Create `MiningConfig` dataclass with YAML loading
2. Refactor `generate_multifactor_candidates()` to use config
3. Add `--config` CLI option
4. Save config snapshot in experiment logs
5. Add validation for config schema

## Related

- ROADMAP 11.3: Experiment reproducibility framework
- ROADMAP 11.4: Agent knowledge transfer format
