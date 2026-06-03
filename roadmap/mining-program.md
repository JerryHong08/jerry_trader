# Event Mining Research Program

## Mission

Find profitable Event definitions using hypothesis-driven validation.

**Primary Metric**: `avg_return_5m` (slippage-adjusted 5-minute return)

**Success Threshold**: avg_return > 2%, win_rate > 45%, positive_dates > 60%

---

## Research Protocol

### 1. Hypothesis Generation

Before modifying Events, answer:
- **Why** might this change improve returns?
- **What** market behavior does it capture?
- **How** does it differ from existing Events?

Bad hypothesis: "Try threshold 0.15 instead of 0.10" (blind tuning)
Good hypothesis: "Signals after 9:45 have higher volatility → increase price_direction threshold for post-opening events" (reasoned change)

### 2. Experiment Execution

```bash
# Standard experiment (10 trading days)
poetry run python -m jerry_trader.services.backtest.mining \
  --date-range START END \
  --events config/events.yaml

# Record to results.tsv
# Format: date | hypothesis | event_name | avg_return | win_rate | positive_dates | signals | verdict
```

**Time Budget**: ~5 minutes for 10 dates

### 3. Evaluation Rules

- **KEEP**: avg_return > 2% AND positive_dates > 60%
- **DISCARD**: avg_return < 0% OR positive_dates < 40%
- **NEUTRAL**: Between thresholds → extend sample size

After evaluation:
- If KEEP: commit changes, record lesson
- If DISCARD: revert changes, record failure reason
- If NEUTRAL: run 30+ dates for final verdict

### 4. Results Logging

Each experiment records one line in `results.tsv`:

```
date	experiment_id	hypothesis	event_name avg_return	win_rate	positive_dates	signals	verdict	notes
2026-04-22	001	"higher price_direction threshold post-opening"	reversal_entry_neutral_gap	0.0312	48.2	50	228	DISCARD	sample_too_small
```

### 5. Autonomous Loop

```
while budget_remaining:
    1. Read current best Events from results.tsv
    2. Generate hypothesis for improvement
    3. Modify config/events.yaml
    4. Run experiment (10 dates)
    5. Evaluate result
    6. KEEP → commit, update best
       DISCARD → revert, record lesson
       NEUTRAL → extend dates
    7. Record to results.tsv
```

---

## Event Definition Schema

Events are defined in `config/events.yaml`:

```yaml
events:
  - name: reversal_entry_neutral_gap
    description: "Gap reversal entry when price direction neutralizes"
    conditions:
      - factor: trade_rate
        op: gt
        value: 10.0
      - factor: gap_pct
        op: between
        value: [0.02, 0.08]
      - factor: price_direction
        op: gt
        value: 0.3
      - factor: session_phase
        op: eq
        value: "mid"
    validation:
      expected_return: 0.02
      min_win_rate: 0.45
      signal_count: 100

anti_patterns:
  - name: reversal_gap_down
    description: "Avoid gap down reversals - high failure rate"
    conditions:
      - factor: gap_pct
        op: lt
        value: -0.03
    action: AVOID
```

### Condition Operators

| op | meaning | example |
|---|---|---|
| `gt` | > | trade_rate > 10.0 |
| `lt` | < | gap_pct < -0.03 |
| `eq` | == | session_phase == "mid" |
| `between` | in range | gap_pct in [0.02, 0.08] |
| `outside` | not in range | price_direction outside [0.2, 0.8] |

### Factor Catalog

| factor | description | typical range |
|---|---|---|
| `trade_rate` | trades per minute | 0-50 |
| `gap_pct` | opening gap % | -0.15 to +0.15 |
| `price_direction` | up trades ratio | 0-1 |
| `volume_ratio` | vol vs 20d avg | 0-10 |
| `float_turnover` | shares traded / float | 0-2 |
| `session_phase` | time window | "pre", "early", "mid", "late" |

---

## Current Baseline

From ROADMAP 11.34 validation (10 dates):

| Event | avg_return | win_rate | positive_dates | signals | Status |
|---|---|---|---|---|---|
| reversal_entry_neutral_gap | -0.97% | 33.18% | 40% | 228 | FAIL |
| dip_buy_neutral_gap | -0.32% | 41.23% | 40% | 319 | FAIL |

**Baseline avg_return**: -0.65%

**Target**: Find Events with avg_return > 2%

---

## Lessons Learned

Record in `roadmap/mining-lessons.md`:

### Lesson Format
```
## [DATE] Lesson: [TITLE]

**Hypothesis**: What we tested
**Result**: What happened
**Insight**: Why it worked/failed
**Action**: What to do next
```

### Example Lessons

1. **Regime Detection Flaw** (2026-04-21)
   - Hypothesis: Use post-hoc statistics (trade_rate_mean) to filter dates
   - Result: Cannot apply in live mode - values unknown before day ends
   - Insight: Event conditions are self-filtering - bad days won't trigger signals
   - Action: Remove explicit regime detection, rely on Event conditions

2. **IC Instability** (2026-04-21)
   - Hypothesis: IC > 0.02 indicates stable factor ranking
   - Result: IC std = 0.29 across dates (highly unstable)
   - Insight: IC measures ranking, Events measure selection - different metrics
   - Action: Use avg_return + win_rate for Event validation

---

## Search Strategy

### Phase 1: Threshold Tuning
- Explore trade_rate thresholds (5-20)
- Explore gap_pct ranges (neutral, wide, narrow)
- Explore price_direction thresholds (0.2-0.5)

### Phase 2: Session Timing
- Test early vs mid vs late session
- Test pre-market vs regular hours
- Test time-based conditions (e.g., "before 10:00")

### Phase 3: Factor Combinations
- Add volume_ratio conditions
- Add float_turnover conditions
- Combine multiple factors

### Phase 4: Anti-pattern Refinement
- Refine reversal_gap_down threshold
- Add new anti-patterns from failure analysis

---

## Constraints

1. **Sample Size**: Minimum 10 dates for initial validation
2. **Statistical Significance**: Positive_dates > 60% required
3. **Execution Cost**: avg_return must exceed slippage (0.1%)
4. **Signal Count**: Minimum 100 signals across dates

---

## Autonomous Agent Behavior

When running research loop:

1. **No blind grid search**: Each change must have a hypothesis
2. **Record everything**: Every experiment logged to results.tsv
3. **Commit successes**: KEEP verdict → git commit
4. **Learn from failures**: DISCARD verdict → record lesson
5. **Budget awareness**: Track total experiments, stop when exhausted

---

*Generated: 2026-04-22*
*Based on: karpathy/autoresearch methodology*
