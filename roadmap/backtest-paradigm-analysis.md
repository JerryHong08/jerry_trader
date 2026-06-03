# Backtest Paradigm Analysis

## Context

**⚠️ CORRECTION (2026-04-20): Earlier analysis contained incorrect data.**

Original incorrect claim: "Same threshold (trade_rate > 150) produces: BIAF 62% win, KIDZ 0% win"

**Actual data from ClickHouse:**

| Ticker | Signals | Win Rate | Avg Return | MFE | MAE |
|--------|---------|----------|------------|-----|-----|
| BIAF | 13 | 61.5% | +1.89% | +21.49% | -8.55% |
| KIDZ | 8 | 62.5% | -0.34% | +6.37% | -20.29% |

**Win rates are nearly identical (61.5% vs 62.5%).**

The real difference is in **MFE/MAE (risk/reward profile)**, not win rate. This changes the paradigm analysis conclusions.

---

During strategy mining exploration, we discovered that:
- Same threshold (trade_rate > 150) produces similar win rates on different tickers
- But risk/reward profiles (MFE/MAE) differ significantly
- Threshold adjustment alone is insufficient for optimizing profitability

## Implicit Assumptions in Current Paradigm

### 1. "Factor threshold trigger = Entry timing"

**Current thinking:**
```
trade_rate > 150 → trigger signal → check 5m return
```

**Implicit assumption:** Profit opportunities appear at the moment a factor reaches a threshold.

**Missed opportunities:**

| Pattern | Example | Why missed |
|---------|---------|------------|
| Trend change | trade_rate from 50 → 150 (sharp rise) | We see "reached 150", not "how it reached" |
| Relative change | trade_rate spikes relative to its own history | Threshold is absolute, ignores individual ticker characteristics |
| Sequential pattern | volume spike → then price momentum | AND means "simultaneously", not "sequentially" |
| Reversal signal | trade_rate drops from 300 → 100 (cooling) | Only GT, not "falling from high" |

### 2. "Fixed time window = Exit timing"

**Current thinking:**
```
Check returns at 1m/5m/15m/60m after trigger
```

**Implicit assumption:** Profit can be measured by price change at fixed time points.

**Missed opportunities:**

| Exit pattern | Example |
|--------------|---------|
| Dynamic exit | Exit when trade_rate drops below 50 (not fixed time) |
| Reversal exit | Exit when price_direction flips from positive to negative |
| MFE/MAE utilization | Stop loss at MAE > -5%, take profit at MFE > 3% |
| Volatility exit | Exit when relative_volume returns to normal |

**Evidence of missed profit:**
```
Avg MFE: +11.93%  ← Best point after trigger
Avg MAE: -15.46%  ← Worst point after trigger
Avg Return 5m: +0.33%  ← Actual result (much worse)
```

We could have exited at +11.93%, but waited to get only +0.33%.

### 3. "Subscription logic = Opportunity pool"

**Current thinking:**
```
Enter top 20 + gain > 2% → subscribe
```

**Implicit assumption:** Profit opportunities only exist in stocks that are "currently rising".

**Missed opportunities:**

| Pool | Example |
|------|---------|
| Rebound opportunities | Rebound from -10% to -5% (not in top N) |
| ETF/ADR | Filtered out, but some ETFs have unique momentum |
| Low gain entry | Gain 0.5% but subsequent momentum is strong |
| High position decline | Falls from top 5 to top 15 (still subscribed but entry timing may be missed) |

### 4. "All signals = Equal importance"

**Current thinking:**
```
13 signals, aggregate win rate 46%
```

**Implicit assumption:** Each signal is independent, aggregate statistics are meaningful.

**Ignored differences:**

| Signal difference | Current handling | Should distinguish |
|-------------------|------------------|--------------------|
| Ticker characteristics | All signals mixed together | BIAF/KIDZ have similar win rates but different MFE/MAE |
| Time point characteristics | No distinction between open vs mid-session | First 5min and 10:00 are different |
| Factor distribution | Only checks if > threshold | Factor value distribution tails may behave differently |

## Root Cause Analysis

### Entry and Exit are Bound Together

Current DSL:
```yaml
trigger:
  conditions:
    - factor: trade_rate
      op: gt
      value: 150

actions:
  - type: record
    track: [returns_5m, returns_15m]  # Fixed time exit
```

**Problem:** Exit logic is "assumed" as fixed time, no DSL support for dynamic exit.

### Factors are "State", not "Change"

Current DSL only supports:
```yaml
- factor: trade_rate
  op: gt
  value: 150  # "Current state > 150"
```

**Not supported:**
```yaml
# We cannot express these (DSL limitation)
- factor: trade_rate_delta  # Rate of change
  op: gt
  value: 50  # "Rising speed > 50/sec"

- factor: trade_rate
  op: cross_above
  value: 150  # "Just crossed threshold" (not "staying above")

- factor: trade_rate
  op: falling_from
  value: 300  # "Dropping from high"
```

## Potential Paradigm Extensions

### 1. Separate Entry and Exit Logic

```yaml
entry:
  conditions:
    - factor: trade_rate
      op: gt
      value: 150

exit:
  conditions:
    - factor: trade_rate
      op: lt
      value: 50  # Dynamic exit: exit when trade_rate cools
```

### 2. Factor Change Rate (Δ factor)

```yaml
- factor: trade_rate_delta  # New factor: change rate
  op: gt
  value: 20  # Accelerating upward
```

### 3. Ticker Stratification

```yaml
entry:
  conditions:
    - factor: trade_rate
      op: gt
      value: 150
    - factor: float_shares  # New factor
      op: lt
      value: 50_000_000  # Only enter on low float
```

### 4. Sequential Conditions (First A, then B)

```yaml
entry:
  sequence:  # New DSL syntax
    - step: 1
      conditions:
        - factor: relative_volume
          op: gt
          value: 3.0
      within: 60s  # Within 60 seconds
    - step: 2
      conditions:
        - factor: trade_rate
          op: gt
          value: 150
      after_step: 1  # Must be after step 1
```

## Priority Assessment

| Priority | Importance | Cost | Pain Point | Profit Potential |
|----------|------------|------|------------|------------------|
| **P0: Ticker Stratification (BIAF vs KIDZ)** | Critical | Low | Biggest pain point | Highest |
| **P1: Dynamic Exit Validation** | High | Low | Medium | High |
| **P2: Δ factor (trade_rate_delta)** | Medium | Medium | Medium | Medium |
| **P3: Ticker Feature Stratification** | Medium | Low | Low | Medium |
| **Deferred: Rebound Pool, Sequential, ETF** | Low | High | Low | Unknown |

### P0: MFE/MAE Stratification — Corrected Analysis

**⚠️ Earlier pain point was based on wrong data.**

Correct observation: Same trade_rate > 150 produces:
- BIAF: 61.5% win, MFE +21.49%, MAE -8.55%
- KIDZ: 62.5% win, MFE +6.37%, MAE -20.29%

**Win rates are similar. The difference is in risk/reward profile.**

**New hypothesis:** Ticker stratification may predict MFE/MAE, not win rate:
- High float + clean TRF → Better MFE/MAE ratio (more profit potential, less risk)
- Low float + contaminated TRF → Worse MFE/MAE ratio

**This needs validation with larger sample size.**

### P1: Dynamic Exit Validation — Data Available

**Evidence:** MFE +11.93% vs Avg Return +0.33%

**Validation approach:**
- Backtest data contains trade_rate at each time point
- Can test: "Exit when trade_rate drops to 50" vs "Fixed 5 minutes"
- No DSL change required, just analysis

### P2: Δ factor — Implementation Cost Medium

- Add delta calculation in Python batch engine first
- If effective, then migrate to Rust

### P3: Ticker Feature Stratification — Simple Proxy

- Use float_shares or sector as stratification proxy
- No need for complex relative thresholds initially

### Deferred Items

| Item | Why deferred |
|------|--------------|
| Rebound pool (negative gain subscription) | Different strategy logic, high risk of noise |
| Sequential conditions (A → B) | DSL change cost high, try AND first |
| ETF/ADR | Not current bottleneck, most ETFs lack pre-market momentum |

## Framework Thinking

**Current paradigm:**
```
Threshold adjustment → Profit discovery
```

**More effective paradigm:**
```
Ticker stratification → Threshold adjustment → Dynamic exit → Profit discovery
```

| Stage | Problem solved | If skipped |
|-------|---------------|------------|
| Ticker stratification | "Which tickers might be profitable" | Adjusting thresholds on wrong tickers |
| Threshold adjustment | "When to enter" | Entry timing not optimized |
| Dynamic exit | "When to exit" | Missing best exit points |

**Current state:** Much time spent on stage 2, but stage 1 was never done.

## Decision

**Next steps in priority order:**

1. **11.14: BIAF vs KIDZ Analysis** — Understand ticker characteristics impact on profit
2. **11.25: Dynamic Exit Validation** — Analyze existing data to validate exit timing hypothesis
3. **11.26: Factor Delta Implementation** — Add change-rate factors if BIAF/KIDZ analysis shows momentum direction matters
4. **11.20: Ticker Adaptive Thresholds** — Implement after understanding ticker stratification

## Related Tasks

- 11.14 — Analyze BIAF vs KIDZ signal quality
- 11.20 — Ticker-specific adaptive thresholds
- 11.24 — Mining batch optimization (completed, enables efficient exploration)
- 7.3 — Agent Factor Mining (depends on paradigm understanding)

## Lessons for Agent Workflow

When mining strategies:
1. First check ticker stratification (don't blindly aggregate)
2. Look at MFE/MAE to validate exit timing assumptions
3. Consider factor change direction, not just threshold crossing
4. Separate entry and exit logic in analysis
5. **CRITICAL: Any conclusion must pass statistical validation** — case study is hypothesis, not proof
6. Minimum standards: ticker sample > 100, date sample > 10, significance tests
