# MOMO Filter Experiment Findings

Date: 2026-05-25 | 6-date dataset: 2026-03-02/03/04/11/12/13 | 595 candidates, 26 MOMO

## Context

Explored multi-dimensional trade-stream features to build a Boolean layer pipeline
for premarket MOMO candidate filtration. MOMO defined as entry_to_peak_pct >= 50%.

## Key Findings

### 1. Trade count in first 5min is the best single liquidity discriminator

- trades >= P80 (~91-124 depending on pool) retains 65-70% MOMO, cuts 80% candidates
- dollar_vol_5min is correlated with trade count (similar retention), not additive
- max_trade_gap_sec has stronger quality signal (PeakDelta +14.2%) but loses far more MOMO

### 2. big_trade_price_delta (btd) is the best quality discriminator

- btd >= median (btd>=0.0): costs ZERO additional MOMO beyond trade count filter alone,
  but boosts median survivor's entry_to_peak by +12.9% and P90 by +42.3%
- This means btd filters "active but price not following" — the true noise source
- uptick_frac is useless as a filter (loses MOMO, minimal quality gain)

### 3. price_range_5min_pct amplifies quality at MOMO cost

- Adding range>=P70 (~14.4%) to trades+btd: keeps 15/26 MOMO (58%), P50 shifts +24%
- Good for aggressive purification, not for Layer 1

### 4. The biggest MOMO is killed by liquidity gate

- VCIG (+322.8%) had <91 trades in first 5min — killed by ALL configs
- This reveals the entry-anchored paradigm limitation: late-activating tickers are missed

### 5. 100% MOMO capture requires a different paradigm

- Current PreFilter entry moment may not be the right decision anchor
- Need "activation detection" — scan trade stream for ignition events, then apply gates
- Same features (trades, btd, range, gap) but computed at activation moment

## Evaluation Framework

- Tiered MOMO retention: 30%+, 50%+, 100%+, 200%+ — not just binary 50% cutoff
- Distribution shift: P25/P50/P75/P90/P95/max of survivors vs baseline
- Lost MOMO profiling: which tickers were filtered, their peak%, and why
- PeakDelta: mean of per-date (median_survivor - median_baseline)

## Design Principle

Layer 1 goal: capture ALL MOMO, cut what's safe to cut.
Subsequent layers: purify survivors using stricter thresholds.
This is the opposite of trying to maximize cut% in a single layer.
