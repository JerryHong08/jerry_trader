# Task: Trajectory Pattern Analysis

## Context

**Goal**: Understand HOW big winners develop, not just IF they have early signals.

**Key Finding from 15.3 Stratified Analysis**:
- Super Winners (Tier 1): 5min gain median = **0%** → Most are "slow burners"
- Big Winners (Tier 2): 5min gain median = 0.66%
- Medium Winners (Tier 3): 5min gain median = 0.20%

**Critical Insight**: Early price gain is NOT the distinguishing feature. Volume and trade rate are.

**Updated Question**:
1. When do Super Winners start to show momentum?
2. What trajectory patterns lead to 100%+ gains vs 40-60% gains?
3. Why do Medium Winners have higher early gains but lower final gains?

## Analysis

### Data Available

- 72 big winners across 10 dates
- Full trade-by-trade data (timestamp, price, volume)
- Pre-market session: 4:00 - 9:30 ET (330 minutes)

### Trajectory Dimensions

1. **Price Trajectory**
   - Shape: sustained rise, late breakout, step-wise, V-shaped
   - Acceleration: when does price move fastest?
   - Volatility: how smooth is the rise?

2. **Volume Trajectory**
   - Distribution: concentrated at open, uniform, late surge
   - Acceleration: when does volume peak?
   - Large trades: when do big players enter?

3. **Time Dynamics**
   - Time to first 10% gain
   - Time to first 25% gain
   - Time to peak
   - Duration of momentum

### Clustering Approach

```python
# Method 1: Shape-based clustering
# Normalize trajectories to [0, 1] range
# Use DTW (Dynamic Time Warping) distance
# K-means clustering on trajectory shapes

# Method 2: Feature-based clustering
features = {
    "time_to_10pct",
    "time_to_25pct",
    "time_to_50pct",
    "peak_time",
    "acceleration_profile",
    "volume_concentration_index",
}
```

## Decision

### Analysis Steps (Updated based on Stratified Analysis)

1. **Tier-Separated Trajectory Analysis**
   - Analyze Super Winners trajectories separately (19 tickers)
   - Analyze Big Winners trajectories separately (20 tickers)
   - Analyze Medium Winners trajectories separately (33 tickers)
   - **Key**: Don't mix tiers - they have different patterns

2. **Volume Trajectory Focus**
   - Volume is the strongest signal (87x difference)
   - Analyze volume growth patterns by tier
   - When does volume acceleration occur?
   - Volume concentration over time

3. **Time-to-Momentum Analysis**
   - Super Winners: When do they start gaining? (median 5min gain = 0%)
   - At what minute do they reach 10%, 25%, 50%?
   - Compare momentum onset across tiers

4. **Medium vs Big Winner Differentiation**
   - Why do Medium Winners have higher early gains (10.3%) but lower final gains?
   - Do they peak early and fade?
   - What prevents them from becoming Big Winners?

## Results (Completed 2026-05-04)

### Key Findings

1. **Super Winners are slow burners**:
   - Time to 10% gain: **215 min** (vs 120 min for Medium)
   - Time to peak: **268 min** (vs 171 min for Medium)
   - Volume peak: **250 min** (vs 185 min for Medium)

2. **Medium vs Big Winner Paradox Explained**:
   - Medium Winners reach 10% at 120 min (faster than Big Winners at 160 min)
   - Medium Winners peak at 171 min (earlier than Big Winners at 182 min)
   - **Hypothesis confirmed**: Medium Winners peak early and fade

3. **Gain Trajectory by Tier**:
   | Minute | Super | Big | Medium |
   |--------|-------|-----|--------|
   | 60     | 1.4%  | 2.8%| 4.7%   |
   | 120    | 3.7%  | 5.0%| 10.7%  |
   | 180    | 4.0%  | 33.6%| 40.9% |
   | 240    | 102.4%| 74.7%| 45.3% |
   | 300    | 126.2%| 78.2%| 50.0% |

4. **Strategy Implications**:
   - Don't chase early high gains (warning sign, not positive signal)
   - Wait for sustained momentum (Super Winners build gradually)
   - Monitor volume trajectory (late volume surge is positive)

### Output Files
- `reports/big_winner_trajectory_analysis.md` - Summary report
- `reports/big_winner_trajectory_data.json` - Full trajectory data

## Plan

### Step 1: Tier-Separated Data Extraction
- [x] Extract price trajectories for Super Winners (19 tickers)
- [x] Extract price trajectories for Big Winners (20 tickers)
- [x] Extract price trajectories for Medium Winners (33 tickers)
- [x] Extract volume trajectories for each tier

### Step 2: Volume Trajectory Analysis
- [x] Plot volume over time for each tier
- [x] Compute volume acceleration curves
- [x] Identify when volume peaks for each tier
- [x] Analyze volume concentration patterns

### Step 3: Momentum Onset Analysis
- [x] For Super Winners: find when momentum starts
- [x] Compute time-to-10%, time-to-25%, time-to-50% by tier
- [x] Identify "tipping point" patterns

### Step 4: Medium vs Big Comparison
- [x] Compare trajectories of Medium vs Big Winners
- [x] Identify where they diverge
- [x] Find features that predict which tier a ticker will reach

### Step 5: Pattern Documentation
- [x] Document trajectory archetypes by tier
- [x] Create tier-specific entry timing rules
- [x] Develop real-time detection criteria

## Expected Outcomes

1. **Tier-Specific Trajectory Patterns**: Distinct patterns for Super/Big/Medium
2. **Volume Acceleration Rules**: When volume signals upcoming momentum
3. **Medium vs Big Differentiation**: Why some winners stop at 40-60%
4. **Entry Timing by Tier**: Optimal entry points for each tier
