# Task: Medium vs Big Differentiation

## Context

**Goal**: Understand why some winners stop at 40-60% while others reach 60-100%+.

**Surprising Finding from 15.3**:
- Medium Winners (Tier 3): Average 5min gain = **10.3%**
- Big Winners (Tier 2): Average 5min gain = **7.7%**

**Paradox**: Medium Winners have HIGHER early gains but LOWER final gains.

**Question**: What prevents Medium Winners from becoming Big Winners?

## Analysis

### Possible Explanations

1. **Early Peak Hypothesis**
   - Medium Winners peak early and fade
   - Big Winners sustain momentum longer
   - Test: Compare time-to-peak for Medium vs Big

2. **Volume Sustainability Hypothesis**
   - Medium Winners have early volume spike but no follow-through
   - Big Winners have sustained volume
   - Test: Compare volume curves

3. **Gap Ceiling Hypothesis**
   - Medium Winners have moderate gaps that limit upside
   - Big Winners have larger gaps or more room to run
   - Test: Compare gap_pct distribution

4. **Trade Quality Hypothesis**
   - Medium Winners have retail-driven trades (small size)
   - Big Winners have institutional trades (large size)
   - Test: Compare trade size distribution

### Data to Analyze

```python
# Compare Medium (Tier 3) vs Big (Tier 2) on:
features_to_compare = [
    "time_to_peak",
    "volume_sustainability",  # vol after peak / vol before peak
    "gap_pct",
    "avg_trade_size",
    "large_trade_ratio",
    "price_trajectory_shape",  # sustained vs peaked
    "volume_trajectory_shape",
]
```

## Decision

### Analysis Approach

1. **Trajectory Comparison**
   - Plot price trajectories for Medium vs Big
   - Identify where they diverge

2. **Feature Comparison**
   - Statistical comparison of key features
   - Find distinguishing features

3. **Case Study**
   - Select specific examples of Medium vs Big
   - Deep dive into what went wrong/right

4. **Predictive Model**
   - Train classifier: Medium vs Big
   - Identify most predictive features

## Plan

### Step 1: Data Segregation
- [ ] Separate Medium Winners (Tier 3) - 33 tickers
- [ ] Separate Big Winners (Tier 2) - 20 tickers
- [ ] Extract all features for both groups

### Step 2: Trajectory Analysis
- [ ] Plot price trajectories for both groups
- [ ] Compare time-to-peak distributions
- [ ] Identify divergence points

### Step 3: Feature Comparison
- [ ] Statistical comparison of features
- [ ] Identify significant differences
- [ ] Effect size analysis

### Step 4: Case Studies
- [ ] Select 5 Medium Winners with highest early gains
- [ ] Select 5 Big Winners with lowest early gains
- [ ] Compare their trajectories in detail

### Step 5: Predictive Insights
- [ ] Train Medium vs Big classifier
- [ ] Feature importance analysis
- [ ] Document distinguishing features

## Expected Outcomes

1. **Understanding**: Why Medium Winners don't become Big
2. **Predictive Features**: Features that distinguish Medium from Big
3. **Strategy Implication**: Avoid "false signals" (high early gain but low final gain)
4. **Model Enhancement**: Add features to distinguish tiers
