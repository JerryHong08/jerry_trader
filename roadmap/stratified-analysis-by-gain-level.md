# Task: Stratified Analysis by Gain Level

## Context

**Goal**: Understand what distinguishes Super Winners (100%+) from Big Winners (40-100%) and Medium Winners (20-40%).

**Current State**: Binary classification (winner vs non-winner) loses important information.

**Hypothesis**: Different gain tiers have different early signatures and optimal entry points.

## Analysis

### Current Data Distribution

From initial analysis of 72 big winners:
- Super Winners (>100%): ~15 tickers (CIIT 6461%, VCIG 314%, MOBX 225%, etc.)
- Big Winners (60-100%): ~25 tickers
- Medium Winners (40-60%): ~32 tickers

### Key Questions

1. **Feature Differences**: What early features distinguish tiers?
   - Do Super Winners have stronger early signals?
   - Do Medium Winners look like failed Big Winners?

2. **Trajectory Differences**: How do trajectories differ by tier?
   - Do Super Winners rise faster?
   - Do Medium Winners peak earlier?

3. **Volume Patterns**: How does volume relate to final gain?
   - Do Super Winners have sustained high volume?
   - Do Medium Winners have volume decay?

4. **Gap Relationship**: How does opening gap affect tier?
   - Do larger gaps lead to higher tiers?
   - Or do moderate gaps allow more room to run?

### Statistical Approach

```python
# For each feature, compute:
# 1. Distribution by tier
# 2. Statistical significance (ANOVA/Kruskal-Wallis)
# 3. Effect size (Cohen's d)
# 4. Predictive power (ROC-AUC for tier classification)

# Key features to analyze:
features = [
    "gain_5min", "gain_10min", "gain_15min",
    "volume_5min", "volume_10min", "volume_15min",
    "trade_rate_5min", "trade_rate_10min",
    "price_acceleration_5min",
    "gap_pct",
    "rel_volume_5min",
    "time_to_10pct", "time_to_25pct",
]
```

## Decision

### Tier Definition

| Tier | Name | Gain | Volume | Rationale |
|------|------|------|--------|-----------|
| 1 | Super Winner | >100% | >500k | Life-changing trades |
| 2 | Big Winner | 60-100% | >1M | Significant profits |
| 3 | Medium Winner | 40-60% | >1M | Good trades |
| 4 | Small Winner | 20-40% | >500k | Minor profits |
| 5 | Failed | <20% | - | Not worth pursuing |

### Analysis Methodology

1. **Feature Distributions**: Plot feature distributions by tier
2. **Statistical Tests**: Test significance of differences
3. **Predictive Modeling**: Train tier classifier
4. **Feature Importance**: Identify most predictive features

## Plan

### Step 1: Tier Classification
- [ ] Classify all 72 winners by tier
- [ ] Verify tier distribution
- [ ] Document tier characteristics

### Step 2: Feature Analysis
- [ ] Compute feature statistics by tier
- [ ] Statistical significance testing
- [ ] Effect size calculation
- [ ] Visualization (box plots, violin plots)

### Step 3: Tier Prediction Model
- [ ] Train multi-class classifier
- [ ] Cross-validation
- [ ] Confusion matrix analysis
- [ ] Feature importance ranking

### Step 4: Tier-Specific Insights
- [ ] What features predict Super Winners?
- [ ] What features distinguish Big from Medium?
- [ ] What causes Medium Winners to not become Big?

### Step 5: Strategy Implications
- [ ] Should we aim for Super Winners only?
- [ ] Or capture all Big+ winners?
- [ ] What's the optimal risk/reward tradeoff?

## Expected Outcomes

1. **Tier Feature Profiles**: Distinctive features for each tier
2. **Tier Prediction Model**: 60%+ accuracy on tier classification
3. **Strategy Recommendations**: Which tier to target, optimal entry criteria
4. **Risk Assessment**: Probability of reaching each tier given early signals
