# Task: Early Signal Feature Engineering

## Context

**Goal**: Build comprehensive feature set for early winner detection.

**Key Findings from 15.3 Stratified Analysis**:

| Feature | Super vs Failed | Importance Rank |
|---------|----------------|-----------------|
| Volume at 5min | **87.53x** | #1 (Critical) |
| Relative Volume | **84.23x** | #2 (Critical) |
| Trade Rate | **20.69x** | #3 (Very Important) |
| Gain at 10min | 3.31x | #4 (Moderate) |
| Gain at 5min | 2.77x | #5 (Moderate) |
| Gap at Open | 1.41x | Low (Not Important) |

**Critical Insight**:
- Volume-related features are **10-40x more important** than price features
- Gap has almost no predictive power
- Focus engineering effort on volume and trade activity features

## Analysis

### Feature Categories (Prioritized by Importance)

#### Priority 1: Volume Features (CRITICAL - 87x difference)
```python
volume_features = {
    # Basic volume - MOST IMPORTANT
    "volume_5min", "volume_10min", "volume_15min",

    # Acceleration - Key for detecting momentum onset
    "volume_acceleration_5to10",  # vol_10min - vol_5min
    "volume_acceleration_10to15",
    "volume_growth_rate",   # vol_10min / vol_5min

    # Distribution - How concentrated is volume?
    "volume_concentration_index",  # Herfindahl index
    "volume_skewness",             # skewed to early or late?
    "volume_peak_time",            # When does volume peak?

    # Trade characteristics
    "trade_count_5min", "trade_count_10min",
    "avg_trade_size_5min",
    "trade_size_variance_5min",
    "large_trade_ratio",  # trades > 10k shares / total trades

    # Relative - Compare to peers
    "rel_volume_5min",  # vs median (CRITICAL)
    "rel_volume_vs_top20",  # vs top 20 tickers
}
```

#### Priority 2: Trade Activity Features (VERY IMPORTANT - 20x difference)
```python
activity_features = {
    # Trade rate - SECOND MOST IMPORTANT
    "trade_rate_5min", "trade_rate_10min",
    "trade_rate_acceleration",  # rate_10min - rate_5min

    # Quote activity (if available)
    "quote_rate_5min",
    "bid_ask_spread_5min",
    "spread_change_rate",

    # Order imbalance (if available)
    "order_imbalance_5min",
}
```

#### Priority 3: Price Features (MODERATE - 3x difference)
```python
price_features = {
    # Basic gains
    "gain_5min", "gain_10min", "gain_15min",

    # Acceleration
    "price_acceleration_5min",  # gain / time
    "price_acceleration_10min",
    "acceleration_change",      # acc_10min - acc_5min

    # Volatility
    "price_volatility_5min",    # std of returns
    "price_range_5min",         # (high - low) / open
}
```

#### Priority 4: Time Features (MODERATE)
```python
time_features = {
    # Time to thresholds
    "time_to_5pct_gain",
    "time_to_10pct_gain",
    "time_to_25pct_gain",
    "time_to_50k_volume",
    "time_to_100k_volume",

    # Relative timing
    "volume_before_gain_ratio",  # vol at gain=5% / total vol
}
```

#### Priority 5: Context Features (LOW - Skip for now)
```python
# Gap features have 1.4x difference - NOT worth engineering
# Skip: gap_pct, gap_vs_median_gap

# Only include if easily available
context_features = {
    # Sector/Market (if available)
    "sector_momentum",
    "market_strength",
}
```

### Feature Importance Analysis

```python
# Methods:
1. Correlation with final gain
2. Mutual information
3. Feature importance from tree model
4. SHAP values for interpretability
```

## Decision

### Implementation Approach

1. **Feature Extraction**: Build feature extraction pipeline
2. **Feature Selection**: Use ML to find most predictive features
3. **Feature Validation**: Cross-validation to avoid overfitting
4. **Feature Documentation**: Document each feature's predictive power

## Plan (Prioritized)

### Step 1: Volume Features (CRITICAL - Do First)
- [ ] Implement volume at multiple windows (5, 10, 15, 30 min)
- [ ] Implement volume acceleration features
- [ ] Implement volume distribution/concentration features
- [ ] Implement relative volume features
- [ ] Implement large trade detection

### Step 2: Trade Activity Features (VERY IMPORTANT)
- [ ] Implement trade rate at multiple windows
- [ ] Implement trade rate acceleration
- [ ] Implement trade size analysis

### Step 3: Price Features (MODERATE)
- [ ] Implement gain calculations at multiple windows
- [ ] Implement acceleration features
- [ ] Implement volatility features

### Step 4: Time Features (MODERATE)
- [ ] Implement time-to-threshold calculations
- [ ] Implement relative timing features

### Step 5: Feature Analysis & Selection
- [ ] Compute all features for all tickers
- [ ] Correlation analysis with final gain
- [ ] Feature importance from Random Forest
- [ ] Select top 15 features (focus on volume + trade rate)
- [ ] Validate feature stability across dates

## Expected Outcomes

1. **Feature Set**: 30 focused features (not 50+)
2. **Top Features**: 15 most predictive features (mostly volume-related)
3. **Feature Importance**: Volume features ranked #1-#5
4. **Gap Features**: Removed from priority list
