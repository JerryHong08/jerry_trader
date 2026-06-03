# Task: Volume Acceleration Analysis

## Context

**Goal**: Understand volume growth patterns and their predictive power.

**Key Finding from 15.3**: Volume is the #1 predictive feature (87x difference between Super and Failed).

**Question**:
- When does volume start accelerating for Super Winners?
- Does volume acceleration precede price movement?
- Can we detect "volume breakout" in real-time?

## Analysis

### Volume Patterns to Analyze

1. **Volume Growth Rate**
   - vol_10min / vol_5min
   - vol_15min / vol_10min
   - vol_30min / vol_15min

2. **Volume Acceleration**
   - Second derivative of volume over time
   - When does acceleration start?
   - How sustained is it?

3. **Volume Concentration**
   - Is volume concentrated in early minutes?
   - Or does it build over time?
   - When does volume peak?

### Hypothesis

**Super Winners may show**:
- Sustained volume growth (not just early spike)
- Volume acceleration before price acceleration
- Volume peak later in session

**Failed tickers may show**:
- Early volume spike then decay
- No sustained volume growth
- Volume peak at open

## Decision

### Analysis Approach

1. **Compute Volume Curves**
   - For each ticker, compute volume at 5min intervals
   - Normalize to compare across tickers

2. **Volume Acceleration Metrics**
   - Volume growth rate at each interval
   - Time to volume peak
   - Volume sustained ratio (vol after peak / vol before peak)

3. **Tier Comparison**
   - Compare volume curves for Super/Big/Medium/Failed
   - Identify distinguishing patterns

4. **Real-time Detection**
   - Develop volume acceleration signal
   - Test on historical data

## Plan

### Step 1: Volume Curve Extraction
- [ ] Extract volume at 5min intervals for all tickers
- [ ] Compute cumulative volume curves
- [ ] Normalize by total volume

### Step 2: Acceleration Metrics
- [ ] Compute volume growth rate at each interval
- [ ] Identify volume peak time
- [ ] Compute volume acceleration (2nd derivative)

### Step 3: Tier Comparison
- [ ] Compare volume curves by tier
- [ ] Statistical analysis of acceleration patterns
- [ ] Identify distinguishing features

### Step 4: Signal Development
- [ ] Develop volume acceleration signal
- [ ] Set thresholds for real-time detection
- [ ] Backtest signal effectiveness

### Step 5: Integration
- [ ] Add volume acceleration to feature set
- [ ] Integrate with ML model

## Expected Outcomes

1. **Volume Acceleration Pattern**: Clear pattern for Super Winners
2. **Detection Signal**: Real-time volume breakout detection
3. **Entry Timing**: When to enter based on volume acceleration
4. **Feature Enhancement**: New high-value features for ML model
