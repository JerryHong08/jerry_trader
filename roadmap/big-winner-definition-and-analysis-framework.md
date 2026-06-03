# Task: Big Winner Definition and Analysis Framework

## Context

**Goal**: Identify and capture the 1-2 huge winners (60%+ gains) each day during pre-market session.

**Current State**:
- Initial analysis completed on 10 dates (2026-03-02 to 2026-03-13)
- 917 tickers analyzed, 72 big winners identified
- Early signal criteria defined: volume>100k, trade_rate>20, gain>5% in first 5 min
- **Precision: 35%** (14/40 signals led to big winners)
- **Recall: 19.4%** (14/72 big winners caught early)

**Key Problem**: 80% of big winners are "slow burners" that don't show early signals.

## Analysis

### Current Definition Issues

1. **"Big Winner" Definition**: gain>40% AND volume>1M
   - Too binary - doesn't distinguish between 45% and 400% gains
   - Volume threshold may exclude low-float high-gainers
   - Doesn't account for different market conditions

2. **Early Signal Criteria**: Hardcoded thresholds
   - volume_5min > 100k OR rel_volume > 10x
   - trade_rate > 20
   - gain_5min > 5%
   - These are arbitrary, not data-driven

3. **Time Window**: Only 5 minutes
   - Misses slow burners that develop over 15-30 min
   - Doesn't capture trajectory dynamics

### Related Work

- `find_big_winners.py` - Initial big winner identification
- `big_winner_early_detection.py` - Early signal analysis
- `big_winner_deep_analysis.py` - Classification into early/slow/false-positive

### Risks

1. **Overfitting**: Optimizing thresholds on limited data (10 dates)
2. **Selection Bias**: Only analyzing top-20 candidates, missing potential winners
3. **Survivorship Bias**: Not accounting for failed signals

## Decision

### Refined Definition Framework

**Stratified Classification**:

| Tier | Name | Gain | Volume | Priority |
|------|------|------|--------|----------|
| 1 | Super Winner | >100% | >500k | Highest |
| 2 | Big Winner | 60-100% | >1M | High |
| 3 | Medium Winner | 40-60% | >1M | Medium |
| 4 | Small Winner | 20-40% | >500k | Low |
| 5 | Failed | <20% | - | None |

**Why This Matters**:
- Different tiers may have different early signatures
- Strategy can target specific tiers (e.g., aim for Super Winners)
- Allows tier-specific entry/exit strategies

### Analysis Methodology

1. **Trajectory Analysis**: Full price/volume path, not just aggregates
2. **Feature Engineering**: Rich set of early indicators
3. **ML-Based Discovery**: Let algorithm find optimal thresholds
4. **Real-time Simulation**: Test strategies in simulated live environment

## Rejected

1. **Single Threshold**: Binary winner/loser classification
   - Rejected: Loses important granularity

2. **Fixed Time Windows**: Only 5min/10min/15min
   - Rejected: Misses dynamic patterns

3. **Pure ML Black Box**: No interpretability
   - Rejected: Need to understand WHY signals work

## Plan

### Phase 1: Trajectory Pattern Analysis (15.2)
- [ ] Visualize price trajectories of all 72 big winners
- [ ] Cluster trajectories by shape (sustained rise, late breakout, step-wise)
- [ ] Identify common inflection points
- [ ] Document trajectory archetypes

### Phase 2: Stratified Analysis (15.3)
- [ ] Classify all winners by tier
- [ ] Analyze early features for each tier
- [ ] Find distinguishing features between tiers
- [ ] Identify tier-specific optimal entry points

### Phase 3: Feature Engineering (15.4)
- [ ] Build comprehensive feature set (50+ features)
- [ ] Feature importance analysis
- [ ] Correlation with final gain
- [ ] Select top predictive features

### Phase 4: ML Model (15.5)
- [ ] Train tier prediction model
- [ ] Cross-validation on held-out dates
- [ ] Feature importance validation
- [ ] Confidence calibration

### Phase 5: Strategy Simulation (15.6)
- [ ] Simulate real-time monitoring
- [ ] Calculate returns at different entry points
- [ ] Optimize entry timing
- [ ] Risk/reward analysis

## Success Metrics

1. **Recall Improvement**: From 19.4% to >50% for Big Winners (Tier 2+)
2. **Precision Maintenance**: Keep >30% precision
3. **Tier Prediction Accuracy**: >60% accuracy on tier classification
4. **Strategy Return**: Positive expected return in simulation
