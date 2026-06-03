# Task: ML Model for Winner Prediction

## Context

**Goal**: Use ML to predict which tickers will become big winners based on early features.

**Key Findings from 15.3 Stratified Analysis**:
- Super Winners (Tier 1): 19 tickers
- Big Winners (Tier 2): 20 tickers
- Medium Winners (Tier 3): 33 tickers
- Small Winners (Tier 4): 55 tickers
- Failed (Tier 5): 790 tickers

**Critical Insight**:
- Tier 2 and Tier 3 have similar early features (gain, volume)
- Main distinction is **Super vs Others** and **Winner vs Failed**
- Volume features are 10-40x more important than price features

## Analysis

### Model Options (Updated based on Stratified Analysis)

#### Option A: Two-Stage Binary Classification (RECOMMENDED)
```python
# Stage 1: Winner vs Failed
# Target: is_winner (Tier 1-3 vs Tier 4-5)
# Data: 72 winners vs 845 failed
# Purpose: Filter out obvious non-winners

# Stage 2: Super vs Big/Medium
# Target: is_super (Tier 1 vs Tier 2-3)
# Data: 19 super vs 53 big/medium
# Purpose: Identify the highest-value targets

# Pros:
# - Handles class imbalance better
# - Each stage has clearer decision boundary
# - Can use different features for each stage
```

#### Option B: Three-Class Classification
```python
# Target:
# - Super Winner (Tier 1): 19 samples
# - Winner (Tier 2-3): 53 samples
# - Failed (Tier 4-5): 845 samples

# Pros: Simpler than two-stage
# Cons: Class imbalance (19 vs 53 vs 845)
```

#### Option C: Regression to Predict Gain
```python
# Target: final_max_gain_pct (continuous)
# Model: XGBoost Regressor

# Pros: Preserves magnitude information
# Cons: Hard to set decision threshold
```

### Recommendation (Updated)

**Two-Stage Binary Classification**:

**Why**:
1. Winner vs Failed is easier to predict (volume-based)
2. Super vs Winner is harder but more valuable
3. Can optimize each stage independently
4. Clearer business logic: "Is it worth trading?" → "Is it a super winner?"

### Training Strategy

```python
# Cross-validation: Leave-one-date-out
# 10 dates → train on 9, test on 1
# Ensures model generalizes to new dates

# Class imbalance handling:
# - Oversample Super Winners
# - Use class weights
# - SMOTE for synthetic samples

# Feature selection:
# - Use top 20 features from feature engineering
# - Remove highly correlated features
```

### Evaluation Metrics

```python
# Classification metrics:
# - Precision@K (top K predictions)
# - Recall@K
# - F1 score
# - Confusion matrix by tier

# Regression metrics:
# - MAE (Mean Absolute Error)
# - R² score
# - RMSE

# Business metrics:
# - Expected return if following predictions
# - Win rate
# - Risk/reward ratio
```

## Decision (Updated)

### Model Architecture: Two-Stage Binary Classification

**Stage 1: Winner Detector**
- Input: Volume + Trade Rate features (Priority 1-2)
- Output: P(is_winner) - probability of being Tier 1-3
- Threshold: Optimize for high recall (catch all potential winners)
- Model: XGBoost Binary Classifier

**Stage 2: Super Winner Detector**
- Input: All features (volume + trade + price + time)
- Output: P(is_super) - probability of being Tier 1
- Threshold: Optimize for high precision (avoid false positives)
- Model: XGBoost Binary Classifier

### Training Pipeline (Updated)

```
Data → Feature Extraction (Volume-focused) →
  Stage 1: Winner Detector →
  Stage 2: Super Winner Detector →
  Evaluate
```

### Feature Priority for Each Stage

**Stage 1 (Winner Detector)**:
- Focus on volume features (87x difference)
- Trade rate features (20x difference)
- Minimal price features needed

**Stage 2 (Super Winner Detector)**:
- All volume features
- All trade rate features
- Price features (to distinguish Super from Big/Medium)
- Time features (Super Winners take longer to peak)

## Plan (Updated)

### Step 1: Data Preparation
- [ ] Extract all features for all tickers
- [ ] Create binary labels: is_winner (Tier 1-3), is_super (Tier 1)
- [ ] Split by date (leave-one-date-out CV)

### Step 2: Stage 1 - Winner Detector
- [ ] Train XGBoost binary classifier (Winner vs Failed)
- [ ] Focus on volume + trade rate features
- [ ] Optimize for recall (catch all winners)
- [ ] Cross-validation
- [ ] Feature importance analysis

### Step 3: Stage 2 - Super Winner Detector
- [ ] Train XGBoost binary classifier (Super vs Big/Medium)
- [ ] Use all features
- [ ] Optimize for precision (avoid false super predictions)
- [ ] Cross-validation
- [ ] Feature importance analysis

### Step 4: Model Evaluation
- [ ] Stage 1: Precision/Recall for winner detection
- [ ] Stage 2: Precision/Recall for super winner detection
- [ ] Combined pipeline evaluation
- [ ] Expected return calculation

### Step 5: Model Deployment
- [ ] Save both models to disk
- [ ] Create prediction API (two-stage)
- [ ] Integrate with backtest pipeline

## Expected Outcomes

1. **Stage 1 (Winner Detector)**: 80%+ recall, 40%+ precision
2. **Stage 2 (Super Winner Detector)**: 60%+ precision
3. **Combined**: Identify 10+ super winners per day with 60% precision
4. **Business Value**: Positive expected return in simulation
