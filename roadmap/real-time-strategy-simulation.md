# Task: Real-time Strategy Simulation

## Context

**Goal**: Simulate real-time trading scenario to test strategy effectiveness.

**Current State**: Static analysis of early signals, but no simulation of actual trading decisions.

**Problem**: We know which signals worked historically, but not:
- When to enter?
- When to exit?
- What's the actual return?
- What's the risk?

## Analysis

### Simulation Scenarios

#### Scenario 1: Single Checkpoint Entry
```
At 5min mark:
- Evaluate all tickers
- Enter if signal strength > threshold
- Hold until 9:30 (market open)
- Calculate return
```

#### Scenario 2: Multi-Checkpoint Entry
```
At 5min, 10min, 15min marks:
- Re-evaluate signals
- Enter at first checkpoint where signal triggers
- Track entry time and price
- Hold until 9:30 or exit signal
```

#### Scenario 3: Dynamic Position Sizing
```
- Enter at first signal
- Add to position if signal strengthens
- Reduce if signal weakens
- Track weighted average entry
```

### Exit Strategies

1. **Time-based**: Exit at 9:30 (market open)
2. **Target-based**: Exit at +50% gain
3. **Trailing stop**: Exit if drops 10% from peak
4. **Signal-based**: Exit if early signal reverses

### Metrics to Track

```python
metrics = {
    # Return metrics
    "total_return",
    "avg_return_per_trade",
    "median_return",
    "win_rate",

    # Risk metrics
    "max_drawdown",
    "sharpe_ratio",
    "sortino_ratio",

    # Execution metrics
    "entry_latency",  # time from signal to entry
    "fill_rate",      # % of signals that got filled
    "slippage",       # actual vs theoretical entry price
}
```

## Decision

### Simulation Framework

**Phase 1: Baseline Strategy**
- Enter at 5min if signal triggers
- Exit at 9:30
- No position sizing (equal weight)

**Phase 2: Optimized Strategy**
- Multi-checkpoint entry
- Dynamic position sizing
- Exit strategy optimization

**Phase 3: Realistic Simulation**
- Add slippage model
- Add fill rate model
- Add position limits

### Implementation

```python
class RealtimeSimulator:
    def __init__(self, strategy_config):
        self.config = strategy_config
        self.positions = {}
        self.trades = []

    def run_simulation(self, date, tickers_data):
        # Simulate real-time monitoring
        for minute in range(5, 330, 5):  # every 5 min
            for ticker in tickers_data:
                signal = self.evaluate_signal(ticker, minute)
                if signal:
                    self.enter_position(ticker, minute)

        # Close all positions at 9:30
        self.close_all_positions()

        return self.calculate_metrics()
```

## Plan

### Step 1: Baseline Simulation
- [ ] Implement basic simulator
- [ ] Run on all 10 dates
- [ ] Calculate baseline metrics
- [ ] Compare vs buy-and-hold

### Step 2: Entry Timing Optimization
- [ ] Test different entry checkpoints (5, 10, 15 min)
- [ ] Test signal strength thresholds
- [ ] Find optimal entry timing

### Step 3: Exit Strategy Optimization
- [ ] Test different exit strategies
- [ ] Find optimal exit timing
- [ ] Calculate risk/reward tradeoffs

### Step 4: Position Sizing
- [ ] Implement dynamic position sizing
- [ ] Test different sizing strategies
- [ ] Optimize for Sharpe ratio

### Step 5: Realistic Simulation
- [ ] Add slippage model
- [ ] Add fill rate model
- [ ] Add position limits
- [ ] Validate against actual trading data

## Expected Outcomes

1. **Strategy Performance**: Expected return, win rate, Sharpe ratio
2. **Optimal Entry**: Best checkpoint and threshold
3. **Optimal Exit**: Best exit strategy
4. **Risk Assessment**: Max drawdown, worst-case scenarios
5. **Production Readiness**: Strategy ready for paper trading
