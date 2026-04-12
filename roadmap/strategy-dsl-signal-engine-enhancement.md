# Strategy DSL & Signal Engine Enhancement

## Current State

Strategy DSL has a solid foundation:
- `Rule` / `Condition` / `Trigger` — Pydantic models, AND/OR/comparison operators
- `RuleParser` — YAML parsing + factor reference validation
- `SignalEngine` — live Redis pub/sub + rule evaluation + cooldown dedup
- `SignalEvaluator` (backtest) — same `evaluate_trigger`, walks factor timeseries
- `Signal` / `RiskLimits` / `RiskState` — basic value objects

## Identified Gaps

### 1. Multi-timeframe Rule Evaluation (Critical)

**Problem:** `SignalEngine._evaluate_rules()` line 365-368 skips AND triggers
when conditions span multiple timeframes (e.g. `trade_rate@trade + ema_20@10s`).

**Solution:** FactorStateAggregator — per-symbol cache of latest factor snapshot
per timeframe. On each factor update, merge all timeframes before evaluation.

```
symbol_factors["AAPL"] = {
    "trade": {"trade_rate": 250.0, "close": 5.50, ...},
    "10s":   {"ema_20": 5.48, "close": 5.50, ...},
    "1m":    {"ema_20": 5.45, "close": 5.50, ...},
}
```

Evaluate rule against merged dict: `{**trade_factors, **10s_factors, ...}`

### 2. Cross Operator Implementation

**Problem:** `CROSS_ABOVE/CROSS_BELOW` in `evaluate_condition()` always returns False.
Needs previous bar's factor value to detect crossing.

**Solution:** FactorHistoryBuffer — rolling window of last N factor snapshots per symbol+tf.
`evaluate_cross(condition, prev_factors, curr_factors) -> bool`

### 3. Pre-market Strategy DSL Extensions

**Missing conditions for core trading focus (pre-market momentum/gap-up):**
- `session` condition — only fire during premarket/regular/afterhours
- `time_since_market_open` — e.g., "within 30 min of open"
- `gap_pct` — gap-up percentage from previous close
- `relative_volume` — volume ratio vs average
- `float_turnover` — shares traded / float
- `rank` condition — e.g., "volume_rank < 20"

### 4. Signal → Order Bridge

**Problem:** SignalEngine actions are `RECORD/NOTIFY/ALERT` only.
No path from signal trigger to order execution.
`RiskLimits` exists but no service consumes it.

**Solution:**
- Add `TRADE` action type to DSL
- `OrderAction` model: size_pct, order_type (market/limit), stop_loss_pct, take_profit_pct
- `SignalExecutor` service: consumes trigger events, applies RiskLimits check, submits to OrderRuntime
- Risk gateway: max_position_size, max_positions, max_daily_loss enforcement

### 5. Unified Evaluation Context

**Problem:** Backtest `SignalEvaluator` and live `SignalEngine` have divergent evaluation:
- Different cooldown units (ms vs sec)
- No shared factor state/history abstraction
- Backtest evaluates per-timestamp, live evaluates per-message

**Solution:** `EvaluationContext` — wraps factor state + history + session + clock
Both engines create context and call shared `evaluate_rule(rule, ctx)`.

## Priority

1. Multi-timeframe evaluation → unblocks complex rules
2. Cross operator → needed for "price crosses above EMA" patterns
3. Pre-market DSL extensions → domain-specific conditions
4. Signal → Order bridge → enables automated trading
5. Unified evaluation context → reduces divergence, better testing
