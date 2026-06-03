# Task: Mining Flow Refactor

## Context

基于 [Event Framework Validation](event-framework-validation.md) 的发现：
- IC 不稳定（std=0.29），不能作为验证指标
- Event Selection 改善 avg return，但 Factor Ranking 无效
- 需改用 Avg Return + Win Rate 作为验证指标

## Implementation

### 1. 验证指标调整

**删除**：
- `ic_analysis.py` - IC 计算模块（已证明无效）
- IC 验证阈值（IC > 0.02）

**新增**：
- `event_validator.py` - EventValidationResult + EventValidator
- 验证阈值：Avg Return > 2%, Win Rate > 45%, Positive Dates > 60%

### 2. Multi-date 稳定性检查

新增 `EventValidationResult` 字段：
- `positive_dates_ratio`: % 日期 Avg Return > 0
- `return_std`: 跨日期收益方差
- `date_returns`: 各日期单独结果

稳定性要求：
- Positive Dates > 60%（至少 6/10 日期正收益）
- Return Std < 50% of Mean（方差不能太大）

### 3. Boolean Mining

新增 `mine_events()` 方法：
- 不做 Factor Ranking
- 使用 EventEvaluator 做 Boolean selection
- 直接验证 Event 效果

```python
# 新流程
miner.mine_events(dates, events)

# 输出
EventValidationResult:
  - avg_return: 0.0308 (+3.08%)
  - win_rate: 48.19%
  - positive_dates_ratio: 50%
  - is_valid: False (positive_dates < 60%)
```

### 4. CLI 支持

新增参数：
```bash
poetry run python -m jerry_trader.services.backtest.mining \
  --events \
  --date-range 2026-03-02 2026-03-13
```

输出 Event Comparison 表格。

## Validation Results

### 10 Date Test (2026-03-02 to 2026-03-13)

| Event | Signals | Avg Return | Win Rate | Positive Dates | Valid |
|-------|---------|------------|----------|---------------|-------|
| reversal_entry | 4,634 | +3.08% | 48.19% | **50%** | FAIL |
| dip_buy | 12,014 | +0.02% | 41.82% | 60% | FAIL |

**结论**：
- Avg Return 改善（vs baseline -0.23%）
- 但跨日期稳定性不足（positive_dates < 60%）
- **样本量不足**：仅 10 天，可能过拟合

### 3 Date Test (精选日期)

| Event | Signals | Avg Return | Win Rate | Positive Dates | Valid |
|-------|---------|------------|----------|---------------|-------|
| reversal_entry | 1,626 | **+12.13%** | **60.90%** | 66.67% | ✅ PASS |
| dip_buy | 4,785 | +2.75% | 45.19% | 100% | ✅ PASS |

**注意**：这是事后选择，不能用于 live mode 预测。

## Caveats

1. **Event 条件即 Regime Filter**：
   - 如果今天 trade_rate 低 → 条件不满足 → 无信号
   - 这是自适应的，不需要额外 regime detection

2. **样本量限制**：
   - 仅 10 天验证，需要扩展到 30+ 天

3. **执行成本**：
   - avg return 未扣除 slippage/spread

## Next Steps

- 11.36 扩展样本量验证（30+ 天）
- 11.37 执行成本分析
- 11.38 Paper Trading 验证

## Files Changed

| File | Status | Changes |
|------|--------|---------|
| `mining.py` | Modified | Add `mine_events()`, `--events` CLI |
| `event_validator.py` | New | EventValidationResult, EventValidator |
| `event_evaluator.py` | New | EventEvaluator (Boolean filter) |
| `domain/event.py` | New | Event, Condition types |
| `domain/session.py` | New | SessionPhase detection |

## Backward Compatibility

- 传统 Mining 流程保持不变（`mine()`, `mine_batch()`）
- `--events` 是新增可选模式
- IC 分析仍可用（但不再作为主要验证指标）

---

*Created: 2026-04-21*
*Status: DONE*
*Depends on: event-framework-validation.md, event-framework-implementation.md*
