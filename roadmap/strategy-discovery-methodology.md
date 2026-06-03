# Strategy Discovery Methodology

## Key Principles

### 1. Avoid Hindsight Bias (后视偏差)

**Problem**: 在回测时使用"未来信息"做决策，导致实盘无法复现。

**Examples**:
- ❌ 选择"最低涨幅窗口"入场 — 回测时能看到所有窗口，实盘时看不到
- ❌ 选择"最优入场时机" — 实盘时不知道哪个是最优
- ❌ 用"最终结果"筛选信号 — 实盘时不知道最终会涨还是跌

**Solution**: 只用**当前时刻能知道的信息**做决策。

```
在60min时刻：
  - 只能观察：当前涨幅、当前因子值、当前价格
  - 不能知道：75min涨幅、最终收益、max_gain
```

### 2. Avoid Parameter Fitting (避免过拟合)

**Problem**: 反复调整参数找"最优"，不是策略发现，是拟合历史。

**Pattern to avoid**:
- 测试各种阈值 → 选最好的 → 再测试 → 再选
- 每次调整都基于"结果不好所以改参数"

**Solution**: 先理解市场现象，再设计Rule。

```
正确的流程：
1. 观察市场现象（为什么有些信号有效？）
2. 理解背后机制（参与者行为、资金流向）
3. 设计捕捉这个现象的Rule
4. 验证Rule在实盘可行
5. 测试效果
```

### 3. Real-time Feasibility (实盘可行性)

**Rule**: 回测逻辑必须能直接应用到实盘。

```
回测逻辑:
  if 信号满足 and gain_pct < threshold:
      入场

实盘逻辑 (必须相同):
  if 信号满足 and gain_pct < threshold:
      入场
```

**不允许的回测逻辑**:
- `if 最终收益 > 20%` — 实盘不知道最终收益
- `if max_gain > 50%` — 实盘不知道max_gain
- `select lowest_gain_window` — 实盘不知道哪个最低

## Current Understanding

### Volume Acceleration Momentum

**What it captures**: 成交量持续加速，暗示动量在加强。

**Entry Condition**:
```
vol_accel_15to30 > vol_accel_5to15  (accel_sustainability > 1.0)
AND vol_accel_30to60 > vol_accel_15to30  (momentum continues)
```

**Problem discovered**:
- 48笔交易，58.3%胜率
- 但75%的亏损交易曾经有过盈利
- 入场时涨幅正相关 → 越涨越入场 → 结果不好

**New insight**:
- 不是"入场时机"问题
- 是"入场涨幅"问题
- 需要理解：为什么同一信号，有的涨50%，有的直接亏？

## Next Steps

### Focus: Understand Winners vs Losers

**Goal**: 找出入场时能观察到的差异。

**Questions**:
1. 大赢家（ATPC, KIDZ, AIFF, BIAF, ROMA）入场时的因子值分布？
2. 直接亏损（CYCU, UGRO, GREE等）入场时的因子值分布？
3. 有什么入场时能观察到的特征区分两者？

**Method**:
- 只分析入场时刻的因子值
- 不用后续收益、max_gain等信息
- 找到区分特征后，设计新的Rule条件

### Not Focus: Parameter Optimization

**Avoid**:
- 找最优涨幅阈值
- 找最优窗口
- 找最优止盈止损比例

**Why**: 这些都是拟合，不是理解。

## Task Tracking

See ROADMAP.md Section 15 for specific tasks.
