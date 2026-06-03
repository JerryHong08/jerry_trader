# Entry Gain % Analysis

## Executive Summary

**核心发现**: 入场涨幅与最终收益呈**负相关** (correlation = -0.34)。

| Entry Gain Range | Count | Win Rate | Avg Return | Avg Max Gain |
|------------------|-------|----------|------------|--------------|
| **Negative (< 0%)** | 23 | **78.3%** | **+7.6%** | **19.6%** |
| 0-2% | 12 | 50.0% | +3.9% | 16.2% |
| 2-5% | 6 | 50.0% | -0.2% | 12.3% |
| 5-10% | 5 | 20.0% | -5.1% | 4.6% |
| **> 10%** | 2 | **0.0%** | **-10.4%** | **0.0%** |

**结论**: 在低位或下跌时入场，胜率和收益都更高。追高入场效果差。

---

## Statistical Summary

### Entry Gain Distribution

- **Total trades**: 48
- **Min**: -15.22%
- **Max**: +16.23%
- **Mean**: +0.22%
- **Median**: 0.00%
- **Std**: 5.17%

### Distribution by Range

| Range | Count | Percentage |
|-------|-------|------------|
| Negative (< 0%) | 23 | 47.9% |
| 0-5% | 18 | 37.5% |
| 5-10% | 5 | 10.4% |
| > 10% | 2 | 4.2% |

---

## Correlation Analysis

| Correlation | Value | Interpretation |
|-------------|-------|----------------|
| Entry Gain vs Return | **-0.34** | Moderate negative |
| Entry Gain vs Max Gain | **-0.30** | Moderate negative |

**Interpretation**: Higher entry gain → lower final return. This suggests a **mean reversion** pattern rather than momentum continuation.

---

## Key Comparison: Low vs High Entry Gain

### Low Entry Gain (< 2%)
- **Count**: 35 trades (72.9% of total)
- **Win Rate**: 68.6%
- **Avg Return**: +6.3%
- **Avg Max Gain**: 18.5%

### High Entry Gain (>= 5%)
- **Count**: 7 trades (14.6% of total)
- **Win Rate**: 14.3%
- **Avg Return**: -6.6%
- **Avg Max Gain**: 3.3%

**Difference in avg return**: 12.9%

---

## Implications for Strategy

### 1. Don't Chase High Gains

入场时涨幅已经很大的信号，后续表现反而更差：
- 涨幅 > 10%: 0% 胜率，平均亏损 -10.4%
- 涨幅 5-10%: 20% 胜率，平均亏损 -5.1%

### 2. Prefer Low or Negative Entry Gain

入场时涨幅低或为负的信号，表现更好：
- 涨幅 < 0%: 78.3% 胜率，平均收益 +7.6%
- 涨幅 0-2%: 50% 胜率，平均收益 +3.9%

### 3. Possible Entry Condition

基于这个发现，可以考虑添加入场条件：

```python
# 建议条件
gain_pct_at_entry < 2%  # 不追高
```

**注意**: 这不是一个"最优阈值"，而是一个基于市场机制的条件：
- 低位入场 → 更好的风险收益比
- 高位入场 → 容易回调被套

---

## Why Does This Happen?

### Hypothesis 1: Risk-Reward Ratio

- **Low entry gain**: 价格相对开盘价较低，上涨空间大，下跌空间有限
- **High entry gain**: 价格已经上涨很多，上涨空间有限，下跌风险大

### Hypothesis 2: Market Psychology

- **Low entry gain**: 市场可能在消化早盘波动，后续趋势更明确
- **High entry gain**: 可能是"假突破"或"冲高回落"模式

### Hypothesis 3: Signal Timing

- 成交量加速信号在低位触发 → 真正的动量启动
- 成交量加速信号在高位触发 → 动量已经衰竭

---

## Methodology Notes

### What We Did

✅ 只用入场时刻信息计算涨幅
✅ 分析涨幅与结果的相关性
✅ 按涨幅区间分组统计表现
✅ 不寻找"最优阈值"，而是理解关系

### What We Found

- 负相关 (-0.34): 涨幅越高，收益越低
- 低位入场胜率高 (78.3% vs 14.3%)
- 高位入场几乎必亏 (> 10%: 0% 胜率)

### Limitations

⚠️ 样本量有限 (48 trades)
⚠️ 相关性不等于因果
⚠️ 需要更多数据验证这个模式是否稳定

---

## Raw Data

详见: `reports/entry_gain_analysis.json`

每个交易的入场涨幅、最终收益、max_gain 都有记录，按入场涨幅排序。
