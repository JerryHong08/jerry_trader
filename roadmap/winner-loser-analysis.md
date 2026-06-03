# Winner vs Loser Entry Analysis

## Executive Summary

通过对比**大赢家**(return > 20%)和**直接亏损**(max_gain < 2%)在入场时刻的因子值，发现了**三个关键区分特征**：

| Metric | Threshold | Separation Rate |
|--------|-----------|-----------------|
| Accel Sustainability | ~5.70 | **86%** |
| Vol Accel 15→30 | ~2.30 | 79% |
| Vol Accel 30→60 | ~14.89 | 71% |

**核心发现**: 大赢家的动量加速更持续、更强劲，而直接亏损的加速较弱或不可持续。

---

## Data Summary

### Trade Categorization

- **Total trades**: 48
- **Big Winners** (>20% return): 5 (AIFF, ATPC-0310, ROMA, KIDZ, BIAF)
- **Direct Losers** (max_gain < 2%): 9 (EOLS, CYCU, UGRO, GREE, BKD, SABR, NXGL, ATPC-0311, SVCO)

### Entry Metrics Comparison

| Metric | Big Winners (mean) | Direct Losers (mean) | Difference |
|--------|-------------------|---------------------|------------|
| Gain % at Entry | **-1.99%** | +4.15% | -6.14 |
| Vol Accel 5→15 | 0.43 | 0.97 | -0.54 |
| Vol Accel 15→30 | **3.21** | 1.79 | +1.42 |
| Vol Accel 30→60 | **101.07** | 7.23 | +93.83 |
| Accel Sustainability | **17.58** | 5.69 | +11.88 |
| Volume 60min | 1,134 | 22,088 | -20,954 |
| Trade Count | 8.6 | 240.8 | -232.2 |
| Open Price | $2.26 | $4.00 | -1.74 |

---

## Key Findings

### 1. Accel Sustainability (86% separation)

**定义**: `vol_accel_15to30 / vol_accel_5to15`

**物理意义**: 动量是否在持续加速

| Category | Min | Max | Median |
|----------|-----|-----|--------|
| Big Winners | 1.19 | 40.00 | **10.00** |
| Direct Losers | 1.01 | 40.00 | **1.41** |

**阈值**: ~5.70
- 大赢家: 4/5 (80%) > 5.70
- 直接亏损: 1/9 (11%) > 5.70

**解读**: 大赢家的成交量加速在15→30分钟窗口内持续增强，而直接亏损的加速趋于平缓。

### 2. Vol Accel 15→30 (79% separation)

**定义**: `vol_30min / vol_15min`

**物理意义**: 15→30分钟窗口的成交量加速比

| Category | Min | Max | Median |
|----------|-----|-----|--------|
| Big Winners | 1.00 | 6.69 | **3.00** |
| Direct Losers | 1.03 | 4.00 | **1.61** |

**阈值**: ~2.30
- 大赢家: 4/5 (80%) > 2.30
- 直接亏损: 2/9 (22%) > 2.30

### 3. Vol Accel 30→60 (71% separation)

**定义**: `vol_60min / vol_30min`

**物理意义**: 30→60分钟窗口的成交量加速比

| Category | Min | Max | Median |
|----------|-----|-----|--------|
| Big Winners | 1.68 | 407.00 | **27.50** |
| Direct Losers | 1.06 | 26.00 | **2.29** |

**阈值**: ~14.89
- 大赢家: 3/5 (60%) > 14.89
- 直接亏损: 1/9 (11%) > 14.89

---

## Surprising Discovery: Gain % at Entry

| Category | Min | Max | Median |
|----------|-----|-----|--------|
| Big Winners | -6.56% | +0.50% | **-1.43%** |
| Direct Losers | -3.82% | +16.23% | **+1.25%** |

**反直觉发现**: 大赢家在入场时往往是**下跌或微涨**，而直接亏损在入场时往往已经**上涨较多**。

**可能的解释**:
1. **低位入场**: 大赢家在开盘后回调时入场，获得更好的风险收益比
2. **追高入场**: 直接亏损在已经上涨较多时入场，容易回调被套
3. **样本量限制**: 只有5个大赢家，需要更多数据验证

---

## Implications for Strategy

### New Entry Condition Proposal

基于以上发现，可以设计新的入场条件：

```python
# 原条件 (vol_accel_momentum)
vol_accel_15to30 > vol_accel_5to15  # accel_sustainability > 1.0
AND vol_accel_30to60 > vol_accel_15to30

# 新增条件 (基于winner vs loser分析)
accel_sustainability > 5.70  # 动量持续加速
AND vol_accel_15to30 > 2.30  # 中期加速强劲
AND gain_pct_at_entry < 2%   # 不追高
```

### Risk Warning

⚠️ **样本量限制**: 只有5个大赢家和9个直接亏损，统计显著性有限。

⚠️ **避免过拟合**: 这些阈值是基于小样本得出的，不应直接用于实盘。需要：
1. 扩大数据集验证
2. 理解背后的市场机制
3. 在新数据上测试泛化能力

---

## Next Steps

1. **扩大数据集**: 收集更多大赢家和直接亏损案例
2. **机制研究**: 为什么加速持续性好的信号更有效？
3. **验证阈值**: 在新数据上测试这些阈值是否仍然有效
4. **实盘可行性**: 确保这些条件可以在实盘中实时计算和判断

---

## Methodology Notes

### What We Did Right

✅ **只用入场时刻信息**: 所有因子值都是在入场时刻(60min)可以观察到的
✅ **避免后视偏差**: 没有使用最终收益、max_gain等信息作为入场条件
✅ **区分明确**: 大赢家 vs 直接亏损的定义清晰

### What We Need to Be Careful About

⚠️ **样本量小**: 5个大赢家，统计结论可能不稳定
⚠️ **阈值过拟合**: 86%分离率可能在小样本上过于乐观
⚠️ **时间窗口固定**: 所有交易都在60min入场，可能遗漏其他窗口的特征

---

## Raw Data

详见: `reports/winner_vs_loser_entry_metrics.json`
