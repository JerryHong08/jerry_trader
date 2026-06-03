# IC Analysis Report

## 日期范围: 2026-03-02 to 2026-03-13

## Summary

本次分析覆盖 10 个交易日，共 101,126 个信号，揭示了当前因子阈值配置的关键问题。

### Key Findings

1. **所有因子 IC 值为负或接近零**（不符合预期）
2. **原因诊断**：阈值过低，包含大量低质量信号
3. **解决方案**：高阈值组合有效，正收益验证

---

## Factor IC Results

| Factor | Horizon | IC | ICIR | Signals | Status |
|--------|---------|----|----|---------|--------|
| trade_rate | 1m | 0.000 | 0.00 | 101,126 | ❌ |
| trade_rate | 5m | **-0.079** | -0.65 | 101,126 | ❌ |
| trade_rate | 15m | **-0.119** | -1.09 | 101,126 | ❌ |
| rel_vol_20 | 1m | -0.014 | -0.12 | 81,371 | ❌ |
| rel_vol_20 | 5m | **-0.094** | -0.51 | 81,371 | ❌ |
| rel_vol_20 | 15m | -0.100 | -0.56 | 81,371 | ❌ |
| price_direction | 1m | -0.000 | -0.00 | 78,029 | ❌ |
| price_direction | 5m | -0.014 | -0.32 | 78,029 | ❌ |
| price_direction | 15m | 0.013 | 0.24 | 78,029 | ❌ |
| gap_pct | 5m | -0.033 | -0.16 | 30,153 | ❌ |

**最佳因子**: trade_rate@15m (IC=-0.119) — 负相关意味着高 trade_rate → 下跌

---

## Per-Date IC Analysis (trade_rate@5m)

| Date | IC | Signals | Notes |
|------|----|---------|-------|
| 2026-03-02 | **-0.308** | 7,563 | 极负，异常日 |
| 2026-03-03 | -0.028 | 12,952 | |
| 2026-03-04 | -0.040 | 6,759 | |
| 2026-03-05 | +0.029 | 11,212 | 转正 |
| 2026-03-06 | +0.031 | 11,044 | 转正 |
| 2026-03-09 | -0.038 | 12,432 | |
| 2026-03-10 | **-0.325** | 7,511 | 极负，异常日 |
| 2026-03-11 | -0.023 | 13,761 | |
| 2026-03-12 | -0.022 | 9,787 | |
| 2026-03-13 | -0.066 | 8,105 | |

**异常日**：03-02 和 03-10 IC=-0.30+，驱动整体负 IC

---

## Threshold Effectiveness Analysis

### rel_vol_20 Threshold Comparison

| Threshold | Signals | Avg Return | Avg Volatility |
|-----------|---------|------------|----------------|
| > 1 | 72,914 | **-0.36%** | 6.13% |
| > 2 | 55,149 | -0.42% | 6.90% |
| > 3 | 42,089 | -0.36% | 7.66% |
| **> 5** | 27,684 | **+0.14%** | 9.43% |

**结论**: `rel_vol_20 > 5` 是有效阈值，低于此包含噪声信号

### trade_rate Threshold Comparison

| Threshold | Signals | Avg Return |
|-----------|---------|------------|
| > 1 | 101,126 | **-0.40%** |
| > 3 | 82,701 | -0.32% |
| > 5 | 70,544 | -0.23% |
| > 8 | 58,537 | -0.12% |

**结论**: 单独 trade_rate 阈值提升改善有限，需组合

### Combined High Threshold Effectiveness

| Filter | Signals | Avg Return | Quality |
|--------|---------|------------|---------|
| tr>1 + rel_vol>1 | 72,914 | **-0.36%** | ❌ 低质量 |
| tr>8 + rel_vol>5 | **17,506** | **+1.27%** | ✓ 有效 |

**关键发现**: 高阈值组合产生正收益，验证 multi-factor 策略价值

---

## Recommendations

### 1. 阈值调整建议

当前 `config/mining.yaml`:
```yaml
trade_rate: values: [1, 2, 3, 5, 8]  # 太低
rel_vol_20: values: [1.0, 2.0, 3.0, 5.0]  # 太低
```

**建议调整**:
```yaml
trade_rate: values: [5, 8, 10, 15]  # 提升下限
rel_vol_20: values: [3.0, 5.0, 7.0, 10.0]  # 提升下限
```

### 2. 信号质量筛选

基于 IC 分析：
- 有效信号特征：`trade_rate > 8 AND rel_vol_20 > 5`
- 预期收益：+1.27% per signal
- 样本量：17,506 signals (足够统计)

### 3. 异常日调查

03-02 和 03-10 IC 极负，需单独分析：
- 是否市场环境特殊（大跌日/反弹日）
- 是否数据质量问题

---

## Next Steps

1. **验证高阈值组合**
   - 用新阈值重新 mining
   - 计算 IC 是否改善

2. **Signal Framework Phase 1**
   - 基于 IC 结果选择有效因子
   - trade_rate + rel_vol_20 组合优先标准化

3. **异常日专项分析**
   - 03-02/03-10 市场特征
   - 是否需要日期筛选规则

---

## Technical Notes

### IC 计算方法
- Spearman rank correlation
- 因子值 vs future return
- 跨日期聚合：IC mean, IC std → ICIR

### 数据来源
- `backtest_results` 表
- mining 结果（10 日期）
- 因子值存储在 `factors` JSON 列

---

*Generated: 2026-04-21*
*Analysis tool: `jerry_trader.services.backtest.ic_analysis`*
