# Experiment Framework (Simplified)

**Status: IMPLEMENTED**

## Overview

Simple experiment logging for backtest results and knowledge transfer.

**Core design:**
- Single experiment file per run (~38 lines)
- Single knowledge.yaml for accumulated lessons
- Auto-record with `--record-experiment` flag

## Directory Structure

```
experiments/
├── 2026-04-13/
│   └── exp_001.yaml     # ~38 lines, simplified format
├── knowledge.yaml       # Accumulated lessons
└── schema.md            # Field definitions
```

## Experiment Format

```yaml
id: exp_YYYYMMDD_NNN
date: YYYY-MM-DD
hypothesis: "..."              # What we're testing

cli_command: "..."             # For reproducibility

# Results
signals: N
win_rate_10m: N%
avg_return_10m: N%
profit_factor: N
mfe: +N%
mae: -N%

# Ticker breakdown
per_ticker:
  BIAF:
    signals: 2
    win: 50%
    avg_ret: -1.13%
    mfe: +22.18%
    mae: -8.64%

# Lessons
lessons:
  - "BIAF 50% win — success pattern"
  - "Single date may be outlier"

validation: quick_check
blockers: [insufficient_sample_size]
```

## Usage

### Record experiment

```bash
poetry run python -m jerry_trader.services.backtest.cli \
    --date 2026-03-13 \
    --record-experiment \
    --hypothesis "Lower threshold increases signals"
```

### Query experiments

```python
from jerry_trader.services.backtest.experiment_logger import (
    find_experiments_by_hypothesis,
    get_all_lessons,
    get_ticker_insights,
)

# Find by keyword
exps = find_experiments_by_hypothesis("threshold")

# Get all lessons
lessons = get_all_lessons()

# Get ticker insights
insights = get_ticker_insights("BIAF")
```

## Knowledge Accumulation

`knowledge.yaml` accumulates:

```yaml
experiments:
  - id: exp_001
    hypothesis: "..."
    signals: 4
    win_rate: 25%

lessons:
  - exp_id: exp_001
    lesson: "BIAF 50% win — success pattern"
```

## Validation Gates (Simplified)

### Statistical Validation Principle (CRITICAL)

**任何数据驱动的结论必须通过统计学验证。**

| 违规示例 | 问题 |
|---------|------|
| "BIAF 比 KIDZ 好" | 单 ticker 对比，样本量 = 2 |
| "阈值 150 有效" | 单日期验证，可能 outlier |
| "策略盈利" | 无置信区间，无显著性检验 |

**正确的验证流程：**

```
观察现象 (case study) → 提出假设 → 统计验证 → 确认/否定
```

- Case study = hypothesis generator，不是 proof
- 需要批量验证后才能 deployment

### Minimum Standards

| 维度 | 最小要求 | 当前状态 |
|-----|---------|---------|
| Ticker sample | > 100 | ❌ 目前只分析了 2 个 |
| Date sample | > 10 | ❌ 目前只验证了 1 天 |
| Significance | p < 0.05 | ❌ 未做检验 |
| Confidence interval | 95% CI | ❌ 未计算 |

### Gates

| Stage | Requirements |
|-------|-------------|
| quick_check | signals > 5, win_rate > 30% |
| thorough | dates > 10, signals > 100, win_rate > 50% |
| paper | 5+ live days, win_rate > 45% |
| production | Sustained profitability |

## Implementation

| File | Purpose |
|------|---------|
| `experiment_logger.py` | Core recording logic |
| `cli.py` | Integration with --record-experiment |
| `schema.md` | Field definitions |

---

Related: roadmap/agent-mining-phase.md
Created: 2026-04-13
