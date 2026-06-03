# Event 架构重构：从条件筛选改为事件触发模式

## Context

当前 Event 架构是"条件筛选"模式：每个满足条件的时间戳都产生 signal。这导致信号过多（2 dates 470 signals），与策略核心不符。

**策略核心：**
- 新 ticker **突然进入** top20 → 判断可持续性
- 已有 ticker **突然异常拉升** → 判断可持续性

**关键点：** "突然进入/拉升" 是 **触发点**，不是持续状态。

## Problem

| 策略核心 | 当前 Event 实现 | 问题 |
|----------|-----------------|------|
| 新 ticker 突然进入 top20 | 无此触发点 | `first_entry_ms` 已追踪，但没作为 Event trigger |
| 已有 ticker 突然异常拉升 | `price_direction > 0.8` | 这是"当前状态筛选"，不是"突然变化检测" |

**结果：** 只要满足条件的时间戳都会产生 signal，而不是只在"突然变化"时触发。

## Decision

重构 Event 架构为 **"事件触发 + 可持续性判断"** 模式：

```
触发点（Event Source）：
  1. first_entry_ms → 新 ticker 进入 top20
  2. price_accel > threshold → 突然加速
  3. trade_rate_spike → 突然放量
  4. gap_breakout → 突破整理区间

辅助条件（Signal Quality Filter）：
  - rel_vol_20 > 2.5
  - bid_ask_spread < 30
  - price_direction > 0.5
  - session_phase = mid
```

## Plan

### Phase 1: first_entry 触发点（最简单，验证架构）

**目标：** 每个 ticker 只在进入 top20 时触发一次，信号数从 470/2dates 降到合理范围。

**改动：**

1. `domain/event.py` — 扩展 Event 定义
   ```python
   @dataclass
   class Event:
       name: str
       trigger: TriggerType  # NEW: first_entry, price_accel, trade_rate_spike, ...
       conditions: list[Condition]  # quality filter
       ...
   ```

2. `event_evaluator.py` — 区分触发点和条件筛选
   ```python
   def evaluate_ticker(event, factor_ts, first_entry_ms, ...):
       if event.trigger == TriggerType.FIRST_ENTRY:
           # 只在 first_entry_ms 时间戳检查条件
           trigger_ts = first_entry_ms
           factors = get_factors_at(factor_ts, trigger_ts)
           if all_conditions_met(event.conditions, factors):
               return [Signal(trigger_ts, factors)]
           return []
       # ... 其他触发类型
   ```

3. `config/events.yaml` — 更新 Event 定义格式
   ```yaml
   events:
     - name: gap_up_new_entry
       trigger: first_entry
       conditions:
         - factor: session_gap_pct
           op: gt
           value: 4.0
         - factor: rel_vol_20
           op: gt
           value: 2.5
         # ...
   ```

### Phase 2: 动态触发点（price_accel, trade_rate_spike）

**需要新增因子：**
- `price_accel` — 价格加速度（Rust 已有 `price_accel()` 函数）
- `trade_rate_spike` — trade_rate 当前值 vs 历史 z-score
- `gap_breakout` — 突破整理区间阈值

**触发逻辑：**
- 不是"当前值 > 阈值"，而是"当前值 > 阈值 且 上一秒不满足"
- 即检测"状态变化"而非"持续状态"

### Phase 3: 组合触发

- 多个触发点可以组合（如 first_entry + price_accel）
- 触发后冷却期（避免短时间内重复触发）

## Validation

**成功标准：**
- 信号数量：每 date 5-20 个（而非 200+）
- 正收益：avg_return > 0 且 win_rate > 50%
- 稳定性：positive_dates > 60%

## Files

| 文件 | 改动 |
|------|------|
| `domain/event.py` | 新增 TriggerType enum，扩展 Event 定义 |
| `event_evaluator.py` | 区分触发点和条件筛选逻辑 |
| `config/events.yaml` | 更新 Event 定义格式 |
| `mining.py` | 传递 first_entry_ms 给 evaluator |

## Related

- 11.41 回测-实盘对齐（已完成）
- `roadmap/backtest-realtime-alignment.md`
