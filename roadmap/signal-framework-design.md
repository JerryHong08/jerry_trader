# Task: Signal Framework Design (Revised)

## Context

基于 [Event Framework Validation](event-framework-validation.md) 的验证结果，重新设计 Signal Framework。

**核心发现**：
1. ✅ Event Selection 有效（avg return 改善 4.52%）
2. ❌ Factor Ranking 无效（IC 不稳定，std = 0.29）
3. trade_rate 排序不单调（tr 20-40 最佳，tr 40+ 反而下降）

**结论**：放弃 IC-based Factor Ranking，采用纯 Event-based Boolean Selection。

---

## Previous Design (Rejected)

原设计采用"混合框架"：
```
因子 → percentile_rank → IC计算 → 加权组合 → 排序选股
```

**问题**：
1. percentile_rank 假设单调排序 → 验证发现不单调
2. IC 验证不稳定 → std = 0.29，无预测力
3. 因子加权组合 → 过拟合风险高

**验证证据**：
| Event | Avg Return | IC |
|-------|------------|-----|
| Baseline (tr>5) | -0.23% | -0.018 |
| Reversal + Neutral Gap | **+4.29%** | +0.043 |

IC 改善 +0.061，但 Avg Return 改善 **4.52%**。Event Selection 的价值在于 Boolean filter，而非 ranking。

---

## New Design: Event-based Boolean Selection

### 核心原则

1. **Event Definition = Boolean Filter**（全选/全弃）
2. **验证指标 = Avg Return + Win Rate**（不用 IC）
3. **跨日期稳定性**是必要条件

### Framework Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Event-based Signal Framework                    │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Layer 1: Event Definition (事件定义)                        │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  class Event:                                          │   │
│  │      name: str                                         │   │
│  │      semantic: str              # "反转入场"            │   │
│  │      conditions: list[Condition]  # Boolean filters   │   │
│  │      expected_return: float      # +4.29%              │   │
│  │      min_win_rate: float         # 45%                 │   │
│  │                                                        │   │
│  │  # 例：Reversal Entry                                  │   │
│  │  ReversalEntry = Event(                               │   │
│  │      name="reversal_entry_neutral_gap",                │   │
│  │      semantic="价格从低位回升，无跳空",                  │   │
│  │      conditions=[                                      │   │
│  │          session_phase == "mid",                       │   │
│  │          price_direction BETWEEN 0.3 AND 0.5,          │   │
│  │          gap_pct BETWEEN -0.5 AND 0.5,                 │   │
│  │          trade_rate > 5,                               │   │
│  │      ],                                                │   │
│  │      expected_return=0.0429,                           │   │
│  │      min_win_rate=0.45,                                │   │
│  │  )                                                     │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  Layer 2: Event Matching (事件匹配)                          │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  def match_signal_to_event(signal) -> Event | None:   │   │
│  │      """Boolean filter: accept or reject"""           │   │
│  │                                                        │   │
│  │      for event in EVENTS:                              │   │
│  │          if all_conditions_match(event, signal):      │   │
│  │              return event  # Accept                    │   │
│  │                                                        │   │
│  │      return None  # Reject                             │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  Layer 3: Validation (验证)                                  │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  @dataclass                                            │   │
│  │  class EventValidation:                                │   │
│  │      avg_return: float          # 必须 > 2%            │   │
│  │      win_rate: float            # 必须 > 45%           │   │
│  │      date_stability: float      # 各日期方差 < 50%     │   │
│  │      positive_dates_ratio: float  # 正收益日期 > 60%   │   │
│  │                                                        │   │
│  │  def is_valid(self):                                   │   │
│  │      return (                                          │   │
│  │          self.avg_return > 0.02                        │   │
│  │          and self.win_rate > 0.45                      │   │
│  │          and self.positive_dates_ratio > 0.6           │   │
│  │      )                                                 │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 与原设计的关键差异

| 方面 | 原设计（拒绝） | 新设计（采用） |
|------|---------------|---------------|
| 选择方式 | percentile_rank 排序 | Boolean filter（全选/全弃） |
| 验证指标 | IC/ICIR | Avg Return + Win Rate |
| 因子处理 | 归一化 + 加权组合 | 直接阈值检查 |
| Event 条件 | rank-based（> 0.8） | value-based（pd 0.3-0.5） |
| 信号输出 | 排序列表 | 匹配的 Event 名称 |

---

## Event Definition Examples

### Valid Events（验证有效）

```yaml
# config/events.yaml
events:
  reversal_entry_neutral_gap:
    description: "价格从低位回升，无跳空"
    conditions:
      - factor: session_phase
        op: eq
        value: "mid"  # 07:00-09:00 ET
      - factor: price_direction
        op: between
        value: [0.3, 0.5]  # reversal zone
      - factor: gap_pct
        op: between
        value: [-0.5, 0.5]  # neutral gap
      - factor: trade_rate
        op: gt
        value: 5
    validation:
      expected_return: 0.0429
      min_win_rate: 0.45
      signal_count: 4840

  dip_buy_neutral_gap:
    description: "抄底入场，无跳空"
    conditions:
      - factor: session_phase
        op: eq
        value: "mid"
      - factor: price_direction
        op: lt
        value: 0.3  # sell pressure
      - factor: gap_pct
        op: between
        value: [-0.5, 0.5]
      - factor: trade_rate
        op: gt
        value: 8
    validation:
      expected_return: 0.0046
      min_win_rate: 0.42
      signal_count: 12014
```

### Anti-patterns（必须避免）

```yaml
anti_patterns:
  # 避免：跳空后的反转尝试
  reversal_gap_down:
    description: "跳空低开后的反转 - 陷阱"
    conditions:
      - factor: price_direction
        op: between
        value: [0.3, 0.5]
      - factor: gap_pct
        op: lt
        value: -0.5
    action: "AVOID"
    reason: "avg_return = -8.18%"
    validation:
      expected_return: -0.0818
      win_rate: 0.41

  # 避免：假突破区
  false_breakout:
    description: "pd 0.5-0.7 假突破区"
    conditions:
      - factor: price_direction
        op: between
        value: [0.5, 0.7]
    action: "AVOID"
    reason: "avg_return = -2.66%"
    validation:
      expected_return: -0.0266
      win_rate: 0.35

  # 避免：非 mid phase
  early_or_late_phase:
    description: "04:00-07:00 或 09:00-09:30"
    conditions:
      - factor: session_phase
        op: ne
        value: "mid"
    action: "AVOID"
    reason: "噪声大，信号质量差"
```

---

## Implementation Plan

### Phase 1: Event Definition Module（优先）

1. 创建 `domain/event.py`
   ```python
   @dataclass
   class Condition:
       factor: str
       op: str  # eq, gt, lt, between
       value: Any

   @dataclass
   class Event:
       name: str
       semantic: str
       conditions: list[Condition]
       expected_return: float
       min_win_rate: float
       action: str = "ACCEPT"  # or "AVOID"
   ```

2. 创建 `services/backtest/event_evaluator.py`
   ```python
   class EventEvaluator:
       EVENTS: list[Event] = load_events_yaml()

       def match_signal(self, signal: Signal) -> Event | None:
           """Boolean filter: match or reject"""
           for event in self.EVENTS:
               if self._check_conditions(event, signal):
                   if event.action == "AVOID":
                       return None  # Explicit reject
                   return event  # Accept
           return None  # No match
   ```

3. 创建 `config/events.yaml`
   - 定义 validated events（reversal_entry, dip_buy）
   - 定义 anti-patterns（gap_down, false_breakout）

### Phase 2: Mining Refactor

1. 修改 `mining.py`
   ```python
   # 旧流程（删除）
   # for tr_threshold in thresholds:
   #     ic = compute_ic(...)
   #     if ic > 0.02: save_rule(...)

   # 新流程
   for event in EVENTS:
       signals = event_evaluator.filter_signals(all_signals, event)
       validation = EventValidator.validate(signals)

       if validation.is_valid():
           save_event(event)
   ```

2. 实现 `EventValidator`
   ```python
   @dataclass
   class EventValidationResult:
       avg_return: float
       win_rate: float
       positive_dates_ratio: float
       date_returns: dict[str, float]

       def is_valid(self) -> bool:
           return (
               self.avg_return > 0.02
               and self.win_rate > 0.45
               and self.positive_dates_ratio > 0.6
           )
   ```

### Phase 3: Multi-date Stability

1. 各日期单独验证
   ```python
   def validate_across_dates(event: Event, dates: list[str]):
       results = {}
       for date in dates:
           signals = filter_signals(event, date)
           results[date] = {
               'avg_return': compute_avg_return(signals),
               'win_rate': compute_win_rate(signals),
               'count': len(signals)
           }

       # Stability check
       positive_count = sum(1 for r in results.values() if r['avg_return'] > 0)
       positive_ratio = positive_count / len(dates)

       return EventValidationResult(
           avg_return=np.mean([r['avg_return'] for r in results.values()]),
           win_rate=np.mean([r['win_rate'] for r in results.values()]),
           positive_dates_ratio=positive_ratio,
           date_returns=results
       )
   ```

### Phase 4: Market Regime Detection（后续）

- 分析各日期特征（gap distribution, trade_rate 分布）
- 预测 "好日期" vs "坏日期"
- 提高跨日期稳定性

---

## Key Design Principles

1. **Boolean Selection** - 全选/全弃，不做排序
2. **Avg Return 验证** - Event 整体效果，不用 IC
3. **跨日期稳定性** - 正收益日期 > 60%
4. **Anti-patterns** - 明确标记需避免的组合
5. **语义化** - 每个事件有明确的市场含义

---

## Questions Resolved

1. **Q: percentile_rank lookback window?**
   A: 不需要 percentile_rank，删除整个 Layer 1

2. **Q: IC 计算用什么返回?**
   A: 不计算 IC，改用 Avg Return + Win Rate

3. **Q: 多事件触发时如何处理?**
   A: 返回匹配的第一个 Event（按 priority 排序）

4. **Q: 如何提高跨日期稳定性?**
   A: Market Regime Detection（后续任务）

---

## Validation Evidence

详见 [event-framework-validation.md](event-framework-validation.md)

| Metric | Baseline | Best Event | Improvement |
|--------|----------|------------|-------------|
| Avg Return | -0.23% | **+4.29%** | **+4.52%** |
| Win Rate | 38.89% | **45.23%** | +6.34% |
| IC | -0.018 | +0.043 | +0.061 |

**结论**：Event Selection 的价值在于 Boolean filter（Avg Return 改善 4.52%），而非 Factor Ranking（IC 改善仅 0.061）。

---

*Revised: 2026-04-21*
*Based on: Event Framework Validation experiment*
*Key change: Boolean Selection, not Factor Ranking*
