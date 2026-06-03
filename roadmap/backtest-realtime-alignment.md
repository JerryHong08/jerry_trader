# 回测-实盘对齐：Snapshot 时序选股问题

## Context

当前回测框架存在**严重的数据泄露问题**：使用了"未来数据"进行选股，导致回测结果无法准确反映实盘行为。

### 问题本质

**实盘流程（正确）**：
```
SnapshotProcessor 每 5s 收到 snapshot → 按 changePercent 排名 → 取 top 20
新进入的 ticker → 此时才开始订阅 → 开始计算 factors → 判断 events
```

**回测流程（当前，错误）**：
```
PreFilter 扫描整个 session → 找出"曾经进入过 top 20"的所有 ticker
加载这些 ticker 的整个 session 数据 → 从 session 开始就计算 factors → 判断 events
```

### 具体例子

| 时间 | 实盘 | 回测（当前） |
|------|------|--------------|
| 4:00 | A 不在 top 20，不处理 | A 被处理 ❌ |
| 5:00 | A 不在 top 20，不处理 | A 被处理 ❌ |
| 6:30 | A 首次进入 top 20，开始处理 | A 被处理 |
| 7:00 | A 在 top 20，继续处理 | A 被处理 |

**后果**：回测会错误地计算 A 在 4:00-6:30 的 signals，而这些 signals 在实盘中根本不会产生。

## Analysis

### 策略理解

实盘信号来源有两种：

1. **新进入 top 20** — 突然冒出来的强势股
   - 刚进入时涨幅已大
   - 判断是否有延续空间
   - 类似 "gap up + 突破确认"

2. **已有 top 20 的异常拉升** — 池子里的股票突然加速
   - 可能是催化剂触发
   - 判断真突破 vs 假突破
   - 类似 "consolidation + breakout"

### 当前架构的问题点

| 组件 | 当前实现 | 问题 |
|------|----------|------|
| PreFilter | 扫描整个 session，找"曾进入 top 20" | 使用未来数据 |
| DataLoader | 加载候选 ticker 的全量数据 | 包含不该存在的时间段 |
| FactorEngine/BatchEngine | 从 session 开始计算 | 计算了不该计算的时间 |
| EventEvaluator | 从 session 开始判断 | 产生了不该产生的信号 |

### `session_gap_pct` 的问题

当前 `session_gap_pct > 4%` 条件：
- 是 session-level 常量（开盘确定，session 内不变）
- 本质是"选股条件"，不是"选时机条件"
- 回测时用"事后知道的开盘价"筛选，实盘无法精确预知

## Decision

需要重构回测框架，实现**时序选股**：

```
1. 按时间顺序遍历 snapshot（模拟 SnapshotProcessor）
2. 每个 snapshot：排名 → 取 top 20 → 记录新进入 ticker 及时间
3. 对每个 ticker：只从"首次进入时间"开始计算 factors 和判断 events
```

### 关键改动

1. **Snapshot Replay Engine** — 模拟 SnapshotProcessor 的时序行为
2. **Subscription Time Tracking** — 记录每个 ticker 的订阅时间
3. **Factor 计算起点** — 只从订阅时间开始计算
4. **Event 判断起点** — 只从订阅时间开始判断

## Plan

### Phase 1：验证问题影响

- [ ] 对比分析：时序选股 vs 全量选股的信号差异
- [ ] 量化影响：有多少信号是"虚假的"（发生在订阅前）

### Phase 2：架构设计

- [ ] Snapshot Replay Engine 设计
- [ ] Subscription Time Tracking 数据结构
- [ ] FactorEngine/BatchEngine 改动方案

### Phase 3：实现

- [ ] Snapshot Replay Engine 实现
- [ ] PreFilter 改造（时序模式）
- [ ] DataLoader 改造（按订阅时间截断）
- [ ] BatchEngine 改造（按订阅时间起点）

### Phase 4：验证

- [ ] 对比新旧回测结果
- [ ] 确认无数据泄露
- [ ] 实盘数据验证

## Rejected

### 方案：保持现状，只分析"订阅后"的信号

- **理由**：事后过滤比架构改动简单
- **问题**：仍然依赖"未来选股"，选股本身就用了未来数据
- **结论**：不可接受，必须从根本上重构

## Related Tasks

- 11.40 SignalEngine 统一消费 events.yaml
- 6.22 统一 SnapshotFilterCriteria 抽象

## References

- `python/src/jerry_trader/services/backtest/pre_filter.py` — 当前 PreFilter 实现
- `python/src/jerry_trader/services/market_snapshot/processor.py` — 实盘 SnapshotProcessor 实现
- `python/src/jerry_trader/services/backtest/batch_engine.py:91-102` — session_gap_pct 计算
