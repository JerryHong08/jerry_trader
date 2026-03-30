# Bootstrap Coordinator Design

## Context

Current EventBus implementation has fundamental orchestration issues:

1. **重复IO问题**: BarsBuilder 和 FactorEngine 各自从 Polygon/parquet fetch 相同的 trades
2. **Race Condition**: FactorEngine 可能还没创建好 TickerState，BarsBuilder 就已经完成 trades_backfill
3. **缺乏统一协调**: 没有全局状态知道 "ticker 是否完全 ready"
4. **完成通知分散**: ChartBFF 需要分别等待多个服务

## Goals

设计一个 **Bootstrap Coordinator** 来统一编排整个订阅流程：

- **单次IO**: Trades 只 fetch 一次，通过 Redis Streams 共享
- **无race condition**: 明确的状态机，服务按序启动
- **统一完成通知**: 单一的 `TickerReady` 事件
- **可扩展**: 未来添加新服务（SignalEngine等）只需订阅协调事件

## Design

### 状态机

```
PENDING ──▶ FETCHING ──▶ TRADES_READY ──▶ TICK_WARMUP ──▶ BAR_WARMUP ──▶ READY
              │                               │              │
              │                               │              ▼
              │                               │         SIGNAL_WARMUP (optional)
              │                               │              │
              └───────────────────────────────┴──────────────┘
```

### 数据流

```
ChartBFF: subscribe_ticker(AAPL, timeframes=["1m", "5m"])
              │
              ▼
┌─────────────────────────────┐
│   BootstrapCoordinator      │
│   ─────────────────────     │
│   1. SET state[AAPL]=PENDING│
│   2. PUBLISH: BootstrapStart│
└──────┬──────────────────────┘
       │
       ├─▶ BarsBuilder: FETCH trades ──▶ SET state=FETCHING
       │              │
       │              ▼
       │         XADD bootstrap:trades:AAPL
       │              │
       │              ▼
       │         PUBLISH: TradesReady(AAPL, trades_key)
       │              │
       ├─▶ FactorEngine: on_TradesReady
       │              │
       │              ├─▶ XREAD bootstrap:trades:AAPL
       │              ├─▶ tick warmup
       │              ├─▶ SET state=TICK_WARMUP
       │              └─▶ PUBLISH: TickWarmupDone(AAPL)
       │
       ├─▶ (parallel) TickDataService: on_TradesReady
       │              └─▶ historical tick query preparation
       │
       ▼
   When FactorEngine tick warmup done:
       │
       ├─▶ bar warmup from ClickHouse
       ├─▶ SET state=BAR_WARMUP
       └─▶ PUBLISH: BarWarmupDone(AAPL)

   When all warmups done:
       │
       ▼
   SET state[AAPL]=READY
   PUBLISH: TickerReady(AAPL)
       │
       ▼
   ChartBFF: start WebSocket streaming to frontend
```

### 关键设计决策

#### 1. Trades 存储: Redis Streams vs 直接传输

**选项 A: Event 包含 trades 数据** (当前设计方向)
```python
# Pros: 简单，无额外查询
# Cons: Event 太大，Redis Streams 有单消息大小限制 (~512MB)

Event.trades_ready(
    symbol="AAPL",
    trades=[(ts, price, size), ...],  # 可能几MB
)
```

**选项 B: Redis Stream 临时存储** (推荐)
```python
# BarsBuilder:
trades_key = f"bootstrap:trades:{symbol}:{uuid}"
redis_client.xadd(trades_key, {"chunk_0": compressed_trades})
redis_client.expire(trades_key, 3600)

event_bus.publish(Event.trades_ready(
    symbol="AAPL",
    trades_key=trades_key,
    trade_count=50000,
))

# FactorEngine:
trades = fetch_from_redis_stream(trades_key)
# 处理完成后 XACK，Coordinator 在所有消费者确认后清理
```

**选项 C: ClickHouse 缓冲** (大数据场景)
```python
# 当 trades > 10MB 时写入 ClickHouse
clickhouse.insert("bootstrap_buffer", trades)

event_bus.publish(Event.trades_ready(
    symbol="AAPL",
    storage="clickhouse",
    table="bootstrap_buffer",
    row_ids=[...],
))
```

**Decision**: 选项 B (Redis Streams) 作为默认，选项 C 作为大数据 fallback。

#### 2. Coordinator 实现方式

**选项 A: 独立服务进程**
```
Pros: 清晰分离，可独立扩展
Cons: 多一个进程，网络调用增加延迟
```

**选项 B: 库模式，ChartBFF 内嵌**
```
Pros: 零延迟，简单
Cons: ChartBFF 成为单点
```

**选项 C: 去中心化，各服务通过 EventBus 协作**
```
Pros: 无单点，每个服务自治
Cons: 复杂，需要分布式状态管理
```

**Decision**: 选项 B (库模式，ChartBFF 内嵌)。理由是：
- ChartBFF 已经是协调入口（处理前端 subscribe 请求）
- 零网络延迟
- 单实例运行（目前架构 ChartBFF 只有一个）

#### 3. 消费者确认机制

```python
# 伪代码
class BootstrapCoordinator:
    def on_tick_warmup_subscribe(self, symbol, consumer_id):
        """FactorEngine 注册为 tick warmup 消费者"""
        self.consumers[symbol]["tick_warmup"].add(consumer_id)

    def on_tick_warmup_done(self, symbol, consumer_id):
        """FactorEngine 完成 tick warmup"""
        self.consumers[symbol]["tick_warmup"].discard(consumer_id)

        if not self.consumers[symbol]["tick_warmup"]:
            # 所有消费者完成
            self.transition(symbol, TICK_WARMUP_DONE)
            self.trigger_bar_warmup(symbol)
```

### 事件定义

```python
class BootstrapEventType(Enum):
    # Coordinator 发起
    BOOTSTRAP_START = "bootstrap_start"

    # BarsBuilder 完成
    TRADES_FETCHED = "trades_fetched"      # Trades 已 fetch 到 Redis
    TRADES_READY = "trades_ready"          # 所有服务可开始消费

    # 各服务完成
    TICK_WARMUP_DONE = "tick_warmup_done"      # FactorEngine tick 指标完成
    BAR_WARMUP_DONE = "bar_warmup_done"        # FactorEngine bar 指标完成
    SIGNAL_WARMUP_DONE = "signal_warmup_done"  # SignalEngine 完成（未来）

    # Coordinator 最终通知
    TICKER_READY = "ticker_ready"

@dataclass
class BootstrapEvent:
    type: BootstrapEventType
    symbol: str
    timestamp_ns: int
    data: dict
```

## 实现计划

### Phase 1: BootstrapCoordinator 核心

- [ ] 创建 `services/orchestration/bootstrap_coordinator.py`
- [ ] 状态机实现 (PENDING → READY)
- [ ] 消费者注册/确认机制
- [ ] 与 ChartBFF 集成

### Phase 2: BarsBuilder 适配

- [ ] 移除 `_bootstrap_events`，改用 Coordinator
- [ ] Trades fetch 后写入 Redis Stream
- [ ] 发布 `TradesFetched` 事件

### Phase 3: FactorEngine 适配

- [ ] 移除 `_tick_bootstrap_events`，改用 Coordinator
- [ ] 从 Redis Stream 读取 trades（而非重新 fetch）
- [ ] 发布 `TickWarmupDone` / `BarWarmupDone`

### Phase 4: EventBus 增强

- [ ] 添加 Bootstrap 相关事件类型
- [ ] 消费者组支持（多个 FactorEngine 实例）

### Phase 5: 测试

- [ ] 单 symbol 完整流程测试
- [ ] 多 symbol 并行测试
- [ ] 故障恢复测试（某服务崩溃重连）

## 风险与缓解

| 风险 | 可能性 | 影响 | 缓解措施 |
|------|--------|------|---------|
| Redis 内存不足 | 低 | 高 | trades stream 1小时 TTL；大数据用 ClickHouse fallback |
| 消费者崩溃未确认 | 中 | 中 | Coordinator 超时机制，强制清理 |
| 顺序依赖复杂 | 中 | 低 | 详细日志，状态机可视化 |
| 延迟增加 | 低 | 高 | 测量基准；优化 Redis pipeline |

## 相关任务

- 替代/重构 [3.15](roadmap/event-bus-architecture.md)
- 与 [7.1](roadmap/event-bus-architecture.md) Redis streams to event_bus migration 相关

## 决策待确认

1. **是否需要持久化状态？**（Redis 重启后恢复 vs 重新 bootstrap）
2. **是否支持部分完成？**（tick warmup 成功但 bar warmup 失败）
3. **消费者超时策略？**（等待多久算失败）
