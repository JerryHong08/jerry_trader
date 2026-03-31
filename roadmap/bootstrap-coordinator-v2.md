# Bootstrap Coordinator V2 - 架构简化设计

## 问题陈述

当前架构使用 **Redis Stream** (`factor_tasks`) 来通知 BarsBuilder 和 FactorEngine 添加 ticker，同时用 **BootstrapCoordinator** 来管理 bootstrap 状态。这导致：

1. **双通道混乱** - Redis Stream 和 Coordinator 并行，时序不可控
2. **消息延迟/丢失** - Redis Stream 的 pending 消息会阻塞新消息 1 分钟以上
3. **状态不一致** - `add_ticker` 和 `start_bootstrap` 是独立的，无法保证执行顺序
4. **超时问题** - REST API 的 `wait_for_bootstrap` 经常超时，因为 Event 还没创建
5. **不必要的 bootstrap** - 即使 ClickHouse 已有数据，仍然触发完整 bootstrap 流程

## 当前问题架构（混乱）

```
ChartBFF ──┬── XADD ──► BarsBuilder ──► _add_ticker()
           │   (Redis Stream)           (异步，不可靠)
           ├── XADD ──► FactorEngine ──► add_ticker()
           │   (Redis Stream)           (异步，不可靠)
           └── coordinator.start_bootstrap()
                   │
                   └── 等待 bars/trades ready
                        (与上面的 add_ticker 并行，时序不确定)
```

## 解决方案：服务自治 + Coordinator 协调

**删除 Redis Stream，服务自己查询 ClickHouse 决定是否需要 bootstrap。**

### 目标架构（简化）

```
Coordinator.start_bootstrap(symbol, timeframes)
    │
    ├── 注册 symbol 到内部状态（轻量，不查询 ClickHouse）
    │
    └── 调用 bars_builder.add_ticker(symbol, timeframes) ──┐
        │                                                   │
        │  BarsBuilder 内部逻辑：                           │
        │  ─────────────────────                           │
        │  1. 查询 ClickHouse 每个 timeframe 的 bars       │
        │  2. 如果全部已有 bars：                           │
        │     ├── coordinator.on_bars_ready(tf) 逐个通知   │
        │     └── 跳过 trades_backfill                     │
        │  3. 如果有 timeframe 没有 bars：                  │
        │     └── 启动 trades_backfill（完成后通知）        │
        │                                                   │
        └── 调用 factor_engine.add_ticker(symbol, timeframes) ──┐
            │                                                   │
            FactorEngine 内部逻辑：                             │
            ───────────────────────                             │
            1. 注册 tick_warmup 消费者                          │
            2. 对于每个 timeframe：                             │
               ├── 查询 ClickHouse bars                        │
               ├── 如果已有 bars：立即 warmup，report_done      │
               └── 如果没有 bars：等待 Coordinator.bars_ready   │
```

## 设计原则

| 原则 | 说明 |
|------|------|
| **Coordinator 轻量** | 只跟踪状态，不查询 ClickHouse，不耦合业务逻辑 |
| **服务自治** | BarsBuilder/FactorEngine 自己查询数据，决定是否需要 bootstrap |
| **按需 bootstrap** | 已有数据时立即完成，避免不必要的延迟 |
| **删除 Redis Stream** | 直接方法调用，可靠、时序可控 |

## 实现计划

### Phase 1: 定义服务接口

创建 `BootstrapableService` 协议，服务启动时向 Coordinator 注册：

```python
class BootstrapableService(Protocol):
    def add_ticker(self, symbol: str, timeframes: list[str]) -> bool: ...
    def remove_ticker(self, symbol: str) -> bool: ...
    def is_ready(self, symbol: str) -> bool: ...
```

### Phase 2: 修改 BootstrapCoordinator

- [ ] 添加 `register_service(name: str, service: BootstrapableService)`
- [ ] 修改 `start_bootstrap` 直接调用已注册服务的 `add_ticker`
- [ ] **不**查询 ClickHouse，保持轻量
- [ ] 删除 Redis Stream 相关逻辑

### Phase 3: 修改 BarsBuilderService

- [ ] 实现 `BootstrapableService` 接口
- [ ] 在 `start()` 中向 Coordinator 注册
- [ ] 在 `_add_ticker` 中：
  - 查询 ClickHouse 判断每个 timeframe 是否已有 bars
  - 如果已有，立即 `coordinator.on_bars_ready(tf)`
  - 只有需要时才启动 `trades_backfill`
- [ ] 删除 `_tasks_listener` 和 `_process_pending_messages`

### Phase 4: 修改 FactorEngine

- [ ] 实现 `BootstrapableService` 接口
- [ ] 在 `start()` 中向 Coordinator 注册
- [ ] 在 `_run_bootstrap` 中：
  - 先查询 ClickHouse，如果已有 bars 直接 warmup
  - 只有没有 bars 时才等待 Coordinator 信号
- [ ] 删除 `_tasks_listener` 和 `_process_pending_messages`

### Phase 5: 修改 ChartBFF

- [ ] 删除所有 `XADD` 逻辑
- [ ] 简化 `_auto_subscribe_factors` 为纯 WebSocket 逻辑
- [ ] REST API `get_factors` 继续使用 `wait_for_bootstrap`

### Phase 6: 清理

- [ ] 删除 Redis Stream key `factor_tasks:{session_id}`
- [ ] 更新文档

## 关键变更

| 组件 | 当前 | 目标 |
|------|------|------|
| ChartBFF | XADD + coordinator.start_bootstrap() | 仅 coordinator.start_bootstrap() |
| BarsBuilder | _tasks_listener 消费 Redis | 自己查询 CH，按需 bootstrap，向 Coordinator 注册 |
| FactorEngine | _tasks_listener 消费 Redis | 自己查询 CH，按需 warmup，向 Coordinator 注册 |
| BootstrapCoordinator | 仅管理 bars/trades 状态 | 管理服务生命周期 + 协调状态 |

## 删除的代码

- `bars_builder_service.py`: `_tasks_listener`, `_process_pending_messages`
- `factor_engine.py`: `_tasks_listener`, `_process_pending_messages`
- `server.py`: 所有 `XADD` 逻辑，`_auto_subscribe_factors` 中的 XADD
- Redis Stream `factor_tasks:{session_id}`

## 好处

1. **时序可控** - 同步方法调用，无消息延迟
2. **按需 bootstrap** - 已有数据时立即完成，无等待
3. **单一状态源** - Coordinator 统一管理
4. **代码简化** - 删除大量 listener 代码
5. **服务自治** - 每个服务自己决定是否需要工作

## 相关文件

- `python/src/jerry_trader/services/orchestration/bootstrap_coordinator.py`
- `python/src/jerry_trader/services/bar_builder/bars_builder_service.py`
- `python/src/jerry_trader/services/factor/factor_engine.py`
- `python/src/jerry_trader/apps/chart_app/server.py`

## 拓扑任务

- **ID**: 6.2.1
- **标题**: 简化 Bootstrap 架构：删除 Redis Stream，使用 Coordinator 直接管理服务生命周期
- **状态**: in-progress
- **父任务**: 6.2 Configurable timeframe switch and bootstrap computation
