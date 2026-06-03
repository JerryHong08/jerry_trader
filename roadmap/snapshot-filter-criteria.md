# SnapshotFilterCriteria 统一抽象设计

## 背景

当前系统中 snapshot 筛选逻辑分散在多处，参数不统一，难以复用：

| 位置 | 用途 | 条件参数 |
|------|------|----------|
| `backtest/data/cli.py` | snapshot_builder filter | `--filter-start-et`, `--filter-end-et` |
| `backtest/pre_filter.py` | PreFilter class | `min_gain_pct`, `min_relative_volume`, `new_entry_only`, `limit` |
| `runtime/backend_starter.py` | bootstrap cfg | `start_time`, `end_time` |
| `config.yaml` | SnapshotProcessor.bootstrap | `start_time`, `end_time` |

**问题：**
- Backtest CLI 只写入 ClickHouse，runtime 依赖 Redis stream
- Runtime replay 模式检测已有 ClickHouse 数据时，无法加载到 Redis stream
- 前端 OverviewChart 和 RankList 依赖 Redis stream，导致 `rankData=[]`, `timestamp=None`

---

## 方案C：Runtime 加载已有数据到 Redis Stream

当 runtime 检测到已有 ClickHouse 数据时，执行类似 pre-filter 的逻辑：

```python
# backend_starter.py
if existing_end_ms:
    # 1. Skip bootstrap (已有)
    clock.jump_to(end_ts_ns)
    clock.resume()

    # 2. 加载最新 rank snapshot 到 Redis stream
    _load_rank_snapshot_to_redis(ch_client, session_id, filter_criteria)

    self._bootstrap_complete_event.set()
```

**`_load_rank_snapshot_to_redis` 逻辑：**
1. 查询 ClickHouse 最新 snapshot (max event_time_ms)
2. 应用 `SnapshotFilterCriteria` 筛选 tickers
3. 写入 Redis stream `market_snapshot_processed`

---

## 统一抽象：SnapshotFilterCriteria

### Domain 层设计

```python
# domain/snapshot/filter.py (纯 domain，无 I/O)

from pydantic import BaseModel

class SnapshotFilterCriteria(BaseModel):
    """统一的 snapshot 筛选条件，backtest 和 runtime 共用"""

    # 时间范围
    start_time: str | None = "040000"  # HHMMSS
    end_time: str | None = "093000"    # HHMMSS

    # 增益/跌幅条件
    min_gain_pct: float = 0.0
    max_gain_pct: float | None = None  # None = 无上限

    # 量比条件
    min_relative_volume: float = 1.0

    # 持仓条件
    new_entry_only: bool = False  # 只选首次进入的

    # 数量限制
    limit: int = 20  # top N by rank

    # 排名范围
    max_rank: int | None = None  # rank <= N
```

### 配置入口

```yaml
# config.yaml - 统一配置
defaults:
  snapshot_filter:
    start_time: "040000"
    end_time: "093000"
    min_gain_pct: 5.0
    min_relative_volume: 2.0
    new_entry_only: false
    limit: 20
```

---

## 复用点分析

| 场景 | 调用入口 | 共享逻辑 |
|------|----------|----------|
| **Backtest CLI prepare** | `snapshot_builder.build()` | 写入 ClickHouse |
| **Backtest CLI run** | `PreFilter.find()` | 筛选 candidates |
| **Runtime bootstrap** | `HistoricalLoader.bootstrap()` | 写入 Redis + ClickHouse |
| **Runtime replay 检测已有数据** | `_load_rank_snapshot_to_redis()` | 筛选 + 写入 Redis stream |

统一后的调用链：

```
SnapshotFilterCriteria (domain)
        ↓
┌───────────────────────────────────────────────┐
│                                               │
│  backtest                          runtime    │
│  ─────────                         ─────────  │
│  PreFilter.find(criteria)          _load_rank_snapshot_to_redis(criteria)
│  snapshot_builder.build(criteria)  bootstrap(criteria)
│                                               │
└───────────────────────────────────────────────┘
```

---

## 实现步骤

### Phase 1: Domain 层创建

1. 创建 `python/src/jerry_trader/domain/snapshot/filter.py`
2. 定义 `SnapshotFilterCriteria` Pydantic model
3. 添加 `from_config()` 工厂方法

### Phase 2: PreFilter 重构

1. 修改 `backtest/pre_filter.py` 接受 `SnapshotFilterCriteria` 参数
2. 保留现有默认值兼容性
3. 添加 `find_by_criteria()` 方法

### Phase 3: Runtime 集成

1. 在 `backend_starter.py` 添加 `_load_rank_snapshot_to_redis()` 函数
2. 从 config 加载 criteria
3. 在检测已有数据分支调用该函数

### Phase 4: 配置统一

1. 更新 `config.yaml.example` 添加 `snapshot_filter` section
2. 更新 CLI 参数解析逻辑

---

## 相关任务

- ROADMAP task: **6.22** 统一 SnapshotFilterCriteria 抽象
- ROADMAP task: **6.22.1** Runtime replay 检测已有数据时加载 rank snapshot 到 Redis stream

---

## 设计决策记录

| 决策 | 理由 |
|------|------|
| 使用 Pydantic BaseModel | 类型安全、自动验证、JSON 序列化支持 |
| 放在 domain 层 | 纯值对象，无 I/O 依赖，符合分层架构 |
| 统一配置入口 | 减少散落的 CLI 参数，提高可维护性 |
| 方案C (加载已有数据) | 避免 backtest CLI 写 Redis 增加复杂度，runtime 启动时一次性加载更简洁 |
