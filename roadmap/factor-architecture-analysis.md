# Factor Engine 架构全貌分析

**日期**: 2026-04-14
**任务**: 12.1 — 架构分析（决定迁移路线）
**状态**: 分析进行中

---

## 1. 现有架构组件

### 1.1 核心服务

| 服务 | 位置 | 职责 | 状态管理 |
|------|------|------|---------|
| **BarBuilder (Rust)** | `rust/src/bars.rs` | Tick → Bar aggregation | Rust 内部 VecDeque |
| **BarsBuilderService** | `services/bar_builder/bars_builder_service.py` | 调用 Rust BarBuilder, 写 ClickHouse, 发 Redis | Python 协调 |
| **FactorEngine** | `services/factor/factor_engine.py` | 计算因子, 发布到 Redis | Python TickerState |
| **ChartBFF** | `apps/chart_app/server.py` | WebSocket → Frontend | Python订阅集 |
| **BootstrapCoordinator** | `services/orchestration/bootstrap_coordinator.py` | Warmup 协调 | Python 状态机 |

### 1.2 Indicator 类（Python）

| 类 | 基类 | 状态位置 | 热插拔方式 |
|---|------|---------|----------|
| **EMA** | `BarIndicator` | Python `_value`, `_count` | 手动实例化 |
| **TradeRate** | `TickIndicator` | Python `_timestamps` deque | 手动实例化 |
| **(未来)** | `QuoteIndicator` | Python | 待实现 |

### 1.3 Rust 纯函数（factors.rs）

| 函数 | 类型 | 用途 |
|------|------|------|
| `volume_ratio` | batch | Backtest 计算 |
| `trade_rate` | batch | 单次查询（非 incremental）|
| `bootstrap_trade_rate` | batch | Warmup 批量计算 |
| `z_score`, `momentum`, `rsi` | batch | Backtest 计算 |

---

## 2. 数据流分析

### 2.1 实时场景数据流

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              实时 Pipeline                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Polygon/ThetaData/Replayer                                                 │
│         │                                                                   │
│         ▼                                                                   │
│  UnifiedTickManager ──────────────────────────────────────────────────────  │
│         │                    fan-out queues                                 │
│         ├──────────────┬──────────────┬──────────────┐                      │
│         ▼              ▼              ▼              ▼                      │
│  BarsBuilder      ChartBFF      FactorEngine    (其他consumer)              │
│  (Rust调用)        (WebSocket)   (TickIndicator)                           │
│         │              │              │                                    │
│         │              │              │                                    │
│         ▼              │              ▼                                    │
│  BarBuilder(Rust)     │         TickIndicator.on_tick()                    │
│  内部VecDeque          │         _timestamps deque                          │
│  维护状态              │              │                                    │
│         │              │              │                                    │
│         ▼              │         每1s: compute()                            │
│  completed bars        │         rust.trade_rate(list(deque))               │
│         │              │              │                                    │
│         ├──────────────┼──────────────┤                                    │
│         │              │              │                                    │
│         ▼              ▼              ▼                                    │
│  ClickHouse ──────── Frontend ──── FactorStorage                            │
│  (持久化)           (chart)       (写ClickHouse)                           │
│         │              │              │                                    │
│         ▼              │              ▼                                    │
│  Redis pub/sub        │         Redis pub/sub                              │
│  "bar_closed"         │         "factor_update"                            │
│         │              │              │                                    │
│         ├──────────────┘              │                                    │
│         ▼                             │                                    │
│  FactorEngine._bars_listener         │                                    │
│  (订阅 bar_closed)                   │                                    │
│         │                             │                                    │
│         ▼                             │                                    │
│  BarIndicator.update(bar)            │                                    │
│  EMA._value 更新                      │                                    │
│         │                             │                                    │
│         ▼                             │                                    │
│  Redis publish ──────────────────────┤                                    │
│  "factor_update"                      │                                    │
│         │                             │                                    │
│         └─────────────────────────────┤                                    │
│                                       │                                    │
│                                       ▼                                    │
│                              ChartBFF 接收                                  │
│                              推送 Frontend                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Backtest 场景数据流

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Backtest Pipeline                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ClickHouse market_snapshot                                                 │
│         │                                                                   │
│         ▼                                                                   │
│  DataLoader ─────────────────────────────────────────────────────────────   │
│         │                                                                   │
│         ▼                                                                   │
│  PreFilter (订阅逻辑)                                                       │
│         │                                                                   │
│         ▼                                                                   │
│  BacktestRunner                                                             │
│         │                                                                   │
│         ├──────────────────────────────────────────────────┐                │
│         │                                                  │                │
│         ▼                                                  ▼                │
│  rust.volume_ratio(volumes, window)                  rust.trade_rate()      │
│  rust.momentum()                                     rust.bootstrap_trade_rate│
│  rust.rsi()                                           (batch warmup)         │
│  (纯函数，传历史数据)                                                         │
│         │                                                  │                │
│         │                                                  │                │
│         ▼                                                  ▼                │
│  SignalEvaluator.evaluate()                           FactorStorage          │
│  Rule 条件判断                                         (可选写 ClickHouse)    │
│         │                                                  │                │
│         ▼                                                  │                │
│  BacktestResult                                         │                │
│  (聚合 metrics)                                         │                │
│         │                                                  │                │
│         ▼                                                  │                │
│  ClickHouse backtest_results                        │                │
│  (signal-level 数据)                                    │                │
│         │                                                  │                │
│         └─────────────────────────────────────────────────┘                │
│                                                                             │
│  experiment_logger.py (可选)                                                │
│  记录 hypothesis + run_id + lessons                                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.3 Warmup 数据流

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Warmup Pipeline                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  BootstrapCoordinator.start_bootstrap("AAPL", ["1m", "5m"])                 │
│         │                                                                   │
│         ▼                                                                   │
│  BarsBuilderService.fetch_trades()                                          │
│         │                                                                   │
│         ▼                                                                   │
│  load_trades_from_parquet(Rust)                                             │
│         │                                                                   │
│         ▼                                                                   │
│  BarBuilder(Rust).process_trades_batch()                                    │
│         │                                                                   │
│         ├───────────────────────────────────────────┐                       │
│         │                                           │                       │
│         ▼                                           ▼                       │
│  ClickHouse bars                               Redis trades(gzip)           │
│         │                                           │                       │
│         │                                           │                       │
│         ▼                                           ▼                       │
│  coordinator.on_bars_ready()                FactorEngine.consume_trades()    │
│         │                                           │                       │
│         ▼                                           ▼                       │
│  EventBus.publish(BarBackfillCompleted)     rust.bootstrap_trade_rate()     │
│         │                                   (批量 warmup, 返回 snapshots)   │
│         │                                           │                       │
│         ▼                                           ▼                       │
│  FactorEngine._on_event_bar_backfill_completed   TradeRate._timestamps      │
│  重试失败的 timeframe bootstrap                   extend(remaining_ts)       │
│         │                                           │                       │
│         └───────────────────────────────────────────┘                       │
│                                                                             │
│  FactorEngine._bootstrap_bars_for_timeframe()                               │
│         │                                                                   │
│         ▼                                                                   │
│  ClickHouse query historical bars                                           │
│         │                                                                   │
│         ▼                                                                   │
│  BarIndicator.update() 循环                                                  │
│  (Python for-loop，逐 bar 更新 EMA)                                          │
│         │                                                                   │
│         ▼                                                                   │
│  Indicator.ready == True                                                    │
│         │                                                                   │
│         ▼                                                                   │
│  coordinator.wait_for_ticker_ready()                                        │
│         │                                                                   │
│         ▼                                                                   │
│  ChartBFF 推送 "ready" 到 Frontend                                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. 状态管理分析

### 3.1 当前状态分布

| 状态 | 位置 | Python/Rust | 数据结构 |
|------|------|-------------|---------|
| **Bar aggregation** | BarBuilder(Rust) | Rust | `HashMap<(String, Timeframe), BarState>` |
| **Tick timestamps** | TradeRate(Python) | Python | `deque[int]` |
| **EMA value** | EMA(Python) | Python | `_value: float`, `_count: int` |
| **Ticker subscription** | FactorEngine(Python) | Python | `ticker_states: dict[str, TickerState]` |
| **Bootstrap progress** | BootstrapCoordinator(Python) | Python | `_states: dict[str, TimeframeBootstrapState]` |
| **Factor values** | FactorStorage(Python) | Python | ClickHouse writer |

### 3.2 状态一致性依赖

```
Python FactorEngine 状态 ←──→ Rust BarBuilder 状态
        ↑                           ↑
        │                           │
    Redis pub/sub               EventBus
        │                           │
        ↓                           ↓
    Python接收                   Python接收
```

**问题**：Python 和 Rust 之间存在 **状态同步依赖**，通过 Redis/EventBus 传递消息。

---

## 4. 统一 Trait 迁移影响评估

### 4.1 如果将状态迁移到 Rust

| 组件 | 当前 | 改动后 | 改动程度 |
|------|------|--------|---------|
| **FactorEngine** | Python TickerState | Rust HashMap<String, TickerState> | ⚠️ **核心重写** |
| **Indicator 类** | Python EMA, TradeRate | Rust RelativeVolume, EMA structs | ⚠️ **全部迁移** |
| **BootstrapCoordinator** | Python 状态机 | Rust 调用 trait.reset() | ⚠️ **重设计** |
| **ChartBFF** | Redis订阅 → Python → WS | Rust → Redis → Python → WS 或 Rust直接 | 中等 |
| **BarsBuilderService** | Python 协调 Rust | Python 协调 Rust（无变化）| ✅ 无变化 |
| **BacktestRunner** | rust 纯函数 | rust trait.compute_batch() | 小改动 |

### 4.2 冲突点详细分析

#### 冲突1: FactorEngine 重写

**当前**：
```python
class FactorEngine:
    ticker_states: dict[str, TickerState]  # Python 维护

    def add_ticker(self, symbol, timeframes):
        for tf in timeframes:
            for indicator in self._get_bar_indicators(tf):
                # 创建 Python Indicator 实例
                tf_state.bar_indicators.append(indicator)
```

**改动后**：
```rust
struct FactorEngine {
    ticker_states: HashMap<String, TickerState>,  // Rust 维护
}

impl FactorEngine {
    fn add_ticker(&mut self, symbol: &str, timeframes: &[Timeframe]) {
        for tf in timeframes {
            // 创建 Rust trait 实例
            let factor = registry.create("ema", params);
            state.bar_factors.insert("ema", factor);
        }
    }
}
```

**影响**：
- FactorEngine 全部迁移到 Rust
- Python 只做配置加载和调用
- 需要设计 PyO3 接口

#### 冲突2: BootstrapCoordinator 集成

**当前**：
```python
class BootstrapCoordinator:
    def wait_for_ticker_ready(self, symbol, timeout):
        # 等待 FactorEngine warmup 完成
        event.wait(timeout)

# FactorEngine warmup:
def _bootstrap_bars_for_timeframe(self, symbol, timeframe, tf_state):
    bars = query_clickhouse_bars(...)
    for bar in bars:
        for indicator in tf_state.bar_indicators:
            indicator.update(bar)  # Python 调用
```

**改动后**：
```rust
// Rust trait 内部 warmup
impl BarFactor for EMA {
    fn compute_batch(&self, bars: &[Bar]) -> Vec<Option<f64>> {
        // 内部 warmup + 计算
    }
}

// Python BootstrapCoordinator:
fn warmup(&mut self, symbol: &str, bars: Vec<Bar>) {
    let results = self.factors.get("ema").compute_batch(&bars);
    // 检查 results 最后是否 ready
}
```

**影响**：
- Warmup 逻辑迁移到 Rust trait.compute_batch()
- BootstrapCoordinator 仍可 Python，但调用 Rust 接口
- 或者 BootstrapCoordinator 也迁移 Rust

#### 冲突3: Redis pub/sub 数据流

**当前**：
```
BarBuilder(Rust) → Redis "bar_closed" → FactorEngine(Python) → Indicator.update()
```

**改动后（选项A）**：
```
BarBuilder(Rust) → 直接调用 → FactorEngine(Rust).update_bar()
                              ↓
                         Redis "factor_update" → ChartBFF → Frontend
``

**改动后（选项B）**：
```
BarBuilder(Rust) → Redis "bar_closed" → FactorEngine(Rust wrapper)
                                      ↓
                               Rust trait.update()
```

**影响**：
- 如果选择选项A：BarBuilder 需要持有 FactorEngine 引用（紧耦合）
- 如果选择选项B：仍通过 Redis 解耦，但 FactorEngine 改 Rust

---

## 5. 迁移路线选项

### Option A: 全盘重构（彻底统一）

**目标**：所有状态在 Rust，Python 只做配置和启动。

**步骤**：
```
1. Rust Factor trait + Registry (12.1设计)
2. 实现 RelativeVolume 验证设计 (12.2)
3. FactorEngine 重写 Rust
4. BootstrapCoordinator 重写 Rust 或简化为调用 Rust
5. ChartBFF 改为只订阅 Redis → WebSocket
6. Python Indicator 类删除 (12.4)
7. BarsBuilder 保持不变（已是 Rust）
```

**优点**：
- 架构统一简洁
- 状态全部 Rust（性能好）
- 减少跨语言调用

**缺点**：
- 大规模改动（FactorEngine + BootstrapCoordinator）
- 需要设计 PyO3 接口复杂
- 风险高（实时系统改动）

**时间预估**：2-3周

---

### Option B: 分层渐进（Backtest 先行）

**目标**：先统一 Backtest，实时暂不动。

**步骤**：
```
1. Rust Factor trait + Registry (12.1设计)
2. 实现 RelativeVolume 验证设计 (12.2)
3. BacktestRunner 改用 trait.compute_batch()
4. 实时场景保持 Python Indicator（暂不迁移）
5. 验证 Backtest 成功后，再迁移实时
```

**优点**：
- 小范围改动，风险可控
- Backtest 立即受益（新因子只需 Rust）
- 可以逐步验证

**缺点**：
- 存在两套系统一段时间
- 最终还是要迁移实时

**时间预估**：
- Phase 1（Backtest）：1周
- Phase 2（实时）：1-2周（后续）

---

### Option C: 混合架构（Python 协调 + Rust 状态）

**目标**：Python FactorEngine 改为调用 Rust trait，但协调逻辑保持 Python。

**步骤**：
```
1. Rust Factor trait + Registry (12.1设计)
2. 实现 RelativeVolume 验证设计 (12.2)
3. PyO3 wrapper: PyBarFactor { inner: Arc<dyn BarFactor> }
4. FactorEngine.ticker_states 改为 HashMap<String, PyBarFactor>
5. add_ticker → registry.create_factor() 返回 PyBarFactor
6. update_bar → PyBarFactor.update()（调用 Rust）
7. Python Indicator 类删除，但 FactorEngine Python shell 保留
```

**优点**：
- FactorEngine 仍 Python（熟悉）
- 状态在 Rust（性能）
- BootstrapCoordinator 不改动
- 改动范围适中

**缺点**：
- Python → Rust 跨语言调用开销（但比当前 deque→list 复制小）
- Python shell 仍需维护

**时间预估**：1-1.5周

---

## 6. 推荐：Option C（混合架构）

**理由**：

| 因素 | Option A | Option B | Option C |
|------|---------|---------|---------|
| 改动范围 | 大 | 小 | 中 |
| 风险 | 高 | 低 | 中 |
| 性能提升 | 最大 | 部分 | 大部分 |
| 架构简洁度 | 最简洁 | 两套系统 | 较简洁 |
| BootstrapCoordinator 改动 | 需改 | 不改 | 不改 |
| 实时可用时间 | 2-3周 | 1周（Backtest only）| 1-1.5周 |

**Option C 平衡了**：
- 改动可控（FactorEngine shell 保留）
- 性能提升（状态 Rust）
- 不改动 BootstrapCoordinator
- 可以快速验证

---

## 7. Option C 实施计划

### Phase 1: Rust Trait + Registry

```
文件结构:
rust/src/
  factor/
    mod.rs           ← 导出
    trait.rs         ← Factor, BarFactor, TickFactor trait
    registry.rs      ← FactorRegistry, FactorFactory
    data.rs          ← Bar, Trade, Quote structs
    relative_volume.rs ← RelativeVolume 实现
    ema.rs           ← EMA 实现（迁移）
    trade_rate.rs    ← TradeRate 实现（迁移）

lib.rs:
  mod factor;
  #[pymodule_export] PyFactorEngine
```

### Phase 2: PyO3 Wrapper

```rust
#[pyclass]
pub struct PyFactorEngine {
    registry: FactorRegistry,
    ticker_factors: HashMap<String, HashMap<String, Arc<dyn Factor>>>,
}

#[pymethods]
impl PyFactorEngine {
    #[pyo3(signature = (ticker, factor_name, params=None))]
    fn create_factor(&mut self, ticker: String, factor_name: String, params: Option<HashMap<String, PyObject>>) -> PyResult<()> {
        ...
    }

    fn update_bar(&mut self, ticker: String, factor_name: String, bar: PyBar) -> Option<f64> {
        ...
    }

    fn compute_batch_bar(&self, factor_name: String, bars: Vec<PyBar>) -> Vec<Option<f64>> {
        ...
    }
}
```

### Phase 3: Python FactorEngine 改调用 Rust

```python
# services/factor/factor_engine.py (改动后)
from jerry_trader._rust import PyFactorEngine

class FactorEngine:
    def __init__(self, ...):
        self._rust_engine = PyFactorEngine()
        # 保留 Redis, EventBus 集成逻辑

    def add_ticker(self, symbol, timeframes):
        # 调用 Rust 创建 factor 实例
        self._rust_engine.create_factor(symbol, "ema", {"period": 20})
        # 保留 BootstrapCoordinator 集成
```

### Phase 4: 删除 Python Indicator 类

```python
# 删除文件:
# - services/factor/indicators/ema.py
# - services/factor/indicators/trade_rate.py
# - services/factor/indicators/base.py（保留作为 stub？或删除）

# 改 factors.yaml 定义:
# class: EMA → 指向 Rust struct name
```

---

## 8. 需要讨论的问题

### 8.1 Bar 数据结构统一

**当前 Python Bar**：
```python
@dataclass
class Bar:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: int
```

**Rust Bar**：
```rust
pub struct Bar {
    pub ts_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,
}
```

**问题**：需要 PyO3 自动转换？

**方案**：使用 `#[pyclass]` + `Into<Bar>` trait

### 8.2 Timeframe 多实例

**问题**：同一 ticker 的多个 timeframe 需要独立 factor 实例

```python
# 当前
ticker_states["BIAF"].timeframe_states["1m"].bar_indicators = [EMA(period=20)]
ticker_states["BIAF"].timeframe_states["5m"].bar_indicators = [EMA(period=20)]
# 两个独立实例
```

**Rust 方案**：
```rust
ticker_factors: HashMap<String, HashMap<String, HashMap<Timeframe, Arc<dyn Factor>>>>
// ticker → factor_name → timeframe → instance
```

### 8.3 Warmup 数据获取

**问题**：warmup 需要从 ClickHouse 获取历史 bars

**当前**：Python query ClickHouse → for-loop → Indicator.update()

**方案**：
- Rust trait.compute_batch() 接收 bars
- Python 获取 bars → 传给 Rust batch compute
- Rust 返回 results，检查是否 ready

### 8.4 Redis pub/sub 保持？

**问题**：BarBuilder → FactorEngine 是否改直接调用？

**方案**：
- 保持 Redis pub/sub（解耦）
- FactorEngine shell 接收 Redis → 调用 Rust.update_bar()

---

## 9. 下一步行动

确认路线后：

1. **如果选择 Option C**：
   - 细化 PyO3 接口设计
   - 确定 Bar/Timeframe 数据结构
   - 开始 12.2: 实现 RelativeVolume

2. **如果选择 Option A 或 B**：
   - 更新设计文档
   - 评估时间线

---

## 附录：现有 Indicator 类代码量

| 文件 | 行数 | 复杂度 |
|------|------|--------|
| `indicators/base.py` | 110 | 简单 abstract class |
| `indicators/ema.py` | 60 | 简单实现 |
| `indicators/trade_rate.py` | 100 | 中等（调用 Rust）|
| **总计** | ~270 | 可一次性迁移 |

---

## 附录：FactorEngine 关键方法

| 方法 | 职责 | 改动程度 |
|------|------|---------|
| `add_ticker()` | 创建 indicator 实例 | 改调用 Rust |
| `remove_ticker()` | 清理状态 | 改调用 Rust |
| `_bars_listener()` | Redis订阅 bar_closed | 保持，改调用 Rust |
| `_tick_consumer()` | 处理实时 ticks | 保持，改调用 Rust |
| `_bootstrap_bars_for_timeframe()` | warmup bars | 改传 bars 给 Rust batch |
| `_process_bootstrap_trades()` | warmup ticks | 改传 trades 给 Rust batch |
| `_publish_factors()` | Redis 发布 | 保持 |
| `start()` / `stop()` | 生命周期 | 保持 |

改动：内部计算逻辑迁移 Rust，外部协调逻辑保持 Python。
