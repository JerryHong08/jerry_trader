# Bootstrap Coordinator Design V2

## 问题澄清

经过深入代码分析，发现 bootstrap 的复杂性远超预期：

### Timeframe 分类与 Bootstrap 策略

| Timeframe | Trades Bootstrap | Custom Bar Backfill | Meeting Bar Merge | FactorEngine 需求 |
|-----------|------------------|---------------------|-------------------|-------------------|
| **10s** | ✅ 必须 (唯一方式) | ❌ 不支持 | ✅ 需要 | Tick only |
| **1m, 5m, 15m, 30m, 1h, 4h** | ✅ 支持 | ✅ 支持 | ✅ 需要 | Tick + Bar |
| **1d, 1w** | ❌ 不支持 (跨 session) | ❌ 不支持 | ❌ N/A | Bar only (实时) |

### 关键洞察

1. **10s 是特殊的**：只能从 trades 构建，BarsBuilder 是唯一生产者
2. **Intraday bars 双重来源**：
   - BarsBuilder 从 trades 构建 (含 meeting bar 处理)
   - ChartBFF 从预聚合 bars 获取 (Polygon/custom)
3. **Meeting bar merge 是 BarsBuilder 的核心职责**：
   - 检测第一个 WS tick 落在哪个 bar
   - 存储 REST half，等待 WS half 完成时合并
4. **FactorEngine 的依赖**：
   - Tick indicators: 只依赖 trades (任意 timeframe 的 ticker)
   - Bar indicators: 依赖 bars (每个 timeframe 独立)

## 最终设计：渐进式 V2（推荐实现）

### 设计原则

1. **Meeting bar 逻辑留在 BarsBuilder** - 不拆散已有复杂逻辑
2. **Coordinator 只协调"何时开始"** - 不管理"数据如何 merge"
3. **Per-timeframe 状态跟踪** - 但简化状态机
4. **Trades 临时存储** - Redis gzip，1小时 TTL

### 架构图

```
┌─────────────────────────────────────────────────────────────┐
│  Coordinator (轻量级状态机)                                    │
│  ─────────────────────────                                    │
│  Per-timeframe: PENDING → FETCHING → BARS_READY → WARMUP → READY│
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌──────────────┐    ┌─────────────────┐    ┌──────────────┐
│ BarsBuilder  │    │ ChartBFF        │    │ FactorEngine │
│ ───────────  │    │ ─────────       │    │ ──────────── │
│ • Fetch trades│    │ • Custom bar    │    │ • Tick warmup│
│ • Build bars │    │   backfill      │    │ • Bar warmup │
│ • Meeting bar│    │ • Query bars    │    │              │
│   merge      │    │   for FE        │    │              │
│ • Store bars │    │                 │    │              │
│   to CH      │    │                 │    │              │
└──────────────┘    └─────────────────┘    └──────────────┘
```

### Bootstrap 策略矩阵（简化版）

| Timeframe | needs_trades | bars_source | 说明 |
|-----------|-------------|-------------|------|
| **10s** | ✅ | `trades_only` | 只能从 trades 构建 |
| **1m-4h** | ✅ | `trades_or_clickhouse` | BarsBuilder 优先，含 meeting bar |
| **1d/1w** | ❌ | `ws_only` | 实时 WS 构建，无 bootstrap |

### 数据流

```
ChartBFF.subscribe("PRSO", timeframes=["10s", "1m"])
    │
    ▼
Coordinator.start_bootstrap("PRSO", ["10s", "1m"])
    │
    ├─▶ 分析：两个 timeframe 都需要 trades
    │
    ├─▶ 触发 BarsBuilder: trades_backfill("PRSO", ["10s", "1m"])
    │       ├─▶ 从 parquet 读取 trades [4:00 → now]
    │       ├─▶ 同时构建 10s bars 和 1m bars
    │       ├─▶ Meeting bar 检测与存储
    │       ├─▶ 写入 ClickHouse (两个 timeframe)
    │       ├─▶ Coordinator.store_trades("PRSO", trades)
    │       └─▶ Coordinator.on_bars_ready("PRSO", "10s")
    │           Coordinator.on_bars_ready("PRSO", "1m")
    │
    ├─▶ FactorEngine 注册消费者
    │       ├─▶ tick_warmup: 等待 trades_ready
    │       ├─▶ bar_warmup:10s: 等待 bars_ready(10s)
    │       └─▶ bar_warmup:1m: 等待 bars_ready(1m)
    │
    └─▶ Coordinator.wait_for_ticker_ready("PRSO")
            │
            ▼
    ChartBFF 开始 WebSocket 推送
```

### 场景处理

#### 场景1：新订阅（无历史数据）
- BarsBuilder 从 session start (4:00 AM) 读取全部 trades
- 构建所有 requested timeframes 的 bars
- FactorEngine 从 trades 做 tick warmup，从 ClickHouse 做 bar warmup

#### 场景2：重新订阅（有历史数据，gap 30分钟）
- BarsBuilder 检测 last_bar_start，只读取 gap trades
- 补充写入 ClickHouse，跳过已有 bars
- FactorEngine EMA 从历史 bars 重新计算（完整历史）
- TradeRate 从 gap trades 补充 warmup

### 关键 API

```python
class BootstrapCoordinator:
    def start_bootstrap(self, symbol: str, timeframes: list[str]) -> BootstrapPlan:
        """分析需求，启动 bootstrap 流程"""

    def store_trades(self, symbol: str, trades: list[tuple]) -> str:
        """BarsBuilder 调用：存储 trades 供 FactorEngine 读取"""

    def get_trades(self, symbol: str) -> list[tuple]:
        """FactorEngine 调用：读取 trades 做 tick warmup"""

    def on_bars_ready(self, symbol: str, timeframe: str):
        """BarsBuilder 调用：通知某 timeframe bars 已就绪"""

    def register_consumer(self, symbol: str, phase: str, consumer_id: str):
        """FactorEngine 调用：注册为消费者"""

    def report_done(self, symbol: str, phase: str, consumer_id: str):
        """FactorEngine 调用：报告某阶段完成"""

    def wait_for_ticker_ready(self, symbol: str, timeout: float) -> bool:
        """ChartBFF 调用：等待全部就绪"""
```

### 实现计划（修订）

#### Phase 1: Coordinator Core V2 ✅
- [x] 重构 BootstrapCoordinator，支持 per-timeframe 状态
- [x] 实现 `store_trades` / `get_trades` (Redis gzip)
- [x] 实现消费者注册与完成跟踪

#### Phase 2: BarsBuilder 集成 ✅
- [x] 调用 `coordinator.store_trades()`
- [x] 调用 `coordinator.on_bars_ready()` per timeframe
- [x] 保留 `_bootstrap_events` 供 ChartBFF REST API 兼容

#### Phase 3: ChartBFF 集成 ✅
- [x] 调用 `coordinator.start_bootstrap()`
- [x] 等待 `wait_for_ticker_ready()`
- [x] WebSocket subscribe 非阻塞等待 (fire-and-forget)
- [x] REST API 阻塞等待直到 ready

#### Phase 4: FactorEngine 集成 ✅
- [x] 从 Coordinator 读取 trades（tick warmup）
- [x] Per-timeframe bar warmup 从 ClickHouse
- [x] 报告各阶段完成 (report_done)
- [x] 移除 legacy bootstrap 代码

#### Phase 5: 测试（待进行）
- [ ] 场景1：新订阅 PRSO (10s + 1m)
- [ ] 场景2：重新订阅（有历史数据）
- [ ] 场景3：仅 1d（ws_only，无 bootstrap）

### 决策确认

1. **Meeting bar 逻辑**: 留在 BarsBuilder，Coordinator 不感知
2. **Bar 来源**: BarsBuilder 优先构建，ChartBFF custom backfill 作为 fallback
3. **TradeRate 历史**: 从 Coordinator 读取 trades，支持 gap fill
4. **Cleanup**: Trades 1小时 TTL，或 FactorEngine 读取后主动删除

### Bootstrap 策略矩阵

```python
BOOTSTRAP_STRATEGIES = {
    # Timeframe: (needs_trades, needs_bars, bar_source)
    "10s":  (True,  True,  "trades_only"),      # 只能从 trades
    "1m":   (True,  True,  "trades_or_bars"),  # 可选，优先 trades (有 merge)
    "5m":   (True,  True,  "trades_or_bars"),
    "15m":  (True,  True,  "trades_or_bars"),
    "30m":  (True,  True,  "trades_or_bars"),
    "1h":   (True,  True,  "trades_or_bars"),
    "4h":   (True,  True,  "trades_or_bars"),
    "1d":   (False, False, "ws_only"),         # 实时 WS 构建
    "1w":   (False, False, "ws_only"),
}
```

### 数据流（最终实现）

```
ChartBFF.ws_endpoint.subscribe(ticker="AAPL", timeframes=["1m", "5m"])
    │
    ├─▶ Coordinator.start_bootstrap("AAPL", ["1m", "5m"])
    │       │
    │       └─▶ 分析 timeframe 需求，创建 bootstrap 计划
    │           - 1m/5m: needs_trades=True, needs_bars=True
    │
    ├─▶ XADD factor_tasks: {"action": "add", "ticker": "AAPL", "timeframes": "1m,5m"}
    │
    └─▶ (async) Coordinator.wait_for_ticker_ready("AAPL") - 后台等待

BarsBuilder._tasks_listener (收到 factor_tasks)
    │
    └─▶ _add_ticker("AAPL", ["1m", "5m"])
        │
        └─▶ 如果是新 ticker: 启动 trades_backfill 线程
            │
            ├─▶ Fetch trades from Polygon/parquet (live/replay)
            ├─▶ Build bars for all bootstrap_tfs (10s, 1m, 5m...)
            ├─▶ Meeting bar detection & merge
            ├─▶ Write bars to ClickHouse
            ├─▶ Coordinator.store_trades("AAPL", trades) - 存储 trades
            ├─▶ Coordinator.on_bars_ready("AAPL", "1m") - 通知 bars ready
            └─▶ Coordinator.on_bars_ready("AAPL", "5m") - 每个 timeframe

FactorEngine._tasks_listener (收到 factor_tasks)
    │
    └─▶ add_ticker("AAPL", ["1m", "5m"])
        │
        └─▶ 启动 _run_bootstrap 线程
            │
            ├─▶ 注册 tick_warmup 消费者 (如果 needs_trades)
            │       │
            │       ├─▶ 等待 trades available (coordinator.get_trades)
            │       ├─▶ Feed trades to tick indicators (TradeRate)
            │       └─▶ Coordinator.report_done("AAPL", "tick_warmup", "factor_engine")
            │
            └─▶ 注册 bar_warmup:1m 和 bar_warmup:5m 消费者
                    │
                    ├─▶ 等待 bars ready (poll coordinator state)
                    ├─▶ Query bars from ClickHouse
                    ├─▶ Feed bars to bar indicators (EMA20)
                    └─▶ Coordinator.report_done("AAPL", "bar_warmup:1m", "factor_engine")

ChartBFF REST API / WebSocket
    │
    └─▶ get_chart_bars("AAPL", "1m") / wait_bootstrap
        │
        └─▶ Coordinator.wait_for_ticker_ready("AAPL", timeout=30s)
            │
            ├─▶ 等待所有消费者完成 (tick_warmup + all bar_warmup)
            └─▶ 返回 ready → 查询 ClickHouse 返回 bars
```

### 关键设计变更

#### 1. Coordinator 职责细化

```python
class BootstrapCoordinatorV2:
    """
    按 timeframe 跟踪 bootstrap 状态，处理复杂的依赖关系。
    """

    def start_bootstrap(self, symbol: str, timeframes: list[str]) -> BootstrapPlan:
        """
        分析 timeframes，生成 bootstrap 计划。

        Returns:
            BootstrapPlan: {
                "needs_trades": ["10s", "1m", "5m"],
                "needs_bars": {"10s": "trades", "1m": "clickhouse_or_trades", "1d": "ws_only"},
                "can_parallel": [...],
            }
        """

    def on_trades_ready(self, symbol: str, trades_key: str, trade_count: int):
        """
        BarsBuilder 调用：trades 已 fetch 并存储。
        触发：
        1. 更新所有需要 trades 的 timeframe 状态
        2. 通知 FactorEngine 可以开始 tick warmup
        3. 通知需要 meeting bar 的 bars 可以继续
        """

    def on_bars_ready(self, symbol: str, timeframe: str, source: str):
        """
        BarsBuilder 或 ChartBFF 调用：bars 已就绪。
        触发：
        1. 更新该 timeframe 的 bar 状态
        2. 通知 FactorEngine 可以开始该 timeframe 的 bar warmup
        """

    def on_meeting_bar_stored(self, symbol: str, timeframe: str, bar_start: int):
        """
        BarsBuilder 调用：检测到 meeting bar，REST half 已存储。
        用于跟踪 merge 状态。
        """
```

#### 2. Meeting Bar 状态跟踪

```python
# BarsBuilder 检测到 meeting bar 时调用
coordinator.on_meeting_bar_detected(
    symbol="AAPL",
    timeframe="1m",
    bar_start=1704067200000,
    rest_half_trades=150,  # 从 trades_backfill 来的 trade 数量
)

# 当 WS bar 完成时，BarsBuilder merge 并调用
coordinator.on_meeting_bar_merged(
    symbol="AAPL",
    timeframe="1m",
    bar_start=1704067200000,
    merged_bar={...},  # 完整的 bar
)
```

#### 3. FactorEngine 消费模型

```python
class FactorEngine:
    def on_subscribe(self, symbol: str, timeframes: list[str]):
        # 1. 注册 tick warmup 消费者 (如果 symbol 需要 trades)
        if coordinator.symbol_needs_trades(symbol):
            coordinator.register_consumer(
                symbol=symbol,
                phase="tick_warmup",
                consumer_id="factor_engine",
            )

        # 2. 注册每个 timeframe 的 bar warmup 消费者
        for tf in timeframes:
            coordinator.register_consumer(
                symbol=symbol,
                phase=f"bar_warmup:{tf}",
                consumer_id="factor_engine",
            )

    def on_trades_ready(self, event):
        """Coordinator 通知 trades 已就绪"""
        trades = coordinator.get_trades(event.data["trades_key"])

        # Feed to tick indicators
        for ts, price, size in trades:
            for ind in self.tick_indicators[event.symbol]:
                ind.on_tick(ts, price, size)

        # Report done
        coordinator.report_done(
            symbol=event.symbol,
            phase="tick_warmup",
            consumer_id="factor_engine",
        )

    def on_bars_ready(self, event):
        """Coordinator 通知某 timeframe bars 已就绪"""
        tf = event.data["timeframe"]
        bars = self.query_bars_from_clickhouse(event.symbol, tf)

        # Feed to bar indicators
        for bar in bars:
            for ind in self.bar_indicators[event.symbol][tf]:
                ind.update(bar)

        # Report done
        coordinator.report_done(
            symbol=event.symbol,
            phase=f"bar_warmup:{tf}",
            consumer_id="factor_engine",
        )
```

### 状态机（按 Timeframe）

```
# For 10s (trades_only):
PENDING ──▶ TRADES_FETCHING ──▶ TRADES_READY ──▶ BARS_BUILDING ──▶ BARS_READY ──▶ WARMUP ──▶ READY

# For 1m (trades_or_bars):
PENDING ──┬─▶ TRADES_FETCHING ──▶ TRADES_READY ──┬─▶ BARS_BUILDING ──▶ BARS_READY ──▶ WARMUP ──▶ READY
          │                                        │
          └─▶ BARS_BACKFILLING ──▶ BARS_READY ────┘  (如果 custom_bar_backfill 更快)

# For 1d (ws_only):
PENDING ──▶ WS_SUBSCRIBED ──▶ READY (无需 bootstrap，直接等待实时 WS)
```

### 事件定义（修订）

```python
class BootstrapEventType(Enum):
    # Coordinator 发起
    BOOTSTRAP_PLANNED = "bootstrap_planned"  # 生成计划完成

    # BarsBuilder 事件
    TRADES_FETCH_START = "trades_fetch_start"
    TRADES_FETCH_DONE = "trades_fetch_done"      # trades 已存储
    MEETING_BAR_DETECTED = "meeting_bar_detected"
    MEETING_BAR_MERGED = "meeting_bar_merged"
    BARS_BUILT = "bars_built"                    # BarsBuilder 构建的 bars

    # ChartBFF 事件
    BARS_BACKFILLED = "bars_backfilled"          # 从外部获取的 bars

    # FactorEngine 完成
    TICK_WARMUP_DONE = "tick_warmup_done"
    BAR_WARMUP_DONE = "bar_warmup_done"          # per timeframe

    # 最终状态
    TIMEFRAME_READY = "timeframe_ready"          # 单个 timeframe 就绪
    TICKER_READY = "ticker_ready"                # 所有 requested timeframes 就绪
```

## 实现计划（修订）

### Phase 1: Coordinator Core V2

- [ ] 重写 BootstrapCoordinator，支持按 timeframe 跟踪
- [ ] 实现 BootstrapPlan 生成逻辑
- [ ] 实现 meeting bar 状态跟踪
- [ ] 更新 EventBus 事件类型

### Phase 2: BarsBuilder 集成

- [ ] 移除 `_bootstrap_events`
- [ ] 调用 coordinator.on_meeting_bar_detected()
- [ ] 调用 coordinator.on_meeting_bar_merged()
- [ ] 调用 coordinator.on_bars_ready()

### Phase 3: ChartBFF 集成

- [ ] 调用 coordinator.start_bootstrap() 替代直接触发
- [ ] 等待 coordinator.on_ticker_ready() 而非单独等待 BarsBuilder

### Phase 4: FactorEngine 集成

- [ ] 按 timeframe 注册消费者
- [ ] 从 Coordinator 读取 trades (tick warmup)
- [ ] 从 ClickHouse 读取 bars (bar warmup per tf)
- [ ] 报告每阶段完成

### Phase 5: 测试

- [ ] 10s only (trades only path)
- [ ] 1m + 5m (trades with meeting bar merge)
- [ ] 1d only (ws_only path)
- [ ] 混合：10s, 1m, 1d

## 与旧设计的区别

| 方面 | V1 (旧) | V2 (新) |
|------|---------|---------|
| 状态粒度 | 全局 per symbol | 按 timeframe |
| 状态机 | 线性 PENDING→READY | 并行多路径 |
| Meeting bar | 未考虑 | 核心跟踪 |
| 10s 特殊处理 | 未考虑 | 显式处理 |
| FactorEngine | 单一 tick warmup | tick + per-tf bar warmup |

## 待确认决策

1. **是否让 BarsBuilder 为所有 intraday TFs 构建 bars？**
   - 优点：统一的 meeting bar merge 逻辑
   - 缺点：可能比预聚合 bars 慢

2. **Custom bar backfill 何时触发？**
   - 选项 A: 如果 ClickHouse 中已存在 bars（来自 BarsBuilder），跳过
   - 选项 B: 总是并行执行，Coordinator 去重

3. **Meeting bar 存储位置？**
   - 选项 A: BarsBuilder 内存 (_rest_meeting_bars)
   - 选项 B: Coordinator 管理，支持跨服务恢复
