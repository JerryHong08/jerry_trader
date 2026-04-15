# Rust Compute Box Architecture

## 状态

**讨论阶段** - 2026-04-15

## 背景

当前架构问题：
1. BarsBuilder 和 FactorEngine trades 数据传递不统一（Replay 模式 bootstrap 不返回 trades）
2. Python ↔ Rust 多次 FFI overhead
3. 数据真理分散（trades 在 Python 层流转）

## 目标

将核心计算逻辑和 IO-heavy 任务封装到 Rust "计算盒子"中：
- 内部处理 trades → bars → factors 全链路
- 直接写入 ClickHouse
- 直接通过 WebSocket 推送前端

---

## 核心架构

### 整体架构图

```
┌─────────────────────────────────────────────────────────────┐
│                      Frontend                                │
│                                                              │
│  WebSocket: ws://localhost:8000 (Rust 实时推送)              │
│  HTTP API: http://localhost:8001/api (Python 历史查询)       │
└─────────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          │                               │
          ↓ WS                            ↓ HTTP
┌─────────────────────┐         ┌─────────────────────────────┐
│  Rust ComputeBox    │         │   Python ChartBFF           │
│  (Port 8000)        │         │   (Port 8001)               │
│                     │         │                             │
│  - BarBuilder       │         │  - BarQueryService          │
│  - FactorEngine     │         │    (历史 bars SELECT CH)    │
│  - WS Publisher     │         │                             │
│  - CH Writer        │         │  - FactorQueryService       │
│  - bars_buffer      │←─FFI───│    (历史 factors SELECT CH) │
│    (recent bars)    │         │                             │
│  - bootstrap_status │         │                             │
└─────────────────────┘         └─────────────────────────────┘
          │
          ↓ INSERT
┌─────────────────────┐
│   ClickHouse        │
│   (bars + factors)  │
└─────────────────────┘
```

### Rust ComputeBox 内部结构

```
┌──────────────────────────────────────────────────────────┐
│                    Rust ComputeBox                        │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              DataLayer (单一数据真理)                 │ │
│  │  - trades: HashMap<Ticker, VecDeque<Trade>>         │ │
│  │  - bars_buffer: HashMap<(Ticker, TF), VecDeque<Bar>>│ │
│  │  - bootstrap_status: HashMap<Ticker, BootstrapStatus│ │
│  └─────────────────────────────────────────────────────┘ │
│                           │                               │
│                           ↓ trades                        │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              BarBuilderEngine                        │ │
│  │  - ingest_trade() → update bars                     │ │
│  │  - 完成 bars:                                       │ │
│  │    1. 写入 bars_buffer                              │ │
│  │    2. INSERT to ClickHouse                          │ │
│  │    3. broadcast to factor_stream                    │ │
│  │    4. broadcast to ws_stream                        │ │
│  └─────────────────────────────────────────────────────┘ │
│                           │                               │
│                           ↓ bars stream                   │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              FactorEngineCore                        │ │
│  │  - subscribe bars_stream                            │ │
│  │  - compute factors on each bar                      │ │
│  │  - 输出:                                            │ │
│  │    1. INSERT to ClickHouse                          │ │
│  │    2. broadcast to ws_stream                        │ │
│  └─────────────────────────────────────────────────────┘ │
│                           │                               │
│                           ↓ bars + factors                │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              WebSocketPublisher                      │ │
│  │  - tokio async WebSocket server                     │ │
│  │  - subscribe/unsubscribe handlers                   │ │
│  │  - 推送 bars + factors 到 subscribers               │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

### Python 层职责

```
┌──────────────────────────────────────────────────────────┐
│                    Python Layer                           │
│                                                           │
│  - 配置管理 (config.yaml, factors.yaml)                   │
│  - CLI entrypoint (backend_starter, backtest CLI)         │
│  - HTTP API (FastAPI endpoints)                           │
│    - /api/bars/recent → Rust get_recent_bars()           │
│    - /api/bars/history → ClickHouse SELECT                │
│    - /api/factors/history → ClickHouse SELECT             │
│  - 服务编排 (进程启动、依赖注入)                            │
│                                                           │
│  不处理：                                                  │
│  - trades/bars/factors 实时数据流转                        │
│  - 计算、聚合、warmup                                      │
│  - ClickHouse 写入（由 Rust 直接处理）                     │
│  - WebSocket 实时推送（由 Rust 直接处理）                  │
└──────────────────────────────────────────────────────────┘
```

---

## 关键设计决策

### 1. Bar → Factor 数据传递

**方案：Rust 内部 broadcast channel**

```rust
pub struct ComputeBox {
    bar_stream: broadcast::Sender<CompletedBar>,
    factor_engine: FactorEngineCore,
}

impl ComputeBox {
    fn on_bar_complete(&mut self, bar: CompletedBar) {
        // 1. 写入 buffer
        self.bars_buffer.push(bar.clone());

        // 2. 写入 ClickHouse
        self.ch_writer.write_bar(&bar);

        // 3. 广播给 FactorEngine（内部 channel，零 FFI）
        self.bar_stream.send(bar.clone());

        // 4. 推送给 WS subscribers
        self.ws_publisher.publish_bar(&bar);
    }
}

impl FactorEngineCore {
    fn run(&mut self, bar_rx: broadcast::Receiver<CompletedBar>) {
        while let Ok(bar) = bar_rx.recv() {
            let factors = self.compute(&bar);
            self.ch_writer.write_factors(&factors);
            self.ws_publisher.publish_factors(&factors);
        }
    }
}
```

**收益**：
- 零 FFI，数据不离开 Rust
- FactorEngine 是 BarBuilder 的消费者，主从关系清晰

---

### 2. Real-time WebSocket Server

**方案：Rust tokio WS server**

```rust
pub struct WSPublisher {
    clients: HashMap<String, Vec<WebSocketStream>>,  // ticker → subscribers
}

impl WSPublisher {
    pub async fn run(&mut self, bar_rx: broadcast::Receiver<CompletedBar>) {
        loop {
            tokio::select! {
                Ok(bar) = bar_rx.recv() => {
                    let msg = serde_json::to_string(&BarMsg {
                        type: "bar",
                        ticker: bar.ticker,
                        timeframe: bar.timeframe,
                        open: bar.open,
                        ...
                    });
                    for client in self.clients.get(&bar.ticker) {
                        client.send(Message::Text(msg)).await;
                    }
                }
                Some(cmd) = self.cmd_rx.recv() => {
                    match cmd.action {
                        "subscribe" => self.handle_subscribe(cmd),
                        "unsubscribe" => self.handle_unsubscribe(cmd),
                    }
                }
            }
        }
    }
}
```

**订阅协议**：
```json
// Frontend → Rust
{ "action": "subscribe", "ticker": "BIAF", "timeframes": ["10s", "1m"] }

// Rust → Frontend (实时 bars)
{ "type": "bar", "ticker": "BIAF", "timeframe": "1m", "open": 1.23, ... }

// Rust → Frontend (实时 factors)
{ "type": "factor", "ticker": "BIAF", "name": "ema20", "value": 1.23 }
```

---

### 3. Live Mode Trades Backfill

**方案：Python fetch + Rust batch ingest（短期），Rust HTTP client（长期）**

**短期过渡方案**：
```python
# Python: 使用现有 Polygon 客户端获取 trades
trades = fetch_polygon_trades(symbol, from_ms)

# Rust: 批量 ingest（单次 FFI）
compute_box.ingest_trades_batch(symbol, trades)
```

**长期方案**：
```rust
pub async fn fetch_polygon_trades(
    api_key: &str,
    symbol: &str,
    from_ms: i64,
) -> Result<Vec<Trade>> {
    let url = format!(
        "https://api.polygon.io/v2/trades/{}/{}?apiKey={}",
        symbol, from_ms, api_key
    );
    let resp = reqwest::get(&url).await?;
    parse_polygon_trades(resp.text().await?)
}
```

**备选方案**：如果 Polygon trades 已实时入库到 CH，Rust 直接从 CH 读。

---

### 4. Factor → Frontend 推送

**方案：与 Bar 统一，通过 Rust WS Publisher**

```rust
impl FactorEngineCore {
    fn compute(&mut self, bar: &CompletedBar) -> Vec<FactorValue> {
        let factors = self.indicators.iter()
            .filter_map(|(name, indicator)| {
                indicator.update(bar).map(|v| FactorValue {
                    ticker: bar.ticker.clone(),
                    name: name.clone(),
                    timeframe: bar.timeframe,
                    value: v,
                    ts: bar.bar_end,
                })
            })
            .collect();

        // 写入 CH + 推送 WS
        self.ch_writer.write_factors(&factors);
        for f in &factors {
            self.ws_publisher.publish_factor(f);
        }

        factors
    }
}
```

**WS 消息格式**：
```json
{
    "type": "factor",
    "ticker": "BIAF",
    "name": "ema20",
    "timeframe": "1m",
    "value": 1.2345,
    "ts": 1710509400000
}
```

---

### 5. ChartBFF 协调 Frontend 和 Orchestration

**职责划分**：

| 组件 | 职责 | 数据路径 |
|------|------|----------|
| Rust ComputeBox | 实时计算、实时推送、写入 CH | trades → bars → factors → CH → WS |
| Python ChartBFF | 历史查询、HTTP API、服务编排 | SELECT CH → HTTP response |

**BarQueryService 设计**：

| 查询场景 | 数据来源 | 理由 |
|---------|----------|------|
| 最近 bars（≤ 1000） | Rust buffer + FFI getter | 零 IO，高频快 |
| 历史 bars（> 1000） | Python → ClickHouse SELECT | 低频，Python 方便 |

```python
# ChartBFF API
@app.get("/api/bars/recent")
def get_recent_bars(ticker: str, tf: str, count: int = 100):
    # 从 Rust buffer 获取
    bars = rust.get_recent_bars(ticker, tf, count)

    # 如果不够，从 CH 补充
    if len(bars) < count:
        ch_bars = query_clickhouse_bars(ticker, tf, count - len(bars))
        bars = merge_bars(bars, ch_bars)

    return {"bars": bars, "count": len(bars)}

@app.get("/api/bars/history")
def get_history_bars(ticker: str, tf: str, from_ts: int, to_ts: int):
    # 直接查询 CH
    bars = clickhouse.query(
        "SELECT * FROM bars WHERE ticker=? AND timeframe=?
         AND bar_start >= ? AND bar_start < ? ORDER BY bar_start",
        ticker, tf, from_ts, to_ts
    )
    return {"bars": bars}
```

---

### 6. Subscribe + Bootstrap 流程

**问题**：前端订阅时可能没有数据，需要触发 bootstrap。

**方案**：订阅触发 bootstrap，通过消息协议同步状态。

**BootstrapStatus 状态机**：
```rust
pub enum BootstrapStatus {
    NotStarted,    // 未 bootstrap
    InProgress,   // 正在 bootstrap
    Ready,        // 已完成
    Failed(String), // 失败
}
```

**流程图**：
```
Frontend subscribe → Rust 收到订阅
        │
        ├─ 检查 bootstrap_status
        │   ├─ Ready → 直接 ack
        │   ├─ NotStarted → 开始 bootstrap + ack "bootstrapping"
        │   └─ InProgress → ack "bootstrapping"
        │
        ↓ 异步 bootstrap（如果需要）
        │   ├─ 加载 trades → 构建 bars → 写入 buffer + CH
        │   ├─ 成功 → 发送 BootstrapReady
        │   └─ 失败 → 发送 BootstrapFailed
        │
Frontend 收到 SubscribeAck/BootstrapReady
        │
        ├─ bootstrapping → 显示 loading
        └─ BootstrapReady → fetch bars
        │
        ↓
Rust buffer 返回 bars → Frontend 显示
```

**消息协议**：

```json
// SubscribeAck
{
    "type": "subscribe_ack",
    "ticker": "BIAF",
    "timeframes": ["10s", "1m"],
    "status": "bootstrapping",  // 或 "ready"
    "message": "Bootstrap in progress..."
}

// BootstrapReady
{
    "type": "bootstrap_ready",
    "ticker": "BIAF",
    "bars_count": { "10s": 520, "1m": 87 },
    "message": "Bootstrap complete"
}

// BootstrapFailed
{
    "type": "bootstrap_failed",
    "ticker": "BIAF",
    "error": "No trades found",
    "message": "Bootstrap failed"
}
```

---

## 数据流向

```
External Data Source (Polygon/Databento/Replay)
         │
         ↓ trades (JSON/Parquet/CH)
    Rust LoadTrades
         │
         ↓
    DataLayer.trades[ticker].push(trade)
         │
         ├──────────────────┐
         ↓                  ↓
   BarBuilderEngine    FactorEngineCore
   ingest_trade()      (subscribe bar stream)
         │                  │
         ↓ bars             ↓ factors
    bars_buffer         factors_cache
         │                  │
         ↓                  ↓
    ClickHouse          ClickHouse
         │                  │
         └──────────────────┘
                ↓
         WebSocketPublisher
                ↓
           Frontend
```

---

## 与当前架构对比

| 维度 | 当前 | 新设计 |
|------|------|--------|
| trades 存储 | Python 层流转 | Rust DataLayer 内部 |
| bars 构建 | Rust BarBuilder，返回 Python | Rust 内部完成，直接写 CH |
| factors 计算 | Python FactorEngine | Rust FactorEngineCore |
| CH 写入 | Python clickhouse-connect | Rust 直接写入 |
| WS 推送 | Python asyncio → Redis → ChartBFF | Rust tokio → WebSocket |
| FFI 频率 | 高（每个 tick） | 低（仅配置/启动 + recent bars query） |
| 数据真理 | 分散 | 统一在 Rust DataLayer |

---

## 核心优势

1. **数据真理统一** - DataLayer 是唯一 trades/bars holder
2. **主从清晰** - BarBuilder 写入，FactorEngine 读取
3. **IO 内部化** - trades → bars → factors → CH → WS 全在 Rust
4. **FFI 最小化** - Python 只做启动、配置、历史查询
5. **性能最大化** - 无 Python ↔ Rust 数据拷贝
6. **架构清晰** - 计算盒子边界明确，职责分离

---

## 技术要点

### Rust ClickHouse 写入

```rust
use clickhouse::Client;

pub struct CHWriter {
    client: Client,
}

impl CHWriter {
    pub async fn write_bar(&self, bar: &CompletedBar) -> Result<()> {
        let mut insert = self.client.insert("bars")?;
        insert.write(bar)?;
        insert.end().await?;
        Ok(())
    }

    pub async fn write_factors(&self, factors: &[FactorValue]) -> Result<()> {
        let mut insert = self.client.insert("factors")?;
        for f in factors {
            insert.write(f)?;
        }
        insert.end().await?;
        Ok(())
    }
}
```

### Rust WebSocket Server

```rust
use tokio_tungstenite::WebSocketStream;

pub struct WSPublisher {
    subscribers: HashMap<String, Vec<WebSocketStream>>,
}

impl WSPublisher {
    pub async fn publish_bar(&mut self, bar: &CompletedBar) {
        let msg = serde_json::to_string(&BarMsg::from(bar));
        for client in self.subscribers.get(&bar.ticker).unwrap_or(&vec![]) {
            client.send(Message::Text(msg.clone())).await;
        }
    }
}
```

### Python 启动入口

```python
# backend_starter.py
def start_chart_machine(config: dict):
    # 启动 Rust ComputeBox（核心计算 + WS）
    compute_box = rust.ComputeBox(
        ch_config=config["clickhouse"],
        ws_port=8000,
        factors_config=load_factors_yaml(),
    )
    compute_box.start()  # Rust tokio runtime

    # 启动 ChartBFF HTTP Server（历史查询）
    uvicorn.run(chart_app, port=8001)
```

---

## 分阶段实施计划

### Phase 1: DataLayer + Bootstrap Status

- [ ] 创建 `rust/src/compute_box/data_layer.rs`
- [ ] trades 内部存储，不返回 Python
- [ ] bars_buffer + get_recent_bars() 接口
- [ ] bootstrap_status 状态管理
- [ ] Subscribe + Bootstrap 流程实现

### Phase 2: WebSocket Publisher

- [ ] 创建 `rust/src/compute_box/ws_publisher.rs`
- [ ] tokio WS server (Port 8000)
- [ ] subscribe/unsubscribe handlers
- [ ] bar + factor 消息推送
- [ ] SubscribeAck/BootstrapReady/BootstrapFailed 消息

### Phase 3: BarBuilderEngine 内部化

- [ ] BarBuilderEngine 直接写入 bars_buffer
- [ ] BarBuilderEngine 直接写入 ClickHouse
- [ ] broadcast channel → FactorEngineCore
- [ ] 删除 Python 层 bars 返回

### Phase 4: FactorEngineCore

- [ ] 创建 `rust/src/compute_box/factor_engine.rs`
- [ ] subscribe bar stream
- [ ] 从 DataLayer warmup
- [ ] compute factors + 写入 CH
- [ ] publish factors to WS

### Phase 5: Python 层简化

- [ ] ChartBFF 只保留 HTTP API（历史查询）
- [ ] BarsBuilderService 删除，变成配置传递层
- [ ] FactorEngine Python 版删除，变成 wrapper
- [ ] backend_starter 只做服务编排

### Phase 6: Live Mode Trades

- [ ] Rust HTTP client (Polygon API)
- [ ] 或 CH 作为统一数据源

---

## ROADMAP Tasks

- [ ] 13.1 DataLayer 统一 trades 存储 + bars_buffer + bootstrap_status
- [ ] 13.2 WebSocket Publisher Rust 实现
- [ ] 13.3 BarBuilderEngine 内部化 bars 写入
- [ ] 13.4 FactorEngineCore Rust 实现
- [ ] 13.5 Python 层简化（ChartBFF HTTP only）
- [ ] 13.6 Subscribe + Bootstrap 流程实现
- [ ] 13.7 Live mode trades backfill（Rust HTTP client）

---

## 风险与缓解

| 风险 | 缓解措施 |
|------|----------|
| Rust WS 稳定性 | 先实现为可选，Python WS 作为 fallback |
| 配置热更新 | 保留 Python 配置层，通过 FFI 传递 |
| 调试困难 | Rust 日志桥接到 Python logging |
| ClickHouse 连接池 | 使用 `clickhouse` crate 的连接池 |
| Bootstrap 时序问题 | SubscribeAck + BootstrapReady 消息协议 |

---

## 参考文档

- `roadmap/factor-plugin-architecture.md` - Factor 设计原则
- `roadmap/barsbuilder-rust-bootstrap.md` - Bootstrap 优化经验
