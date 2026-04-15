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

## 设计方向

### Rust Compute Box 内部结构

```
┌──────────────────────────────────────────────────────────┐
│                    Rust Compute Box                       │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              DataLayer (单一数据真理)                 │ │
│  │  - trades: HashMap<Ticker, VecDeque<Trade>>         │ │
│  │  - bars: HashMap<(Ticker, TF), Vec<Bar>>            │ │
│  │  - 提供查询接口给 FactorEngine warmup                │ │
│  └─────────────────────────────────────────────────────┘ │
│                           │                               │
│                           ↓ trades                        │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              BarBuilderEngine                        │ │
│  │  - ingest_trade() → update bars                     │ │
│  │  - 内部持有 trades（不再返回 Python）                 │ │
│  │  - 完成 bars 直接写入 ClickHouse                     │ │
│  │  - 发布 bars 到 stream                              │ │
│  └─────────────────────────────────────────────────────┘ │
│                           │                               │
│                           ↓ bars stream                   │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              FactorEngineCore                        │ │
│  │  - subscribe bars stream                            │ │
│  │  - 从 DataLayer warmup trades                       │ │
│  │  - compute factors                                  │ │
│  │  - 完成写入 ClickHouse                              │ │
│  │  - 发布 factors 到 stream                           │ │
│  └─────────────────────────────────────────────────────┘ │
│                           │                               │
│                           ↓ bars + factors                │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              WebSocketPublisher                      │ │
│  │  - 直接推送 bars + factors 到前端                    │ │
│  │  - 通过 tokio async websocket                       │ │
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
│  - 服务编排 (进程启动、依赖注入)                            │
│  - Redis event bus (跨服务通信)                            │
│                                                           │
│  不处理：                                                  │
│  - trades/bars/factors 数据流转                            │
│  - 计算、聚合、warmup                                      │
│  - ClickHouse 写入（由 Rust 直接处理）                     │
└──────────────────────────────────────────────────────────┘
```

### 数据流向

```
External Data Source (Polygon/Databento/Replay)
         │
         ↓ trades (JSON/Parquet)
    Rust LoadTrades
         │
         ↓
    DataLayer.trades[ticker].push(trade)
         │
         ├──────────────────┐
         ↓                  ↓
   BarBuilderEngine    FactorEngineCore
   ingest_trade()      warmup from DataLayer
         │                  │
         ↓ bars             ↓ factors
   ClickHouse          ClickHouse
         │                  │
         └──────────────────┘
                ↓
         WebSocketPublisher
                ↓
           Frontend
```

### 与当前架构对比

| 维度 | 当前 | 新设计 |
|------|------|--------|
| trades 存储 | Python 层流转 | Rust DataLayer 内部 |
| bars 构建 | Rust BarBuilder，返回 Python | Rust 内部完成，直接写 CH |
| factors 计算 | Python FactorEngine | Rust FactorEngineCore |
| CH 写入 | Python 调用 clickhouse-connect | Rust 直接写入 |
| WS 推送 | Python asyncio → Redis → ChartBFF | Rust tokio → WebSocket |
| FFI 频率 | 高（每个 tick） | 低（仅配置/启动） |

### 核心优势

1. **数据真理统一** - DataLayer 是唯一 trades/bars holder
2. **主从清晰** - BarBuilder 写入，FactorEngine 读取
3. **IO 内部化** - trades → bars → factors → CH → WS 全在 Rust
4. **FFI 最小化** - Python 只做启动和配置
5. **性能最大化** - 无 Python ↔ Rust 数据拷贝

### 技术要点

#### Rust ClickHouse 写入

```rust
use clickhouse::Client;

pub async fn write_bars_batch(client: &Client, bars: &[CompletedBar]) -> Result<()> {
    let mut insert = client.insert("bars")?;
    for bar in bars {
        insert.write(bar)?;
    }
    insert.end().await?;
    Ok(())
}
```

#### Rust WebSocket Server

```rust
use tokio_tungstenite::WebSocketStream;

pub struct WSPublisher {
    clients: Vec<WebSocketStream>,
}

impl WSPublisher {
    pub async fn publish_bar(&mut self, bar: &CompletedBar) {
        let msg = serde_json::to_string(bar).unwrap();
        for client in &mut self.clients {
            client.send(Message::Text(msg.clone())).await;
        }
    }
}
```

#### Python 启动入口

```python
# backend_starter.py
def start_compute_box(config: dict):
    # 仅传递配置
    rust_compute_box = ComputeBox(
        ch_config=config["clickhouse"],
        ws_port=config["ws_port"],
        factors_config=load_factors_yaml(),
    )
    rust_compute_box.run()  # Rust tokio runtime
```

## 分阶段实施计划

### Phase 1: DataLayer 统一 trades 存储

- [ ] 创建 `rust/src/data_layer.rs` - trades 内部存储
- [ ] `load_trades_sync` 写入 DataLayer，不返回 Python
- [ ] 暴露 `get_trades_for_warmup(ticker)` 接口

### Phase 2: BarBuilder 内部化

- [ ] `BarBuilder.ingest_trade()` 写入 DataLayer
- [ ] 完成 bars 直接写入 ClickHouse
- [ ] 删除 Python 层 trades 返回

### Phase 3: FactorEngineCore Rust 实现

- [ ] 创建 `rust/src/factor_engine.rs`
- [ ] subscribe bars stream
- [ ] 从 DataLayer warmup
- [ ] compute factors + 写入 ClickHouse

### Phase 4: WebSocket Publisher

- [ ] Rust tokio WebSocket server
- [ ] 直接推送 bars + factors
- [ ] Python ChartBFF 变成可选（或完全移除）

### Phase 5: Python 层简化

- [ ] FactorEngine 变成 Rust wrapper
- [ ] BarsBuilderService 变成配置传递层
- [ ] 删除中间数据流转逻辑

## 风险与缓解

| 风险 | 缓解措施 |
|------|----------|
| Rust WebSocket 稳定性 | 先实现为可选，Python WS 作为 fallback |
| 配置热更新 | 保留 Python 配置层，通过 FFI 传递 |
| 调试困难 | Rust 日志桥接到 Python logging |
| ClickHouse 连接池 | 使用 `clickhouse` crate 的连接池 |

## 相关任务

- [ ] Task 13.1: DataLayer trades 统一存储
- [ ] Task 13.2: BarBuilder 内部化 bars 写入
- [ ] Task 13.3: FactorEngineCore Rust 实现
- [ ] Task 13.4: WebSocket Publisher Rust 实现
- [ ] Task 13.5: Python 层简化

## 参考文档

- `roadmap/factor-plugin-architecture.md` - Factor 设计原则
- `roadmap/barsbuilder-rust-bootstrap.md` - Bootstrap 优化经验
- `rust/src/bootstrap.rs` - 单 FFI bootstrap 实现
