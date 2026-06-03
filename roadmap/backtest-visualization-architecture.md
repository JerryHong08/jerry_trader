# Backtest Visualization Architecture

## Context

当前回测可视化通过 JSON 文件方式不够灵活：
- 需要手动导出 JSON
- 无法实时查看回测进度
- 无法交互式调整参数
- 前端无法反向控制后端

需要设计一个类似 OrderApp 的架构：
- Backend 作为服务运行
- WebSocket 实时同步进度
- ClickHouse 存储结果
- 前端可控制和可视化

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Frontend                                   │
│  ┌─────────────────┐    ┌─────────────────┐    ┌──────────────┐ │
│  │ BacktestConfig  │    │ BacktestProgress│    │ BacktestChart│ │
│  │ - 选择日期范围   │    │ - 实时进度条    │    │ - K线+信号   │ │
│  │ - 选择事件      │    │ - 日志输出      │    │ - 因子曲线   │ │
│  │ - 参数配置      │    │ - 错误提示      │    │ - 统计汇总   │ │
│  └────────┬────────┘    └────────┬────────┘    └──────┬───────┘ │
│           │                      │                     │         │
│           └──────────────────────┼─────────────────────┘         │
│                                  │ WebSocket                     │
└──────────────────────────────────┼───────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Backtest Backend (FastAPI)                    │
│  ┌─────────────────┐    ┌─────────────────┐    ┌──────────────┐ │
│  │ POST /backtest  │    │ WS /ws/backtest │    │ GET /results │ │
│  │ 启动回测任务    │    │ 实时进度推送    │    │ 查询历史结果 │ │
│  └────────┬────────┘    └────────┬────────┘    └──────┬───────┘ │
│           │                      │                     │         │
│           ▼                      ▼                     ▼         │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │              BacktestRunner (异步任务)                       ││
│  │  1. PreFilter → DataLoader → FactorEngine                   ││
│  │  2. EventEvaluator → SignalMatcher                          ││
│  │  3. 写入 ClickHouse backtest_signals                        ││
│  │  4. 通过 WebSocket 推送进度                                   ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                       ClickHouse                                 │
│  backtest_signals 表:                                            │
│  - experiment_id (UUID)                                         │
│  - date, ticker, event_name                                     │
│  - entry_time, entry_price, exit_time, exit_price               │
│  - return_pct, factors (Map)                                    │
│  - max_price, min_price, time_to_max_ms, time_to_min_ms         │
│                                                                  │
│  查询流程:                                                        │
│  1. backtest_signals → 信号点                                   │
│  2. ohlcv_bars → K线数据 (ticker + time range)                  │
└─────────────────────────────────────────────────────────────────┘
```

## Data Model

### ClickHouse Schema

```sql
CREATE TABLE backtest_signals (
    experiment_id String,      -- UUID
    date Date,
    ticker String,
    event_name String,

    -- Signal info
    entry_time DateTime64(3),
    entry_price Float64,
    exit_time DateTime64(3),
    exit_price Float64,
    return_pct Float64,
    exit_reason String,        -- 'hold_duration', 'stop_loss', 'take_profit'

    -- Factors at entry
    factors Map(String, Float64),

    -- Price path analysis
    max_price Float64,
    min_price Float64,
    time_to_max_ms Int64,
    time_to_min_ms Int64,

    -- Metadata
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (experiment_id, date, ticker, entry_time);
```

**设计决策**: 不重复存储 bar_data，利用现有 ohlcv_bars 表。

### API Design

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/backtest/start` | POST | 启动回测任务 |
| `/ws/backtest/{exp_id}` | WS | 实时进度推送 |
| `/api/backtest/results/{exp_id}` | GET | 获取实验汇总 |
| `/api/backtest/chart/{exp_id}/{ticker}` | GET | 单 ticker 详情（signals + bars） |
| `/api/backtest/experiments` | GET | 历史实验列表 |

### WebSocket Protocol

```json
// Progress update
{"type": "progress", "date": "2026-03-13", "step": "FactorEngine", "percent": 45}

// Signal found
{"type": "signal", "ticker": "BTCT", "entry_time": 1773388850000, "entry_price": 4.08}

// Error
{"type": "error", "message": "No trades data for ticker XYZ"}

// Complete
{"type": "complete", "experiment_id": "uuid", "total_signals": 123}
```

## Plan

### Phase 1: ClickHouse Schema + Backend API

1. 创建 `backtest_signals` 表
2. 实现 BacktestApp FastAPI 服务
3. 实现异步 BacktestRunner
4. 实现 WebSocket 进度推送

### Phase 2: Frontend Integration

1. 修复 BacktestChartModule (setMarkers API)
2. 实现 BacktestConfigModule（参数配置）
3. 实现 BacktestProgressModule（进度显示）
4. 连接 WebSocket 接收实时进度

### Phase 3: Signal Diagnostics

1. 扩展信号数据：price_path analysis
2. 实现亏损归因分析
3. 实现因子相关性分析

## Rejected Alternatives

1. **JSON 文件方式** - 不够灵活，无法实时交互
2. **bar_data 存为 JSON** - 重复存储，利用现有 ohlcv_bars 表更优
