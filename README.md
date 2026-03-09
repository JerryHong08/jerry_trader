# Introduction

this file has my trading system module summary and the roadmap from very early experimental stage to planned.

Details in [Jerry_Trader.pdf](docs/jerry_trader.pdf)

## Installation & Quick Start

This project is designed for a multi-machine setup connected via a local network or [Tailscale](https://tailscale.com/). Each machine's role is defined in [`config.yaml`](/config.yaml.example).

### 1. Clone & Configure

```bash
git clone <repo-url> jerry_trader && cd jerry_trader
```

#### 1.1 `basic_config.yaml`

| Key | Description |
|-----|-------------|
| `data.data_dir` | Local path where market data (Parquet files) is stored |

#### 1.2 `.env`

At minimum you need:

| Variable | Purpose |
|----------|---------|
| `POLYGON_API_KEY` | Polygon.io Advanced subscription (real-time snapshot + tick data) |
| `DATABASE_URL` | PostgreSQL connection string (order persistence) |
| `INFLUXDB_URL` / `INFLUXDB_TOKEN` | InfluxDB connection (factor engine metrics) |
| `IB_PORT` | `7496` (TWS paper) / `7497` (TWS live) / `4002` (Gateway paper) / `4001` (Gateway live) |

> For the IB API client library, see [Download the TWS API](https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/#find-the-api).

#### 1.3 `config.yaml` — Machine Roles

The system has multiple services that can be distributed across machines. my setup uses 3:

| Machine | Services |
|---------|----------|
| **A** | TickData Engine, BarBuilder, Factor Engine, Order Execution |
| **B** | Market Snapshot engine (collect/replay), strategy-driving pre-processing(top20, normalization) |
| **C** | News Module (fetch, LLM classification) |

See [`config.yaml.example`](/config.yaml.example) for the full schema and role definitions.

I recommend at least 2 machines, and highly recommend separate order execution and tickdata process with other parts.

### 2. Install Python Dependencies

```bash
# Create virtualenv and install all deps
poetry install

# Verify
poetry env info
```

### 3. Build the Rust Extension

```bash
# before run maturin, make sure you have cargo installed and restarted the shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build and install the Rust extension into the Poetry venv
poetry run maturin develop

# Verify
poetry run python -c "from jerry_trader._rust import sum_as_string; print(sum_as_string(1, 2))"
# → 3
```

For release-optimized builds: `poetry run maturin develop --release`

### 4. Database Setup

```bash
# Run Alembic migrations (requires postgres URL in alembic.ini)
poetry run alembic upgrade head
```

Make sure Redis is running (`redis-server` or via Docker).

### 5. Start the Backend

```bash
# Start all backend services for a machine profile defined in config.yaml
poetry run python -m jerry_trader.backend_starter --machine wsl2

# Dry-run to see what would start
poetry run python -m jerry_trader.backend_starter --machine wsl2 --dry-run

# With replay mode (historical date)
poetry run python -m jerry_trader.backend_starter --machine wsl2 --defaults.replay_date 20260115
```

### 6. Start the Frontend

```bash
cd frontend
pnpm install
pnpm dev
```

### 7. Development Workflow

```bash
# After editing Rust code
poetry run maturin develop

# After editing Python code — no rebuild needed (editable install)

# Run tests
poetry run pytest

# Code formatting
poetry run black python/
poetry run isort python/
```

## roadmap

### Stage 1 & Initial Start

mainly focus on basic function test and expriment.

✅IBKR Order Placement & Realtime Order Book Web App (Python + React)

- ✅React frontend + backend
  - websocket backend server
  - websocket backend debug server
  - single ticker real-time quote
  - multi ticker real-time quote
  - add trades
- ✅IBKR Order Placement Engine
  - IBKR Gateway/TWS connect & starter(using `/opt/ibc/gatewaystart.sh`). now moved to use windows gateway for wsl mirrored network mode.
  - Basic IB Classes like Contract, Order...

### Stage2(Current)

mainly focus on basic modules development and strcuture buidling.

- ✅using figma make frontend code package.
  this frontend layout of trading system is gird layout, and each grid/module is seperated for easy to develop and customize.
  - Top Gainers/Rank List
  - Overview Chart
  - Order Management
  - Stock Detail
  - Portfolio
  - Chart

- migrate the backend data source to the frontend replacing the mock data.
  - ✅start with building a backend for frontend(bff), then migrate data step by step.
  - first start with the Top Gainers column data.
    - ✅first we test with basic direct emit columns like ['symbol', 'rank', 'price', 'change', 'changePercent', 'volume', 'relativeVolume5min', 'relativeVolumeDaily'].
    - ✅then we test the state column
    - ✅then we test the passively fetched and emitted columns like ['float_share','marketCap','news'].
  - ✅after that or during test of column data, we can test the overviewchartdata emit, which needs to normalize the frontend chart and get_chart_data in overviewchartdataManager.py.
  - ✅then Stock Detail. the data request is driven by top gainers and backend data management, so it should use cache, and also the data request can be driven by the frontend button. it's basic the same as the static data column update in top gainers column.
  - ✅then the Portfolio and Order Management. this module is isolated, so it's easier to integrate.

### Stage2.5

✅the last and the hardest one is the Chart module, based on tradingview lightweight chart, the focus is balance between the real-time update and historical data retrival. **data management and historical data bootstrap. also build a architecture prepared for stage3 strategy real-time/replay computation, execution, analysis**

- ✅add frontend request_id to prevent race conditions.
- [ ]frontend charts timespan seperated.
- [ ]Downstream consumers + InfluxDB→ClickHouse migration
- [ ]FactorEngine consumes batched bars/data (not raw ticks)
- [ ]Snapshot data: InfluxDB → ClickHouse
- [ ]Foundation for v3.0 stream bus architecture

### Stage3

mainly focus on strategy computation,execution,replay backtest.

- [ ]tickers fit in strategy/condition pre locate.
- [ ]pre-located tickers pipeline backtest visualization& validation.
- ✅separated works to other computuers using ssh. configured in config.yaml.
- [ ]visualization of factors in chart module.
- [ ]real-time risk management engine/trigger.
  - [ ]risk manage rule
- [ ]developing machine learning module.
  - [ ]build breakout-compute-analyze oriented Context Model using current recored files.
  - [ ]simulate market_snapshot replay using historical trade&quote bulk file.
  - build historical context model.
- [ ]rewrite stateEngine in rust.
- [ ]rewrite factorEegine in Rust.
- [ ]Factor output: InfluxDB → ClickHouse
- [ ]Factor visualization overlay in Chart module
- [ ]News engine output from log to json log. route to openclaw/heartbeat llm in the future.

### optional features

some other features to make it better.

- [] historical orders analysis modules.
- [] Add more modules, like Agent module to monitor the global staus also focus on one ticker at the same time.
- [] news room

### open issues

- [ ]frontend chart newest bar render will cover up the the last bar timespan duration time open,high,low price. always start a new bar based on incoming websocket since connected.
- [ ]4h local data in replay fetch has a problem overlapping the bar though we have cut_off before resample in local_data_loader.

### Current frontend preview

#### Overview Layout Set

<p align="center">
  <img src="./assets/JerryTrader.png" alt="JerryTrader" width="600">
  <br><em>Portfolio, order history and chart are using mock data</em>
</p>

#### Trade Layout Set

<p align="center">
  <img src="./assets/trade_layout01.png" alt="trade_layout01" width="800">
</p>

#### Layout Setting

<p align="center">
  <img src="./assets/layout_setting.png" alt="layout_setting" width="450">
  <br><em>Import, load and customize your layout in the setting panel</em>
</p>
