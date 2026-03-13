# Introduction

this file has my trading system module summary and the roadmap from very early experimental stage to planned.

Details in [Jerry_Trader.pdf](docs/jerry_trader.pdf)

## Installation & Quick Start

This project is designed for a multi-machine setup connected via a local network or [Tailscale](https://tailscale.com/). Each machine's role is defined in [`config.yaml`](/config.yaml.example).

### 1. Clone & Configure

```bash
git clone https://github.com/JerryHong08/jerry_trader.git && cd jerry_trader
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
# before run maturin, make sure you have cargo installed,
# if not, run below command then restarted the shell.
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

# ibkr order management backend
uvicorn python.src.jerry_trader.order_management.main:app --reload --port 8888
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

## Roadmap

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

### Stage2

mainly focus on basic modules development and strcuture buidling.

- ✅ using figma make frontend code package.
  this frontend layout of trading system is gird layout, and each grid/module is seperated for easy to develop and customize.
  - Top Gainers/Rank List
  - Overview Chart
  - Order Management
  - Stock Detail
  - Portfolio
  - Chart

- migrate the backend data source to the frontend replacing the mock data.
  - ✅ start with building a backend for frontend(bff), then migrate data step by step.
  - first start with the Top Gainers column data.
    - ✅ first we test with basic direct emit columns like ['symbol', 'rank', 'price', 'change', 'changePercent', 'volume', 'relativeVolume5min', 'relativeVolumeDaily'].
    - ✅ then we test the state column
    - ✅ then we test the passively fetched and emitted columns like ['float_share','marketCap','news'].
  - ✅ after that or during test of column data, we can test the overviewchartdata emit, which needs to normalize the frontend chart and get_chart_data in overviewchartdataManager.py.
  - ✅ then Stock Detail. the data request is driven by top gainers and backend data management, so it should use cache, and also the data request can be driven by the frontend button. it's basic the same as the static data column update in top gainers column.
  - ✅ then the Portfolio and Order Management. this module is isolated, so it's easier to integrate.

### Stage2.5(Current)

✅ the last and the hardest one is the Chart module, based on tradingview lightweight chart, the focus is balance between the real-time update and historical data retrival. **data management and historical data bootstrap. also build a architecture prepared for stage3 strategy real-time/replay computation, execution, analysis**

- ✅ add frontend request_id to prevent race conditions.
- ✅ frontend charts seperated.
- ✅ deep review how current bar_buidler builds the bars in different senarios. 10s bootstrap done.
- ✅ _needs_historical_backfill logic refine.
- ✅ Snapshot processor to rust
- ✅ Snapshot data: InfluxDB → ClickHouse
- [ ] Downstream consumers + InfluxDB→ClickHouse migration
- [ ] FactorEngine consumes batched bars/data (not raw ticks)
- [ ] Foundation for v3.0 stream bus architecture

In this stage we introduce rust based global clock to maintain acuuracy among the whole project running time. In replay mode, The **tickdataSever machine** is the clock domain master (also runs`local_tickdata_replayer` in-process). Remote machines (running
`MarketSnapshotReplayer`) follow via Redis heartbeat.

- ✅ add replay global clock in rust to maintain time accuracy in replay mode.
- ✅ Remote machine sync + snapshot replayer
- ✅ Redis heartbeat publisher in `clock.py` (100ms interval, from ChartBFF machine)
- ✅ `RemoteClockFollower` class (monotonic interpolation between heartbeats)
- ✅ Modify `MarketSnapshotReplayer` to poll `RemoteClockFollower.now_ns()` instead of `asyncio.sleep`
- ✅ Test cross-machine sync (same network + Tailscale)

### Stage3

mainly focus on strategy computation,execution,replay backtest. and the optimization for UX/UI and orchestration/backend.

strategy:

- [ ] rewrite stateEngine in rust.
  - [ ] Factor output: InfluxDB → ClickHouse
- [ ] rewrite factorEegine in Rust.
- [ ] real-time risk management engine/trigger.
  - [ ] risk manage rule.
- [ ] developing machine learning module.
  - [ ] build breakout-compute-analyze oriented Context Model using current recored files.
  - [ ] simulate market_snapshot replay using historical trade&quote bulk file.
  - [ ] build historical context model.

ochesration:

- [ ] tickers fit in strategy/condition pre locate.
- [ ] Strategy pre-locate orchestrator: accepts `(ticker, timestamp)` list, auto-sequences jumps
- [ ] pre-located tickers pipeline backtest visualization& validation.
- ✅ preload parquet load method optimization.

frontend:

- [ ] simple indicators visualization
- [ ] visualization of factors in chart module.
- [ ] abstract all the search box into one.
- [ ] better UX
  - ✅ keyboard shortcut
  - [ ] frame group

### optional features

some other features to make it better.

- ✅ separated works to other computuers using ssh. configured in config.yaml.
- [ ] historical orders analysis modules.
- [ ] Add more modules, like Agent module to monitor the global staus also focus on one ticker at the same time.
  - [ ] News engine output from log to json log. route to openclaw/heartbeat llm in the future.
    - ✅ news room
      - [ ] focus mode
    - [ ] chatbox

### open issues

- [ ] when switch to frontend chart [10s,1m] timespan, the newest bar always start a new bar based on incoming websocket since connected, the new rendered bar will cover up the the last bar timespan duration time open,high,low price. this seems not to be fixed in a short term for not effect the current tickdata orchestration of between frontend and backend.
- ✅ 4h local data in replay fetch has a problem overlapping the bar though we have cut_off before resample in local_data_loader.
- ✅ some of the bars are built empty volume. this originated from a race between ingest_trade and time-driven close; current work has migrated to `advance` + Rust completed-queue flow and is being validated in runtime.
  - ✅ Rust output queue path integrated; completed bars are now drained from Rust via `drain_completed()` in flush loop (thread hazard reduced, second-flush burst reduced).
  - ✅ Rename `check_expired` → `advance` across runtime and tests (bars_builder_service + Rust API + test suite updated).
  - ✅ Min-heap scheduling added in Rust (`expiry_heap` with stale-entry seq guard) so expiry scan is boundary-driven instead of full map sweep.
  - ✅ Watermark-based close + late-arrival window added (`configure_watermark(late_arrival_ms, idle_close_ms)`) with idle fallback; tests run with strict window for deterministic boundary assertions.

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

>You can customize your layout in setting.
