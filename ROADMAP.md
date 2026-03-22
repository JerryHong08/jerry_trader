# Roadmap

## Stage 1 & Initial Start

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

## Stage2

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

### Stage2.5

✅ the last and the hardest one is the Chart module, based on tradingview lightweight chart, the focus is balance between the real-time update and historical data retrival. **data management and historical data bootstrap. also build a architecture prepared for stage3 strategy real-time/replay computation, execution, analysis**

- ✅ add frontend request_id to prevent race conditions.
- ✅ frontend charts seperated.
- ✅ deep review how current bar_buidler builds the bars in different senarios. 10s bootstrap done.
- ✅ _needs_historical_backfill logic refine.
- ✅ Snapshot processor to rust
- ✅ Snapshot data: InfluxDB → ClickHouse
In this stage we introduce rust based global clock to maintain acuuracy among the whole project running time. In replay mode, The **chartdata machine** is the clock domain master (also runs`local_tickdata_replayer` in-process). Remote machines (running
`MarketSnapshotReplayer`) follow via Redis heartbeat.

- ✅ add replay global clock in rust to maintain time accuracy in replay mode.
- ✅ Remote machine sync + snapshot replayer
- ✅ Redis heartbeat publisher in `clock.py` (100ms interval, from ChartBFF machine)
- ✅ `RemoteClockFollower` class (monotonic interpolation between heartbeats)
- ✅ Modify `MarketSnapshotReplayer` to poll `RemoteClockFollower.now_ns()` instead of `asyncio.sleep`
- ✅ Test cross-machine sync (same network + Tailscale)

- ✅ Downstream consumers + InfluxDB→ClickHouse migration
- ✅ FactorEngine consumes batched bars/data, while also consumes raw ticks for real-time update.
- ✅ Foundation for v3.0 stream bus architecture

## Stage3(Current)

mainly focus on strategy computation,execution,replay backtest. and the optimization for UX/UI and orchestration/backend.

strategy:

- ✅ rewrite factorEegine in Rust.
  - ✅ factor output: InfluxDB → ClickHouse
  - [ ] factor chart module behaves as a sub follower chart as main chart.
    - [ ] real-time update not done yet.
  - [ ] factor bootstrap sync to the trades_bootstrap.(quotes_bootstrap needed in the future for factors like spread etc.)
    - [ ] it triggers trades_backfill every time main bar chart switch the timeframe, need to be decoupled and fixed.
  - [ ] configurable timeframe switch and bootstrap computation
  - ✅ delete the deprecated factor_manager.py code.
- [ ] rewrite stateEngine in rust.
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

- [ ] abstract all the search box into one.
- [ ] bar chart and factor chart style unified.
- [ ] better UX
  - ✅ keyboard shortcut
  - [ ] frame group

## Stage 4

- [ ] AI Agent powered Event-Driven System
  - [ ] redis streams to event_bus
  - [ ] rpc
  - [ ] basic tools build up, cli & skills instruction
  - [ ] memory system
  - [ ] txt to tool-calling validation
- [ ] strategy pipeline agent validation
- [ ] ml pipeline validation

```bash
apps/agent_app/          ← AgentBFF HTTP + WebSocket interface
services/agent/          ← (new) agent loop, tool dispatch, memory
    ├── agent.py         ← core agent loop
    ├── tools.py         ← tool registry (wraps existing services)
    ├── memory.py        ← short + long-term memory store
    └── planner.py       ← [future] planning module
platform/messaging/rpc/  ← Redis-based RPC for cross-service tool calls
skills/                  ← Markdown skill instruction files (already at root)
```

## optional features

some other features to make it better.

- ✅ separated works to other computuers using ssh. configured in config.yaml.
- [ ] historical orders analysis modules.
- [ ] Add more modules, like Agent module to monitor the global staus also focus on one ticker at the same time.
  - [ ] News engine output from log to json log. route to openclaw/heartbeat llm in the future.
    - ✅ news room
      - ✅ focus mode(synced with group)
    - [ ] chatbox

## open issues

- ✅ when switch to frontend chart [10s,1m] timespan, the newest bar always start a new bar based on incoming websocket since connected, the new rendered bar will cover up the the last bar timespan duration time open,high,low price. this seems not to be fixed in a short term for not effect the current tickdata orchestration of between frontend and backend.
- ✅ there is bar close problem, the root cause might be rust `bar.rs`
- ✅ 4h local data in replay fetch has a problem overlapping the bar though we have cut_off before resample in local_data_loader.
- ✅ some of the bars are built empty volume. this originated from a race between ingest_trade and time-driven close; current work has migrated to `advance` + Rust completed-queue flow and is being validated in runtime.
  - ✅ Rust output queue path integrated; completed bars are now drained from Rust via `drain_completed()` in flush loop (thread hazard reduced, second-flush burst reduced).
  - ✅ Rename `check_expired` → `advance` across runtime and tests (bars_builder_service + Rust API + test suite updated).
  - ✅ Min-heap scheduling added in Rust (`expiry_heap` with stale-entry seq guard) so expiry scan is boundary-driven instead of full map sweep.
  - ✅ Watermark-based close + late-arrival window added (`configure_watermark(late_arrival_ms, idle_close_ms)`) with idle fallback; tests run with strict window for deterministic boundary assertions.
