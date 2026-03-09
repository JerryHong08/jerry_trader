## 1.1.0 (2026-03-09)

### Feat

- **rust/clock**: `ReplayClock` `#[pyclass]` — drift-proof global wall-time clock with `now_ns/ms`, `jump_to`, `set_speed`, `pause/resume` (11 Rust unit tests)
- **clock**: `clock.py` Python singleton — `now_ms()`, `now_datetime()`, `pause()`, `resume()`, transparent live-mode fallback (`_clock is None` → `time.time()`)
- **frontend**: `TimelineClock.tsx` — polls `GET /api/clock` every 1s, interpolates between polls; orange `REPLAY` badge, speed multiplier, `⏸ PAUSED` state
- **rust/replayer**: TickDataReplayer ported from standalone binary into `jerry_trader._rust` — 6 Rust files (`config.rs`, `types.rs`, `stats.rs`, `loader.rs`, `engine.rs`, `mod.rs`), PyO3 callback delivery (replaces WebSocket JSON, ~10-30× lower latency)
- **replayer**: `SyncedReplayerManager` — drop-in replacement for WebSocket path, in-process tick delivery via `TickDataReplayer` callback
- **replayer**: `UnifiedTickManager` supports `provider="synced-replayer"` with pre-built manager parameter
- **rust/replayer**: `batch_preload(symbols, events)` — scans each Parquet file once for all tickers using `is_in()` filter, partitions by ticker; subsequent `subscribe()` calls skip I/O entirely
- **rust/bars**: `BarBuilder.check_expired(now_ms)` — closes bars at correct wall-time boundary even without trades
- **backend**: `preload_tickers` config field under `TickDataServer` in `config.yaml`; `_preload_tickers()` calls `batch_preload()` with clock paused during I/O
- **backend**: `utils/config_builder.py` extracted from `backend_starter.py` (`load_yaml_config`, `build_runtime_config`, `parse_override_args`, `set_nested_value`, `deep_merge`)
- **chart**: Replay-mode chart backfill — `ChartDataService.get_bars()` detects `clock.is_replay()` and routes to local `data_loader.py` (Parquet) instead of Polygon API; ClickHouse backfill path unchanged

### Fix

- **modules**: 7 `time.time()`/`datetime.now()` calls replaced with `clock.now_ms()`/`clock.now_datetime()` across FactorEngine, StateEngine, ChartDataBFF, StaticDataWorker, NewsWorker
- **backend**: clock init order — `_init_clock()` called before `_init_services()` (Rust replayer needs clock at subscribe time)
- **backend**: `replay_time` leading-zero preservation — `set_nested_value()` no longer coerces `"081500"` to int
- **bars_builder**: flush loop rewritten — 50ms real-time poll, fires `check_expired` on virtual-time 500ms boundaries via `clock_mod.now_ms()`, immediate ClickHouse flush
- **bars_builder**: `from jerry_trader import clock as clock_mod` — fixed `ImportError: cannot import name 'clock'`

### Perf

- **replayer**: tick delivery latency ~5-20µs (PyO3 dict) vs ~200-600µs (WebSocket JSON) — eliminated WS serialization + TCP loopback
- **replayer**: preloading 2 tickers reduced from ~252s (4 separate Parquet scans) to ~60s (2 scans with `is_in` filter)
- **bars_builder**: bar expiry detection within 50ms of boundary crossing (was up to 4s)

### Tests

- **clock**: 46 pytest tests (`test_replay_clock.py`) — Rust direct, Python singleton, drift accuracy, monotonicity
- **replayer**: 35 pytest tests (`test_synced_replayer.py`) — payload conversion, subscribe lifecycle, queue fan-out, UnifiedTickManager integration, real Parquet integration
- **bars_builder**: 9 pytest tests in `TestCheckExpired` — empty, unexpired, expired, removed-from-state, mixed timeframes, multi-ticker, re-ingest, field validation
- **rust**: 2 new Rust unit tests (`test_bar_state_expired_detection`, `test_ticker_bars_expired_drain`)

## 1.0.0 (2026-03-06)

### BREAKING CHANGES

- **Project renamed** from `jerryib_trade` to `jerry_trader`
- **Directory restructured** to a multi-language monorepo: `python/`, `rust/`, `frontend/`
- **All Python imports** changed from bare subpackage names (`from DataManager.X`) to fully qualified (`from jerry_trader.DataManager.X`)
- **Build system** switched from pure Poetry to **maturin + Poetry** hybrid (Rust extension compiled via PyO3)

### Feat

- **rust**: introduced Rust computation core (`rust/src/lib.rs`) compiled via maturin as `jerry_trader._rust` PyO3 extension module
- **project**: added `utils/paths.py` with marker-file-based `PROJECT_ROOT` discovery — replaces fragile `parents[N]` offsets, robust against future restructuring
- **config**: added `config.yaml.example` with anonymized machine names and role descriptions for onboarding
- **chart**: Chart module 1D data bootstrap working (TradingView lightweight chart integration in progress)
- **config**: added per-role `limit` config for machine-level service lifecycle control (`market_open` / `market_close` / `null`)
- **frontend**: GitHub Pages deploy support; frontend path and icon cleanup

### Fix

- **config**: fixed config path resolution — `config.py`, `backend_starter.py`, and `utils/logger.py` now use centralized `PROJECT_ROOT`
- **frontend**: fixed stock detail search
- **DataSupply**: snapshot schema type mismatch fixed

### Refactor

- **project**: flattened Rust layout from `rust/jerry_trader/` to `rust/` (single crate)
- **project**: removed redundant `rust/jerry_trader/pyproject.toml` (maturin init leftover)
- **project**: fixed `Cargo.toml` lib name and `lib.rs` module name to match maturin `module-name = "jerry_trader._rust"`
- **project**: converted dependency specifiers from Poetry parenthesis syntax to PEP 508 for Poetry 2.x compatibility
- **project**: added `_rust.pyi` type stubs for IDE autocompletion of Rust extension
- **project**: added `[tool.pytest.ini_options]` testpaths and VS Code `python.analysis.extraPaths` for proper import resolution
- **project**: reorganized `.gitignore` into categorized sections (Rust, Python, Frontend, IDE, OS, Project-specific)
- **project**: updated all `python -m src.X` docstring references to `python -m jerry_trader.X`
- **poetry**: removed orphaned `jerryib-trade` virtualenv, recreated as `jerry-trader` with fresh `poetry.lock`
- **frontend** is now just frontend, remove 'GridTrader' name.

## 0.2.0 (2026-02-28)

### Feat

#### Backend

- **config**: multi-machine support via `config.yaml` and `machine_config.yaml` for SSH-separated workloads
- **BackendForFrontend**: BFF layer (`bff.py`) connecting backend data pipeline to frontend over WebSocket
- **DataSupply/snapshotDataSupply**: `collector.py` writes full raw market snapshots to `.parquet` (migrated from CSV); `replayer.py` supports `start-from` param and structured logging
- **DataSupply/staticdataSupply**: static data fetch workers for fundamentals (`fundamentals_fetch.py`), borrow fee (`borrow_fee_fetch.py`), and news (`news_fetch.py`)
- **DataSupply/tickDataSupply**: unified tick data manager (`unified_tick_manager.py`) integrating Polygon and ThetaData sources; supports multiple event types `['Q', 'T']`
- **DataManager**: `overviewchartdat_manager.py`, `static_data_worker.py`, `news_worker.py`, `tickdata_server.py` for backend data orchestration
- **ComputeEngine/StateEngine**: real-time state engine (`state_engine.py`) for market state tracking and top gainers ranking
- **ComputeEngine/FactorEngine**: factor engine (`factor_engine.py`) for computing derived market factors
- **ComputeEngine/NewsProcessor**: LLM-based news classifier (`news_processor.py`) with versioned prompts; news update decoupled from static data stream and runs asynchronously
- **OrderManagement**: full order lifecycle management — order placement, status callbacks, portfolio & position updates; backed by PostgreSQL via SQLAlchemy + Alembic
- **OrderManagement/persistence**: database bootstrap migration (`20251215_01`) for order and portfolio persistence
- **IBBot**: established core IB trading app — event bus system, order placement, order status callbacks, portfolio and position update callbacks

#### Frontend (`GridTrader`)

- **GridTrader**: initialized grid-layout React (Vite + TypeScript) trading dashboard; each module is independently developed and customizable
- **Layout**: draggable/resizable grid system (`GridContainer`, `GridItem`), layout import/export via `SettingsMenu`
- **RankList**: Top Gainers module — real-time columns: `symbol`, `rank`, `price`, `change`, `changePercent`, `volume`, `relativeVolume5min`, `relativeVolumeDaily`, `float_share`, `marketCap`, `news`, state column
- **OverviewChartModule**: normalized chart data emission from backend; overview chart connected to live data
- **StockDetail**: stock detail module driven by top gainers selection and frontend button; uses backend cache
- **OrderManagement**: order management module integrated with backend order service (portfolio + order history)
- **ChartModule**: TradingView lightweight chart skeleton (in progress — historical data bootstrap pending)
- **stores**: Zustand state management added; frontend column updates decoupled from backend emit cycle

### Fix

- **OrderManagement**: sell order percentage-to-quantity calculation corrected
- **DataSupply**: renamed `replay_date` & `suffix_id` → `session_id`; renamed `data_discrepancy_fixed` → `manual_fixed_data`
- **DataSupply**: static update stream decoupled for independent column refresh
- **src**: file name refactoring and module restructuring pass

### Refactor

- **src**: full module restructure — `BackendForFrontend`, `ComputeEngine`, `DataManager`, `DataSupply`, `OrderManagement`, `MachineLearning`, `utils` layout established
- **frontend**: GridTrader frontend migrated from figma prototype to production component structure

### Perf

- **IBGateway**: IB trading app connection and gateway initialisation improvements
