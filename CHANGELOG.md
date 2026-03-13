## Unreleased (2026-03-13)

### Feat

- **clock**: `start_heartbeat_publisher(redis_client, session_id, interval_ms=100)` — background daemon thread publishes `ReplayClock` state (`ts_ns`, `speed`, `is_paused`, `wall_ns`) to `clock:heartbeat:{session_id}` every 100 ms; no-op fallback in live mode
- **utils/remote_clock**: `RemoteClockFollower` — subscribes to clock heartbeat channel, monotonically interpolates virtual time between heartbeats using local `time.time_ns()` offset; implements `now_ms()`, `now_ns()`, `is_paused`, `speed`, `has_sync`
- **replayer**: `MarketSnapshotReplayer` accepts `remote_clock: RemoteClockFollower`; timing loop now polls `remote_clock.now_ns() < target_ts_ns` (50 ms interval) instead of `asyncio.sleep(time_diff / speed)`, eliminating local-speed drift across machines
- **backend_starter**: auto-starts heartbeat publisher in `run()` when in replay mode using first available redis config; wires `RemoteClockFollower` into `MarketSnapshotReplayer` when `clock_redis` is set in Replayer role config
- **redis_keys**: `clock_heartbeat_channel(session_id)` — canonical key `clock:heartbeat:{session_id}`
- **config**: `clock_redis` field added (commented-out example) under `Replayer` role for cross-machine clock sync configuration

### Refactor

- **clock**: added `import json`, `import threading` to support heartbeat publisher
- **replayer**: timing branch split — `remote_clock` path (virtual-time poll) vs local `asyncio.sleep` fallback (backward compatible)

## Unreleased (2026-03-12)

### Feat

- **tickdata/chart architecture**: started merge of `ChartDataBFF` into `TickDataServer` to reduce one backend process and one frontend WebSocket hop (work in progress)
- **chart bootstrap**: intraday timespan bootstrap and gap-bootstrap flow added/refined. chart bootstrap path now combines custom bar fetch + trades rebuild for meeting-bar continuity.
- **openclaw integration**: openclaw client bootstrap added;
- **subscriptions**: first-subscribe and re-subscribe behavior refined in chart/tickdata orchestration

### Fix

- **frontend/chart**: fixed two-chart synchronized render issue
- **frontend/chart UX**: lock behavior now applies only on zoom interactions

### Chore

- **ci**: multiple CI fix commits and stabilization updates

## 1.2.0 (2026-03-11)

### Feat

- **bars_builder**: 10s bar bootstrap — on subscription, checks ClickHouse for existing 10s bars; if empty, fetches historical trades (REST in live, Parquet in replay) and builds bars with a dedicated `BarBuilder(["10s"])` to avoid thread-race with the real-time WS path
- **bars_builder**: meeting-bar merge — records first WS tick timestamp, filters bootstrap trades at boundary, merges the straddling 10s bar from both REST/Parquet prefix and WS suffix (open=REST, close=WS, high/low=max/min, volume/trades=sum, vwap=weighted)
- **rust/bars**: `ingest_trades_batch(ticker, trades)` — bulk FFI method processes all trades in a single Rust call, accumulates `CompletedBar` structs internally, converts to Python dicts once at the end; **4.2× faster** than per-trade calls (0.039s vs 0.164s for 441K trades)
- **rust/replayer**: `load_trades_from_parquet(lake_data_dir, symbol, date, end_ts_ms)` — standalone synchronous Parquet trade loader for bootstrap; tries partitioned file first, falls back to monolithic with ticker filter; `end_ts_ms` predicate pushdown filters at scan time (replay clock boundary)
- **news_room**: news room agent BFF endpoint and frontend module
- **data_pipeline**: `tickdata_extract` script for per-ticker partitioned Parquet generation from monolithic files

### Fix

- **bars_builder**: thread-safety — bootstrap uses separate `BarBuilder(["10s"])` instance; WS ticks no longer jump shared BarBuilder state forward, preventing all-trades-in-one-bar mega-bar bug
- **bars_builder**: spurious `MEETING_BAR` log after bootstrap completes — added `_bootstrap_done` set to skip meeting-bar re-detection
- **bars_builder**: concurrent ClickHouse session error — removed direct `_flush_to_clickhouse()` from bootstrap thread; flush loop handles all persistence
- **chart**: ticker chart switch price axis autoscale
- **chart**: replay mode 4h timespan cutoff in local data loader
- **news_processor**: output log format changed to JSON for downstream consumption

### Perf

- **rust/bars**: `ingest_trades_batch` single-FFI bulk ingestion — 4.2× speedup over per-trade `ingest_trade` calls (441K trades: 39ms vs 164ms)
- **rust/replayer**: per-ticker partitioned Parquet loading ~1s per ticker (was full-file scan + filter)
- **rust/replayer**: `load_trades_from_parquet` with `end_ts_ms` predicate pushdown — avoids materializing future trades in replay mode

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
- **data_loader**: `_resample_calendar()` — weekly/monthly resampling via `group_by_dynamic` moved from `chart_data_service` into `data_loader.py`; extended `parse_timeframe()` for `w`/`mo` units
- **data_loader**: `LoaderConfig.cutoff_ts` — pre-resample replay cutoff so aggregated bars (4h, 1W, 1M) never incorporate future source data; cache auto-disabled when cutoff is active
- **data_loader**: mixed Parquet schema handling — per-file `scan_parquet` + `pl.concat(how="diagonal_relaxed")` auto-supertypes UInt32/UInt64 volume columns across dates

### Fix

- **modules**: 7 `time.time()`/`datetime.now()` calls replaced with `clock.now_ms()`/`clock.now_datetime()` across FactorEngine, StateEngine, ChartDataBFF, StaticDataWorker, NewsWorker
- **backend**: clock init order — `_init_clock()` called before `_init_services()` (Rust replayer needs clock at subscribe time)
- **backend**: `replay_time` leading-zero preservation — `set_nested_value()` no longer coerces `"081500"` to int
- **bars_builder**: flush loop rewritten — 50ms real-time poll, fires `check_expired` on virtual-time 500ms boundaries via `clock_mod.now_ms()`, immediate ClickHouse flush
- **bars_builder**: `from jerry_trader import clock as clock_mod` — fixed `ImportError: cannot import name 'clock'`
- **data_loader**: `_fill_missing_and_resample` routing — `"7d"`/`"30d"` parsed as `unit="d"` were incorrectly routed to session-based resampler; fixed with `or (unit == "d" and value > 1)` guard
- **data_loader**: `localdata_loader/__init__.py` relative imports + try/except guards; `path_loader.py` lazy `@property` for s3fs import
- **chart**: removed stale `day_aggs_v1` cache files (67 files) serving un-resampled daily bars as weekly/monthly
- **chart**: removed redundant `lf.cast({"volume": pl.UInt64})` from `_fetch_from_local()` — handled by loader

### Perf

- **replayer**: tick delivery latency ~5-20µs (PyO3 dict) vs ~200-600µs (WebSocket JSON) — eliminated WS serialization + TCP loopback
- **replayer**: preloading 2 tickers reduced from ~252s (4 separate Parquet scans) to ~60s (2 scans with `is_in` filter)
- **bars_builder**: bar expiry detection within 50ms of boundary crossing (was up to 4s)

### Tests

- **clock**: 46 pytest tests (`test_replay_clock.py`) — Rust direct, Python singleton, drift accuracy, monotonicity
- **replayer**: 35 pytest tests (`test_synced_replayer.py`) — payload conversion, subscribe lifecycle, queue fan-out, UnifiedTickManager integration, real Parquet integration
- **bars_builder**: 9 pytest tests in `TestCheckExpired` — empty, unexpired, expired, removed-from-state, mixed timeframes, multi-ticker, re-ingest, field validation
- **rust**: 2 new Rust unit tests (`test_bar_state_expired_detection`, `test_ticker_bars_expired_drain`)
- **data_loader**: 57 pytest tests (`test_data_loader.py`) — parse_timeframe, _resample_calendar, timestamp generation, session resampling, forward-fill, routing, config consistency, pre-resample cutoff
- **chart**: 62 pytest tests (`test_chart_data_service.py`) — added cutoff propagation (3), replay cutoff (2), updated weekly/monthly config assertions

## 1.0.0 (2026-03-06)

### BREAKING CHANGES

- **Project renamed** from `jerryib_trade` to `jerry_trader`
- **Directory restructured** to a multi-language monorepo: `python/`, `rust/`, `frontend/`
- **All Python imports** changed from bare subpackage names (`from DataManager.X`) to fully qualified (`from jerry_trader.data_manager.X`)
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
- **DataSupply/snapshot_data_supply**: `collector.py` writes full raw market snapshots to `.parquet` (migrated from CSV); `replayer.py` supports `start-from` param and structured logging
- **DataSupply/static_data_supply**: static data fetch workers for fundamentals (`fundamentals_fetch.py`), borrow fee (`borrow_fee_fetch.py`), and news (`news_fetch.py`)
- **DataSupply/tick_data_supply**: unified tick data manager (`unified_tick_manager.py`) integrating Polygon and ThetaData sources; supports multiple event types `['Q', 'T']`
- **DataManager**: `overviewchartdat_manager.py`, `static_data_worker.py`, `news_worker.py`, `tickdata_server.py` for backend data orchestration
- **ComputeEngine/StateEngine**: real-time state engine (`state_engine.py`) for market state tracking and top gainers ranking
- **ComputeEngine/FactorEngine**: factor engine (`factor_engine.py`) for computing derived market factors
- **ComputeEngine/NewsProcessor**: LLM-based news classifier (`news_processor.py`) with versioned prompts; news update decoupled from static data stream and runs asynchronously
- **order_management**: full order lifecycle management — order placement, status callbacks, portfolio & position updates; backed by PostgreSQL via SQLAlchemy + Alembic
- **order_management/persistence**: database bootstrap migration (`20251215_01`) for order and portfolio persistence
- **IBBot**: established core IB trading app — event bus system, order placement, order status callbacks, portfolio and position update callbacks

#### Frontend (`GridTrader`)

- **GridTrader**: initialized grid-layout React (Vite + TypeScript) trading dashboard; each module is independently developed and customizable
- **Layout**: draggable/resizable grid system (`GridContainer`, `GridItem`), layout import/export via `SettingsMenu`
- **RankList**: Top Gainers module — real-time columns: `symbol`, `rank`, `price`, `change`, `changePercent`, `volume`, `relativeVolume5min`, `relativeVolumeDaily`, `float_share`, `marketCap`, `news`, state column
- **OverviewChartModule**: normalized chart data emission from backend; overview chart connected to live data
- **StockDetail**: stock detail module driven by top gainers selection and frontend button; uses backend cache
- **order_management**: order management module integrated with backend order service (portfolio + order history)
- **ChartModule**: TradingView lightweight chart skeleton (in progress — historical data bootstrap pending)
- **stores**: Zustand state management added; frontend column updates decoupled from backend emit cycle

### Fix

- **order_management**: sell order percentage-to-quantity calculation corrected
- **DataSupply**: renamed `replay_date` & `suffix_id` → `session_id`; renamed `data_discrepancy_fixed` → `manual_fixed_data`
- **DataSupply**: static update stream decoupled for independent column refresh
- **src**: file name refactoring and module restructuring pass

### Refactor

- **src**: full module restructure — `BackendForFrontend`, `ComputeEngine`, `DataManager`, `DataSupply`, `order_management`, `MachineLearning`, `utils` layout established
- **frontend**: GridTrader frontend migrated from figma prototype to production component structure

### Perf

- **IBGateway**: IB trading app connection and gateway initialisation improvements
