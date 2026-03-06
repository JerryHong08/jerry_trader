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
