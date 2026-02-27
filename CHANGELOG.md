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
