## 0.2.0 (2026-02-28)

### Feat

- **src**: add config.yaml, patch tasks for different machines
- **src-&-frontend**: separate news from static update and to async update
- **src-&-frontend-stock-detail-module**: stock detail update done. add news llm classificator, need to refactor in the future for better performance
- **src-&-frontend**: before static data update backup
- **src/DataSupply**: collector write full raw snapshot data into .parquet, not csv anymore
- **frontend-&-src**: frontend columns update decoupled. add zustand. collector rewrite backup
- **frontend-&-src**: feature:1.bff connected frontend and backend. 2.frontend top gainers snapshot data columns update. 3.frontend overviewchart update. but has performance issuse, need to optimize. 4.replayer add logger and replayer start-from param
- **frontend-&-src**: initialization of grid trader frontend and backend. bff, chartdataManger, stateEngine, backendStarter, staticdataSupply
- **src/FactorEngine**: add factor engine
- **src/IBBot-frontend**: 1. add postgres as database, alembic for database manage 2. build a trading console frontend, embedded with orderbook, order placer and so on
- **src/IBBot-frontend**: 1. add postgres as database, alembic for database manage 2. build a trading console frontend, embedded with orderbook, order placer and so on
- **src/IBBot**: establish basic ib trading app functionalities including orderplace, orderstatus callback, portfolio and position upd a te and callback, build a event bus system. SQL and frontend needed to be added in the future
- **src/**: add thetadata, but not work for its speeded replay terminal. unified all the DataSupply to server.py
- **src/-&&-frontend**: support for multiple event_type ['Q', 'T']
- **OrderBook&frontend**: multi-ticker version
- **OrderBook**: add backend debug

### Fix

- **src/OrderManagement**: sell order pct to qty calculation fixed
- **src**: 1.readme update. 2. file name refactor. 3.change replay_date&suffix_id -> session_id 4. rename data_discrepancy_fixed to manual_fixed_data
- **src/NewsProcessor.py**: preparation for 2rd machine git version
- **src-&-frontend**: decouple static update stream
- **src/**: reconstuced for upgrade

### Refactor

- frontend backup

### Perf

- **src/IBGateway**: IBTrading app preparation
