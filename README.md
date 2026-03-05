# Introduction

this file has my trading system module summary and the roadmap from very early experimental stage to planned.

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
  - 📌the last one is the Chart module, based on tradingview lightweight chart, the focus is balance between the real-time update and historical data retrival. **data management and historical data bootstrap.**
    - bootstrap using api&cache

### Stage3

mainly focus on factor computing and advanced integration of system.

- ✅separated works to other computuers using ssh. configured in config.yaml.
- visualization of factors in chart module.
- rewrite stateEngine in rust.
- real-time risk management engine/trigger.
  - risk manage rule
- developing machine learning module.
  - build breakout-compute-analyze oriented Context Model using current recored files.
  - simulate market_snapshot replay using historical trade&quote bulk file.
  - build historical context model.
- rewrite factorEegine in Rust.

### optional features

some other features to make it better.

- historical orders analysis modules.
- Add more modules, like Agent module to monitor the global staus also focus on one ticker at the same time.
- news room

### Current frontend preview

#### Overview Layout Set

<p align="center">
  <img src="./assets/gridtrader.png" alt="gridtrader" width="600">
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
