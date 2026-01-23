# Introduction

this file have my tarding system module summary and roadmap from very early experimental stage to mature stage.

## roadmap

### Stage 1 & Initial Start

✅IBKR Order Placement & Realtime Order Book Web App (Python + React)

- ✅React frontend + backend
  - websocket backend server
  - websocket backend debug server
  - single ticker real-time quote
  - multi ticker real-time quote
  - add trades
- ✅IBKR Order Placement Engine
  - IBKR Gateway/TWS connect & starter(using `/opt/ibc/gatewaystart.sh`)
  - Basic IB Classes like Contract, Order...

### Stage2(Current)

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
    - 📌then we test the passively fetched and emitted columns like ['float_share','marketCap','news'].
  - ✅after that or during test of column data, we can test the overviewchartdata emit, which needs to normalize the frontend chart and get_chart_data in overviewchartdataManager.py.
  - then Stock Detail. the data request is driven by top gainers and backend data management, so it should use cache, and also the data request can be driven by the frontend button. it's basic the same as the static data column update in top gainers column.
  - then the Portfolio and Order Management. this module is isolated, so it's easier to integrate.
  - the last one is the Chart module, based on tradingview lightweight chart, the focus is balance between the real-time update and historical data retrival.

### Stage3

- Add more modules, like Agent module to monitor the global staus also focus on one ticker at the same time.
- Rewrite StateEngine using Rust.
- separated works to other computuers using ssh.
