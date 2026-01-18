# Introduction

## Stage 1 & Initial Start

IBKR Order Placement & Realtime Order Book Web App (Python + React)

### roadmap

- React frontend + backend
  - websocket backend server
  - websocket backend debug server
  - single ticker real-time quote
  - multi ticker real-time quote
  - add trades
- IBKR Order Placement Engine
  - IBKR Gateway/TWS connect & starter(using `/opt/ibc/gatewaystart.sh`)
  - Basic IB Classes like Contract, Order...

## Stage2

- using figma make frontend code package.
  this frontend layout of trading system is gird layout, and each grid/module is seperated for easy to develop and customize.
  - Top Gainers/Rank List
  - Overview Chart
  - Order Management
  - Stock Detail
  - Portfolio
  - Chart

- migrate the backend data source to the frontend replacing the mock data.
  - start with building a backend for frontend(bff), then migrate data step by step.
  - first start with the Top Gainers data and Overview Chart data.
  - then Stock Detail. the data is driven by top gainers and backend data management, so it should use cache, and also the data request can be driven by the frontend button.
  - then the Portfolio and Order Management. this module is isolated, so it's easier to integrate.
  - the last one is the Chart module, based on tradingview lightweight chart, the focus is balance between the real-time update and historical data retrival.
