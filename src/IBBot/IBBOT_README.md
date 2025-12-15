# IBBot（IB 下单器）功能模块文档

> 目标：说明 `src/IBBot` 当前阶段的架构与功能实现，并给出未来“数据库持久化扩展”和“前端连接/联调”的演进建议。

## 1. 模块定位与边界

`IBBot` 是交易执行与账户/持仓查询模块，主要职责：

- 对接 IBKR 官方 Python API（`ibapi`），提供“下单/撤单/订单状态追踪”。
- 拉取并维护“账户摘要 + 持仓列表”。
- 通过 FastAPI 暴露 REST API，并通过 WebSocket 实时推送事件（订单/持仓/账户更新）。
- 内部采用 EventBus 解耦：Adapter 只发布事件，Service 订阅事件并维护状态。

**非目标（当前阶段未覆盖）**

- 历史订单/成交的完整持久化与查询（IB 网关 API 本身也有限制）。
- 策略管理、风控、权限鉴权、数据库等（可扩展点见后文）。

## 2. 目录结构与分层

```
src/IBBot/
  api/                 # FastAPI 对外接口层
    server.py          # 应用启动、lifespan、WebSocket 广播
    routes_orders.py   # 订单 REST 路由
    routes_portfolio.py# 持仓/账户 REST 路由

  services/            # 业务服务层（状态管理 + 事件订阅）
    order_service.py
    portfolio_service.py

  adapter/             # IBKR 适配层（连接与回调 -> 事件发布）
    ib_gateway.py
    ibkr_client.py
    ibkr_wrapper.py
    event_bus.py

  models/              # 数据模型（请求、状态、事件、合约）
    order_models.py
    portfolio_models.py
    event_models.py
    order.py
    contract.py

  main.py               # 启动入口（调用 api.server.start_server）
```

### 2.1 分层职责

- **API 层**（`api/*`）
  - 只做参数接收/错误封装/返回 JSON。
  - 通过 `init_*_service()` 注入全局 Service 实例。

- **Service 层**（`services/*`）
  - 维护内存态状态（订单字典、持仓字典、账户摘要）。
  - 订阅 EventBus（订单状态、持仓更新、账户更新），完成状态更新与日志输出。

- **Adapter 层**（`adapter/*`）
  - `IBClient`：对 `ibapi.client.EClient` 的薄封装，负责发送请求（下单/撤单/请求账户更新）。
  - `IBWrapper`：继承 `ibapi.wrapper.EWrapper`，接收 IB 回调并发布事件（不持久化状态）。
  - `IBGateway`：聚合 client + wrapper，提供上层统一调用接口。
  - `EventBus`：类型安全的发布/订阅中心。

- **Models 层**（`models/*`）
  - `OrderRequest`：API 下单参数模型 + 校验。
  - `OrderState`：本地追踪订单生命周期状态。
  - `AccountSummary` / `Position`：账户摘要与持仓结构。
  - `*Event`：事件总线的载体。

## 3. 运行时架构与数据流

### 3.1 启动流程（FastAPI lifespan）

文件：`src/IBBot/api/server.py`

1. 创建 `IBGateway()`（未连接）。
2. 初始化 `OrderService(ib_gateway)` 与 `PortfolioService(ib_gateway)`。
   - 两者在构造时订阅 EventBus。
3. 连接 IB Gateway（host/port/client_id 来自环境变量）。
4. 若已连接：请求初始持仓/账户，并等待数据返回。
5. 完成启动时订单同步标记（`order_service.finish_startup_sync()`）。
6. 在 EventBus 上订阅事件，并将事件广播到所有 WebSocket 客户端：
   - `OrderStatusEvent`
   - `PositionUpdatedEvent`
   - `AccountUpdatedEvent`

**关键点：先订阅再连接**

服务先订阅事件，再连接 IB，避免“连接后立即推送事件但服务未准备好”导致丢事件。

### 3.2 下单链路（REST → Service → IB → 回调 → 事件 → Service）

文件：`src/IBBot/api/routes_orders.py` + `src/IBBot/services/order_service.py`

- 客户端：`POST /orders/place` 提交 `OrderRequest`
- `OrderService.place_order()`：
  1. `models.contract.from_request(req)` 创建合约（当前仅 STK）。
  2. `models.order.from_request(req)` 创建 IBKR Order（内部会调用 `req.validate()` 做统一校验）。
  3. 使用 `ib_gateway.next_order_id` 或 wrapper 的 `nextValidOrderId`（本项目实际使用 `self.ib.next_order_id`）作为订单号。
  4. 本地写入 `OrderState.initial(order_id, req)`（初始状态 `PreSubmitted`）。
  5. `ib_gateway.place_order(contract, order)` 发单。

- IB 回调：
  - `IBWrapper.openOrder()`/`orderStatus()` 将回报转换为 `OrderStatusEvent` 发布到 EventBus。
- `OrderService._on_order_update()` 订阅并更新 `self.orders[order_id]`。

### 3.3 持仓/账户链路（刷新 → 回调 → 事件 → Service）

文件：`src/IBBot/services/portfolio_service.py` + `src/IBBot/adapter/ib_gateway.py` + `src/IBBot/adapter/ibkr_wrapper.py`

- `POST /portfolio/refresh` → `portfolio_service.refresh()`：
  - `ib_gateway.request_positions()` 与 `request_account_summary()`（当前实现都通过 `reqAccountUpdates(True, "")` 触发回调）。

- IB 回调：
  - `IBWrapper.updatePortfolio()` → 发布 `PositionUpdatedEvent`
  - `IBWrapper.updateAccountValue()` → 发布 `AccountUpdatedEvent`

- `PortfolioService` 订阅：
  - `_on_position()` 更新 `positions` 字典（数量为 0 则删除）。
  - `_on_account_summary()` 调用 `AccountSummary.update_from_tag()` 进行字段更新与类型转换。

### 3.4 WebSocket 广播（事件驱动推送）

文件：`src/IBBot/api/server.py`

- WebSocket 端点：`GET ws://<host>:<port>/ws`
- 服务器订阅 EventBus，并把事件封装为带 `event_name` 的 envelope 广播：

```json
{
  "type": "event",
  "event_name": "OrderStatusEvent",
  "data": {
    "order_id": 123,
    "status": "Submitted",
    "filled": 0,
    "remaining": 100,
    "avg_fill_price": 0.0,
    "commission": null,
    "symbol": "AAPL",
    "action": "BUY"
  }
}
```

> 说明：当前广播时会移除 `timestamp` 字段（见 `event_to_dict_wrapper()`），并在 envelope 上提供 `event_name` 供前端做类型分发。

## 4. 当前已实现功能清单

### 4.1 订单（Orders）

- 市价单/限价单下单（`MKT` / `LMT`）
- 撤单（单笔撤单）
- 内存态订单列表查询
- 活跃订单过滤（`PreSubmitted/Submitted/PendingCancel`）
- 启动阶段订单“同步”：
  - 当启动时收到历史 `OrderStatusEvent` 且本地尚无该订单，尝试用事件内容拼装一个不完整的 `OrderRequest` 进行追踪。

### 4.2 持仓/账户（Portfolio）

- 账户摘要查询：`/portfolio/`
- 持仓列表查询：`/portfolio/positions`
- 单标的持仓查询：`/portfolio/positions/{symbol}`
- 触发刷新：`/portfolio/refresh`
- 汇总接口：`/portfolio/summary`（账户 + 持仓 + 总市值）
  - `positions` 现在会包含更完整字段：`market_price / market_value / unrealized_pnl / realized_pnl`
  - `total_market_value` 优先使用 IB 回调提供的 `market_value`，缺失时回退为 `quantity × average_cost`

### 4.3 实时推送

- WebSocket 推送订单状态、持仓、账户更新事件。

## 5. 对外接口说明（当前版本）

### 5.1 基础

- `GET /`：服务状态 + 是否连接 IB
- `GET /health`：健康检查

### 5.2 订单 API（前缀 `/orders`）

- `POST /orders/place`：下单
- `POST /orders/cancel/{order_id}`：撤单
- `GET /orders/list`：全部订单
- `GET /orders/active`：活跃订单
- `GET /orders/{order_id}`：单笔订单详情

`OrderRequest` 关键字段：

- `symbol`（必填）
- `action`：`BUY|SELL`
- `quantity`：>0
- `order_type`：`MKT|LMT`
- `limit_price`：`LMT` 必填
- `tif`：`DAY|GTC`（目前透传）
- `OutsideRth`：是否允许盘前盘后

### 5.3 持仓/账户 API（前缀 `/portfolio`）

- `GET /portfolio/`：账户摘要
- `GET /portfolio/positions`：持仓列表
- `GET /portfolio/positions/{symbol}`：单标的持仓
- `POST /portfolio/refresh`：触发刷新
- `GET /portfolio/summary`：账户 + 持仓 + 总市值

`GET /portfolio/summary` 返回示例（字段可能按账户类型不同而有差异）：

```json
{
  "account": {
    "NetLiquidation": 12345.67,
    "TotalCashValue": 890.12,
    "BuyingPower": 3456.78
  },
  "positions": [
    {
      "symbol": "AAPL",
      "quantity": 10,
      "average_cost": 279.85,
      "market_price": 281.12,
      "market_value": 2811.2,
      "unrealized_pnl": 12.7,
      "realized_pnl": 0.0
    }
  ],
  "total_market_value": 2811.2,
  "position_count": 1
}
```

### 5.4 WebSocket

- `GET /ws`：事件推送
  - 初次连接会收到 `type=connection` 的欢迎消息
  - 后续持续接收 `type=event` 事件消息

## 6. 关键实现细节与已知限制

### 6.1 订单历史的结构性限制

- `IBClient.get_open_orders()` 只能拿到“当前活跃订单”。
- `IBClient.get_all_orders()` 只能拿到“本进程生命周期内的订单”。

因此：**要实现跨进程、跨天的订单/成交历史查询，必须自行做数据库持久化。**（见第 7 节）

### 6.2 启动同步信息不完整

- `IBWrapper.orderStatus()` 回调不包含 `symbol/action/commission` 等，需要依赖 `openOrder()` 才可能补全。
- 当前 `OrderService._sync_existing_order()` 会用默认值构造 `OrderRequest`（如默认 `MKT`），因此仅用于“追踪状态”，不保证复原原始订单参数。

### 6.3 连接与线程模型

- `IBGateway.connect()` 内部启动 `EClient.run()` 的后台线程。
- `EventBus.publish_event()` 在调用订阅者时不持锁（避免死锁），但回调仍需保持“快速/无阻塞”。

### 6.4 生产化注意事项（当前未做）

- CORS 目前 `allow_origins=["*"]`，生产环境需收敛。
- 无鉴权、无权限隔离。
- WebSocket 广播已包含 `event_name`（见第 3.4 节），前端可用它做事件类型分发与 TypeScript 联合类型收敛。

## 7. 数据库持久化（PostgreSQL）与 Alembic 迁移

> 目标：把“订单/状态流/成交/持仓快照/账户快照/事件日志”写入 Postgres，支撑跨进程/跨重启的查询与审计。

### 7.1 启用方式（`DATABASE_URL`）

数据库是**可选**组件：只有设置了 `DATABASE_URL` 才会启用落库。

- 启用开关：环境变量 `DATABASE_URL`
- 推荐格式（SQLAlchemy）：
  - `postgresql+psycopg://USER:PASSWORD@HOST:5432/DBNAME`

运行时行为：

- `DatabaseService` 订阅 EventBus 的关键事件并写库（best-effort）。
- 写库通过 `asyncio.to_thread(...)` 执行，避免阻塞 FastAPI event loop / IB 回调线程。

### 7.2 第一次运行（全新数据库）

1. 确认 Postgres 可用

- 检查本机是否能连上：
  - `psql -h HOST -p 5432 -U USER -d postgres -c 'select 1;'`

1. 创建数据库（如果你还没建）

- `createdb -h HOST -p 5432 -U USER DBNAME`

1. 设置 `DATABASE_URL`

- 例：`export DATABASE_URL='postgresql+psycopg://USER:PASSWORD@HOST:5432/DBNAME'`

1. 执行迁移（推荐先迁移再启动）

- `poetry run alembic upgrade head`

1. 启动 IBBot API

- 方式 A（项目自带入口）：`poetry run python -m IBBot.main`
- 方式 B（uvicorn）：`poetry run uvicorn IBBot.main:app --host 0.0.0.0 --port 8888`

1. 用 `psql` 验证表已创建

注意：`psql` 不认识 `postgresql+psycopg://...` 里的 `+psycopg`。

- 快速替换（bash/zsh）：`export PSQL_URL="${DATABASE_URL/+psycopg/}"`
- 然后：`psql "$PSQL_URL" -c '\\dt'`

也可以直接指定连接参数：

- `psql -h HOST -p 5432 -U USER -d DBNAME -c '\\dt'`

### 7.3 已存在旧表（曾经用 `create_tables()` / `Base.metadata.create_all()` 建过）

如果你的库里已经有旧表（例如缺少 `account` 列、或 timestamp 还是无时区类型），请先跑迁移：

- `poetry run alembic upgrade head`

这次的 bootstrap migration 做了两类事情：

- 表不存在：创建全套表。
- 表已存在：
  - 补齐 `account` 区分字段（用于多 IBKR 账户数据隔离）；
  - 将 `orders` 的唯一约束升级为 `(account, order_id)`，避免多账户/多连接时 `order_id` 冲突；
  - 在 Postgres 上把旧的“UTC-naive 时间戳”升级为 `timestamptz`（按“旧值是 UTC”来解释）。

### 7.4 日常运行（正常启停）

- 确保服务启动时环境里有 `DATABASE_URL`。
- 启动后在日志里应能看到：`Database persistence enabled`（若失败会降级为不写库）。

常用自检 SQL：

- 最近事件：`select event_name, account, recorded_at from event_log order by recorded_at desc limit 20;`
- 最近订单：`select account, order_id, symbol, last_status, updated_at from orders order by updated_at desc limit 20;`

### 7.5 Alembic 维护工作流（后续 schema 演进）

1. 生成新迁移

- `poetry run alembic revision -m "add some field" --autogenerate`

1. 应用迁移

- `poetry run alembic upgrade head`

1. 回滚（谨慎）

- `poetry run alembic downgrade -1`

1. 查看迁移状态

- `poetry run alembic current`
- `poetry run alembic history --verbose`
- `poetry run alembic show head`

### 7.6 时间与时区（America/New_York）

本项目的 ORM 默认时间使用 `America/New_York`，并以 `timestamptz` 落库。

- 数据库里“显示成什么时区”取决于 session 的 `TimeZone`。
- 你可以在 `psql` 里设置：`set time zone 'America/New_York';`

## 7. 未来扩展：数据库持久化（推荐设计）

目标：补齐“订单/成交/持仓/账户/事件流”的可追溯性，让系统具备：

- 历史订单查询（跨重启、跨天）
- 成交记录与手续费归集
- 账户与持仓的时间序列/快照
- 审计与回放（从事件流重建状态）

### 7.1 扩展方式：新增 `DatabaseService`（事件订阅者）

新增一个 Service：`services/database_service.py`（建议）

- 在 `api/server.py` lifespan 中初始化，并订阅 EventBus。
- 订阅的典型事件：
  - `OrderPlacedEvent`、`OrderStatusEvent`、`ExecutionReceivedEvent`
  - `PositionUpdatedEvent`、`AccountUpdatedEvent`
  - `ErrorEvent`、`ConnectionEvent`

优势：

- 不侵入现有下单/持仓业务逻辑。
- 利用事件驱动架构保证写库与业务解耦。

### 7.2 数据模型建议（以 PostgreSQL 为例）

建议最小可用表：

- `orders`：订单主表（order_id、symbol、action、qty、order_type、limit_price、tif、outside_rth、created_at、source）
- `order_status`：订单状态变更表（order_id、status、filled、remaining、avg_fill_price、commission、ts）
- `executions`：成交表（exec_id、order_id、symbol、side、shares、price、exchange、time、ts）
- `positions_snapshot`：持仓快照（symbol、qty、avg_cost、market_price、market_value、unrealized_pnl、ts）
- `account_snapshot`：账户快照（tag/value 或宽表字段、currency、ts）
- `event_log`：原始事件归档（event_type、payload_json、ts）

### 7.3 幂等与一致性

- `order_id` 在 IB 的语义下对单个 client/session 稳定，但跨 client_id 可能冲突：
  - 推荐引入 `broker` + `client_id` + `order_id` 的复合键，或使用内部 `uuid` 作为主键。
- `ExecutionReceivedEvent.exec_id` 通常可作为成交幂等键。

### 7.4 技术栈建议

- ORM：SQLAlchemy 2.x（同步/异步都可），或直接 `psycopg`。
- 迁移：Alembic。
- 若不想引入数据库，可先用 SQLite 作为单机落地。

## 8. 未来扩展：前端连接与接口约定

目标：让前端（例如 `frontend/` 的 Vite React）能稳定消费：

- REST：用于“初始状态”拉取（订单列表、持仓、账户摘要）。
- WebSocket：用于“增量事件”订阅（状态变化推送）。

### 8.1 推荐的前端数据流

- 页面加载：
  - `GET /health` 检查服务与 IB 连接
  - `GET /portfolio/summary` 拉初始账户/持仓
  - `GET /orders/list` 拉初始订单
- 用户交互：
  - 撤单：`POST /orders/cancel/{order_id}`
- 建立 WebSocket：
  - 订阅实时更新，增量更新前端 store（Redux/Zustand 等）

补充说明（为什么需要“REST snapshot + WS stream”两条腿）：

- WebSocket 推送是“增量事件流”，不保证为后来连接的客户端补发历史事件。
- 因此：前端应先用 `GET /portfolio/summary` / `GET /orders/list` 拉到“当前状态快照”，再用 WS 做增量更新，避免出现“后端日志有更新但前端没看到”的错觉。

### 8.2 WebSocket 协议（当前实现）

当前协议（`type=event` + `event_name` + `data`）：

```json
{
  "type": "event",
  "event_name": "OrderStatusEvent",
  "data": { "order_id": 1, "status": "Filled", "filled": 100, "remaining": 0 }
}
```

好处：

- 前端不需要通过字段推断事件类型。
- 更适合做类型定义（TypeScript discriminated union）。

### 8.3 TypeScript 类型与消费示例（前端）

前端类型定义已整理在：`frontend/src/types/ibbot.ts`（`WsMessage`、`WsEventMessage`、各事件 payload）。

消费示例（按 `type` / `event_name` 分发）：

```ts
import type { WsMessage } from '../types/ibbot';

ws.onmessage = (evt) => {
  const msg = JSON.parse(evt.data) as WsMessage;
  if (msg.type === 'connection') return;

  switch (msg.event_name) {
    case 'OrderStatusEvent':
      // msg.data.order_id / msg.data.status / ...
      break;
    case 'PositionUpdatedEvent':
      break;
    case 'AccountUpdatedEvent':
      break;
  }
};
```

### 8.4 前端联调注意

- CORS：开发期 `*` 方便；生产期应限制 `allow_origins`。
- WebSocket 心跳：当前服务端会 `receive_text()` 阻塞读取客户端消息；建议前端定时发送 ping 字符串，避免中间层断开。

## 9. 后续演进建议（非强制）

- 支持更多品种：期权/期货合约模型与下单参数扩展（`sec_type != STK`）。
- 丰富订单类型：STP / STP LMT 已在 `models/order.py` 中有构造函数，但 API/Service 暂未暴露。
- 风控：下单前规则校验（最大仓位、最大单笔、交易时段、黑名单）。
- 鉴权：API Key/JWT + 细粒度权限，避免公开下单端口。

---

## 附录 A：关键配置（环境变量）

- `API_HOST` / `API_PORT`：FastAPI 监听地址（默认 0.0.0.0:8888）
- `IB_GATEWAY_HOST` / `IB_GATEWAY_PORT`：IB Gateway 地址（默认 127.0.0.1:4002）
- `IB_CLIENT_ID`：IB 客户端 ID（默认 1）
- `DATABASE_URL`：启用 PostgreSQL 持久化（可选）
  - 示例：`postgresql+psycopg://postgres:postgres@127.0.0.1:5432/ibbot`
  - 配置后服务启动会自动 `create_tables()`（生产建议改用 Alembic 迁移）
