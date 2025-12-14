"""
FastAPI Server - 主服务器入口
整合所有路由和服务，提供 REST API 和 WebSocket
"""

import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import Set

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from IBBot.adapter.event_bus import get_event_bus
from IBBot.adapter.ib_gateway import IBGateway
from IBBot.api.routes_orders import init_order_service
from IBBot.api.routes_orders import router as orders_router
from IBBot.api.routes_portfolio import init_portfolio_service
from IBBot.api.routes_portfolio import router as portfolio_router
from IBBot.models.event_models import (
    AccountUpdatedEvent,
    OrderStatusEvent,
    PositionUpdatedEvent,
)
from IBBot.services.order_service import OrderService
from IBBot.services.portfolio_service import PortfolioService
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)

# ===== global =====
ib_gateway: IBGateway = None
order_service: OrderService = None
portfolio_service: PortfolioService = None
websocket_clients: Set[WebSocket] = set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    lifespan for app start and disconnect
    """
    global ib_gateway, order_service, portfolio_service

    logger.info("🚀 Starting IBKR Bot API Server")

    # 1. Create IB Gateway instance (but don't connect yet)
    ib_gateway = IBGateway()

    # 2. Initialize services FIRST (so they can subscribe to events)
    logger.info("🔧 Initializing services...")
    order_service = OrderService(ib_gateway)
    portfolio_service = PortfolioService(ib_gateway)

    # init for routes
    init_order_service(order_service)
    init_portfolio_service(portfolio_service)
    logger.info("✅ Services initialized and subscribed to events")

    # 3. NOW connect to IB Gateway (services are ready to receive events)
    host = os.getenv("IB_GATEWAY_HOST", "127.0.0.1")
    port = int(os.getenv("IB_GATEWAY_PORT", "4002"))
    client_id = int(os.getenv("IB_CLIENT_ID", "1"))

    logger.info(f"📡 Connecting to IB Gateway at {host}:{port}...")
    ib_gateway.connect(host, port, client_id)

    # wait for connected
    await asyncio.sleep(2)

    if ib_gateway.is_connected:
        logger.info("lifespan - ✅ Connected to IB Gateway")
    else:
        logger.error(
            "lifespan - ⚠️  Failed to connect to IB Gateway - some features may not work"
        )

    # request portfolio data
    if ib_gateway.is_connected:
        logger.info("lifespan - 🔄 Requesting initial portfolio data...")
        portfolio_service.refresh()
        await asyncio.sleep(2)  # 等待数据返回
        logger.info(
            f"lifespan - ✅ Portfolio loaded: {len(portfolio_service.get_positions())} positions"
        )

        # 完成启动时的订单同步
        order_service.finish_startup_sync()

    # 6. 订阅事件并广播到 WebSocket 客户端
    def broadcast_to_websockets(event_data):
        """广播事件到所有 WebSocket 客户端"""
        message = {
            "type": "event",
            "data": event_data,
        }
        for client in websocket_clients.copy():
            try:
                asyncio.create_task(client.send_json(message))
            except:
                websocket_clients.discard(client)

    # 直接订阅 EventBus，手动转换事件为字典
    def event_to_dict_wrapper(event):
        # 转换 BaseEvent 为字典，移除 timestamp
        event_dict = asdict(event)
        event_dict.pop("timestamp", None)
        broadcast_to_websockets(event_dict)

    event_bus = get_event_bus()
    event_bus.subscribe(OrderStatusEvent, event_to_dict_wrapper)
    event_bus.subscribe(PositionUpdatedEvent, event_to_dict_wrapper)
    event_bus.subscribe(AccountUpdatedEvent, event_to_dict_wrapper)

    logger.info("lifespan - ✅ WebSocket broadcast subscriptions configured")
    logger.info("lifespan - 📊 Server ready!")

    yield

    # shut down and clean up, disconnect from the ibkr.
    logger.info("\nlifespan - 🛑 Shutting down...")
    if ib_gateway:
        ib_gateway.disconnect()
    logger.info("lifespan - ✅ Cleanup complete")


# ===== 创建 FastAPI 应用 =====

app = FastAPI(
    title="IBKR Bot API",
    description="Interactive Brokers Trading Bot with Event-Driven Architecture",
    version="1.0.0",
    lifespan=lifespan,
)


# ===== CORS 中间件 =====

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境应该限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ===== 注册路由 =====

app.include_router(orders_router, prefix="/orders")
app.include_router(portfolio_router, prefix="/portfolio")


# ===== 基础路由 =====


@app.get("/")
def root():
    """服务状态"""
    return {
        "status": "ok",
        "service": "IBKR Bot API",
        "version": "1.0.0",
        "ib_connected": ib_gateway.is_connected if ib_gateway else False,
    }


@app.get("/health")
def health_check():
    """健康检查"""
    return {
        "status": "ok",
        "ib_connected": ib_gateway.is_connected if ib_gateway else False,
        "order_service": order_service is not None,
        "portfolio_service": portfolio_service is not None,
    }


# ===== WebSocket =====


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket 端点 - 实时推送订单和持仓更新

    客户端会接收到以下类型的消息：
    - order_status: 订单状态更新
    - position: 持仓更新
    - account_summary: 账户信息更新
    """
    await websocket.accept()
    websocket_clients.add(websocket)

    logger.info(
        f"websocket_endpoint - 🔌 WebSocket client connected (total: {len(websocket_clients)})"
    )

    try:
        # 发送欢迎消息
        await websocket.send_json(
            {
                "type": "connection",
                "data": {
                    "status": "connected",
                    "message": "Welcome to IBKR Bot WebSocket",
                },
            }
        )

        # 保持连接
        while True:
            # 接收客户端消息（心跳等）
            data = await websocket.receive_text()
            # 可以在这里处理客户端发来的命令

    except WebSocketDisconnect:
        websocket_clients.discard(websocket)
        logger.info(
            f"websocket_endpoint - 🔌 WebSocket client disconnected (total: {len(websocket_clients)})"
        )
    except Exception as e:
        logger.error(f"websocket_endpoint - ❌ WebSocket error: {e}")
        websocket_clients.discard(websocket)


def start_server(host="0.0.0.0", port=8888):
    """
    start server

    Args:
        host: addr
        port: port
    """
    print(f"\n🌐 Starting server on http://{host}:{port}")
    print(f"📚 API Documentation: http://{host}:{port}/docs")
    print(f"🔌 WebSocket: ws://{host}:{port}/ws\n")

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
    )


if __name__ == "__main__":
    start_server()
