"""
IB Gateway - 统一的 IBKR 网关接口
整合 IBClient 和 IBWrapper，提供事件驱动的 API
"""

import threading

from IBBot.adapter.event_bus import get_event_bus
from IBBot.adapter.ibkr_client import IBClient
from IBBot.adapter.ibkr_wrapper import IBWrapper
from IBBot.models.event_models import (
    BaseEvent,
    ConnectionEvent,
    ErrorEvent,
    OrderCancelledEvent,
    OrderPlacedEvent,
)
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)


class IBGateway:
    """
    IBKR Gateway - 统一的交易网关

    特点：
    - 整合 Client 和 Wrapper
    - 提供事件总线接口
    - 简化的 API 调用
    - 线程安全
    """

    def __init__(self):
        """初始化 IB Gateway"""
        self.event_bus = get_event_bus()
        self.wrapper = IBWrapper()
        self.client = IBClient(self.wrapper)
        self.thread = None
        self.connected = False

    def connect(self, host="127.0.0.1", port=4001, client_id=1):
        """
        连接到 IB Gateway

        Args:
            host: IB Gateway 主机地址
            port: IB Gateway 端口（Paper: 4002, Live: 4001）
            client_id: 客户端 ID
        """
        try:
            self.client.connect(host, port, client_id)

            # 启动消息处理线程
            self.thread = threading.Thread(target=self.client.run, daemon=True)
            self.thread.start()

            self.connected = True
            logger.info(f"✓ Connected to IB Gateway at {host}:{port}")

            # 发布连接事件
            self.event_bus.publish_event(ConnectionEvent(status="connected"))

        except Exception as e:
            logger.error(f"✗ Failed to connect to IB Gateway: {e}")
            self.connected = False
            self.event_bus.publish_event(ErrorEvent(error_message=str(e)))

    def disconnect(self):
        """断开与 IB Gateway 的连接"""
        if self.connected:
            self.client.disconnect()
            self.connected = False
            self.event_bus.publish_event(ConnectionEvent(status="disconnected"))
            logger.info("✓ Disconnected from IB Gateway")

    # ====== Order APIs ======

    def place_order(self, contract, order):
        """
        下单

        Args:
            contract: IBKR Contract 对象
            order: IBKR Order 对象

        Returns:
            order_id: 订单 ID
        """
        order_id = self.client.place_order(contract, order)

        # 发布订单提交事件
        self.event_bus.publish_event(
            OrderPlacedEvent(
                order_id=order_id,
                symbol=contract.symbol,
                action=order.action,
                quantity=int(order.totalQuantity),
                order_type=order.orderType,
                limit_price=float(order.lmtPrice) if order.lmtPrice else None,
                outsideRth=order.outsideRth,
            )
        )

        return order_id

    def cancel_order(self, order_id: int, reason: str = "User cancelled"):
        """
        取消订单

        Args:
            order_id: 订单 ID
            reason: 取消原因
        """
        self.client.cancel_order(order_id)
        self.event_bus.publish_event(
            OrderCancelledEvent(order_id=order_id, reason=reason)
        )

    def cancel_all_orders(self):
        """取消所有订单"""
        self.client.cancel_all_orders()
        self.event_bus.publish_event(
            OrderCancelledEvent(all=True, reason="Cancel all orders")
        )

    def get_open_orders(self):
        """
        获取所有未完成订单

        Returns:
            Dict[order_id, order_data]
        """
        return self.client.get_open_orders()

    def get_all_orders(self):
        """
        获取所有订单（包括已完成）

        Returns:
            Dict[order_id, order_data]
        """
        return self.client.get_all_orders()

    # ====== Portfolio APIs ======

    def request_positions(self):
        """
        请求持仓信息
        持仓数据会通过 'position' 事件返回
        """
        print("📡 Requesting positions from IB Gateway...")
        # 使用 reqAccountUpdates 触发 updatePortfolio 回调
        self.client.reqAccountUpdates(True, "")

    def request_account_summary(self):
        """
        请求账户摘要信息
        账户数据会通过 'account_summary' 事件返回
        """
        print("📡 Requesting account summary from IB Gateway...")
        # reqAccountUpdates 也会触发 updateAccountValue 回调
        self.client.reqAccountUpdates(True, "")

    def get_positions(self):
        """
        获取当前持仓

        Returns:
            Dict[symbol, position_data]
        """
        return self.wrapper.positions.copy()

    def get_account_values(self):
        """
        获取账户信息

        Returns:
            Dict[key, value]
        """
        return self.wrapper.account_values.copy()

    # ====== 便捷属性 ======

    @property
    def is_connected(self):
        """检查是否已连接"""
        return self.connected and self.client.isConnected()

    @property
    def next_order_id(self):
        """获取下一个有效的订单 ID"""
        return self.wrapper.nextValidOrderId
