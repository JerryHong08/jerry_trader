"""
Order Service - 订单管理服务
处理订单的生命周期：下单 -> 追踪 -> 更新 -> 完成
"""

import logging
from typing import Dict, List

from IBBot.adapter.event_bus import get_event_bus
from IBBot.adapter.ib_gateway import IBGateway
from IBBot.models.contract import from_request as contract_from_request
from IBBot.models.event_models import OrderStatusEvent
from IBBot.models.order import from_request as order_from_request
from IBBot.models.order_models import OrderRequest, OrderState
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.INFO)


class OrderService:
    """
    订单管理服务

    职责：
    - 接收订单请求并转换为 IBKR 格式
    - 追踪所有订单状态
    - 通过事件总线发布订单更新
    """

    def __init__(self, ib: IBGateway):
        """
        初始化订单服务

        Args:
            ib: IB Gateway 实例
        """
        self.ib = ib
        self.orders: Dict[int, OrderState] = {}
        self._syncing_startup_orders = True  # flag for if it' the first logging time.
        self.event_bus = get_event_bus()

        # subscribe using event bus
        self.event_bus.subscribe(OrderStatusEvent, self._on_order_update)

    def place_order(self, req: OrderRequest) -> int:
        """
        下单

        Args:
            req: 订单请求

        Returns:
            order_id: 订单 ID

        Raises:
            ValueError: 订单参数无效

        Example:
            req = OrderRequest(symbol="AAPL", action="BUY", quantity=100)
            order_id = order_service.place_order(req)
        """
        # 转换为 IBKR Contract / Order（集中在 models 层）
        contract = contract_from_request(req)
        order = order_from_request(req)

        # 获取订单 ID
        order_id = self.ib.next_order_id

        # 创建订单状态记录
        self.orders[order_id] = OrderState.initial(order_id, req)

        # 发送订单到 IB
        self.ib.place_order(contract, order, reason=req.reason)

        logger.info(
            f"place_order - ✅ Order placed: {order_id} - {req.action} {req.quantity} {req.symbol}, reason: {req.reason}"
        )

        return order_id

    def cancel_order(self, order_id: int, reason: str | None = None):
        """
        取消订单

        Args:
            order_id: 订单 ID

        Example:
            order_service.cancel_order(123)
        """
        if order_id not in self.orders:
            raise ValueError(f"Order {order_id} not found")

        state = self.orders[order_id]
        if (state.status or "").lower() == "pendingcancel":
            raise ValueError(f"Order {order_id} is already cancelling")
        if not state.is_active:
            raise ValueError(
                f"Order {order_id} is not cancellable (current status: {state.status})"
            )

        cancel_reason = (reason or "").strip() or "User cancelled"
        self.ib.cancel_order(order_id, reason=cancel_reason)
        logger.info(f"✅ Order cancelled: {order_id} (reason={cancel_reason})")

    def list_orders(self) -> List[OrderState]:
        """
        获取所有订单列表

        Returns:
            List[OrderState]: 订单状态列表
        """
        return list(self.orders.values())

    def get_order(self, order_id: int) -> OrderState:
        """
        获取单个订单状态

        Args:
            order_id: 订单 ID

        Returns:
            OrderState: 订单状态
        """
        if order_id not in self.orders:
            logger.error(f"Order {order_id} not found")
            raise ValueError(f"Order {order_id} not found")
        return self.orders[order_id]

    def get_active_orders(self) -> List[OrderState]:
        """
        获取所有活跃订单（未完成的订单）

        Returns:
            List[OrderState]: 活跃订单列表
        """
        return [order for order in self.orders.values() if order.is_active]

    # ===== 事件回调 =====
    def _on_order_update(self, event: OrderStatusEvent):
        """
        处理订单状态更新事件

        Args:
            event: 订单状态事件对象（OrderStatusEvent）
        """
        # 使用正确的属性名（下划线命名）
        order_id = event.order_id

        # if there is no orders before, it's the intialization.
        if order_id not in self.orders:
            if self._syncing_startup_orders:
                self._sync_existing_order(event)
            return

        # 更新订单状态
        state = self.orders[order_id]

        # Best-effort: enrich stored request details from callbacks (useful for startup-synced orders)
        is_placeholder = state.request.symbol in ("UNKNOWN", "")
        if event.symbol and is_placeholder:
            state.request.symbol = event.symbol
        if event.action and is_placeholder:
            state.request.action = event.action
        if event.quantity is not None and is_placeholder:
            state.request.quantity = int(event.quantity)
        if event.order_type and is_placeholder:
            state.request.order_type = event.order_type
        if event.limit_price is not None and is_placeholder:
            state.request.limit_price = event.limit_price
        if event.tif and is_placeholder:
            state.request.tif = event.tif
        if event.outsideRth is not None and is_placeholder:
            state.request.OutsideRth = bool(event.outsideRth)

        old_status = state.status
        new_status = event.status

        old_filled = state.filled
        new_filled = event.filled

        state.status = new_status
        state.filled = new_filled
        state.remaining = event.remaining
        state.avg_fill_price = event.avg_fill_price

        # if commission data recieved for the first time.
        commission = event.commission
        if commission is not None and commission != state.commission:
            state.commission = commission
            logger.info(
                f"_on_order_update - Order {order_id}: Commission updated: ${commission}"
            )

        # if order status change
        if old_status != new_status:
            logger.info(
                f"_on_order_update - 📊 Order {order_id}: {old_status} → {new_status} "
                f"(filled: {state.filled}/{state.filled + state.remaining})"
            )
        # or not change, but filled changed.
        elif (
            old_filled != new_filled
            and new_filled > 0
            and old_status not in ["Filled", "Cancelled"]
        ):
            logger.info(
                f"_on_order_update - 📊 Order {order_id}: Partial fill "
                f"(filled: {state.filled}/{state.filled + state.remaining})"
            )

    def _sync_existing_order(self, event: OrderStatusEvent):
        """
        同步已存在的订单（启动时 IB Gateway 推送的历史订单）

        Args:
            event: 订单状态事件对象（OrderStatusEvent）
        """
        order_id = event.order_id

        # 如果订单已存在，跳过
        if not order_id or order_id in self.orders:
            return

        # 从事件数据重建 OrderRequest
        try:
            symbol = event.symbol or "UNKNOWN"
            action = event.action or "BUY"
            quantity = (
                int(event.quantity)
                if event.quantity is not None
                else int(event.filled) + int(event.remaining)
            )

            # 创建 OrderRequest（尽量用 callback 补齐字段；缺失则降级到合理默认）
            req = OrderRequest(
                symbol=symbol,
                action=action,
                quantity=quantity,
                order_type=event.order_type or "MKT",
                limit_price=event.limit_price,
                tif=event.tif or "DAY",
                OutsideRth=True if event.outsideRth is None else bool(event.outsideRth),
                sec_type=event.sec_type or "STK",
            )

            # 创建 OrderState
            self.orders[order_id] = OrderState(
                order_id=order_id,
                status=event.status,
                filled=event.filled,
                remaining=event.remaining,
                avg_fill_price=event.avg_fill_price,
                request=req,
                commission=event.commission,
            )

            logger.info(
                f"_sync_existing_order - 📥 Synced existing order {order_id}: {symbol} {action} {quantity} (status: {event.status})"
            )

        except Exception as e:
            logger.error(f"❌ Failed to sync order {order_id}: {e}")

    def finish_startup_sync(self):
        """完成启动时的订单同步，之后不再自动同步"""
        self._syncing_startup_orders = False
        logger.info(
            f"✅ Startup order sync complete. Tracking {len(self.orders)} orders."
        )
