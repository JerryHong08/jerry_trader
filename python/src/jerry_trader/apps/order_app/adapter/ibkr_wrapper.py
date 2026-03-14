import logging
import threading

from ibapi.const import UNSET_DOUBLE
from ibapi.wrapper import EWrapper

from jerry_trader.apps.order_app.adapter.event_bus import get_event_bus
from jerry_trader.apps.order_app.models.event_models import (
    AccountUpdatedEvent,
    CompletedOrdersSyncEndEvent,
    ExecutionReceivedEvent,
    OpenOrdersSyncEndEvent,
    OrderStatusEvent,
    PositionUpdatedEvent,
)
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class IBWrapper(EWrapper):
    """
    无状态的 IB API Wrapper - 只负责接收回调并发布事件

    所有状态管理由订阅事件的服务负责：
    - OrderService: 管理订单状态
    - PortfolioService: 管理持仓和账户
    - DatabaseService: 持久化所有数据
    """

    def __init__(self):
        EWrapper.__init__(self)
        # 只保留必要的运行时状态
        self.nextValidOrderId = None
        self.stream_event = threading.Event()  # 用于同步等待流数据

        # Best-effort caches to enrich callbacks:
        # - openOrder provides contract+order details, but orderStatus often doesn't.
        # - We cache by order_id so orderStatus can publish richer events.
        self._order_details_by_id = {}
        self._exec_to_order_id = {}

        # Completed orders response tracking
        self._completed_orders_seen = 0

        # Sync window flags (to avoid counting openOrder/completedOrder triggered by non-sync actions)
        self._open_orders_sync_in_progress = False
        self._completed_orders_sync_in_progress = False

        # Open orders response tracking (reqOpenOrders -> openOrder -> openOrderEnd)
        self._open_orders_seen = 0

        # EventBus - 唯一的对外接口
        self.event_bus = get_event_bus()

    # ===== Sync window helpers (called by IBGateway before requests) =====

    def begin_open_orders_sync(self):
        """Mark the start of an open-orders sync request window."""
        self._open_orders_seen = 0
        self._open_orders_sync_in_progress = True

    def begin_completed_orders_sync(self):
        """Mark the start of a completed-orders sync request window."""
        self._completed_orders_seen = 0
        self._completed_orders_sync_in_progress = True

    def nextValidId(self, order_id):
        super().nextValidId(order_id)
        self.nextValidOrderId = order_id

    def orderStatus(
        self,
        order_id,
        status,
        filled,
        remaining,
        avg_fill_price,
        perm_id,
        parent_id,
        last_fill_price,
        client_id,
        why_held,
        mkt_cap_price,
    ):
        """订单状态更新回调 - 只发布事件，不存储状态"""
        # 只在关键状态时输出 INFO
        if status in ["Filled", "Cancelled", "Error"]:
            logger.info(
                f"orderStatus - orderId: {order_id}, status: {status}, filled: {filled}/{filled+remaining}"
            )
        else:
            logger.debug(
                f"orderStatus - orderId: {order_id}, status: {status}, filled: {filled}, "
                f"remaining: {remaining}, lastFillPrice: {last_fill_price}"
            )

        # ✅ 直接发布事件，不存储状态
        # 注意：orderStatus 天然缺少很多字段（symbol/action/order_type/price 等），尽量从 openOrder 缓存补全。
        details = self._order_details_by_id.get(int(order_id), {})
        filled_i = int(filled)
        remaining_i = int(remaining)
        total_qty = details.get("quantity")
        if total_qty is None:
            total_qty = filled_i + remaining_i
        self.event_bus.publish_event(
            OrderStatusEvent(
                order_id=int(order_id),
                status=status,
                filled=filled_i,
                remaining=remaining_i,
                avg_fill_price=avg_fill_price,
                commission=None,  # orderStatus 没有 commission
                symbol=details.get("symbol"),
                action=details.get("action"),
                quantity=total_qty,
                account=details.get("account"),
                order_type=details.get("order_type"),
                limit_price=details.get("limit_price"),
                stop_price=details.get("stop_price"),
                tif=details.get("tif"),
                outsideRth=details.get("outsideRth"),
                sec_type=details.get("sec_type"),
                exchange=details.get("exchange"),
                currency=details.get("currency"),
                perm_id=int(perm_id) if perm_id is not None else None,
                parent_id=int(parent_id) if parent_id is not None else None,
                client_id=int(client_id) if client_id is not None else None,
                last_fill_price=(
                    float(last_fill_price) if last_fill_price is not None else None
                ),
            )
        )

        # Cleanup cache when order reaches a terminal state.
        if status in ["Filled", "Cancelled", "ApiCancelled", "Inactive", "Error"]:
            try:
                self._order_details_by_id.pop(int(order_id), None)
            except Exception:
                pass

    def openOrder(self, order_id, contract, order, order_state):
        """订单详情回调 - 只发布事件，不存储状态"""
        logger.info(
            f"openOrder - id: {order_id}, {contract.symbol} {contract.secType} @ {contract.exchange}: "
            f"{order.action} {order.orderType} {order.totalQuantity}, {order.lmtPrice if hasattr(order, 'lmtPrice') else None}, status: {order_state.status}"
        )

        # Extract commission if available
        commission = None
        if (
            hasattr(order_state, "commissionAndFees")
            and order_state.commissionAndFees != UNSET_DOUBLE
        ):
            commission = order_state.commissionAndFees

        order_id_i = int(order_id)
        if self._open_orders_sync_in_progress:
            self._open_orders_seen += 1
        quantity = int(getattr(order, "totalQuantity", 0) or 0)
        order_type = getattr(order, "orderType", None)
        limit_price = getattr(order, "lmtPrice", None)
        stop_price = getattr(order, "auxPrice", None)
        tif = getattr(order, "tif", None)
        outside_rth = getattr(order, "outsideRth", None)

        # Normalize unset / sentinel values
        if limit_price in (None, UNSET_DOUBLE):
            limit_price = None
        else:
            try:
                limit_price = float(limit_price)
            except Exception:
                limit_price = None
        if stop_price in (None, UNSET_DOUBLE):
            stop_price = None
        else:
            try:
                stop_price = float(stop_price)
            except Exception:
                stop_price = None

        # Cache details so later orderStatus callbacks can be enriched.
        self._order_details_by_id[order_id_i] = {
            "symbol": getattr(contract, "symbol", None),
            "action": getattr(order, "action", None),
            "account": getattr(order, "account", None),
            "quantity": quantity if quantity > 0 else None,
            "order_type": order_type,
            "limit_price": limit_price,
            "stop_price": stop_price,
            "tif": tif,
            "outsideRth": bool(outside_rth) if outside_rth is not None else None,
            "sec_type": getattr(contract, "secType", None),
            "exchange": getattr(contract, "exchange", None),
            "currency": getattr(contract, "currency", None),
        }

        # ✅ 直接发布事件，包含尽可能完整的订单信息（用于启动同步）
        self.event_bus.publish_event(
            OrderStatusEvent(
                order_id=order_id_i,
                status=order_state.status,
                filled=0,  # openOrder 通常在订单刚创建时调用
                remaining=quantity,
                avg_fill_price=0.0,
                commission=commission,
                symbol=contract.symbol,
                action=order.action,
                quantity=quantity,
                account=getattr(order, "account", None),
                order_type=order_type,
                limit_price=limit_price,
                stop_price=stop_price,
                tif=tif,
                outsideRth=bool(outside_rth) if outside_rth is not None else None,
                sec_type=getattr(contract, "secType", None),
                exchange=getattr(contract, "exchange", None),
                currency=getattr(contract, "currency", None),
            )
        )

    def openOrderEnd(self):
        """Signals end of open orders response (after reqOpenOrders)."""
        self._open_orders_sync_in_progress = False
        logger.info(
            f"openOrderEnd - ✅ Open orders download finished (count={self._open_orders_seen})"
        )
        self.event_bus.publish_event(
            OpenOrdersSyncEndEvent(count=int(self._open_orders_seen))
        )

    def execDetails(self, request_id, contract, execution):
        """成交回报回调 - 只发布事件，不存储状态"""
        logger.info(
            f"execDetails - ✅ Order Executed - {contract.symbol}: {execution.side} {execution.shares} @ ${execution.price} "
            f"(orderId: {execution.orderId}, execId: {execution.execId})"
        )

        # Keep mapping for potential enrichment (e.g., commissionReport)
        try:
            self._exec_to_order_id[str(execution.execId)] = int(execution.orderId)
        except Exception:
            pass

        # ✅ 直接发布事件
        self.event_bus.publish_event(
            ExecutionReceivedEvent(
                exec_id=execution.execId,
                order_id=execution.orderId,
                symbol=contract.symbol,
                side=execution.side,  # BOT/SLD
                shares=execution.shares,
                price=execution.price,
                exchange=execution.exchange,
                time=execution.time,
                account=self._order_details_by_id.get(int(execution.orderId), {}).get(
                    "account"
                ),
            )
        )

    # ===== Completed Orders (reqCompletedOrders) =====

    def completedOrder(self, contract, order, order_state):
        """Completed order callback (triggered by reqCompletedOrders).

        Use this to enrich startup sync with details that may not arrive via openOrder/orderStatus.
        """
        try:
            order_id = int(getattr(order, "orderId", 0) or 0)
        except Exception:
            order_id = 0

        if order_id:
            if self._completed_orders_sync_in_progress:
                self._completed_orders_seen += 1

            quantity = int(getattr(order, "totalQuantity", 0) or 0)
            order_type = getattr(order, "orderType", None)
            limit_price = getattr(order, "lmtPrice", None)
            stop_price = getattr(order, "auxPrice", None)
            tif = getattr(order, "tif", None)
            outside_rth = getattr(order, "outsideRth", None)

            # Normalize unset values
            if limit_price in (None, UNSET_DOUBLE):
                limit_price = None
            else:
                try:
                    limit_price = float(limit_price)
                except Exception:
                    limit_price = None
            if stop_price in (None, UNSET_DOUBLE):
                stop_price = None
            else:
                try:
                    stop_price = float(stop_price)
                except Exception:
                    stop_price = None

            # Cache details (same structure as openOrder)
            self._order_details_by_id[order_id] = {
                "symbol": getattr(contract, "symbol", None),
                "action": getattr(order, "action", None),
                "account": getattr(order, "account", None),
                "quantity": quantity if quantity > 0 else None,
                "order_type": order_type,
                "limit_price": limit_price,
                "stop_price": stop_price,
                "tif": tif,
                "outsideRth": bool(outside_rth) if outside_rth is not None else None,
                "sec_type": getattr(contract, "secType", None),
                "exchange": getattr(contract, "exchange", None),
                "currency": getattr(contract, "currency", None),
            }

            # Publish as an OrderStatusEvent for unified downstream handling
            status = getattr(order_state, "status", None) or "Filled"
            remaining = 0
            filled = quantity
            self.event_bus.publish_event(
                OrderStatusEvent(
                    order_id=order_id,
                    status=status,
                    filled=filled,
                    remaining=remaining,
                    avg_fill_price=0.0,
                    commission=None,
                    symbol=getattr(contract, "symbol", None),
                    action=getattr(order, "action", None),
                    quantity=quantity if quantity > 0 else None,
                    account=getattr(order, "account", None),
                    order_type=order_type,
                    limit_price=limit_price,
                    stop_price=stop_price,
                    tif=tif,
                    outsideRth=bool(outside_rth) if outside_rth is not None else None,
                    sec_type=getattr(contract, "secType", None),
                    exchange=getattr(contract, "exchange", None),
                    currency=getattr(contract, "currency", None),
                )
            )

            # Completed orders are typically terminal; cleanup cached details to avoid growth.
            if status in ["Filled", "Cancelled", "ApiCancelled", "Inactive", "Error"]:
                self._order_details_by_id.pop(order_id, None)

        logger.info(
            f"completedOrder - ✅ Completed order received: id={order_id}, symbol={getattr(contract, 'symbol', None)}, "
            f"action={getattr(order, 'action', None)}, type={getattr(order, 'orderType', None)}, status={getattr(order_state, 'status', None)}"
        )

    def completedOrdersEnd(self):
        """Signals end of completed orders response."""
        self._completed_orders_sync_in_progress = False
        logger.info(
            f"completedOrdersEnd - ✅ Completed orders download finished (count={self._completed_orders_seen})"
        )

        # Publish an explicit end-of-sync event so startup can wait without arbitrary sleeps.
        self.event_bus.publish_event(
            CompletedOrdersSyncEndEvent(count=int(self._completed_orders_seen))
        )

    def updateAccountValue(self, key, val, currency, account):
        """账户数据更新回调 - 只发布事件，不存储状态"""
        # ✅ 直接发布事件
        self.event_bus.publish_event(
            AccountUpdatedEvent(tag=key, value=val, currency=currency, account=account)
        )

    def updatePortfolio(
        self,
        contract,
        position,
        market_price,
        market_value,
        average_cost,
        unrealized_pnl,
        realized_pnl,
        account_name,
    ):
        """持仓更新回调 - 只发布事件，不存储状态"""
        # ✅ 直接发布事件
        self.event_bus.publish_event(
            PositionUpdatedEvent(
                symbol=contract.symbol,
                position=position,
                average_cost=average_cost,
                market_price=market_price,
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
                realized_pnl=realized_pnl,
                account=account_name,
            )
        )

    def pnl(self, request_id, daily_pnl, unrealized_pnl, realized_pnl):
        """PnL 更新回调 - 可以添加 PnL 事件"""
        logger.debug(
            f"pnl - reqId: {request_id}, daily: {daily_pnl}, "
            f"unrealized: {unrealized_pnl}, realized: {realized_pnl}"
        )
        # TODO: 添加 PnLUpdatedEvent 并发布
