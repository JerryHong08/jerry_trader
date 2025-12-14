import logging
import threading

from ibapi.const import UNSET_DOUBLE
from ibapi.wrapper import EWrapper

from IBBot.adapter.event_bus import get_event_bus
from IBBot.models.event_models import (
    AccountUpdatedEvent,
    ExecutionReceivedEvent,
    OrderStatusEvent,
    PositionUpdatedEvent,
)
from utils.logger import setup_logger

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

        # EventBus - 唯一的对外接口
        self.event_bus = get_event_bus()

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
        # 注意：这里没有 symbol/action/commission，需要从 openOrder 获取
        self.event_bus.publish_event(
            OrderStatusEvent(
                order_id=order_id,
                status=status,
                filled=filled,
                remaining=remaining,
                avg_fill_price=avg_fill_price,
                commission=None,  # orderStatus 没有 commission
                symbol=None,  # orderStatus 没有 symbol
                action=None,  # orderStatus 没有 action
            )
        )

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

        # ✅ 直接发布事件，包含完整的订单信息
        self.event_bus.publish_event(
            OrderStatusEvent(
                order_id=order_id,
                status=order_state.status,
                filled=0,  # openOrder 通常在订单刚创建时调用
                remaining=int(order.totalQuantity),
                avg_fill_price=0.0,
                commission=commission,
                symbol=contract.symbol,
                action=order.action,
            )
        )

    def execDetails(self, request_id, contract, execution):
        """成交回报回调 - 只发布事件，不存储状态"""
        logger.info(
            f"execDetails - ✅ Order Executed - {contract.symbol}: {execution.side} {execution.shares} @ ${execution.price} "
            f"(orderId: {execution.orderId}, execId: {execution.execId})"
        )

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
            )
        )

    def updateAccountValue(self, key, val, currency, account):
        """账户数据更新回调 - 只发布事件，不存储状态"""
        # ✅ 直接发布事件
        self.event_bus.publish_event(
            AccountUpdatedEvent(tag=key, value=val, currency=currency)  # 保持字符串格式
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
            )
        )

    def pnl(self, request_id, daily_pnl, unrealized_pnl, realized_pnl):
        """PnL 更新回调 - 可以添加 PnL 事件"""
        logger.debug(
            f"pnl - reqId: {request_id}, daily: {daily_pnl}, "
            f"unrealized: {unrealized_pnl}, realized: {realized_pnl}"
        )
        # TODO: 添加 PnLUpdatedEvent 并发布
