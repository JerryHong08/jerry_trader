"""
Portfolio Service - 持仓和账户管理服务
管理持仓信息和账户摘要
"""

import logging
from typing import Dict, List

from IBBot.adapter.event_bus import get_event_bus
from IBBot.adapter.ib_gateway import IBGateway
from IBBot.models.event_models import AccountUpdatedEvent, PositionUpdatedEvent
from IBBot.models.portfolio_models import AccountSummary, Position
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class PortfolioService:
    """
    持仓管理服务

    职责：
    - 追踪所有持仓
    - 追踪账户信息
    - 定期刷新数据
    - 通过事件总线发布更新
    """

    def __init__(self, ib: IBGateway):
        """
        初始化持仓服务

        Args:
            ib: IB Gateway 实例
        """
        self.ib = ib
        self.positions: Dict[str, Position] = {}
        self.account = AccountSummary()
        self.event_bus = get_event_bus()

        self.event_bus.subscribe(PositionUpdatedEvent, self._on_position)
        self.event_bus.subscribe(AccountUpdatedEvent, self._on_account_summary)

    def refresh(self):
        """
        刷新持仓和账户信息
        触发 IB Gateway 请求最新数据

        Example:
            portfolio_service.refresh()
            positions = portfolio_service.get_positions()
        """
        logger.info("🔄 Refreshing portfolio data...")
        self.ib.request_positions()
        self.ib.request_account_summary()

    def get_positions(self) -> List[Position]:
        """
        获取所有持仓列表

        Returns:
            List[Position]: 持仓列表
        """
        return list(self.positions.values())

    def get_position(self, symbol: str) -> Position:
        """
        获取指定股票的持仓

        Args:
            symbol: 股票代码

        Returns:
            Position: 持仓信息

        Raises:
            KeyError: 如果没有该持仓
        """
        if symbol not in self.positions:
            raise KeyError(f"No position found for {symbol}")
        return self.positions[symbol]

    def get_account(self) -> AccountSummary:
        """
        获取账户摘要

        Returns:
            AccountSummary: 账户摘要信息
        """
        logger.info(f"get_account - {self.account}")
        return self.account

    def has_position(self, symbol: str) -> bool:
        """
        检查是否持有某个股票

        Args:
            symbol: 股票代码

        Returns:
            bool: 是否持有
        """
        return symbol in self.positions and self.positions[symbol].quantity != 0

    def get_total_market_value(self) -> float:
        """
        计算所有持仓的总市值

        Returns:
            float: 总市值（简化计算：数量 × 成本价）
        """
        total = 0.0
        for position in self.positions.values():
            total += float(position.quantity) * float(position.average_cost)
        return total

    # ===== 事件回调 =====

    def _on_position(self, event):
        """
        处理持仓更新事件

        Args:
            event: 持仓事件数据
                - symbol: 股票代码
                - position: 持仓数量
                - avgCost: 平均成本
        """
        symbol = event.symbol
        quantity = event.position
        average_cost = event.average_cost

        # 确保转换为 float
        try:
            quantity = float(quantity)
            average_cost = float(average_cost)
        except (ValueError, TypeError):
            return

        if quantity != 0:
            # 更新或创建持仓记录
            self.positions[symbol] = Position(
                symbol=symbol,
                quantity=quantity,
                average_cost=average_cost,
            )
            logger.info(
                f"_on_position - 📊 Position updated: {symbol} - {quantity} @ ${average_cost:.2f}"
            )
        else:
            # 持仓为 0，删除记录
            if symbol in self.positions:
                del self.positions[symbol]
                logger.info(f"_on_position - 📊 Position closed: {symbol}")

    def _on_account_summary(self, event: AccountUpdatedEvent):
        """
        处理账户摘要更新事件

        Args:
            event: 账户事件数据
                - tag: 账户字段名称（如 'NetLiquidation', 'AccountType'）
                - value: 字段值（字符串格式）
                - currency: 货币
        """
        logger.debug(f"received event: {event}")
        tag = event.tag
        value = event.value  # 保持字符串格式

        # 使用 AccountSummary 的 update_from_tag 方法处理类型转换
        self.account.update_from_tag(tag, value)

        # 记录重要的账户更新
        if tag == "NetLiquidation":
            logger.info(
                f"_on_account_summary - 💰 Account Net Liquidation: ${float(value):,.2f}"
            )
        elif tag == "TotalCashValue":
            logger.info(f"_on_account_summary - 💵 Account Cash: ${float(value):,.2f}")
        elif tag == "BuyingPower":
            logger.info(f"_on_account_summary - 💪 Buying Power: ${float(value):,.2f}")
        elif tag == "AccountType":
            logger.info(f"_on_account_summary - 📋 Account Type: {value}")
