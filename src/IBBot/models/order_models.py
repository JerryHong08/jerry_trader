"""
Order Models - 订单相关数据模型
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class OrderRequest:
    """
    订单请求 - 用于创建新订单

    Examples:
        # 市价买入
        req = OrderRequest(symbol="AAPL", action="BUY", quantity=100)

        # 限价卖出
        req = OrderRequest(
            symbol="TSLA",
            action="SELL",
            quantity=50,
            order_type="LMT",
            limit_price=250.00
        )
    """

    symbol: str  # 股票代码
    sec_type: str = "STK"  # 证券类型（STK=股票, OPT=期权, FUT=期货）
    action: str = "BUY"  # 操作（BUY/SELL）
    quantity: int = 1  # 数量
    order_type: str = "MKT"  # 订单类型（MKT=市价, LMT=限价）
    limit_price: Optional[float] = None  # 限价价格（限价单必填）
    tif: str = "DAY"  # 有效期（DAY=当日有效, GTC=撤销前有效）
    OutsideRth: bool = True  # Outside of the regular hour

    def validate(self):
        """验证订单参数"""
        if self.order_type == "LMT" and self.limit_price is None:
            raise ValueError("Limit orders require limit_price")

        if self.action not in ["BUY", "SELL"]:
            raise ValueError(f"Invalid action: {self.action}")

        if self.quantity <= 0:
            raise ValueError(f"Invalid quantity: {self.quantity}")


@dataclass
class OrderState:
    """
    订单状态 - 追踪订单的完整生命周期

    状态说明：
        - PreSubmitted: 预提交
        - Submitted: 已提交
        - Filled: 完全成交
        - Cancelled: 已取消
        - PendingCancel: 取消中
    """

    order_id: int  # 订单 ID
    status: str  # 订单状态
    filled: int  # 已成交数量
    remaining: int  # 剩余数量
    avg_fill_price: float  # 平均成交价
    request: OrderRequest  # 原始订单请求
    commission: Optional[float] = None  # 佣金费用

    def to_dict(self):
        """转换为字典格式"""
        result = {
            "order_id": self.order_id,
            "status": self.status,
            "filled": self.filled,
            "remaining": self.remaining,
            "avg_fill_price": self.avg_fill_price,
            "symbol": self.request.symbol,
            "action": self.request.action,
            "quantity": self.request.quantity,
            "order_type": self.request.order_type,
            "OutsideRth": self.request.OutsideRth,
        }
        if self.commission is not None:
            result["commission"] = self.commission
        return result

    @property
    def is_active(self):
        """判断订单是否仍在活动状态"""
        active_statuses = ["PreSubmitted", "Submitted", "PendingCancel"]
        return self.status in active_statuses

    @property
    def is_filled(self):
        """判断订单是否完全成交"""
        return self.status == "Filled"

    @property
    def is_cancelled(self):
        """判断订单是否已取消"""
        return self.status == "Cancelled"
