"""
事件模型定义 - 所有类型安全的事件类
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

# ============================================================
# 基础事件类
# ============================================================


@dataclass
class BaseEvent:
    """所有事件的基类"""

    def __post_init__(self):
        # 子类会处理 timestamp
        pass


# ============================================================
# 订单事件
# ============================================================


@dataclass
class OrderSubmittedEvent(BaseEvent):
    """订单提交事件 - 用户/策略提交订单到 OrderManager"""

    order_id: int
    symbol: str
    action: str  # BUY/SELL
    quantity: int
    order_type: str = "MKT"  # MKT/LMT
    limit_price: Optional[float] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class OrderPlacedEvent(BaseEvent):
    """订单已下达事件 - 订单已发送到 IB Gateway"""

    order_id: int
    symbol: str
    action: str
    quantity: int
    outsideRth: bool
    order_type: str = "MKT"
    limit_price: Optional[float] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class OrderStatusEvent(BaseEvent):
    """订单状态更新事件 - 来自 IB Gateway 的回报"""

    order_id: int
    status: str  # PreSubmitted/Submitted/Filled/Cancelled
    filled: int
    remaining: int
    avg_fill_price: float = 0.0
    commission: Optional[float] = None
    symbol: Optional[str] = None
    action: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class OrderCancelledEvent(BaseEvent):
    """订单取消事件"""

    order_id: Optional[int] = None
    symbol: Optional[str] = None
    reason: Optional[str] = None
    all: bool = False  # True if cancelling all orders
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


# ============================================================
# 成交回报事件
# ============================================================


@dataclass
class ExecutionReceivedEvent(BaseEvent):
    """成交回报事件 - 来自 execDetails"""

    exec_id: str
    order_id: int
    symbol: str
    side: str  # BOT/SLD
    shares: int
    price: float
    exchange: str
    time: str
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


# ============================================================
# 持仓和账户事件
# ============================================================


@dataclass
class PositionUpdatedEvent(BaseEvent):
    """持仓更新事件"""

    symbol: str
    position: int
    average_cost: float
    market_price: float
    market_value: float
    unrealized_pnl: float
    realized_pnl: float
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class AccountUpdatedEvent(BaseEvent):
    """账户信息更新事件"""

    tag: str  # NetLiquidation/TotalCashValue/等
    value: Any
    currency: str
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


# ============================================================
# 系统事件
# ============================================================


@dataclass
class ConnectionEvent(BaseEvent):
    """连接状态事件"""

    status: str  # connected/disconnected
    host: Optional[str] = None
    port: Optional[int] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class ErrorEvent(BaseEvent):
    """错误事件"""

    error_message: str
    error_code: Optional[int] = None
    source: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
