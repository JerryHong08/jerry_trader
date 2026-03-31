"""Domain layer — pure business logic, no I/O.

This module contains immutable value objects and domain logic.
No dependencies on infrastructure (Redis, ClickHouse, etc.).
"""

# Market data
from jerry_trader.domain.market import (
    Bar,
    BarPeriod,
    Quote,
    Session,
    Tick,
    Timeframe,
    Trade,
)

# Orders
from jerry_trader.domain.order import (
    Fill,
    Order,
    OrderSide,
    OrderState,
    OrderStatus,
    OrderType,
    TimeInForce,
)

# Strategy
from jerry_trader.domain.strategy.signal import (
    RiskLimits,
    RiskState,
    Signal,
    SignalType,
)

__all__ = [
    # Market
    "Bar",
    "BarPeriod",
    "Quote",
    "Tick",
    "Trade",
    "Session",
    "Timeframe",
    # Orders
    "Order",
    "OrderSide",
    "OrderType",
    "OrderStatus",
    "TimeInForce",
    "Fill",
    "OrderState",
    # Strategy
    "Signal",
    "SignalType",
    "RiskLimits",
    "RiskState",
]
