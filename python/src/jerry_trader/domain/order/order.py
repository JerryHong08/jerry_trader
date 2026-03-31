"""Order domain models

Pure domain models for order lifecycle management.
Immutable value objects for orders, mutable state for order tracking.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Literal, Optional


class OrderSide(Enum):
    """Order side"""

    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """Order type"""

    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class OrderStatus(Enum):
    """Order lifecycle status

    State machine:
        PENDING -> SUBMITTED -> [PARTIAL_FILL] -> [FILLED | CANCELLED | REJECTED]
    """

    PENDING = auto()  # Created but not yet sent to exchange
    SUBMITTED = auto()  # Sent to exchange
    PARTIAL_FILL = auto()  # Partially filled
    FILLED = auto()  # Fully filled
    CANCELLED = auto()  # Cancelled by user
    REJECTED = auto()  # Rejected by exchange
    ERROR = auto()  # System error


class TimeInForce(Enum):
    """Time in force"""

    DAY = "day"
    GTC = "gtc"  # Good till cancelled
    IOC = "ioc"  # Immediate or cancel
    FOK = "fok"  # Fill or kill


@dataclass(frozen=True, slots=True)
class Order:
    """Order value object — immutable order request

    Represents an order at a point in time. Order status changes
    are tracked via OrderState, not by mutating this object.
    """

    # Identity
    order_id: str  # System-generated unique ID
    symbol: str

    # Order parameters
    side: Literal["buy", "sell"]
    quantity: int  # Number of shares/contracts
    order_type: Literal["market", "limit", "stop", "stop_limit"]

    # Price (for limit/stop orders)
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None

    # Execution parameters
    time_in_force: Literal["day", "gtc", "ioc", "fok"] = "day"

    # Risk controls
    max_slippage: Optional[float] = None  # Maximum allowed slippage (0.01 = 1%)

    # Metadata
    strategy_id: Optional[str] = None  # Which strategy generated this order
    signal_id: Optional[str] = None  # Link to originating signal

    def __post_init__(self):
        """Validate order parameters"""
        if self.quantity <= 0:
            raise ValueError(f"Quantity must be positive, got {self.quantity}")

        if self.order_type in ("limit", "stop_limit") and self.limit_price is None:
            raise ValueError(f"{self.order_type} order requires limit_price")

        if self.order_type in ("stop", "stop_limit") and self.stop_price is None:
            raise ValueError(f"{self.order_type} order requires stop_price")

        if self.limit_price is not None and self.limit_price <= 0:
            raise ValueError(f"Limit price must be positive, got {self.limit_price}")

        if self.stop_price is not None and self.stop_price <= 0:
            raise ValueError(f"Stop price must be positive, got {self.stop_price}")

        if self.max_slippage is not None and not 0 <= self.max_slippage <= 1:
            raise ValueError(
                f"Max slippage must be between 0 and 1, got {self.max_slippage}"
            )

    @property
    def is_long(self) -> bool:
        """Check if this is a long order (buy)"""
        return self.side == "buy"

    @property
    def is_short(self) -> bool:
        """Check if this is a short order (sell)"""
        return self.side == "sell"

    @property
    def is_market(self) -> bool:
        """Check if market order"""
        return self.order_type == "market"

    @property
    def is_limit(self) -> bool:
        """Check if limit order"""
        return self.order_type == "limit"

    def can_cancel(self) -> bool:
        """Check if order can be cancelled in its current state"""
        # Note: This checks the order type, not the status.
        # For status check, use OrderState.can_cancel()
        return self.order_type != "market"  # Market orders typically can't be cancelled


@dataclass
class Fill:
    """Fill event — immutable record of a partial or complete fill"""

    fill_id: str
    order_id: str
    timestamp_ns: int
    quantity: int
    price: float
    commission: float = 0.0
    exchange: Optional[str] = None

    def __post_init__(self):
        if self.quantity <= 0:
            raise ValueError(f"Fill quantity must be positive, got {self.quantity}")
        if self.price <= 0:
            raise ValueError(f"Fill price must be positive, got {self.price}")
        if self.commission < 0:
            raise ValueError(f"Commission must be non-negative, got {self.commission}")

    @property
    def value(self) -> float:
        """Fill value (quantity * price)"""
        return self.quantity * self.price

    @property
    def cost_basis(self) -> float:
        """Total cost including commission"""
        return self.value + self.commission


@dataclass
class OrderState:
    """Mutable order state tracking

    Tracks the current state of an order as it progresses through
    its lifecycle. This is mutable because it changes frequently.

    Separated from Order (immutable) to distinguish between:
    - Order: What was requested (never changes)
    - OrderState: What happened (changes as fills arrive)
    """

    order_id: str
    status: OrderStatus = OrderStatus.PENDING

    # Fill tracking
    filled_qty: int = 0
    remaining_qty: int = 0
    avg_fill_price: float = 0.0
    fills: list[Fill] = field(default_factory=list)

    # Timing
    created_at_ns: int = 0
    submitted_at_ns: Optional[int] = None
    filled_at_ns: Optional[int] = None
    cancelled_at_ns: Optional[int] = None

    # Error/rejection info
    error_message: Optional[str] = None

    def __post_init__(self):
        if self.filled_qty < 0:
            raise ValueError(f"Filled qty must be non-negative, got {self.filled_qty}")
        if self.remaining_qty < 0:
            raise ValueError(
                f"Remaining qty must be non-negative, got {self.remaining_qty}"
            )
        if self.avg_fill_price < 0:
            raise ValueError(
                f"Avg fill price must be non-negative, got {self.avg_fill_price}"
            )

    @property
    def is_complete(self) -> bool:
        """Check if order is in a terminal state"""
        return self.status in (
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.ERROR,
        )

    @property
    def is_active(self) -> bool:
        """Check if order is still active on exchange"""
        return self.status in (OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILL)

    def can_cancel(self) -> bool:
        """Check if order can be cancelled in its current state"""
        return self.status in (
            OrderStatus.PENDING,
            OrderStatus.SUBMITTED,
            OrderStatus.PARTIAL_FILL,
        )

    def apply_fill(self, fill: Fill, total_qty: int) -> None:
        """Apply a fill event to update state

        Args:
            fill: The fill event
            total_qty: Original order quantity (from Order object)
        """
        self.fills.append(fill)

        # Update filled quantity
        old_filled = self.filled_qty
        self.filled_qty += fill.quantity

        # Update average fill price (volume-weighted)
        if self.filled_qty > 0:
            old_value = self.avg_fill_price * old_filled
            new_value = fill.price * fill.quantity
            self.avg_fill_price = (old_value + new_value) / self.filled_qty

        # Update remaining
        self.remaining_qty = total_qty - self.filled_qty

        # Update status
        if self.remaining_qty <= 0:
            self.status = OrderStatus.FILLED
            self.filled_at_ns = fill.timestamp_ns
        else:
            self.status = OrderStatus.PARTIAL_FILL

    def mark_submitted(self, timestamp_ns: int) -> None:
        """Mark order as submitted to exchange"""
        if self.status == OrderStatus.PENDING:
            self.status = OrderStatus.SUBMITTED
            self.submitted_at_ns = timestamp_ns

    def mark_cancelled(self, timestamp_ns: int) -> None:
        """Mark order as cancelled"""
        if self.can_cancel():
            self.status = OrderStatus.CANCELLED
            self.cancelled_at_ns = timestamp_ns

    def mark_rejected(self, reason: str, timestamp_ns: int) -> None:
        """Mark order as rejected"""
        self.status = OrderStatus.REJECTED
        self.error_message = reason
        self.cancelled_at_ns = timestamp_ns  # Use cancelled_at for rejection time

    @property
    def total_commission(self) -> float:
        """Total commission paid"""
        return sum(f.commission for f in self.fills)

    @property
    def total_value(self) -> float:
        """Total fill value (qty * price)"""
        return sum(f.value for f in self.fills)
