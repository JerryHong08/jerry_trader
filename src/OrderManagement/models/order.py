"""Order construction helpers.

Keep "data models" (OrderRequest / OrderState) in `OrderManagement.models.order_models`.
This module focuses on converting an OrderRequest into an IBKR-native
`ibapi.order.Order`.
"""

from __future__ import annotations

from ibapi.order import Order

from OrderManagement.models.order_models import OrderRequest

BUY = "BUY"
SELL = "SELL"


def market(
    action: str, quantity: int, OutsideRth: bool = False, tif: str = "DAY"
) -> Order:
    """Create an IBKR market order."""
    order = Order()
    order.action = action
    order.orderType = "MKT"
    order.totalQuantity = quantity
    order.outsideRth = OutsideRth
    order.tif = tif
    return order


def limit(
    action: str,
    quantity: int,
    limit_price: float,
    OutsideRth: bool = False,
    tif: str = "DAY",
) -> Order:
    """Create an IBKR limit order."""
    order = Order()
    order.action = action
    order.orderType = "LMT"
    order.totalQuantity = quantity
    order.lmtPrice = limit_price
    order.outsideRth = OutsideRth
    order.tif = tif
    return order


def stop(
    action: str,
    quantity: int,
    stop_price: float,
    OutsideRth: bool = False,
    tif: str = "DAY",
) -> Order:
    """Create an IBKR stop order."""
    order = Order()
    order.action = action
    order.orderType = "STP"
    order.auxPrice = stop_price
    order.totalQuantity = quantity
    order.outsideRth = OutsideRth
    order.tif = tif
    return order


def stop_limit(
    action: str,
    quantity: int,
    stop_price: float,
    limit_price: float,
    OutsideRth: bool = False,
    tif: str = "DAY",
) -> Order:
    """Create an IBKR stop-limit order."""
    order = Order()
    order.action = action
    order.orderType = "STP LMT"
    order.auxPrice = stop_price
    order.lmtPrice = limit_price
    order.totalQuantity = quantity
    order.outsideRth = OutsideRth
    order.tif = tif
    return order


def from_request(req: OrderRequest) -> Order:
    """Convert an OrderRequest into an IBKR Order."""
    # Ensure common validations (action/qty/limit_price etc.) are always applied
    req.validate()
    if req.order_type == "MKT":
        return market(req.action, req.quantity, req.OutsideRth, req.tif)
    if req.order_type == "LMT":
        if req.limit_price is None:
            raise ValueError("Limit price required for limit orders")
        return limit(req.action, req.quantity, req.limit_price, req.OutsideRth, req.tif)
    if req.order_type == "STP":
        raise ValueError("Unsupported order type: STP (not yet wired to OrderRequest)")
    if req.order_type in {"STP LMT", "STP_LMT"}:
        raise ValueError(
            "Unsupported order type: STP LMT (not yet wired to OrderRequest)"
        )
    raise ValueError(f"Unsupported order type: {req.order_type}")


__all__ = [
    "BUY",
    "SELL",
    "market",
    "limit",
    "stop",
    "stop_limit",
    "from_request",
]
