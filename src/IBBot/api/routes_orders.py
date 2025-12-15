"""
Orders Routes - 订单相关的 API 路由
"""

from dataclasses import dataclass
from typing import List, Optional

from fastapi import APIRouter, Body, HTTPException

from IBBot.models.order_models import OrderRequest

# 创建路由器
router = APIRouter(tags=["Orders"])


# 全局变量 - 会在 server.py 中初始化
order_service = None


def init_order_service(service):
    """初始化订单服务（由 server.py 调用）"""
    global order_service
    order_service = service


@router.post("/place")
def place_order(req: OrderRequest):
    """
    下单

    Args:
        req: 订单请求
            - symbol: 股票代码
            - action: BUY/SELL
            - quantity: 数量
            - order_type: MKT/LMT
            - limit_price: 限价（限价单必填）

    Returns:
        {"order_id": int, "status": "ok"}

    Example:
        POST /orders/place
        {
            "symbol": "AAPL",
            "action": "BUY",
            "quantity": 100,
            "order_type": "MKT",
            "OutsideRth": True
        }
    """
    try:
        order_id = order_service.place_order(req)
        return {
            "status": "ok",
            "order_id": order_id,
            "message": f"Order placed: {req.action} {req.quantity} {req.symbol}",
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/cancel/{order_id}")
def cancel_order(
    order_id: int,
    payload: Optional["CancelOrderRequest"] = Body(default=None),
    reason: Optional[str] = None,
):
    """
    取消订单

    Args:
        order_id: 订单 ID

    Returns:
        {"status": "ok", "order_id": int}

    Example:
        POST /orders/cancel/123
    """
    try:
        body_reason = payload.reason if payload is not None else None
        final_reason = (body_reason or reason or "").strip() or None
        order_service.cancel_order(order_id, reason=final_reason)
        return {
            "status": "ok",
            "order_id": order_id,
            "message": f"Order {order_id} cancelled",
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@dataclass
class CancelOrderRequest:
    reason: Optional[str] = None


@router.get("/list")
def list_orders():
    """
    获取所有订单列表

    Returns:
        List[OrderState]: 订单状态列表

    Example:
        GET /orders/list
    """
    try:
        orders = order_service.list_orders()
        return [order.to_dict() for order in orders]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/active")
def list_active_orders():
    """
    获取所有活跃订单（未完成的订单）

    Returns:
        List[OrderState]: 活跃订单列表

    Example:
        GET /orders/active
    """
    try:
        orders = order_service.get_active_orders()
        return [order.to_dict() for order in orders]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{order_id}")
def get_order(order_id: int):
    """
    获取单个订单详情

    Args:
        order_id: 订单 ID

    Returns:
        OrderState: 订单状态

    Example:
        GET /orders/123
    """
    try:
        order = order_service.get_order(order_id)
        return order.to_dict()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
