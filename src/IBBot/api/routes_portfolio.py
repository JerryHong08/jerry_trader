"""
Portfolio Routes - 持仓和账户相关的 API 路由
"""

from fastapi import APIRouter, HTTPException

# 创建路由器
router = APIRouter(tags=["Portfolio"])


# 全局变量 - 会在 server.py 中初始化
portfolio_service = None


def init_portfolio_service(service):
    """初始化持仓服务（由 server.py 调用）"""
    global portfolio_service
    portfolio_service = service


@router.get("/")
def get_account():
    """
    获取账户摘要

    Returns:
        AccountSummary: 账户信息
            - net_liquidation: 净清算价值
            - cash: 现金余额
            - buying_power: 购买力

    Example:
        GET /portfolio/
    """
    try:
        account = portfolio_service.get_account()
        return account.to_dict()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/positions")
def get_positions():
    """
    获取所有持仓

    Returns:
        List[Position]: 持仓列表

    Example:
        GET /portfolio/positions
    """
    try:
        positions = portfolio_service.get_positions()
        return [pos.to_dict() for pos in positions]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/positions/{symbol}")
def get_position(symbol: str):
    """
    获取指定股票的持仓

    Args:
        symbol: 股票代码

    Returns:
        Position: 持仓信息

    Example:
        GET /portfolio/positions/AAPL
    """
    try:
        position = portfolio_service.get_position(symbol.upper())
        return position.to_dict()
    except KeyError:
        raise HTTPException(status_code=404, detail=f"No position found for {symbol}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/refresh")
def refresh_portfolio():
    """
    刷新持仓和账户数据
    触发从 IB Gateway 拉取最新数据

    Returns:
        {"status": "ok", "message": str}

    Example:
        POST /portfolio/refresh
    """
    try:
        portfolio_service.refresh()
        return {"status": "ok", "message": "Portfolio refresh requested"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary")
def get_portfolio_summary():
    """
    获取持仓总览

    Returns:
        {
            "account": AccountSummary,
            "positions": List[Position],
            "total_market_value": float,
            "position_count": int
        }

    Example:
        GET /portfolio/summary
    """
    try:
        account = portfolio_service.get_account()
        positions = portfolio_service.get_positions()
        total_value = portfolio_service.get_total_market_value()

        return {
            "account": account.to_dict(),
            "positions": [pos.to_dict() for pos in positions],
            "total_market_value": total_value,
            "position_count": len(positions),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
