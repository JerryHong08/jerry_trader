"""
Risk Management API Routes

Provides REST endpoints for querying real-time PnL data computed by the
C++ RiskEngine.

Routes:
    GET  /risk/pnl            — Full PnL snapshot (all positions)
    GET  /risk/pnl/{symbol}   — PnL for a single symbol
    GET  /risk/positions      — List all tracked positions
"""

from typing import Optional

from fastapi import APIRouter, HTTPException

router = APIRouter(tags=["Risk"])

# Injected by server.py at startup
risk_manager = None


def init_risk_manager(manager) -> None:
    """Initialize the risk manager reference (called from server.py)."""
    global risk_manager
    risk_manager = manager


@router.get("/pnl")
def get_pnl_snapshot():
    """
    Return a full PnL snapshot for all open positions.

    Response:
        {
          "timestamp": "2025-01-01T12:00:00Z",
          "total_unrealized_pnl": float,
          "total_realized_pnl": float,
          "positions": [
            {
              "symbol": "AAPL",
              "quantity": 100,
              "avg_fill_price": 150.0,
              "current_price": 155.0,
              "unrealized_pnl": 500.0,
              "realized_pnl": 0.0
            },
            ...
          ]
        }
    """
    if risk_manager is None:
        raise HTTPException(status_code=503, detail="RiskManager not initialized")
    return risk_manager.get_pnl_snapshot()


@router.get("/pnl/{symbol}")
def get_symbol_pnl(symbol: str):
    """
    Return PnL data for a single symbol.

    Args:
        symbol: Ticker symbol (case-insensitive)

    Response:
        {
          "symbol": "AAPL",
          "quantity": 100,
          "avg_fill_price": 150.0,
          "current_price": 155.0,
          "unrealized_pnl": 500.0,
          "realized_pnl": 0.0
        }
    """
    if risk_manager is None:
        raise HTTPException(status_code=503, detail="RiskManager not initialized")
    result = risk_manager.get_symbol_pnl(symbol.upper())
    if result is None:
        raise HTTPException(
            status_code=404, detail=f"Symbol {symbol.upper()} not tracked"
        )
    return result


@router.get("/positions")
def get_positions():
    """
    Return a list of all tracked symbols and their positions.

    Response:
        {
          "symbols": ["AAPL", "TSLA"],
          "count": 2
        }
    """
    if risk_manager is None:
        raise HTTPException(status_code=503, detail="RiskManager not initialized")
    symbols = risk_manager.tracked_symbols
    return {"symbols": symbols, "count": len(symbols)}
