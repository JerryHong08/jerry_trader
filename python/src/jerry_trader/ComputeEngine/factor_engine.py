"""Backward-compatibility shim — moved to jerry_trader.core.factors.engine"""

from jerry_trader.core.factors.engine import FactorManager, TickerContext  # noqa: F401

__all__ = ["FactorManager", "TickerContext"]
