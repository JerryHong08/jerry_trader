"""Backtest domain models.

Pure value objects for backtest results — no I/O, no service dependencies.
"""

from jerry_trader.domain.backtest.types import BacktestResult, Candidate, SignalResult

__all__ = [
    "Candidate",
    "SignalResult",
    "BacktestResult",
]
