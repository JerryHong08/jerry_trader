"""Backtest domain types — pure value objects, no I/O.

Candidate: a stock that newly entered top N during a session.
SignalResult: a triggered signal with computed returns and metrics.
BacktestResult: aggregate result for a full backtest run.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Candidate:
    """A stock that qualified for backtesting.

    Selected by PreFilter from market_snapshot — represents a stock that
    newly entered the top N movers during a trading session.

    Attributes:
        symbol: Ticker symbol (e.g. "AAPL").
        first_entry_ms: Epoch ms when the stock first entered top N.
        gain_at_entry: changePercent at first_entry_ms.
        price_at_entry: Price at first_entry_ms.
        prev_close: Previous session close price.
        volume_at_entry: Volume at first_entry_ms.
        relative_volume: relativeVolumeDaily at first_entry_ms.
        max_gain: Maximum changePercent across all snapshots (reference only).
    """

    symbol: str
    first_entry_ms: int
    gain_at_entry: float
    price_at_entry: float
    prev_close: float
    volume_at_entry: float
    relative_volume: float
    max_gain: float
    peak_volume: float = 0.0


@dataclass(frozen=True, slots=True)
class SignalResult:
    """A triggered signal with computed returns and metrics.

    Produced by the backtest pipeline after signal evaluation and return
    computation. Immutable to ensure result integrity.

    Attributes:
        rule_id: DSL rule that triggered.
        symbol: Ticker symbol.
        trigger_time_ns: Epoch nanoseconds when signal fired.
        trigger_price: Raw price at trigger time.
        entry_price: Slippage-adjusted entry price (ask * (1 + buffer)).
        slippage_pct: (entry_price - trigger_price) / trigger_price.
        factors: Factor values at trigger time {name: value}.
        ask_price: Ask price at trigger (for slippage model).
        returns: Horizon → return pct, e.g. {"1m": 0.023, "5m": -0.015}.
        mfe: Maximum Favorable Excursion.
        mae: Maximum Adverse Excursion.
        time_to_peak_ms: Epoch ms when max return occurred (None if no peak).
    """

    rule_id: str
    symbol: str
    trigger_time_ns: int
    trigger_price: float
    entry_price: float
    slippage_pct: float
    factors: dict[str, float]
    ask_price: float | None = None
    returns: dict[str, float] = field(default_factory=dict)
    mfe: float | None = None
    mae: float | None = None
    time_to_peak_ms: int | None = None

    def __post_init__(self):
        if self.entry_price <= 0:
            raise ValueError(f"entry_price must be positive, got {self.entry_price}")
        if self.trigger_price <= 0:
            raise ValueError(
                f"trigger_price must be positive, got {self.trigger_price}"
            )

    @property
    def trigger_time_ms(self) -> int:
        """Trigger time in milliseconds."""
        return self.trigger_time_ns // 1_000_000

    @property
    def is_winner(self) -> bool:
        """Check if any positive return exists."""
        return any(v > 0 for v in self.returns.values())


@dataclass(frozen=True, slots=True)
class BacktestResult:
    """Aggregate result for a full backtest run.

    Summarizes all signals and their performance across one or more rules
    for a given date.

    Attributes:
        date: Backtest date (YYYY-MM-DD).
        run_id: Unique run identifier (links to ClickHouse backtest_results).
        rules_tested: Rule IDs that were evaluated.
        total_signals: Total number of signals triggered.
        signals: Individual signal results.
        win_rate: Horizon → fraction of winning trades.
        avg_return: Horizon → average return.
        profit_factor: sum(wins) / sum(abs(losses)). 0.0 if no losses.
        avg_slippage: Average slippage across all signals.
        avg_mfe: Average MFE across all signals.
        avg_mae: Average MAE across all signals.
        avg_time_to_peak_ms: Average time to peak return.
    """

    date: str
    rules_tested: list[str]
    total_signals: int
    signals: list[SignalResult]
    run_id: str | None = None  # Links to ClickHouse backtest_results
    win_rate: dict[str, float] = field(default_factory=dict)
    avg_return: dict[str, float] = field(default_factory=dict)
    profit_factor: float = 0.0
    avg_slippage: float = 0.0
    avg_mfe: float = 0.0
    avg_mae: float = 0.0
    avg_time_to_peak_ms: float = 0.0
