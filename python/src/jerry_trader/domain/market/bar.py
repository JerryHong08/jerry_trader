"""Bar Domain Models

Pure domain models for OHLCV bar data.
Immutable value objects with no I/O dependencies.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Bar:
    """OHLCV bar value object

    Represents aggregated price data over a time period.
    Immutable to ensure data integrity.
    """

    symbol: str
    timestamp_ns: int  # Bar start time in nanoseconds
    timespan: str  # "10s", "1m", "5m", "1h", "1d", etc.
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float] = None  # Volume-weighted average price
    trade_count: Optional[int] = None  # Number of trades in bar

    def __post_init__(self):
        """Validate bar data"""
        if self.open <= 0 or self.high <= 0 or self.low <= 0 or self.close <= 0:
            raise ValueError("OHLC prices must be positive")

        if self.high < self.low:
            raise ValueError(f"High ({self.high}) cannot be less than low ({self.low})")

        if self.high < self.open or self.high < self.close:
            raise ValueError(
                f"High ({self.high}) must be >= open ({self.open}) and close ({self.close})"
            )

        if self.low > self.open or self.low > self.close:
            raise ValueError(
                f"Low ({self.low}) must be <= open ({self.open}) and close ({self.close})"
            )

        if self.volume < 0:
            raise ValueError(f"Volume must be non-negative, got {self.volume}")

        if self.vwap is not None and self.vwap <= 0:
            raise ValueError(f"VWAP must be positive, got {self.vwap}")

        if self.trade_count is not None and self.trade_count < 0:
            raise ValueError(
                f"Trade count must be non-negative, got {self.trade_count}"
            )

    @property
    def range(self) -> float:
        """Calculate bar range (high - low)"""
        return self.high - self.low

    @property
    def body(self) -> float:
        """Calculate bar body (abs(close - open))"""
        return abs(self.close - self.open)

    @property
    def is_bullish(self) -> bool:
        """Check if bar is bullish (close > open)"""
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        """Check if bar is bearish (close < open)"""
        return self.close < self.open

    @property
    def is_doji(self) -> bool:
        """Check if bar is doji (close ≈ open)"""
        return abs(self.close - self.open) < (self.range * 0.1)
