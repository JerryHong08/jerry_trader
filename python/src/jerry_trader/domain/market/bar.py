"""Bar Domain Models

Pure domain models for OHLCV bar data.
Immutable value objects with no I/O dependencies.
"""

from dataclasses import dataclass, field
from typing import Literal, Optional

# Timeframe literals matching Rust Timeframe enum
Timeframe = Literal["10s", "1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]
TIMEFRAME_LABELS = ["10s", "1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]

# Session literals matching Rust Session enum
Session = Literal["premarket", "regular", "afterhours", "closed"]


# Duration lookup table (matches Rust)
_TIMEFRAME_DURATIONS: dict[Timeframe, int] = {
    "10s": 10_000,
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
    "1w": 604_800_000,
}


@dataclass(frozen=True, slots=True)
class BarPeriod:
    """Represents a bar's time period (start and end).

    This encapsulates all timeframe boundary calculations that were
    previously scattered across services.
    """

    timeframe: Timeframe
    start_ms: int  # epoch milliseconds

    @property
    def duration_ms(self) -> int:
        """Duration of this timeframe in milliseconds."""
        return _TIMEFRAME_DURATIONS[self.timeframe]

    @property
    def end_ms(self) -> int:
        """End timestamp (exclusive)."""
        return self.start_ms + self.duration_ms

    def contains(self, timestamp_ms: int) -> bool:
        """Check if a timestamp falls within this period."""
        return self.start_ms <= timestamp_ms < self.end_ms

    def is_complete(self, now_ms: int) -> bool:
        """Check if this period has completed (time has passed end)."""
        return now_ms >= self.end_ms

    @classmethod
    def from_timestamp(cls, timestamp_ms: int, timeframe: Timeframe) -> "BarPeriod":
        """Calculate the bar period containing a given timestamp.

        This centralizes the "bar start" calculation that was previously
        duplicated in Rust and Python.
        """
        duration = _TIMEFRAME_DURATIONS[timeframe]
        start_ms = (timestamp_ms // duration) * duration
        return cls(timeframe=timeframe, start_ms=start_ms)

    def __str__(self) -> str:
        return f"{self.timeframe}@{self.start_ms}"


@dataclass(frozen=True, slots=True)
class Bar:
    """OHLCV bar value object — mirrors Rust CompletedBar struct.

    Represents aggregated price data over a time period.
    Immutable to ensure data integrity.

    This is the primary exchange format between Rust BarBuilder and Python services.
    """

    symbol: str
    timeframe: Timeframe
    open: float
    high: float
    low: float
    close: float
    volume: float  # Note: Rust uses f64 for volume
    trade_count: int
    vwap: float
    bar_start: int  # epoch milliseconds (matches Rust)
    bar_end: int  # epoch milliseconds (matches Rust)
    session: Session

    def __post_init__(self):
        """Validate bar data integrity."""
        # Validate OHLC relationships
        if self.high < self.low:
            raise ValueError(f"High ({self.high}) < Low ({self.low})")
        if self.high < max(self.open, self.close):
            raise ValueError(f"High ({self.high}) < max(Open, Close)")
        if self.low > min(self.open, self.close):
            raise ValueError(f"Low ({self.low}) > min(Open, Close)")

        # Validate non-negative values
        if self.volume < 0:
            raise ValueError(f"Volume must be non-negative, got {self.volume}")
        if self.trade_count < 0:
            raise ValueError(
                f"Trade count must be non-negative, got {self.trade_count}"
            )

        # Validate timeframe
        if self.timeframe not in TIMEFRAME_LABELS:
            raise ValueError(f"Invalid timeframe: {self.timeframe}")

    @property
    def period(self) -> BarPeriod:
        """Get the bar period for time-based calculations."""
        return BarPeriod(timeframe=self.timeframe, start_ms=self.bar_start)

    @property
    def range(self) -> float:
        """Calculate bar range (high - low)."""
        return self.high - self.low

    @property
    def body(self) -> float:
        """Calculate bar body (abs(close - open))."""
        return abs(self.close - self.open)

    @property
    def is_bullish(self) -> bool:
        """Check if bar is bullish (close > open)."""
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        """Check if bar is bearish (close < open)."""
        return self.close < self.open

    @property
    def is_doji(self, threshold: float = 0.1) -> bool:
        """Check if bar is doji (close ≈ open within threshold)."""
        if self.range == 0:
            return True
        return self.body < (self.range * threshold)

    def merge(self, other: "Bar") -> "Bar":
        """Merge two bars from the same period (meeting bar merge).

        Used when combining REST-fetched historical bars with WebSocket
        real-time bars that straddle the same time period.

        Preconditions:
            - self.timeframe == other.timeframe
            - self.bar_start == other.bar_start
        """
        if self.timeframe != other.timeframe:
            raise ValueError(
                f"Cannot merge bars with different timeframes: "
                f"{self.timeframe} vs {other.timeframe}"
            )
        if self.bar_start != other.bar_start:
            raise ValueError(
                f"Cannot merge bars from different periods: "
                f"{self.bar_start} vs {other.bar_start}"
            )

        total_volume = self.volume + other.volume

        # VWAP weighted average
        if total_volume > 0:
            vwap = (self.vwap * self.volume + other.vwap * other.volume) / total_volume
        else:
            vwap = 0.0

        return Bar(
            symbol=self.symbol,
            timeframe=self.timeframe,
            open=self.open,  # Earlier bar has correct open
            high=max(self.high, other.high),
            low=min(self.low, other.low),
            close=other.close,  # Later bar has correct close
            volume=total_volume,
            trade_count=self.trade_count + other.trade_count,
            vwap=vwap,
            bar_start=self.bar_start,
            bar_end=self.bar_end,
            session=self.session,
        )

    # ------------------------------------------------------------------
    # Interop with Rust BarBuilder
    # ------------------------------------------------------------------

    @classmethod
    def from_rust_dict(cls, d: dict) -> "Bar":
        """Construct from Rust BarBuilder output (ingest_trade, drain_completed).

        Example:
            bars = bar_builder.drain_completed()  # list[dict] from Rust
            bars = [Bar.from_rust_dict(b) for b in bars]
        """
        return cls(
            symbol=d["ticker"],
            timeframe=d["timeframe"],
            open=d["open"],
            high=d["high"],
            low=d["low"],
            close=d["close"],
            volume=d["volume"],
            trade_count=d["trade_count"],
            vwap=d["vwap"],
            bar_start=d["bar_start"],
            bar_end=d["bar_end"],
            session=d["session"],
        )

    def to_clickhouse_dict(self) -> dict:
        """Convert to dict format expected by ClickHouse ohlcv_writer.

        ClickHouse schema uses slightly different field names.
        """
        return {
            "ticker": self.symbol,
            "timeframe": self.timeframe,
            "bar_start": self.bar_start,
            "bar_end": self.bar_end,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "trade_count": self.trade_count,
            "vwap": self.vwap,
            "session": self.session,
        }

    def to_event_dict(self) -> dict:
        """Convert to dict format for EventBus BarClosed event."""
        return {
            "ticker": self.symbol,
            "timeframe": self.timeframe,
            "bar_start": self.bar_start,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "trade_count": self.trade_count,
            "vwap": self.vwap,
            "session": self.session,
        }
