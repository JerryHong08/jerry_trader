"""Tick Domain Models

Pure domain models for tick data (trades and quotes).
Immutable value objects with no I/O dependencies.
"""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class Tick:
    """Immutable tick data value object

    Represents a single market data event (trade or quote).
    """

    symbol: str
    timestamp_ns: int  # Nanosecond precision timestamp
    price: float
    size: int
    exchange: str
    conditions: tuple[str, ...]  # Immutable tuple of condition codes

    def __post_init__(self):
        """Validate tick data"""
        if self.price <= 0:
            raise ValueError(f"Price must be positive, got {self.price}")
        if self.size < 0:
            raise ValueError(f"Size must be non-negative, got {self.size}")


@dataclass(frozen=True)
class Trade(Tick):
    """Trade tick (subset of Tick)

    Represents an executed trade.
    """

    pass


@dataclass(frozen=True)
class Quote:
    """Quote tick (bid/ask)

    Represents a market quote with bid and ask prices.
    """

    symbol: str
    timestamp_ns: int
    bid_price: float
    bid_size: int
    ask_price: float
    ask_size: int
    exchange: str
    conditions: tuple[str, ...] = ()

    def __post_init__(self):
        """Validate quote data"""
        if self.bid_price < 0:
            raise ValueError(f"Bid price must be non-negative, got {self.bid_price}")
        if self.ask_price < 0:
            raise ValueError(f"Ask price must be non-negative, got {self.ask_price}")
        if self.bid_size < 0:
            raise ValueError(f"Bid size must be non-negative, got {self.bid_size}")
        if self.ask_size < 0:
            raise ValueError(f"Ask size must be non-negative, got {self.ask_size}")
        if (
            self.bid_price > 0
            and self.ask_price > 0
            and self.bid_price > self.ask_price
        ):
            raise ValueError(
                f"Bid price ({self.bid_price}) cannot exceed ask price ({self.ask_price})"
            )

    @property
    def spread(self) -> float:
        """Calculate bid-ask spread"""
        return self.ask_price - self.bid_price

    @property
    def mid_price(self) -> float:
        """Calculate mid price"""
        return (self.bid_price + self.ask_price) / 2.0
