"""Base classes for indicators."""

from abc import ABC, abstractmethod

from jerry_trader.domain.market import Bar


class BarIndicator(ABC):
    """Base class for bar-based indicators.

    Updated on each completed bar. Stateful and incremental.
    """

    name: str

    @abstractmethod
    def update(self, bar: Bar) -> float | None:
        """Update indicator with new bar.

        Args:
            bar: Completed bar

        Returns:
            Indicator value if ready, None if warming up
        """
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset indicator state."""
        pass

    @property
    @abstractmethod
    def ready(self) -> bool:
        """Whether indicator has enough data to produce values."""
        pass


class TickIndicator(ABC):
    """Base class for tick-based indicators.

    Ingests ticks continuously, computes on demand (e.g., every 1s).
    """

    name: str

    @abstractmethod
    def on_tick(self, ts_ms: int, price: float, size: int) -> None:
        """Ingest a trade tick.

        Args:
            ts_ms: Trade timestamp in milliseconds
            price: Trade price
            size: Trade size
        """
        pass

    @abstractmethod
    def compute(self, ts_ms: int) -> float | None:
        """Compute current indicator value.

        Args:
            ts_ms: Current timestamp in milliseconds

        Returns:
            Indicator value if ready, None if insufficient data
        """
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset indicator state."""
        pass

    @property
    @abstractmethod
    def ready(self) -> bool:
        """Whether indicator has enough data to produce values."""
        pass
