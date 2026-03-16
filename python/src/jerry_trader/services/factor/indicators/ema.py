"""Exponential Moving Average indicator."""

from jerry_trader.domain.market import Bar
from jerry_trader.services.factor.indicators.base import BarIndicator


class EMA(BarIndicator):
    """Exponential Moving Average (EMA) indicator.

    Computed incrementally on each bar close:
        EMA = α * close + (1 - α) * prev_EMA
        where α = 2 / (period + 1)
    """

    def __init__(self, period: int = 20):
        """Initialize EMA indicator.

        Args:
            period: EMA period (default: 20)
        """
        self.period = period
        self.alpha = 2.0 / (period + 1)
        self.name = f"ema_{period}"

        self._value: float | None = None
        self._count: int = 0

    def update(self, bar: Bar) -> float | None:
        """Update EMA with new bar.

        Args:
            bar: Completed bar

        Returns:
            EMA value if ready (count >= period), None otherwise
        """
        if self._value is None:
            self._value = bar.close
        else:
            self._value = self.alpha * bar.close + (1 - self.alpha) * self._value

        self._count += 1

        return self._value if self._count >= self.period else None

    def reset(self) -> None:
        """Reset indicator state."""
        self._value = None
        self._count = 0

    @property
    def ready(self) -> bool:
        """Whether indicator has enough data."""
        return self._count >= self.period

    @property
    def value(self) -> float | None:
        """Current EMA value."""
        return self._value if self.ready else None
