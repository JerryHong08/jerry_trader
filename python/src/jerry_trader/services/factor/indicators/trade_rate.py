"""Trade rate indicator."""

from collections import deque

from jerry_trader._rust import trade_rate as rust_trade_rate
from jerry_trader.services.factor.indicators.base import TickIndicator


class TradeRate(TickIndicator):
    """Trade rate indicator (trades per second).

    Counts trades within a rolling time window and computes rate.
    Uses Rust for efficient window counting.
    """

    name = "trade_rate"

    def __init__(
        self,
        window_ms: int = 20_000,
        min_trades: int = 5,
        max_len: int = 50_000,
    ):
        """Initialize TradeRate indicator.

        Args:
            window_ms: Time window in milliseconds (default: 20s)
            min_trades: Minimum trades required to compute rate (default: 5)
            max_len: Maximum trade timestamps to keep (default: 50K)
        """
        self.window_ms = window_ms
        self.min_trades = min_trades
        self.max_len = max_len

        self._timestamps: deque[int] = deque(maxlen=max_len)
        self._last_value: float | None = None

    def on_tick(self, ts_ms: int, price: float, size: int) -> None:
        """Ingest a trade tick.

        Args:
            ts_ms: Trade timestamp in milliseconds
            price: Trade price (unused for trade_rate)
            size: Trade size (unused for trade_rate)
        """
        self._timestamps.append(ts_ms)

    def compute(self, ts_ms: int) -> float | None:
        """Compute trade rate at given timestamp.

        Args:
            ts_ms: Current timestamp in milliseconds

        Returns:
            Trades per second if enough data, None otherwise
        """
        if len(self._timestamps) < self.min_trades:
            return None

        # Pass timestamps to Rust for efficient counting
        timestamps = list(self._timestamps)
        result = rust_trade_rate(
            timestamps,
            ts_ms,
            self.window_ms,
            self.min_trades,
        )

        if result is not None:
            self._last_value = result

        return result

    def reset(self) -> None:
        """Reset indicator state."""
        self._timestamps.clear()
        self._last_value = None

    @property
    def ready(self) -> bool:
        """Whether indicator has enough data."""
        return len(self._timestamps) >= self.min_trades

    @property
    def value(self) -> float | None:
        """Last computed trade rate value."""
        return self._last_value

    def prune(self, cutoff_ms: int) -> None:
        """Remove timestamps older than cutoff to save memory.

        Args:
            cutoff_ms: Remove timestamps before this time
        """
        while self._timestamps and self._timestamps[0] < cutoff_ms:
            self._timestamps.popleft()
