"""Bar context for rolling window management."""

from collections import deque
from dataclasses import dataclass, field

from jerry_trader.domain.market import Bar


@dataclass
class BarContext:
    """Manages rolling window of bars for a ticker.

    Maintains a fixed-size deque of completed bars for factor computation.
    Thread-safe for single-writer, multiple-reader scenarios.
    """

    symbol: str
    max_bars: int = 200
    bars: deque[Bar] = field(default_factory=deque)

    def __post_init__(self):
        """Initialize deque with maxlen."""
        if not isinstance(self.bars, deque) or self.bars.maxlen != self.max_bars:
            self.bars = deque(self.bars, maxlen=self.max_bars)

    def add_bar(self, bar: Bar) -> None:
        """Add a completed bar to the context."""
        if bar.symbol != self.symbol:
            raise ValueError(f"Bar symbol {bar.symbol} != context symbol {self.symbol}")
        self.bars.append(bar)

    def get_bars(self, window: int | None = None) -> list[Bar]:
        """Get recent bars (all or last N)."""
        if window is None:
            return list(self.bars)
        return list(self.bars)[-window:]

    def has_enough_bars(self, window: int) -> bool:
        """Check if we have enough bars for computation."""
        return len(self.bars) >= window

    @property
    def count(self) -> int:
        """Number of bars in context."""
        return len(self.bars)

    @property
    def latest_bar(self) -> Bar | None:
        """Get the most recent bar, or None if empty."""
        return self.bars[-1] if self.bars else None

    def get_closes(self) -> list[float]:
        """Extract close prices from all bars."""
        return [b.close for b in self.bars]

    def get_volumes(self) -> list[int]:
        """Extract volumes from all bars."""
        return [int(b.volume) for b in self.bars]

    def get_price_volume_pairs(self) -> list[tuple[float, int]]:
        """Extract (close, volume) pairs for VWAP calculation."""
        return [(b.close, int(b.volume)) for b in self.bars]
