"""Type stubs for the Rust extension module (jerry_trader._rust)."""

from typing import Optional

def z_score(value: float, history: list[float]) -> Optional[float]:
    """Return z-score of value relative to history, or None if < 2 samples.

    z = (value - mean) / std  (population std)
    """
    ...

def price_accel(
    recent: list[tuple[int, float]], older: list[tuple[int, float]]
) -> float:
    """Direction-aware price acceleration.

    Returns rate_recent - rate_older where rate is fractional return per second.
    """
    ...

class BarBuilder:
    """High-performance tick-to-OHLCV bar builder.

    Maintains per-ticker, per-timeframe rolling bar state with
    session-aware boundaries (premarket / regular / afterhours).
    """

    def __init__(self, timeframes: Optional[list[str]] = None) -> None:
        """Create a new BarBuilder.

        Args:
            timeframes: List of timeframe labels (e.g. ["1m", "5m"]).
                        If None, all 9 timeframes are used:
                        10s, 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w.
        """
        ...

    def ingest_trade(
        self,
        ticker: str,
        price: float,
        size: float,
        timestamp_ms: int,
    ) -> list[dict]:
        """Ingest a single trade and return any completed bars.

        Args:
            ticker: Symbol (e.g. "AAPL")
            price: Trade price
            size: Trade size (shares)
            timestamp_ms: Epoch ms (ET-aligned)

        Returns:
            List of completed bar dicts with keys:
            ticker, timeframe, open, high, low, close, volume,
            trade_count, vwap, bar_start, bar_end, session.
        """
        ...

    def get_current_bar(
        self,
        ticker: str,
        timeframe: str,
    ) -> Optional[dict]:
        """Get the current partial bar, or None if no bar in progress."""
        ...

    def flush(self) -> list[dict]:
        """Force-complete all open bars and return them."""
        ...

    def active_tickers(self) -> list[str]:
        """Get all tickers currently tracked."""
        ...

    def remove_ticker(self, ticker: str) -> list[dict]:
        """Remove a ticker, flushing its bars."""
        ...

    def ticker_count(self) -> int:
        """Number of tickers currently tracked."""
        ...

    def __repr__(self) -> str: ...
