"""Type stubs for the Rust extension module (jerry_trader._rust)."""

from typing import Callable, Optional

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

    def check_expired(self, now_ms: int) -> list[dict]:
        """Complete any bars whose boundary time has passed.

        Call periodically with ``clock.now_ms()`` so bars close at the
        correct wall-time boundary even when no trade arrives.

        Args:
            now_ms: Current time in epoch milliseconds.

        Returns:
            List of completed bar dicts (may be empty).
        """
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

class ReplayClock:
    """Monotonic, drift-free virtual clock for replay mode.

    Maps wall-clock elapsed time to a data-time range anchored at
    ``data_start_ts_ns``.  Supports pause/resume, speed control, and
    arbitrary seek (``jump_to``).

    In live mode the Python ``clock.py`` singleton bypasses this entirely.
    """

    def __init__(self, data_start_ts_ns: int, speed: float = 1.0) -> None:
        """Create a new ReplayClock.

        Args:
            data_start_ts_ns: Market-data epoch nanosecond timestamp that
                corresponds to the replay start.
            speed: Replay speed multiplier (1.0 = real-time).
        """
        ...

    def now_ns(self) -> int:
        """Current replay time as epoch nanoseconds."""
        ...

    def now_ms(self) -> int:
        """Current replay time as epoch milliseconds."""
        ...

    def elapsed_ns(self) -> int:
        """Effective wall-clock nanoseconds elapsed (speed-adjusted)."""
        ...

    def set_speed(self, speed: float) -> None:
        """Change replay speed (re-anchors current position)."""
        ...

    @property
    def speed(self) -> float:
        """Current speed multiplier."""
        ...

    def pause(self) -> None:
        """Freeze the clock."""
        ...

    def resume(self) -> None:
        """Resume from where it was frozen."""
        ...

    @property
    def is_paused(self) -> bool:
        """Whether the clock is currently paused."""
        ...

    def jump_to(self, target_ts_ns: int) -> None:
        """Seek to an arbitrary point in market-data time."""
        ...

    @property
    def data_start_ts_ns(self) -> int:
        """The current data-start anchor (epoch ns)."""
        ...

    def __repr__(self) -> str: ...

class TickDataReplayer:
    """Tick-level data replayer, embedded in the Python process.

    Reads Parquet files from the data lake, replays quotes and trades
    at the correct pace using a virtual timeline, and delivers payloads
    to a Python callback.

    Example::

        replayer = TickDataReplayer(
            replay_date="20251113",
            lake_data_dir="/mnt/data/lake",
            data_start_ts_ns=clock.data_start_ts_ns,
        )
        replayer.subscribe("AAPL", ["Q", "T"], on_tick)
    """

    def __init__(
        self,
        replay_date: str,
        lake_data_dir: str,
        data_start_ts_ns: int,
        speed: float = 1.0,
        start_time: Optional[str] = None,
        max_gap_ms: Optional[int] = None,
    ) -> None:
        """Create a new replayer.

        Args:
            replay_date: Date in YYYYMMDD format.
            lake_data_dir: Path to the data-lake root.
            data_start_ts_ns: Epoch-ns anchor for the virtual clock.
            speed: Replay speed multiplier (1.0 = real-time).
            start_time: Optional ``"HH:MM"`` or ``"HH:MM:SS"`` (ET).
            max_gap_ms: Threshold for logging large time gaps (ms).
        """
        ...

    def subscribe(
        self,
        symbol: str,
        events: list[str],
        callback: Callable[[str, dict], None],
    ) -> None:
        """Subscribe a symbol for replay.

        Blocks until Parquet data is loaded; replay starts immediately.

        Args:
            symbol: Ticker symbol (e.g. ``"AAPL"``).
            events: ``["Q"]``, ``["T"]``, or ``["Q", "T"]``.
            callback: ``fn(symbol: str, payload: dict) -> None``
        """
        ...

    def unsubscribe(self, symbol: str) -> None:
        """Unsubscribe a symbol (stops its replay tasks)."""
        ...

    def batch_preload(
        self,
        symbols: list[str],
        events: list[str],
    ) -> None:
        """Batch-preload Parquet data for multiple symbols at once.

        Each data-type file is scanned only once (with ``is_in`` filter)
        instead of once per symbol.  Subsequent :meth:`subscribe` calls
        for these symbols will skip I/O entirely.

        Args:
            symbols: List of tickers, e.g. ``["AAPL", "MSFT"]``.
            events: ``["Q"]``, ``["T"]``, or ``["Q", "T"]``.
        """
        ...

    def set_speed(self, speed: float) -> None:
        """Change replay speed (re-anchors the timeline)."""
        ...

    def pause(self) -> None:
        """Pause playback."""
        ...

    def resume(self) -> None:
        """Resume playback."""
        ...

    def jump_to(self, target_ts_ns: int) -> None:
        """Jump to a specific data timestamp (epoch ns)."""
        ...

    def now_ns(self) -> int:
        """Current virtual time as epoch nanoseconds."""
        ...

    def now_ms(self) -> int:
        """Current virtual time as epoch milliseconds."""
        ...

    @property
    def is_paused(self) -> bool:
        """Whether playback is paused."""
        ...

    @property
    def speed(self) -> float:
        """Current speed multiplier."""
        ...

    @property
    def data_start_ts_ns(self) -> int:
        """Current data-start anchor (epoch ns)."""
        ...

    def get_stats(self) -> dict:
        """Get replay statistics as a dict."""
        ...

    def shutdown(self) -> None:
        """Shut down the engine thread."""
        ...

    def __repr__(self) -> str: ...
