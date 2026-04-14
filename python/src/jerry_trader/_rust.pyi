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

def load_trades_from_parquet(
    lake_data_dir: str,
    symbol: str,
    date_yyyymmdd: str,
    end_ts_ms: int = 0,
    start_ts_ms: int = 0,
) -> list[tuple[int, float, int]]:
    """Load trades from parquet for a single symbol/date.

    Tries partitioned file first, falls back to monolithic.
    Returns List[(ts_ms, price, size)] sorted ascending.

    If start_ts_ms > 0, only trades with timestamp >= start_ts_ms are returned.
    If end_ts_ms > 0, only trades with timestamp < end_ts_ms are returned.
    Both filters use predicate pushdown at scan time.
    Raises RuntimeError if no parquet file is found.
    """
    ...

def load_quotes_from_parquet(
    lake_data_dir: str,
    symbol: str,
    date_yyyymmdd: str,
    end_ts_ms: int = 0,
    start_ts_ms: int = 0,
) -> list[tuple[int, float, float, int, int]]:
    """Load quotes from parquet for a single symbol/date.

    Tries partitioned file first, falls back to monolithic.
    Returns List[(ts_ms, bid, ask, bid_size, ask_size)] sorted ascending.

    If start_ts_ms > 0, only quotes with timestamp >= start_ts_ms are returned.
    If end_ts_ms > 0, only quotes with timestamp < end_ts_ms are returned.
    Both filters use predicate pushdown at scan time.
    Raises RuntimeError if no parquet file is found.
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

    def ingest_trades_batch(
        self,
        ticker: str,
        trades: list[tuple[int, float, float]],
    ) -> list[dict]:
        """Bulk-ingest trades for a single ticker. Returns all completed bars.

        Much faster than calling ingest_trade() N times because the FFI
        boundary is crossed only once.

        Args:
            ticker: Symbol (e.g. "AAPL")
            trades: List of (timestamp_ms, price, size) tuples, sorted
                    ascending by timestamp.

        Returns:
            List of completed bar dicts.
        """
        ...

    def get_current_bar(
        self,
        ticker: str,
        timeframe: str,
    ) -> Optional[dict]:
        """Get the current partial bar, or None if no bar in progress."""
        ...

    def configure_watermark(
        self, late_arrival_ms: int = 200, idle_close_ms: int = 2000
    ) -> None:
        """Configure late-arrival hold and idle close windows in milliseconds."""
        ...

    def advance(self, now_ms: int) -> list[dict]:
        """Advance builder time and close bars that have crossed watermark.

        Call periodically with ``clock.now_ms()`` so bars close at the
        correct boundary even when no trade arrives.

        Args:
            now_ms: Current time in epoch milliseconds.

        Returns:
            List of completed bar dicts (may be empty).
        """
        ...

    def drain_completed(self) -> list[dict]:
        """Drain completed bars currently queued inside Rust."""
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

class SharedTimeHandle:
    """A handle for reading the ReplayClock time without GIL contention.

    Created by `ReplayClock.get_shared_time()` and passed to other
    Rust components (e.g., TickDataReplayer) that need to read the clock.
    """

    def now_ns(self) -> int:
        """Read the current clock time (epoch nanoseconds)."""
        ...

    def now_ms(self) -> int:
        """Read the current clock time (epoch milliseconds)."""
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

    def get_shared_time(self) -> SharedTimeHandle:
        """Get a shared time handle for zero-GIL time reads.

        The handle can be passed to other Rust components (e.g.,
        TickDataReplayer) so they can read the clock time without
        acquiring the GIL.

        Returns:
            SharedTimeHandle: A handle for reading the clock time.
        """
        ...

    def __repr__(self) -> str: ...

class TickDataReplayer:
    """Tick-level data replayer, embedded in the Python process.

    Reads Parquet files from the data lake, replays quotes and trades
    at the correct pace using a shared virtual timeline, and delivers payloads
    to a Python callback.

    **Clock Synchronization:**
    When created with `shared_time`, the replayer uses the same clock
    as the ReplayClock, ensuring all components see the same time without drift.

    Example (shared time mode - recommended)::

        clock = ReplayClock(data_start_ts_ns=..., speed=1.0)
        replayer = TickDataReplayer(
            replay_date="20251113",
            lake_data_dir="/mnt/data/lake",
            shared_time=clock.get_shared_time(),
        )
        replayer.subscribe("AAPL", ["Q", "T"], on_tick)

    Example (legacy mode - standalone timeline)::

        replayer = TickDataReplayer(
            replay_date="20251113",
            lake_data_dir="/mnt/data/lake",
            data_start_ts_ns=clock.data_start_ts_ns,
            speed=1.0,
        )
        replayer.subscribe("AAPL", ["Q", "T"], on_tick)
    """

    def __init__(
        self,
        replay_date: str,
        lake_data_dir: str,
        shared_time: Optional[SharedTimeHandle] = None,
        data_start_ts_ns: int = 0,
        speed: float = 1.0,
        start_time: Optional[str] = None,
        max_gap_ms: Optional[int] = None,
        clickhouse_url: Optional[str] = None,
        clickhouse_user: Optional[str] = None,
        clickhouse_password: Optional[str] = None,
        clickhouse_database: Optional[str] = None,
    ) -> None:
        """Create a new replayer.

        **Recommended (shared time mode):**
            Pass `shared_time` from `ReplayClock.get_shared_time()` to
            synchronize the replayer with the master clock.

        **Legacy (standalone timeline):**
            Pass `data_start_ts_ns` and `speed` to create an independent timeline.

        Args:
            replay_date: Date in YYYYMMDD format.
            lake_data_dir: Path to the data-lake root.
            shared_time: A `SharedTimeHandle` from `ReplayClock.get_shared_time()`.
                When provided, the replayer uses this as the time source (recommended).
            data_start_ts_ns: (Legacy) Epoch-ns anchor for the virtual clock.
                Ignored if `shared_time` is provided.
            speed: (Legacy) Replay speed multiplier. Ignored if `shared_time` is provided.
            start_time: Optional ``"HH:MM"`` or ``"HH:MM:SS"`` (ET).
            max_gap_ms: Threshold for logging large time gaps (ms).
            clickhouse_url: ClickHouse HTTP URL, e.g. ``"http://localhost:8123"``.
                When provided, replayer queries CH first (Parquet fallback).
            clickhouse_user: ClickHouse username.
            clickhouse_password: ClickHouse password.
            clickhouse_database: ClickHouse database name.
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
        """Change replay speed (legacy mode only).

        In shared time mode, this is a no-op. Use ReplayClock.set_speed() instead.
        """
        ...

    def pause(self) -> None:
        """Pause playback (legacy mode only).

        In shared time mode, this is a no-op. Use ReplayClock.pause() instead.
        """
        ...

    def resume(self) -> None:
        """Resume playback (legacy mode only).

        In shared time mode, this is a no-op. Use ReplayClock.resume() instead.
        """
        ...

    def jump_to(self, target_ts_ns: int) -> None:
        """Jump to a specific data timestamp (legacy mode only).

        In shared time mode, this is a no-op. Use ReplayClock.jump_to() instead.
        """
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

class VolumeTracker:
    """Track relative volume for a single ticker."""

    def __init__(self, window_ms: int = 3900000) -> None:
        """Create a new VolumeTracker.

        Args:
            window_ms: Rolling window in milliseconds (default 3900000 = 6.5 hours).
        """
        ...

    def on_trade(self, timestamp_ms: int, volume: int) -> None:
        """Record a trade."""
        ...

    def compute(self, current_ms: int) -> Optional[float]:
        """Compute relative volume (current vol / average vol).

        Returns None if window is empty.
        """
        ...

    def reset(self) -> None:
        """Reset all state."""
        ...

    def __repr__(self) -> str: ...
