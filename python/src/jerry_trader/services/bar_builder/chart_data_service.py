"""Chart Data Service – OHLCV bar data provider for frontend ChartModule.

Orchestrates historical bar data from multiple sources.

Data Source Resolution:
    - Replay mode → local data loader (Parquet) only
    - Live mode  → Polygon.io API (via CustomBarsFetcher, has its own Redis cache)

Architecture Notes:
    - Real-time bar updates are handled on the frontend via trade ticks
      from tickDataStore — this service only provides bootstrap/historical data.
    - CustomBarsFetcher handles its own Polygon API caching layer.
    - The local data loader supports minimum minute-level bars and requires
      historical data files to exist. Used primarily for replay mode.

Usage:
    service = ChartDataService(session_id=session_id)
    result = service.get_bars("AAPL", "5m")

Data Flow:
    Frontend ChartModule
        → REST GET /api/chart/bars/{ticker}?timeframe=5m
        → BFF → ChartDataService.get_bars()
        → [Polygon API | Local data]
        → Returns complete OHLCV array → Frontend renders candlestick chart
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import polars as pl

from jerry_trader.services.market_data.bootstrap.polygon_fetcher import (
    CustomBarsFetcher,
)
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Timeframe mapping
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Frontend ChartTimeframe → (polygon multiplier, polygon timespan, bar_duration_sec)
TIMEFRAME_MAP: Dict[str, Tuple[int, str, int]] = {
    "1m": (1, "minute", 60),
    "5m": (5, "minute", 300),
    "15m": (15, "minute", 900),
    "30m": (30, "minute", 1800),
    "1h": (1, "hour", 3600),
    "4h": (4, "hour", 14400),
    "1D": (1, "day", 86400),
    "1W": (1, "week", 604800),
    "1M": (1, "month", 2592000),
}

# Default lookback days per timeframe (how much history to fetch)
DEFAULT_LOOKBACK: Dict[str, int] = {
    "1m": 1,
    "5m": 5,
    "15m": 10,
    "30m": 20,
    "1h": 30,
    "4h": 90,
    "1D": 365,
    "1W": 730,
    "1M": 1825,
}

# Replay mode lookback — longer window so the chart starts with enough
# historical context.  In live mode, Polygon always serves the most recent
# bars, but in replay the clock starts at data_start and the chart must
# pre-populate from local Parquet files.
REPLAY_LOOKBACK: Dict[str, int] = {
    "1m": 5,
    "5m": 10,
    "15m": 20,
    "30m": 30,
    "1h": 60,
    "4h": 120,
    "1D": 365,
    "1W": 730,
    "1M": 1825,
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Intraday helper (used for gap-filling in _format_dataframe)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_INTRADAY_TIMESPANS = {"minute", "hour"}


def _is_intraday(timespan: str) -> bool:
    return timespan in _INTRADAY_TIMESPANS


# Cache TTL passed through to CustomBarsFetcher's own Redis cache layer
CACHE_TTL: Dict[str, int] = {
    "1m": 120,
    "5m": 300,
    "15m": 600,
    "30m": 900,
    "1h": 1800,
    "4h": 3600,
    "1D": 7200,
    "1W": 14400,
    "1M": 28800,
}


class ChartDataService:
    """
    Provides OHLCV bar data for the frontend ChartModule.

    Wraps CustomBarsFetcher (Polygon API) with smart defaults.
    Falls back to local data loader when API is unavailable and local files exist.
    In replay mode, goes directly to local data.

    Thread Safety:
        Safe for concurrent calls — CustomBarsFetcher is thread-safe.
    """

    def __init__(
        self,
        redis_config: Optional[Dict[str, Any]] = None,
        session_id: Optional[str] = None,
    ):
        # Polygon bars fetcher (has its own Redis caching layer)
        self.fetcher = CustomBarsFetcher(
            session_id=session_id,
            redis_config=redis_config,
        )

        # Optional: local data loader (lazy import, may not be configured)
        self._local_loader = None

        logger.info("ChartDataService initialized")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Public API
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def get_bars(
        self,
        ticker: str,
        timeframe: str = "1D",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        limit: int = 5000,
        use_cache: bool = True,
    ) -> Optional[Dict]:
        """
        Get OHLCV bars for the frontend ChartModule.

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            timeframe: Frontend timeframe string ('1m', '5m', '1h', '1D', etc.)
            from_date: Start date 'YYYY-MM-DD' (default: auto from timeframe)
            to_date: End date 'YYYY-MM-DD' (default: today)
            limit: Max bars to return
            use_cache: Whether to use Redis cache for Polygon data (default True).
                       Set to False for backfill-to-ClickHouse paths to avoid
                       serving stale cached bars.

        Returns:
            Dict with bars data formatted for lightweight-charts, or None on failure.
        """
        ticker = ticker.upper().strip()
        if not ticker:
            logger.error("get_bars - empty ticker")
            return None

        # Parse timeframe
        if timeframe not in TIMEFRAME_MAP:
            logger.error(f"get_bars - unknown timeframe: {timeframe}")
            return None

        multiplier, timespan, bar_duration_sec = TIMEFRAME_MAP[timeframe]

        # Resolve date range (uses global clock — replay-aware)
        from jerry_trader import clock as _clock

        is_replay = _clock.is_replay()
        now = _clock.now_datetime()
        to_dt = datetime.strptime(to_date, "%Y-%m-%d") if to_date else now
        if from_date:
            from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        else:
            # Replay mode uses a larger lookback so the chart starts with
            # enough historical context (local data only, no live API).
            lb_table = REPLAY_LOOKBACK if is_replay else DEFAULT_LOOKBACK
            lookback = lb_table.get(timeframe, 30)
            from_dt = to_dt - timedelta(days=lookback)

        from_str = from_dt.strftime("%Y-%m-%d")
        to_str = to_dt.strftime("%Y-%m-%d")

        logger.debug(
            f"get_bars → {ticker} {timeframe} | "
            f"range={from_str}..{to_str} | "
            f"polygon_params=({multiplier},{timespan}) | "
            f"bar_dur={bar_duration_sec}s | limit={limit}"
        )

        # ── 0. Replay mode → local data only (no Polygon API) ─────────
        if is_replay:
            # Cutoff: drop bars beyond the current replay timestamp.
            # For daily+ bars, the bar timestamp is midnight of that day.
            # An incomplete day/week/month should NOT appear, so we use
            # the start-of-day as cutoff for daily+ timeframes.
            replay_now_s = int(_clock.now_ms() / 1000)
            if not _is_intraday(timespan):
                # Truncate to midnight of the current day (ET)
                from zoneinfo import ZoneInfo

                et = ZoneInfo("America/New_York")
                replay_dt = datetime.fromtimestamp(replay_now_s, tz=et)
                midnight = replay_dt.replace(hour=0, minute=0, second=0, microsecond=0)
                cutoff_epoch_s = int(midnight.timestamp()) - 1  # exclude today
            else:
                cutoff_epoch_s = replay_now_s

            result = self._fetch_from_local(
                ticker,
                timeframe,
                from_str,
                to_str,
                bar_duration_sec,
                cutoff_epoch_s=cutoff_epoch_s,
            )
            if result:
                self._log_result(ticker, timeframe, "LOCAL (replay)", result)
                return result
            logger.debug(f"get_bars ~ {ticker} {timeframe} | replay local miss → skip")
            return None

        # ── 1. Polygon API ───────────────────────────────────────────
        result = self._fetch_from_polygon(
            ticker,
            multiplier,
            timespan,
            from_dt,
            to_dt,
            timeframe,
            bar_duration_sec,
            limit,
            use_cache=use_cache,
        )
        if result:
            self._log_result(ticker, timeframe, "POLYGON", result)
            return result

        logger.warning(f"get_bars - no data available for {ticker} {timeframe}")
        return None

    def get_timeframes(self) -> List[Dict[str, Any]]:
        """Return available timeframes with metadata (for frontend dropdown)."""
        return [
            {
                "value": tf,
                "label": tf,
                "barDurationSec": TIMEFRAME_MAP[tf][2],
                "defaultLookbackDays": DEFAULT_LOOKBACK[tf],
            }
            for tf in TIMEFRAME_MAP
        ]

    @staticmethod
    def _log_result(ticker: str, timeframe: str, label: str, result: Dict):
        bars = result.get("bars", [])
        first_t = bars[0]["time"] if bars else None
        last_t = bars[-1]["time"] if bars else None
        logger.info(
            f"get_bars ← {ticker} {timeframe} | {label} | "
            f"{len(bars)} bars | "
            f"first={datetime.fromtimestamp(first_t, tz=timezone.utc).isoformat() if first_t else 'N/A'} | "
            f"last={datetime.fromtimestamp(last_t, tz=timezone.utc).isoformat() if last_t else 'N/A'}"
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Data source: Polygon API
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _fetch_from_polygon(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        from_dt: datetime,
        to_dt: datetime,
        timeframe: str,
        bar_duration_sec: int,
        limit: int,
        use_cache: bool = True,
    ) -> Optional[Dict]:
        """Fetch bars via Polygon.io API (CustomBarsFetcher)."""
        try:
            # Use polygon-level cache TTL so intraday bars stay fresh
            polygon_cache_ttl = CACHE_TTL.get(timeframe, 3600)
            df = self.fetcher.bars_fetch(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_=from_dt,
                to_=to_dt,
                limit=limit,
                timeout=20,
                use_cache=use_cache,
                return_dataframe=True,
                cache_ttl=polygon_cache_ttl,
            )

            if df is None or df.is_empty():
                return None

            return self._format_dataframe(
                df,
                ticker,
                timeframe,
                bar_duration_sec,
                from_dt.strftime("%Y-%m-%d"),
                to_dt.strftime("%Y-%m-%d"),
                source="polygon",
            )
        except Exception as e:
            logger.error(f"_fetch_from_polygon - {ticker}: {e}")
            return None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Data source: Local data loader (fallback)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _fetch_from_local(
        self,
        ticker: str,
        timeframe: str,
        from_date: str,
        to_date: str,
        bar_duration_sec: int,
        cutoff_epoch_s: Optional[int] = None,
    ) -> Optional[Dict]:
        """Load bars from local historical data files (Parquet).

        Primarily used in replay mode.  For multi-day timeframes (1W, 1M)
        the loader fetches daily bars and resamples them here.
        """
        try:
            # Lazy import — local loader is optional and heavyweight
            if self._local_loader is None:
                from jerry_trader.services.market_data.bootstrap.data_loader import (
                    LoaderConfig,
                    StockDataLoader,
                )

                self._local_loader = StockDataLoader()
                self._LoaderConfig = LoaderConfig

            # Map frontend timeframe to local data loader format
            local_tf = self._to_local_timeframe(timeframe)
            if local_tf is None:
                return None

            # Build cutoff datetime for pre-resample filtering.
            # This prevents aggregated bars (4h, 1W, 1M) from
            # incorporating source bars beyond the replay clock.
            cutoff_dt = None
            if cutoff_epoch_s is not None:
                from zoneinfo import ZoneInfo

                cutoff_dt = datetime.fromtimestamp(
                    cutoff_epoch_s, tz=ZoneInfo("America/New_York")
                )

            config = self._LoaderConfig(
                tickers=[ticker],
                start_date=from_date,
                end_date=to_date,
                timedelta=None,
                timeframe=local_tf,
                asset="us_stocks_sip",
                data_type=(
                    "day_aggs_v1"
                    if local_tf in ("1d", "7d", "30d")
                    else "minute_aggs_v1"
                ),
                full_hour=True,
                lake=True,
                use_s3=False,
                # Disable cache when a cutoff is active — cached data
                # was resampled with the full date range and would
                # contain future data baked into aggregated bars.
                use_cache=cutoff_dt is None,
                use_duck_db=False,
                skip_low_volume=False,
                cutoff_ts=cutoff_dt,
            )

            lf = self._local_loader.load(config)
            if lf is None:
                return None

            df = lf.collect()
            if df.is_empty():
                return None

            # Convert local data format to chart format
            return self._format_local_dataframe(
                df,
                ticker,
                timeframe,
                bar_duration_sec,
                from_date,
                to_date,
                cutoff_epoch_s=cutoff_epoch_s,
            )

        except ImportError:
            logger.debug("_fetch_from_local - local data loader not available")
            return None
        except FileNotFoundError:
            logger.debug(f"_fetch_from_local - no local data for {ticker}")
            return None
        except Exception as e:
            logger.warning(f"_fetch_from_local - {ticker}: {e}")
            return None

    @staticmethod
    def _to_local_timeframe(timeframe: str) -> Optional[str]:
        """Map frontend timeframe to local data loader timeframe string."""
        mapping = {
            "1m": "1m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1h",
            "4h": "4h",
            "1D": "1d",
            "1W": "7d",
            "1M": "30d",
        }
        return mapping.get(timeframe)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Response formatting
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @staticmethod
    def _format_dataframe(
        df: pl.DataFrame,
        ticker: str,
        timeframe: str,
        bar_duration_sec: int,
        from_date: str,
        to_date: str,
        source: str = "polygon",
    ) -> Dict:
        """Convert Polars DataFrame (from CustomBarsFetcher) to chart response dict.

        CustomBarsFetcher returns columns:
            timestamp (i64 ms), open, high, low, close, volume, vwap, transactions, datetime
        """
        bars = []
        for row in df.iter_rows(named=True):
            # timestamp is in ms, lightweight-charts needs seconds
            time_sec = row["timestamp"] // 1000
            bars.append(
                {
                    "time": time_sec,
                    "open": round(row["open"], 4),
                    "high": round(row["high"], 4),
                    "low": round(row["low"], 4),
                    "close": round(row["close"], 4),
                    "volume": row["volume"],
                }
            )

        # Forward-fill gaps for intraday data
        if _is_intraday(TIMEFRAME_MAP.get(timeframe, (0, "day", 0))[1]) and bars:
            bars = ChartDataService._fill_intraday_gaps(bars, bar_duration_sec)

        last_t = bars[-1]["time"] if bars else None
        return {
            "ticker": ticker,
            "timeframe": timeframe,
            "bars": bars,
            "barCount": len(bars),
            "barDurationSec": bar_duration_sec,
            "source": source,
            "from": from_date,
            "to": to_date,
            "lastBarTime": last_t,
        }

    @staticmethod
    def _format_local_dataframe(
        df: pl.DataFrame,
        ticker: str,
        timeframe: str,
        bar_duration_sec: int,
        from_date: str,
        to_date: str,
        cutoff_epoch_s: Optional[int] = None,
    ) -> Dict:
        """Convert local data loader DataFrame to chart response dict.

        Local data loader returns columns:
            timestamps (datetime), open, high, low, close, volume, ticker, [transactions]

        Args:
            cutoff_epoch_s: If set, drop bars whose time > cutoff (replay-time pruning).
        """
        # Filter to requested ticker (loader may return multiple)
        if "ticker" in df.columns:
            df = df.filter(pl.col("ticker") == ticker)

        # Drop rows where OHLCV data is null (forward-fill gaps at the
        # start of data where there is no prior close to carry forward).
        df = df.drop_nulls(subset=["open", "high", "low", "close"])

        bars = []
        for row in df.sort("timestamps").iter_rows(named=True):
            ts = row["timestamps"]
            # Convert datetime to epoch seconds
            if hasattr(ts, "timestamp"):
                time_sec = int(ts.timestamp())
            else:
                time_sec = int(ts) // 1_000_000_000  # ns → sec
            bars.append(
                {
                    "time": time_sec,
                    "open": round(float(row["open"]), 4),
                    "high": round(float(row["high"]), 4),
                    "low": round(float(row["low"]), 4),
                    "close": round(float(row["close"]), 4),
                    "volume": int(row.get("volume", 0) or 0),
                }
            )

        # Replay-time pruning: discard bars from the future
        if cutoff_epoch_s is not None:
            bars = [b for b in bars if b["time"] <= cutoff_epoch_s]

        return {
            "ticker": ticker,
            "timeframe": timeframe,
            "bars": bars,
            "barCount": len(bars),
            "barDurationSec": bar_duration_sec,
            "source": "local",
            "from": from_date,
            "to": to_date,
            "lastBarTime": bars[-1]["time"] if bars else None,
        }

    @staticmethod
    def _fill_intraday_gaps(bars: List[Dict], bar_duration_sec: int) -> List[Dict]:
        """Forward-fill missing intraday bars.

        If there's a gap > 1 bar_duration between consecutive bars,
        fill with synthetic bars that carry the previous close forward
        (open = high = low = close = prev_close, volume = 0).

        This mirrors the _forward_fill_missing pattern in data_loader.py
        but operates on the lightweight bar-dict format.

        Limited to intraday to avoid creating bars on weekends/holidays
        for daily+ timespans.
        """
        if len(bars) < 2:
            return bars

        filled: List[Dict] = [bars[0]]
        for i in range(1, len(bars)):
            prev = filled[-1]
            curr = bars[i]
            gap = curr["time"] - prev["time"]

            # Only fill gaps that are multiples of bar_duration and reasonable
            # (max 60 synthetic bars to avoid blowing up on overnight gaps)
            if gap > bar_duration_sec:
                n_missing = min(int(gap / bar_duration_sec) - 1, 60)
                prev_close = prev["close"]
                for j in range(1, n_missing + 1):
                    filled.append(
                        {
                            "time": prev["time"] + j * bar_duration_sec,
                            "open": prev_close,
                            "high": prev_close,
                            "low": prev_close,
                            "close": prev_close,
                            "volume": 0,
                        }
                    )
            filled.append(curr)

        return filled
