"""
Chart Data Service – OHLCV bar data provider for frontend ChartModule.

Orchestrates historical bar data from multiple sources with unified caching
and incremental fetch support.

Data Source Resolution (in order):
    1. Redis cache (time-bucketed for intraday, date-based for daily+)
    2. Polygon.io API (via CustomBarsFetcher) — incremental when possible
    3. Local data loader (for replay mode / API failure fallback)

Incremental Fetch:
    When the backend Redis cache is stale (gap between last cached bar and
    now exceeds the timeframe threshold), the service fetches only the
    delta from Polygon, merges with the cached bars, and returns the
    combined result.  This is entirely backend-internal; the frontend
    always receives a complete bar set.

Architecture Notes:
    - Real-time bar updates are handled on the frontend via trade ticks
      from tickDataStore — this service only provides bootstrap/historical data.
    - CustomBarsFetcher handles its own Polygon API caching layer; this service
      adds a higher-level cache keyed by the chart request parameters.
    - The local data loader supports minimum minute-level bars and requires
      historical data files to exist. Used primarily for replay mode.

Usage:
    service = ChartDataService(redis_config={"host": "127.0.0.1"})
    result = service.get_bars("AAPL", "5m")

Data Flow:
    Frontend ChartModule
        → REST GET /api/chart/bars/{ticker}?timeframe=5m
        → BFF → ChartDataService.get_bars()
        → [Redis cache | Polygon API (full or incremental) | Local data]
        → Returns complete OHLCV array → Frontend renders candlestick chart
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
import redis

from jerry_trader.DataSupply.bootstrapdataSupply.polygon_fetcher import (
    CustomBarsFetcher,
)
from jerry_trader.utils.logger import setup_logger
from jerry_trader.utils.redis_keys import chart_bars_cache

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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Intraday vs daily helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Timespans considered "intraday" for cache-key bucketing
_INTRADAY_TIMESPANS = {"minute", "hour"}


def _is_intraday(timespan: str) -> bool:
    return timespan in _INTRADAY_TIMESPANS


def _time_bucket(timespan: str, multiplier: int) -> str:
    """Compute a time-bucket suffix for intraday cache keys.

    Bucket granularity scales with bar size:
      minute-1   → bucket every 5 min  (e.g. '1422'  → minute 1422 of day // 5)
      minute-5   → bucket every 15 min
      minute-15  → bucket every 30 min
      minute-30  → bucket every 60 min
      hour-*     → bucket every hour
    """
    now = datetime.now()
    minutes_of_day = now.hour * 60 + now.minute

    if timespan == "minute":
        if multiplier <= 1:
            bucket_size = 5
        elif multiplier <= 5:
            bucket_size = 15
        elif multiplier <= 15:
            bucket_size = 30
        else:
            bucket_size = 60
        return str(minutes_of_day // bucket_size)
    elif timespan == "hour":
        return str(now.hour)
    return ""


# Cache TTL (seconds) per timeframe granularity
# Intraday bars expire faster (data still forming during market hours)
CACHE_TTL: Dict[str, int] = {
    "1m": 120,  # 2 min — changes every minute
    "5m": 300,  # 5 min
    "15m": 600,  # 10 min
    "30m": 900,  # 15 min
    "1h": 1800,  # 30 min
    "4h": 3600,  # 1 hr
    "1D": 7200,  # 2 hr — daily bars stable after close
    "1W": 14400,  # 4 hr
    "1M": 28800,  # 8 hr
}

# Minimum gap (seconds) before we bother doing an incremental fetch.
# If the gap is smaller than this, the current cached / returned data is fresh enough.
MIN_INCREMENTAL_GAP: Dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1D": 86400,
    "1W": 86400,
    "1M": 86400,
}


class ChartDataService:
    """
    Provides OHLCV bar data for the frontend ChartModule.

    Wraps CustomBarsFetcher (Polygon API) with smart defaults and caching.
    Falls back to local data loader when API is unavailable and local files exist.
    Supports incremental fetch to avoid re-fetching already-known bars.

    Thread Safety:
        Safe for concurrent calls — Redis and CustomBarsFetcher are thread-safe.
    """

    def __init__(
        self,
        redis_config: Optional[Dict[str, Any]] = None,
        session_id: Optional[str] = None,
    ):
        # Redis for chart-level caching
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)
        self.r = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )

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
    ) -> Optional[Dict]:
        """
        Get OHLCV bars for the frontend ChartModule.

        Returns a complete bar set.  Internally uses incremental fetch
        when the Redis cache is stale (fetches only the gap from Polygon,
        merges with cached bars).  The frontend always receives a full
        replacement — no client-side merge needed.

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            timeframe: Frontend timeframe string ('1m', '5m', '1h', '1D', etc.)
            from_date: Start date 'YYYY-MM-DD' (default: auto from timeframe)
            to_date: End date 'YYYY-MM-DD' (default: today)
            limit: Max bars to return

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

        # Resolve date range
        now = datetime.now()
        to_dt = datetime.strptime(to_date, "%Y-%m-%d") if to_date else now
        if from_date:
            from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        else:
            lookback = DEFAULT_LOOKBACK.get(timeframe, 30)
            from_dt = to_dt - timedelta(days=lookback)

        from_str = from_dt.strftime("%Y-%m-%d")
        to_str = to_dt.strftime("%Y-%m-%d")

        # Time bucket for intraday cache keys (avoids serving stale 10am data at 2pm)
        bucket = _time_bucket(timespan, multiplier) if _is_intraday(timespan) else ""

        logger.debug(
            f"get_bars → {ticker} {timeframe} | "
            f"range={from_str}..{to_str} | "
            f"polygon_params=({multiplier},{timespan}) | "
            f"bar_dur={bar_duration_sec}s | limit={limit} | "
            f"bucket={bucket!r}"
        )

        # ── 1. Check chart-level Redis cache ──────────────────────────
        cache_key = chart_bars_cache(
            ticker, multiplier, timespan, from_str, to_str, time_bucket=bucket
        )
        cached = self._get_chart_cache(cache_key)

        if cached:
            cached_bars = cached.get("bars", [])
            cached_last = cached_bars[-1]["time"] if cached_bars else 0
            now_epoch = int(now.timestamp())
            gap = now_epoch - cached_last

            # If the gap is small enough, serve cache directly
            min_gap = MIN_INCREMENTAL_GAP.get(timeframe, bar_duration_sec)
            if gap < min_gap:
                logger.debug(
                    f"get_bars ← {ticker} {timeframe} | CACHE HIT (fresh) | "
                    f"{len(cached_bars)} bars | gap={gap}s < {min_gap}s | key={cache_key}"
                )
                return cached

            # Cache exists but is stale — try incremental fetch from the gap
            logger.debug(
                f"get_bars ~ {ticker} {timeframe} | CACHE STALE | "
                f"{len(cached_bars)} bars | gap={gap}s ≥ {min_gap}s → incremental"
            )
            incremental = self._incremental_fetch(
                ticker,
                multiplier,
                timespan,
                cached_last,
                to_dt,
                timeframe,
                bar_duration_sec,
                limit,
            )
            if incremental:
                merged = self._merge_bars(cached_bars, incremental)
                result = self._build_response(
                    merged,
                    ticker,
                    timeframe,
                    bar_duration_sec,
                    from_str,
                    to_str,
                    source="polygon_incremental",
                )
                self._set_chart_cache(cache_key, result, timeframe)
                self._log_result(ticker, timeframe, "INCREMENTAL", result)
                return result
            # If incremental fetch failed, still serve stale cache
            logger.debug(f"get_bars ~ incremental failed, returning stale cache")
            return cached

        # ── 2. Full fetch from Polygon API ────────────────────────────
        result = self._fetch_from_polygon(
            ticker,
            multiplier,
            timespan,
            from_dt,
            to_dt,
            timeframe,
            bar_duration_sec,
            limit,
        )
        if result:
            self._set_chart_cache(cache_key, result, timeframe)
            self._log_result(ticker, timeframe, "POLYGON FULL", result)
            return result

        # ── 3. Fallback to local data ─────────────────────────────────
        result = self._fetch_from_local(
            ticker,
            timeframe,
            from_str,
            to_str,
            bar_duration_sec,
        )
        if result:
            self._set_chart_cache(cache_key, result, timeframe)
            self._log_result(ticker, timeframe, "LOCAL", result)
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

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Incremental fetch & merge helpers
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _incremental_fetch(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        last_bar_time: int,
        to_dt: datetime,
        timeframe: str,
        bar_duration_sec: int,
        limit: int,
    ) -> Optional[List[Dict]]:
        """Fetch only the bars newer than `last_bar_time` from Polygon.

        Returns a list of bar dicts, or None on failure.
        """
        try:
            # Start 1 bar before last_bar_time to ensure overlap for dedup
            gap_from = datetime.utcfromtimestamp(last_bar_time)
            logger.debug(
                f"_incremental_fetch {ticker} {timeframe} | "
                f"from={gap_from.isoformat()} to={to_dt.date()}"
            )
            polygon_cache_ttl = CACHE_TTL.get(timeframe, 3600)
            df = self.fetcher.bars_fetch(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_=gap_from,
                to_=to_dt,
                limit=limit,
                timeout=20,
                use_cache=False,  # Never use polygon cache for incremental
                return_dataframe=True,
                cache_ttl=polygon_cache_ttl,
            )
            if df is None or df.is_empty():
                return None

            bars = []
            for row in df.iter_rows(named=True):
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
            logger.debug(f"_incremental_fetch → {len(bars)} new bars for {ticker}")
            return bars
        except Exception as e:
            logger.warning(f"_incremental_fetch failed for {ticker}: {e}")
            return None

    @staticmethod
    def _merge_bars(existing: List[Dict], new_bars: List[Dict]) -> List[Dict]:
        """Merge existing cached bars with newly fetched bars.

        For overlapping timestamps, the new bar's data wins (it has the latest
        volume / close from Polygon).  Result is sorted by time, deduplicated.
        """
        merged = {b["time"]: b for b in existing}
        for b in new_bars:
            merged[b["time"]] = b  # new overwrites old on overlap
        result = sorted(merged.values(), key=lambda b: b["time"])
        return result

    @staticmethod
    def _build_response(
        bars: List[Dict],
        ticker: str,
        timeframe: str,
        bar_duration_sec: int,
        from_date: str,
        to_date: str,
        source: str = "polygon",
    ) -> Dict:
        """Build a ChartBarsResponse dict from a bar list."""
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
    def _log_result(ticker: str, timeframe: str, label: str, result: Dict):
        bars = result.get("bars", [])
        first_t = bars[0]["time"] if bars else None
        last_t = bars[-1]["time"] if bars else None
        logger.info(
            f"get_bars ← {ticker} {timeframe} | {label} | "
            f"{len(bars)} bars | "
            f"first={datetime.utcfromtimestamp(first_t).isoformat() if first_t else 'N/A'} | "
            f"last={datetime.utcfromtimestamp(last_t).isoformat() if last_t else 'N/A'}"
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
    ) -> Optional[Dict]:
        """Fetch bars via Polygon.io API (CustomBarsFetcher)."""
        try:
            # Use same TTL for the polygon-level cache as the chart-level cache
            # so intraday bars don't serve stale data from the polygon cache
            polygon_cache_ttl = CACHE_TTL.get(timeframe, 3600)
            df = self.fetcher.bars_fetch(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_=from_dt,
                to_=to_dt,
                limit=limit,
                timeout=20,
                use_cache=True,
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
    ) -> Optional[Dict]:
        """Fallback: load bars from local historical data files.

        Only supports minute-level and above timespans.
        Requires local data files to exist (data lake / raw data directory).
        Primarily useful for replay mode or when API quota is exhausted.
        """
        try:
            # Lazy import — local loader is optional and heavyweight
            if self._local_loader is None:
                from jerry_trader.DataSupply.bootstrapdataSupply.localdata_loader.data_loader import (
                    LoaderConfig,
                    StockDataLoader,
                )

                self._local_loader = StockDataLoader()
                self._LoaderConfig = LoaderConfig

            # Map frontend timeframe to local data loader format
            local_tf = self._to_local_timeframe(timeframe)
            if local_tf is None:
                return None

            config = self._LoaderConfig(
                tickers=[ticker],
                start_date=from_date,
                end_date=to_date,
                timedelta=None,
                timeframe=local_tf,
                asset="us_stocks_sip",
                data_type="day_aggs_v1" if local_tf == "1d" else "minute_aggs_v1",
                full_hour=True,
                lake=True,
                use_s3=False,
                use_cache=True,
                use_duck_db=False,
                skip_low_volume=False,
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
            "1W": None,
            "1M": None,
        }
        return mapping.get(timeframe)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Chart-level Redis cache
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _get_chart_cache(self, cache_key: str) -> Optional[Dict]:
        """Read from chart-level cache."""
        try:
            cached = self.r.get(cache_key)
            if cached:
                logger.debug(f"📦 Chart cache hit: {cache_key}")
                return json.loads(cached)
        except Exception as e:
            logger.warning(f"_get_chart_cache failed: {e}")
        return None

    def _set_chart_cache(self, cache_key: str, data: Dict, timeframe: str) -> None:
        """Write to chart-level cache with timeframe-based TTL."""
        try:
            ttl = CACHE_TTL.get(timeframe, 3600)
            self.r.setex(cache_key, ttl, json.dumps(data))
            logger.debug(f"💾 Chart cached: {cache_key} (TTL={ttl}s)")
        except Exception as e:
            logger.warning(f"_set_chart_cache failed: {e}")

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
    ) -> Dict:
        """Convert local data loader DataFrame to chart response dict.

        Local data loader returns columns:
            timestamps (datetime), open, high, low, close, volume, ticker, [transactions]
        """
        # Filter to requested ticker (loader may return multiple)
        if "ticker" in df.columns:
            df = df.filter(pl.col("ticker") == ticker)

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
                    "volume": int(row.get("volume", 0)),
                }
            )

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
