"""
Chart Data Service – OHLCV bar data provider for frontend ChartModule.

Orchestrates historical bar data from multiple sources with unified caching.
This is the backend counterpart of the frontend's chartDataStore.

Data Source Resolution (in order):
    1. Redis cache (keyed by ticker + timeframe + date range)
    2. Polygon.io API (via CustomBarsFetcher)
    3. Local data loader (for replay mode / API failure fallback)

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
    # result = { "ticker": "AAPL", "timeframe": "5m", "bars": [...], ... }

Data Flow:
    Frontend ChartModule
        → REST GET /api/chart/bars/{ticker}?timeframe=5m
        → BFF → ChartDataService.get_bars()
        → [Redis cache | Polygon API | Local data]
        → Returns OHLCV array → Frontend renders candlestick chart
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
import redis

from DataSupply.bootstrapdataSupply.polygon_fetcher import CustomBarsFetcher
from utils.logger import setup_logger
from utils.redis_keys import chart_bars_cache

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


class ChartDataService:
    """
    Provides OHLCV bar data for the frontend ChartModule.

    Wraps CustomBarsFetcher (Polygon API) with smart defaults and caching.
    Falls back to local data loader when API is unavailable and local files exist.

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

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            timeframe: Frontend timeframe string ('1m', '5m', '1h', '1D', etc.)
            from_date: Start date 'YYYY-MM-DD' (default: auto from timeframe)
            to_date: End date 'YYYY-MM-DD' (default: today)
            limit: Max bars to return

        Returns:
            Dict with bars data formatted for lightweight-charts, or None on failure.
            {
                "ticker": "AAPL",
                "timeframe": "5m",
                "bars": [{"time": 1709600100, "open": 170.5, "high": 171.0,
                          "low": 170.2, "close": 170.8, "volume": 12345}, ...],
                "barCount": 390,
                "source": "polygon" | "polygon_cache" | "local" | "cache",
                "barDurationSec": 300,
                "from": "2026-03-01",
                "to": "2026-03-05"
            }
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
        to_dt = datetime.strptime(to_date, "%Y-%m-%d") if to_date else datetime.now()
        if from_date:
            from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        else:
            lookback = DEFAULT_LOOKBACK.get(timeframe, 30)
            from_dt = to_dt - timedelta(days=lookback)

        from_str = from_dt.strftime("%Y-%m-%d")
        to_str = to_dt.strftime("%Y-%m-%d")

        # 1. Check chart-level Redis cache
        cache_key = chart_bars_cache(ticker, multiplier, timespan, from_str, to_str)
        cached = self._get_chart_cache(cache_key)
        if cached:
            return cached

        # 2. Try Polygon API (CustomBarsFetcher handles its own caching)
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
            return result

        # 3. Fallback to local data
        result = self._fetch_from_local(
            ticker,
            timeframe,
            from_str,
            to_str,
            bar_duration_sec,
        )
        if result:
            self._set_chart_cache(cache_key, result, timeframe)
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
                from DataSupply.bootstrapdataSupply.localdata_loader.data_loader import (
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

        return {
            "ticker": ticker,
            "timeframe": timeframe,
            "bars": bars,
            "barCount": len(bars),
            "barDurationSec": bar_duration_sec,
            "source": source,
            "from": from_date,
            "to": to_date,
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
        }
