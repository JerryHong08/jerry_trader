"""
Docstring for DataSupply.chartdataSupply.chartdata_fetcher
fetch historical custom bars data through api Polygon.io API using for chart/Computation bootstrap
"""

import json
import logging
import os
import threading
import time
from datetime import datetime
from datetime import time as dtime
from datetime import timedelta
from queue import Empty, Queue
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import polars as pl
import redis
import requests
from dotenv import load_dotenv

from jerry_trader.config import cache_dir

# from jerry_trader.DataUtils.schema import
from jerry_trader.utils.logger import setup_logger
from jerry_trader.utils.redis_keys import market_snapshot_stream
from jerry_trader.utils.session import make_session_id

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)

load_dotenv()

api_key = os.getenv("POLYGON_API_KEY")

if not api_key:
    logger.error(f"_bootstrap_ticker - No POLYGON_API_KEY")


# ── REST bootstrap ───────────────────────────────────────────────────────
POLYGON_REST_BASE = "https://api.polygon.io"
BOOTSTRAP_BATCH_SIZE = 50_000  # max per page
BOOTSTRAP_MARKET_OPEN_ET = 4  # 4:00 AM ET (pre-market open)


class CustomBarsFetcher:
    """Fetcher for historical custom bars data from Polygon.io API"""

    VALID_TIMESPANS = [
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
    ]
    CACHE_TTL = 3600  # Cache for 1 hour

    def __init__(
        self,
        session_id: Optional[str] = None,
        redis_config: Optional[Dict[str, Any]] = None,
    ):
        self.session_id = session_id or make_session_id()

        # Parse redis config
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)
        self.r = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )

        # Verify API key
        self.api_key = os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            logger.warning("POLYGON_API_KEY not found in environment variables")

    def _validate_inputs(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        from_: datetime,
        to_: datetime,
    ) -> bool:
        """Validate input parameters"""
        if not ticker or not isinstance(ticker, str):
            logger.error("Invalid ticker: must be a non-empty string")
            return False

        if not multiplier or multiplier <= 0:
            logger.error("Invalid multiplier: must be a positive integer")
            return False

        if timespan not in self.VALID_TIMESPANS:
            logger.error(f"Invalid timespan: must be one of {self.VALID_TIMESPANS}")
            return False

        if from_ >= to_:
            logger.error("Invalid date range: from_ must be before to_")
            return False

        return True

    def _get_cache_key(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        from_: datetime,
        to_: datetime,
    ) -> str:
        """Generate Redis cache key for bars data"""
        from_str = from_.strftime("%Y-%m-%d")
        to_str = to_.strftime("%Y-%m-%d")
        return f"polygon:bars:{ticker}:{multiplier}:{timespan}:{from_str}:{to_str}"

    def _get_cached_data(self, cache_key: str) -> Optional[Dict]:
        """Retrieve cached data from Redis"""
        try:
            cached = self.r.get(cache_key)
            if cached:
                logger.info(f"📦 Cache hit for {cache_key}")
                return json.loads(cached)
        except Exception as e:
            logger.warning(f"Failed to retrieve cache: {e}")
        return None

    def _cache_data(self, cache_key: str, data: Dict) -> None:
        """Cache data to Redis"""
        try:
            self.r.setex(cache_key, self.CACHE_TTL, json.dumps(data))
            logger.debug(f"💾 Cached data for {cache_key}")
        except Exception as e:
            logger.warning(f"Failed to cache data: {e}")

    def bars_fetch_worker(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        from_: datetime,
        to_: datetime,
        limit: int,
        timeout: int,
    ) -> Optional[Dict]:
        """Fetch bars data from Polygon.io API with timeout using threading"""
        result_queue = Queue()

        def fetch_worker():
            try:
                proxy = os.environ.get("HTTP_PROXY")
                proxies = {"http": proxy, "https": proxy} if proxy else None

                url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_.strftime('%Y-%m-%d')}/{to_.strftime('%Y-%m-%d')}"
                params = {
                    "adjusted": "true",
                    "sort": "asc",
                    "limit": limit,
                    "apiKey": self.api_key,
                }

                logger.debug(f"🌐 Fetching: {url}")
                response = requests.get(
                    url, params=params, proxies=proxies, timeout=timeout
                )
                response.raise_for_status()
                data = response.json()

                # Check for API errors
                if data.get("status") == "ERROR":
                    error_msg = data.get("error", "Unknown API error")
                    result_queue.put(("error", Exception(error_msg)))
                elif data.get("resultsCount", 0) == 0:
                    logger.warning(f"No data returned for {ticker}")
                    result_queue.put(("success", data))
                else:
                    result_queue.put(("success", data))

            except requests.exceptions.Timeout:
                result_queue.put(("error", Exception("Request timeout")))
            except requests.exceptions.HTTPError as e:
                result_queue.put(("error", Exception(f"HTTP error: {e}")))
            except Exception as e:
                result_queue.put(("error", e))

        fetch_thread = threading.Thread(target=fetch_worker, daemon=True)
        fetch_thread.start()

        try:
            status, data = result_queue.get(timeout=timeout + 5)  # Add buffer
            if status == "success":
                return data
            else:
                logger.error(f"bars_fetch_worker - ❌ Fetch error: {data}")
                return None
        except Empty:
            logger.error(
                f"bars_fetch_worker - ⚠️  Fetch timeout after {timeout}s - connection appears stuck"
            )
            return None

    def _parse_bars_to_dataframe(self, data: Dict) -> Optional[pl.DataFrame]:
        """Convert API response to Polars DataFrame"""
        try:
            if not data or not data.get("results"):
                logger.warning("No results in API response")
                return None

            results = data["results"]
            df = pl.DataFrame(
                {
                    "timestamp": [r["t"] for r in results],
                    "open": [float(r["o"]) for r in results],
                    "high": [float(r["h"]) for r in results],
                    "low": [float(r["l"]) for r in results],
                    "close": [float(r["c"]) for r in results],
                    "volume": [float(r["v"]) for r in results],
                    "vwap": [
                        float(r["vw"]) if r.get("vw") is not None else None
                        for r in results
                    ],
                    "transactions": [r.get("n") for r in results],
                }
            )

            # Convert timestamp to datetime
            df = df.with_columns(
                pl.from_epoch(pl.col("timestamp"), time_unit="ms").alias("datetime")
            )

            logger.info(f"✅ Parsed {len(df)} bars into DataFrame")
            return df

        except Exception as e:
            logger.error(f"Failed to parse bars to DataFrame: {e}")
            return None

    def bars_fetch(
        self,
        ticker: str = "AAPL",
        multiplier: int = 1,
        timespan: str = "day",
        from_: Optional[datetime] = None,
        to_: Optional[datetime] = None,
        limit: int = 10,
        timeout: int = 20,
        use_cache: bool = True,
        return_dataframe: bool = True,
        cache_ttl: Optional[int] = None,
    ) -> Optional[pl.DataFrame | Dict]:
        """
        Fetch bars data from Polygon.io API

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            multiplier: Size of the timespan multiplier (e.g., 1 for 1 day)
            timespan: Size of the time window (minute, hour, day, week, month, quarter, year)
            from_: Start date for the bars (default: 30 days ago)
            to_: End date for the bars (default: today)
            limit: Maximum number of bars to return (default: 5000)
            timeout: Request timeout in seconds (default: 20)
            use_cache: Whether to use Redis caching (default: True)
            return_dataframe: Return as Polars DataFrame instead of raw dict (default: True)
            cache_ttl: Override cache TTL in seconds (default: None → uses CACHE_TTL)

        Returns:
            Polars DataFrame or dict with bars data, or None if failed
        """
        # Set default dates if not provided
        if to_ is None:
            to_ = datetime.now()
        if from_ is None:
            from_ = to_ - timedelta(days=30)

        # Validate inputs
        if not self._validate_inputs(ticker, multiplier, timespan, from_, to_):
            return None

        if not self.api_key:
            logger.error("POLYGON_API_KEY not configured")
            return None

        # Check cache first
        cache_key = self._get_cache_key(ticker, multiplier, timespan, from_, to_)
        effective_ttl = cache_ttl if cache_ttl is not None else self.CACHE_TTL
        if use_cache:
            cached_data = self._get_cached_data(cache_key)
            if cached_data:
                if return_dataframe:
                    return self._parse_bars_to_dataframe(cached_data)
                return cached_data

        # Fetch from API
        logger.info(
            f"📊 Fetching bars: {ticker} {multiplier}{timespan} from {from_.date()} to {to_.date()}"
        )
        data = self.bars_fetch_worker(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=from_,
            to_=to_,
            limit=limit,
            timeout=timeout,
        )

        if data is None:
            logger.error("bars_fetch - Failed to fetch data")
            return None

        # Cache the data with effective TTL
        if use_cache and data.get("resultsCount", 0) > 0:
            try:
                self.r.setex(cache_key, effective_ttl, json.dumps(data))
                logger.debug(f"💾 Cached {cache_key} (TTL={effective_ttl}s)")
            except Exception as e:
                logger.warning(f"Failed to cache data: {e}")

        logger.info(
            f"✅ Successfully fetched {data.get('resultsCount', 0)} bars for {ticker}"
        )

        # Return as DataFrame or raw dict
        if return_dataframe:
            return self._parse_bars_to_dataframe(data)
        return data


def fetch_polygon_trades(
    symbol: str,
    from_ms: Optional[int] = None,
) -> List[Tuple[int, float, int]]:
    """Paginate Polygon /v3/trades/{symbol} from a start point to now.

    Args:
        symbol: Ticker symbol.
        from_ms: Start of fetch window (epoch ms, inclusive).
                 If None, defaults to 4:00 AM ET today (session start).

    Returns list of (ts_ms, price, size) tuples, sorted ascending by ts.
    Timestamps are from Polygon (UTC epoch ms).
    """
    if from_ms is not None:
        cutoff_ns = int(from_ms * 1_000_000)
    else:
        # Default: today 4:00 AM ET
        now_et = datetime.now(ZoneInfo("America/New_York"))
        cutoff_et = now_et.replace(
            hour=BOOTSTRAP_MARKET_OPEN_ET, minute=0, second=0, microsecond=0
        )
        cutoff_ns = int(cutoff_et.timestamp() * 1e9)  # API uses nanoseconds

    all_trades: List[Tuple[int, float]] = []
    url = (
        f"{POLYGON_REST_BASE}/v3/trades/{symbol}"
        f"?order=desc&limit={BOOTSTRAP_BATCH_SIZE}&sort=timestamp"
        f"&timestamp.gte={cutoff_ns}"
        f"&apiKey={api_key}"
    )

    page = 0
    while url:
        page += 1
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"_fetch_polygon_trades - {symbol} page {page}: {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        for trade in results:
            sip_ts_ns = trade.get("sip_timestamp", 0)
            price = trade.get("price", 0.0)
            size = trade.get("size", 0)
            ts_ms = sip_ts_ns // 1_000_000  # ns → ms
            if price > 0:
                all_trades.append((ts_ms, price, size))

        # Check if we've reached data before cutoff
        oldest_ns = results[-1].get("sip_timestamp", 0)
        if oldest_ns and oldest_ns < cutoff_ns:
            break

        # Pagination: follow next_url if present
        next_url = data.get("next_url")
        if next_url:
            # next_url already has query params; just append apiKey
            url = f"{next_url}&apiKey={api_key}"
        else:
            break

        # Ticker may have been removed while we're fetching
        # if symbol not in self.active_tickers:
        #     logger.info(f"_fetch_polygon_trades - {symbol} removed, aborting")
        #     return []

    logger.info(
        f"_fetch_polygon_trades - {symbol}: fetched {len(all_trades)} trades "
        f"in {page} pages"
    )
    # Return sorted ascending by timestamp
    all_trades.sort(key=lambda t: t[0])
    return all_trades


def fetch_polygon_trades_window(
    symbol: str,
    from_ms: int,
    to_ms: int,
) -> List[Tuple[int, float, float]]:
    """Fetch trades from Polygon /v3/trades for a specific time window.

    Unlike fetch_polygon_trades() which only returns (ts_ms, price),
    this returns (ts_ms, price, size) tuples — needed for bar building
    with accurate volume.

    Args:
        symbol: Ticker symbol
        from_ms: Start of window (epoch ms, inclusive)
        to_ms: End of window (epoch ms, inclusive)

    Returns:
        List of (ts_ms, price, size) tuples, sorted ascending by ts.
    """
    from_ns = int(from_ms * 1_000_000)
    to_ns = int(to_ms * 1_000_000)

    all_trades: List[Tuple[int, float, float]] = []
    url = (
        f"{POLYGON_REST_BASE}/v3/trades/{symbol}"
        f"?order=asc&limit={BOOTSTRAP_BATCH_SIZE}&sort=timestamp"
        f"&timestamp.gte={from_ns}&timestamp.lte={to_ns}"
        f"&apiKey={api_key}"
    )

    page = 0
    while url:
        page += 1
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"fetch_polygon_trades_window - {symbol} page {page}: {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        for trade in results:
            sip_ts_ns = trade.get("sip_timestamp", 0)
            price = trade.get("price", 0.0)
            size = trade.get("size", 0.0)
            ts_ms = sip_ts_ns // 1_000_000  # ns → ms
            if price > 0:
                all_trades.append((ts_ms, price, float(size)))

        # Pagination: follow next_url if present
        next_url = data.get("next_url")
        if next_url:
            url = f"{next_url}&apiKey={api_key}"
        else:
            break

    logger.info(
        f"fetch_polygon_trades_window - {symbol}: "
        f"fetched {len(all_trades)} trades in {page} pages "
        f"[{from_ms} → {to_ms}]"
    )
    return all_trades


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch historical bars data from Polygon.io API"
    )
    parser.add_argument(
        "--ticker", type=str, default="AAPL", help="Ticker symbol (default: AAPL)"
    )
    parser.add_argument(
        "--multiplier", type=int, default=1, help="Multiplier for bars (default: 1)"
    )
    parser.add_argument(
        "--timespan", type=str, default="day", help="Timespan for bars (default: day)"
    )
    parser.add_argument(
        "--from",
        dest="from_",
        type=str,
        default="2026-02-01",
        help="Start date YYYY-MM-DD (default: 2026-02-01)",
    )
    parser.add_argument(
        "--to",
        dest="to_",
        type=str,
        default="2026-03-05",
        help="End date YYYY-MM-DD (default: 2026-03-05)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Limit for number of bars (default: 5000)",
    )
    parser.add_argument(
        "--timeout", type=int, default=20, help="Timeout in seconds (default: 20)"
    )
    parser.add_argument("--no-cache", action="store_true", help="Disable caching")
    parser.add_argument(
        "--raw", action="store_true", help="Return raw JSON instead of DataFrame"
    )

    args = parser.parse_args()

    from_ = datetime.strptime(args.from_, "%Y-%m-%d")
    to_ = datetime.strptime(args.to_, "%Y-%m-%d")

    fetcher = CustomBarsFetcher()
    result = fetcher.bars_fetch(
        ticker=args.ticker,
        multiplier=args.multiplier,
        timespan=args.timespan,
        from_=from_,
        to_=to_,
        limit=args.limit,
        timeout=args.timeout,
        use_cache=not args.no_cache,
        return_dataframe=not args.raw,
    )

    if result is not None:
        print("\n" + "=" * 60)
        print(f"📊 Bars Data for {args.ticker}")
        print("=" * 60)
        if isinstance(result, pl.DataFrame):
            print(result)
            print(f"\n📈 Total bars: {len(result)}")
        else:
            print(json.dumps(result, indent=2))
            print(f"\n📈 Total bars: {result.get('resultsCount', 0)}")
    else:
        print("❌ Failed to fetch bars data")
