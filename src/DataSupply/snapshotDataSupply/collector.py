"""
Docstring for DataSupply.snapshotDataSupply.collector
MarketsnapshotCollector: Collects market snapshot data from Polygon.io API, validates it, saves to cache, and publishes to Redis stream.
"""

import json
import logging
import os
import threading
import time
from datetime import datetime
from datetime import time as dtime
from queue import Empty, Queue
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import polars as pl
import redis
from dotenv import load_dotenv

from config import cache_dir
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)

load_dotenv()

EST = ZoneInfo("America/New_York")


class MarketsnapshotCollector:
    def __init__(self, limit: str = "market_open"):
        self.limit = limit
        self.r = redis.Redis(host="localhost", port=6379, db=0)

        self.tz = EST
        self.calendar = xcals.get_calendar("XNYS")
        self.last_successful_fetch = None
        self.last_request_time = None
        self.fetch_timeout = 30  # seconds to consider connection stuck
        self.min_interval = 5  # minimum seconds between requests

    def is_trading_day_today(self):
        today = datetime.now(self.tz).date()
        return self.calendar.is_session(today)

    def in_limit_window(self):
        now = datetime.now(EST).time()
        if self.limit == "market_open":
            return dtime(4, 0) <= now < dtime(9, 30)
        if self.limit == "market_close":
            return dtime(4, 0) <= now < dtime(16, 0)
        else:
            return True  # No limit

    def should_fetch_now(self):
        """Check if enough time has passed since last request"""
        if self.last_request_time is None:
            return True

        elapsed = time.time() - self.last_request_time
        if elapsed >= self.min_interval:
            return True

        # Sleep for remaining time
        remaining = self.min_interval - elapsed
        time.sleep(remaining)
        return True

    def is_connection_stuck(self):
        """Check if connection appears to be stuck"""
        if self.last_successful_fetch is None:
            return False

        elapsed = time.time() - self.last_successful_fetch
        return elapsed > self.fetch_timeout

    def fetch_snapshot_with_timeout(self, timeout=20):
        """Fetch snapshot with timeout using threading"""
        result_queue = Queue()

        def fetch_worker():
            try:
                # Use requests directly with proxy support
                import requests

                proxy = os.environ.get("HTTP_PROXY")
                # logger.debug(f'fetch_snapshot_with_timeout - Using proxy: {proxy}')
                proxies = {"http": proxy, "https": proxy} if proxy else None

                url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
                params = {
                    "include_otc": "false",
                    "apiKey": os.getenv("POLYGON_API_KEY"),
                }

                response = requests.get(
                    url, params=params, proxies=proxies, timeout=timeout
                )
                response.raise_for_status()
                data = response.json()

                # Convert to TickerSnapshot objects if needed, or just return raw data
                result_queue.put(("success", data["tickers"]))
            except Exception as e:
                result_queue.put(("error", e))

        # Start fetch in separate thread
        fetch_thread = threading.Thread(target=fetch_worker, daemon=True)
        fetch_thread.start()

        # Wait for result with timeout
        try:
            status, data = result_queue.get(timeout=timeout)
            if status == "success":
                return data
            else:
                logger.info(f"fetch_snapshot_with_timeout - ❌ Fetch error: {data}")
                return None
        except Empty:
            logger.info(
                f"fetch_snapshot_with_timeout - ⚠️  Fetch timeout after {timeout}s - connection appears stuck"
            )
            return None

    def run_collector_engine(self):
        if not self.is_trading_day_today():
            logger.info("run_collector_engine - 🚫 Not a trading day. Exit.")
            return

        while self.in_limit_window():
            try:
                # Check if we should wait before fetching
                if not self.should_fetch_now():
                    continue

                # Record request time
                self.last_request_time = time.time()

                # Fetch with timeout
                snapshot = self.fetch_snapshot_with_timeout(timeout=self.fetch_timeout)

                # Handle timeout/stuck connection
                if snapshot is None:
                    logger.info(
                        "run_collector_engine - 🔄 Will retry on next iteration..."
                    )
                    continue

                # Record successful fetch
                self.last_successful_fetch = time.time()

                # Process data - flatten all nested dicts and save everything
                data_list = []
                for item in snapshot:
                    # Flatten the nested structure
                    flattened = {
                        "ticker": item.get("ticker"),
                        "todaysChange": item.get("todaysChange"),
                        "todaysChangePerc": item.get("todaysChangePerc"),
                        "updated": item.get("updated"),
                    }

                    # Flatten day
                    if item.get("day"):
                        for key, val in item["day"].items():
                            flattened[f"day_{key}"] = val

                    # Flatten prevDay
                    if item.get("prevDay"):
                        for key, val in item["prevDay"].items():
                            flattened[f"prevDay_{key}"] = val

                    # Flatten min
                    if item.get("min"):
                        for key, val in item["min"].items():
                            flattened[f"min_{key}"] = val

                    # Flatten lastTrade
                    if item.get("lastTrade"):
                        for key, val in item["lastTrade"].items():
                            if key == "c":  # c is an array, convert to string
                                flattened[f"lastTrade_{key}"] = json.dumps(val)
                            else:
                                flattened[f"lastTrade_{key}"] = val

                    # Flatten lastQuote
                    if item.get("lastQuote"):
                        for key, val in item["lastQuote"].items():
                            flattened[f"lastQuote_{key}"] = val

                    data_list.append(flattened)

                if not data_list:
                    logger.info("run_collector_engine - ⚠️  No data collected from API")
                    continue

                df = pl.DataFrame(data_list)

                logger.info(
                    f"run_collector_engine - ✓ Collected {len(df)} rows with {len(df.columns)} columns"
                )

                # Save full snapshot to CSV
                market_mover_file = self.save_snapshot(df)

                # Prepare subset for Redis stream (original format)
                stream_df = pl.DataFrame(
                    [
                        {
                            "ticker": item.get("ticker"),
                            "percent_change": item.get("todaysChangePerc", 0),
                            "accumulated_volume": float(
                                item.get("min", {}).get("av", 0)
                            ),
                            "current_price": float(
                                item.get("lastTrade", {}).get("p", 0)
                            ),
                            "prev_close": float(item.get("prevDay", {}).get("c", 0)),
                            "timestamp": item.get("updated", 0),
                            "prev_volume": item.get("prevDay", {}).get("v", 0),
                            "vwap": float(item.get("min", {}).get("vwap", 0)),
                        }
                        for item in snapshot
                        if item.get("day")
                        and item.get("prevDay")
                        and float(item.get("prevDay", {}).get("c", 0)) != 0
                    ]
                )

                stream_df = stream_df.with_columns(
                    (pl.col("timestamp") // 1_000_000).alias("timestamp")
                )

                # Publish to Redis
                payload = stream_df.write_json()
                today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
                STREAM_NAME = f"market_snapshot_stream:{today}"
                assert ":" in STREAM_NAME, "STREAM_NAME must include a date suffix!"

                message_id = self.r.xadd(STREAM_NAME, {"data": payload}, maxlen=100)
                if self.r.ttl(STREAM_NAME) < 0:
                    self.r.expire(STREAM_NAME, 1 * 19 * 3600)

                logger.info(
                    f"run_collector_engine - Published {len(df)} rows at {datetime.now(ZoneInfo('America/New_York'))}"
                )

            except Exception as e:
                logger.info(
                    f"run_collector_engine - ❌ Error in collector loop, retrying...: {e}"
                )
                # Don't update last_successful_fetch on error
                continue
        logger.info("run_collector_engine - ⏹ Premarket ended. Exit cleanly.")

    def save_snapshot(self, df: pl.DataFrame):
        """save into cache dir"""
        updated_time = datetime.now(ZoneInfo("America/New_York")).strftime(
            "%Y%m%d%H%M%S"
        )
        year = updated_time[:4]
        month = updated_time[4:6]
        date = updated_time[6:8]

        market_mover_dir = os.path.join(cache_dir, "market_mover", year, month, date)
        os.makedirs(market_mover_dir, exist_ok=True)
        market_mover_file = os.path.join(
            market_mover_dir, f"{updated_time}_market_snapshot.csv"
        )

        df.write_csv(market_mover_file)
        return market_mover_file


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=str,
        default="market_open",
        help="Limit of collector to stop at certain market event",
    )
    args = parser.parse_args()
    collector = MarketsnapshotCollector(limit=args.limit)
    collector.run_collector_engine()
