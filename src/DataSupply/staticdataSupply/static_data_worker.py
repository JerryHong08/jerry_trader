"""
Static Data Worker - Async Background Service

This worker consumes from the static:pending Redis Set and fetches static data
(fundamentals, float, company profile) for newly subscribed tickers.

Data Flow:
    static:pending (SET) -> StaticDataWorker -> Redis HASHes -> static_update_stream

Redis Schema:
    static:pending                  -> SET (symbols awaiting fetch)
    static:ticker:summary:{symbol}  -> HASH (summary for RankList)
    static:ticker:profile:{symbol}  -> HASH (full profile for StockDetail)
    static_update_stream            -> STREAM (notifications)

Design Principles:
    - Static data is low-frequency, cacheable, shared across modules
    - Never blocks snapshot pipeline
    - Patch-based updates only
    - Separate from snapshot/state/news pipelines

Note:
    - News processing is handled by separate news_worker.py
"""

import asyncio
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

import redis

from DataSupply.staticdataSupply.fundamentals_fetch import (
    FloatSharesProvider,
    FundamentalsFetcher,
)
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)


class StaticDataWorker:
    """
    Async worker that fetches static data for newly subscribed tickers.

    Consumes from static:pending SET and writes to:
    - static:ticker:summary:{symbol} (for RankList)
    - static:ticker:profile:{symbol} (for StockDetail)

    Emits notifications to static_update_stream.

    Note: News processing is handled by separate NewsWorker.
    """

    # Redis key patterns
    PENDING_SET = "static:pending"
    PROCESSING_SET = "static:processing"  # In-progress items for fault tolerance
    SUMMARY_KEY_PREFIX = "static:ticker:summary"
    PROFILE_KEY_PREFIX = "static:ticker:profile"
    UPDATE_STREAM = "static_update_stream"

    # Summary fields (for RankList - lightweight)
    SUMMARY_FIELDS = [
        "marketCap",
        "float",
        "country",
        "sector",
        "hasNews",
        "lastUpdated",
    ]

    # Profile fields (for StockDetail - full)
    PROFILE_FIELDS = [
        "symbol",
        "companyName",
        "country",
        "sector",
        "industry",
        "marketCap",
        "range",
        "averageVolume",
        "ipoDate",
        "fullTimeEmployees",
        "ceo",
        "website",
        "description",
        "exchange",
        "address",
        "city",
        "state",
        "image",
        "float",
        "lastUpdated",
    ]

    def __init__(
        self,
        poll_interval: float = 1.0,
        batch_size: int = 5,
        replay_date: Optional[str] = None,
        redis_config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the static data worker.

        Args:
            poll_interval: How often to check for pending symbols (seconds)
            batch_size: Max symbols to process per batch
            replay_date: Replay date (YYYYMMDD) for time reference, None for live mode
            redis_config: Redis connection config dict with host, port, db keys
        """
        # Parse redis config (with defaults)
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)
        self.r = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )
        self.poll_interval = poll_interval
        self.batch_size = batch_size

        # Mode detection
        self.replay_date = replay_date
        self.run_mode = "replay" if replay_date else "live"
        self.db_date = (
            replay_date
            if replay_date
            else datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
        )

        # Version tracking keys (for monotonic versioning per symbol/domain)
        self.VERSION_KEY_PREFIX = f"static:version:{self.db_date}"  # static:version:{self.db_date}:{symbol}:{domain} -> INTEGER

        # Initialize fetchers
        self.fundamentals_fetcher = FundamentalsFetcher()
        self.float_provider = FloatSharesProvider()

        self._running = False
        self._processed_count = 0

        logger.info(
            f"StaticDataWorker initialized: mode={self.run_mode}, db_date={self.db_date}, "
            f"poll_interval={poll_interval}s, batch_size={batch_size}"
        )

    async def start(self):
        """Start the worker loop."""
        self._running = True
        logger.info("StaticDataWorker starting...")

        # Recovery: move any items stuck in processing set back to pending
        # This handles cases where worker crashed mid-processing
        stuck_count = self._recover_stuck_items()
        if stuck_count > 0:
            logger.info(f"Recovered {stuck_count} stuck items from previous run")

        while self._running:
            try:
                # Process static data pending set
                pending_symbols = self._pop_pending_batch()

                if pending_symbols:
                    logger.info(
                        f"Processing {len(pending_symbols)} pending symbols: {pending_symbols}"
                    )

                    # Process each symbol (full static data)
                    for symbol in pending_symbols:
                        try:
                            await self._process_static(symbol)
                            self._mark_completed(symbol)  # Remove from processing set
                            self._processed_count += 1
                        except Exception as e:
                            logger.error(f"Error processing {symbol}: {e}")
                            # Move back to pending on failure for retry
                            self.r.smove(self.PROCESSING_SET, self.PENDING_SET, symbol)

                # Wait before next poll
                await asyncio.sleep(self.poll_interval)

            except asyncio.CancelledError:
                logger.info("StaticDataWorker cancelled")
                break
            except Exception as e:
                logger.error(f"StaticDataWorker error: {e}")
                await asyncio.sleep(5)  # Back off on error

        logger.info(
            f"StaticDataWorker stopped. Processed {self._processed_count} symbols."
        )

    def stop(self):
        """Stop the worker loop."""
        self._running = False

    def _recover_stuck_items(self) -> int:
        """Move any items stuck in processing set back to pending set."""
        stuck = self.r.smembers(self.PROCESSING_SET)
        if stuck:
            for symbol in stuck:
                self.r.smove(self.PROCESSING_SET, self.PENDING_SET, symbol)
            return len(stuck)
        return 0

    def _pop_pending_batch(self) -> List[str]:
        """
        Move a batch of symbols from pending to processing set.
        Uses SMOVE for atomic transfer - items stay in processing until complete.
        """
        symbols = []
        for _ in range(self.batch_size):
            # Get a random member without removing
            members = self.r.srandmember(self.PENDING_SET, 1)
            if not members:
                break
            symbol = members[0]
            # Atomically move to processing set
            if self.r.smove(self.PENDING_SET, self.PROCESSING_SET, symbol):
                symbols.append(symbol)
        return symbols

    def _mark_completed(self, symbol: str):
        """Remove symbol from processing set after successful completion."""
        self.r.srem(self.PROCESSING_SET, symbol)

    def _increment_version(self, symbol: str, domain: str) -> int:
        """
        Atomically increment and return the version for a (symbol, domain) pair.

        Uses Redis INCR for atomic monotonic versioning.
        Version key: static:version:{self.db_date}:{symbol}:{domain}

        Args:
            symbol: Stock ticker symbol
            domain: Static data domain (summary, profile, news)

        Returns:
            New version number (1-indexed, monotonically increasing)
        """
        version_key = f"{self.VERSION_KEY_PREFIX}:{symbol}:{domain}"
        return self.r.incr(version_key)

    def _get_version(self, symbol: str, domain: str) -> int:
        """
        Get current version for a (symbol, domain) pair without incrementing.

        Returns:
            Current version number, or 0 if not set
        """
        version_key = f"{self.VERSION_KEY_PREFIX}:{symbol}:{domain}"
        val = self.r.get(version_key)
        return int(val) if val else 0

    async def _process_static(self, symbol: str):
        """
        Process a single symbol: fetch profile&summary static data and cache it.

        Steps:
        1. Fetch fundamentals (company profile)
        2. Fetch float shares
        3. Write to summary HASH
        4. Write to profile HASH
        5. Emit notification to stream
        """
        logger.info(f"Fetching static data for {symbol}")
        current_timestamp = datetime.now(ZoneInfo("America/New_York"))

        # Collect data
        profile_data: Dict[str, Any] = {"symbol": symbol}
        summary_data: Dict[str, Any] = {}
        fields_updated: List[str] = []

        # 1. Fetch fundamentals (company profile from FMP)
        try:
            fundamentals_df = self.fundamentals_fetcher.fetch_fundamentals_fmp(symbol)
            if fundamentals_df is not None and len(fundamentals_df) > 0:
                row = fundamentals_df.to_dicts()[0]

                # Populate profile data
                for field in [
                    "companyName",
                    "country",
                    "sector",
                    "industry",
                    "marketCap",
                    "range",
                    "averageVolume",
                    "ipoDate",
                    "fullTimeEmployees",
                    "ceo",
                    "website",
                    "description",
                    "exchange",
                    "address",
                    "city",
                    "state",
                    "image",
                ]:
                    if field in row and row[field] is not None:
                        profile_data[field] = row[field]

                # Summary data
                if "marketCap" in row:
                    summary_data["marketCap"] = row["marketCap"]
                    fields_updated.append("marketCap")
                if "country" in row:
                    summary_data["country"] = row["country"]
                    fields_updated.append("country")
                if "sector" in row:
                    summary_data["sector"] = row["sector"]
                    fields_updated.append("sector")

                logger.debug(f"Fetched fundamentals for {symbol}")

        except Exception as e:
            logger.warning(f"Failed to fetch fundamentals for {symbol}: {e}")

        # 2. Fetch float shares (from local cache)
        try:
            float_data = self.float_provider.fetch_from_local(symbol)
            if float_data and float_data.data:
                # Use first available source
                for source_data in float_data.data:
                    if source_data.float_shares:
                        summary_data["float"] = source_data.float_shares
                        profile_data["float"] = source_data.float_shares
                        fields_updated.append("float")
                        break

                logger.debug(f"Fetched float shares for {symbol}")

        except Exception as e:
            logger.warning(f"Failed to fetch float shares for {symbol}: {e}")

        # 3. Add timestamps
        timestamp_str = current_timestamp.isoformat()
        summary_data["lastUpdated"] = timestamp_str
        profile_data["lastUpdated"] = timestamp_str

        # 4. Write to Redis
        summary_key = f"{self.SUMMARY_KEY_PREFIX}:{symbol}"
        profile_key = f"{self.PROFILE_KEY_PREFIX}:{symbol}"

        if summary_data:
            # Convert all values to strings for HSET
            string_summary = {k: str(v) for k, v in summary_data.items()}
            self.r.hset(summary_key, mapping=string_summary)
            logger.debug(f"Cached summary for {symbol}: {list(string_summary.keys())}")

        if profile_data:
            string_profile = {k: str(v) for k, v in profile_data.items()}
            self.r.hset(profile_key, mapping=string_profile)
            logger.debug(f"Cached profile for {symbol}: {list(string_profile.keys())}")

        # 5. Emit versioned notification to stream
        # Determine which domains were updated
        domains = []
        if summary_data:
            domains.append("summary")
        if profile_data:
            domains.append("profile")

        if domains:
            # Increment version for each updated domain
            versions = {}
            for domain in domains:
                versions[domain] = self._increment_version(symbol, domain)

            self.r.xadd(
                self.UPDATE_STREAM,
                {
                    "symbol": symbol,
                    "update_type": "static",
                    "domains": json.dumps(domains),
                    "version": json.dumps(versions),
                    "timestamp": timestamp_str,
                },
            )
            logger.info(
                f"Emitted versioned static update for {symbol}: "
                f"domains={domains}, versions={versions}"
            )

    def get_summary(self, symbol: str) -> Optional[Dict[str, str]]:
        """Get cached summary for a symbol."""
        key = f"{self.SUMMARY_KEY_PREFIX}:{symbol}"
        data = self.r.hgetall(key)
        return data if data else None

    def get_profile(self, symbol: str) -> Optional[Dict[str, str]]:
        """Get cached profile for a symbol."""
        key = f"{self.PROFILE_KEY_PREFIX}:{symbol}"
        data = self.r.hgetall(key)
        return data if data else None

    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics."""
        pending_count = self.r.scard(self.PENDING_SET)
        stream_len = self.r.xlen(self.UPDATE_STREAM)

        return {
            "running": self._running,
            "processed_count": self._processed_count,
            "pending_count": pending_count,
            "stream_length": stream_len,
        }


# ============ Standalone Entry Point ============


async def main():
    """Run the worker as a standalone service."""
    import argparse

    parser = argparse.ArgumentParser(description="Static Data Worker")
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Poll interval in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Batch size for processing (default: 5)",
    )
    parser.add_argument(
        "--replay-date",
        type=str,
        default=None,
        help="Replay date (YYYYMMDD) for time reference, None for live mode",
    )

    args = parser.parse_args()

    worker = StaticDataWorker(
        poll_interval=args.poll_interval,
        batch_size=args.batch_size,
        replay_date=args.replay_date,
    )

    logger.info("Starting StaticDataWorker...")
    logger.info(f"Configuration: {args}")

    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
