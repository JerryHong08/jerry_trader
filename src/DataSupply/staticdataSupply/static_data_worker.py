"""
Static Data Worker - Async Background Service

This worker consumes from the static:pending Redis Set and fetches static data
(fundamentals, float, company profile, news) for newly subscribed tickers.

Data Flow:
    static:pending (SET) -> StaticDataWorker -> Redis HASHes -> static_update_stream

Redis Schema:
    static:pending                  -> SET (symbols awaiting fetch)
    static:ticker:summary:{symbol}  -> HASH (summary for RankList)
    static:ticker:profile:{symbol}  -> HASH (full profile for StockDetail)
    news:ticker:{symbol}            -> ZSET (news IDs sorted by timestamp)
    news:item:{news_id}             -> HASH (news article content)
    static_update_stream            -> STREAM (notifications)

Design Principles:
    - Static data is low-frequency, cacheable, shared across modules
    - Never blocks snapshot pipeline
    - Patch-based updates only
    - Separate from snapshot/state/news pipelines

News Classification:
    - hasNews = has(recent, good) news
    - Uses LLM API for sentiment/relevance classification
    - Compares news time with system time (live) or replay time (replay mode)
"""

import asyncio
import hashlib
import json
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from zoneinfo import ZoneInfo

import redis

from ComputeEngine.NewsProcessor import NewsClassificator
from DataSupply.staticdataSupply.fundamentals_fetch import (
    FloatSharesProvider,
    FundamentalsFetcher,
)
from DataSupply.staticdataSupply.news_fetch import (
    API_NewsFetchers,
    MoomooStockResolver,
    get_news_persistence,
)
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)


class StaticDataWorker:
    """
    Async worker that fetches static data for newly subscribed tickers.

    Consumes from static:pending SET and writes to:
    - static:ticker:summary:{symbol} (for RankList)
    - static:ticker:profile:{symbol} (for StockDetail)
    - news:ticker:{symbol} + news:item:{news_id} (for news)

    Emits notifications to static_update_stream.
    """

    # Redis key patterns
    PENDING_SET = "static:pending"
    NEWS_PENDING_SET = "static:pending:news"  # News-only refetch
    PROCESSING_SET = "static:processing"  # In-progress items for fault tolerance
    NEWS_PROCESSING_SET = "static:processing:news"  # News-only processing
    SUMMARY_KEY_PREFIX = "static:ticker:summary"
    PROFILE_KEY_PREFIX = "static:ticker:profile"
    NEWS_TICKER_PREFIX = "news:ticker"
    NEWS_ITEM_PREFIX = "news:item"
    UPDATE_STREAM = "static_update_stream"

    # Version tracking keys (for monotonic versioning per symbol/domain)
    VERSION_KEY_PREFIX = "static:version"  # static:version:{symbol}:{domain} -> INTEGER

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
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        poll_interval: float = 1.0,
        batch_size: int = 5,
        news_limit: int = 5,
        replay_date: Optional[str] = None,
        news_recency_hours: float = 24.0,
    ):
        """
        Initialize the static data worker.

        Args:
            redis_host: Redis host
            redis_port: Redis port
            redis_db: Redis database number
            poll_interval: How often to check for pending symbols (seconds)
            batch_size: Max symbols to process per batch
            news_limit: Number of news articles to fetch per ticker
            replay_date: Replay date (YYYYMMDD) for time reference, None for live mode
            news_recency_hours: Max age of news to be considered "recent" (default: 24 hours)
        """
        self.r = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.news_limit = news_limit
        self.news_recency_hours = news_recency_hours

        # Mode detection
        self.replay_date = replay_date
        self.run_mode = "replay" if replay_date else "live"
        self.db_date = (
            replay_date
            if replay_date
            else datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
        )

        # Redis key for ticker subscription time reference
        self.SUBSCRIBED_SET_NAME = f"movers_subscribed_set:{self.db_date}"

        # Initialize fetchers
        self.fundamentals_fetcher = FundamentalsFetcher()
        self.float_provider = FloatSharesProvider()
        self.momo_news = MoomooStockResolver()
        self.api_news = API_NewsFetchers()
        self.news_classificator = NewsClassificator()
        self.news_persistence = get_news_persistence()

        self._running = False
        self._processed_count = 0
        self._news_only_count = 0  # Track news-only refetches

        logger.info(
            f"StaticDataWorker initialized: mode={self.run_mode}, db_date={self.db_date}, "
            f"poll_interval={poll_interval}s, batch_size={batch_size}, "
            f"news_recency_hours={news_recency_hours}"
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
                # 1. Process full static data pending set
                pending_symbols = self._pop_pending_batch()

                if pending_symbols:
                    logger.info(
                        f"Processing {len(pending_symbols)} pending symbols: {pending_symbols}"
                    )

                    # Process each symbol (full static data)
                    for symbol in pending_symbols:
                        try:
                            await self._process_symbol(symbol)
                            self._mark_completed(symbol)  # Remove from processing set
                            self._processed_count += 1
                        except Exception as e:
                            logger.error(f"Error processing {symbol}: {e}")
                            # Move back to pending on failure for retry
                            self.r.smove(self.PROCESSING_SET, self.PENDING_SET, symbol)

                # 2. Process news-only pending set
                news_only_symbols = self._pop_news_pending_batch()

                if news_only_symbols:
                    logger.info(
                        f"Processing {len(news_only_symbols)} news-only symbols: {news_only_symbols}"
                    )

                    for symbol in news_only_symbols:
                        try:
                            await self._process_news_only(symbol)
                            self._mark_news_completed(symbol)
                            self._news_only_count += 1
                        except Exception as e:
                            logger.error(f"Error processing news for {symbol}: {e}")
                            self.r.smove(
                                self.NEWS_PROCESSING_SET, self.NEWS_PENDING_SET, symbol
                            )

                # Wait before next poll
                await asyncio.sleep(self.poll_interval)

            except asyncio.CancelledError:
                logger.info("StaticDataWorker cancelled")
                break
            except Exception as e:
                logger.error(f"StaticDataWorker error: {e}")
                await asyncio.sleep(5)  # Back off on error

        logger.info(
            f"StaticDataWorker stopped. Processed {self._processed_count} full, "
            f"{self._news_only_count} news-only symbols."
        )

    def stop(self):
        """Stop the worker loop."""
        self._running = False

    def _recover_stuck_items(self) -> int:
        """Move any items stuck in processing sets back to pending sets."""
        count = 0

        # Recover full static processing
        stuck = self.r.smembers(self.PROCESSING_SET)
        if stuck:
            for symbol in stuck:
                self.r.smove(self.PROCESSING_SET, self.PENDING_SET, symbol)
            count += len(stuck)

        # Recover news-only processing
        news_stuck = self.r.smembers(self.NEWS_PROCESSING_SET)
        if news_stuck:
            for symbol in news_stuck:
                self.r.smove(self.NEWS_PROCESSING_SET, self.NEWS_PENDING_SET, symbol)
            count += len(news_stuck)

        return count

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

    def _pop_news_pending_batch(self) -> List[str]:
        """
        Move a batch of symbols from news-only pending to processing set.
        """
        symbols = []
        for _ in range(self.batch_size):
            members = self.r.srandmember(self.NEWS_PENDING_SET, 1)
            if not members:
                break
            symbol = members[0]
            if self.r.smove(self.NEWS_PENDING_SET, self.NEWS_PROCESSING_SET, symbol):
                symbols.append(symbol)
        return symbols

    def _mark_completed(self, symbol: str):
        """Remove symbol from processing set after successful completion."""
        self.r.srem(self.PROCESSING_SET, symbol)

    def _mark_news_completed(self, symbol: str):
        """Remove symbol from news processing set after successful completion."""
        self.r.srem(self.NEWS_PROCESSING_SET, symbol)

    def _increment_version(self, symbol: str, domain: str) -> int:
        """
        Atomically increment and return the version for a (symbol, domain) pair.

        Uses Redis INCR for atomic monotonic versioning.
        Version key: static:version:{symbol}:{domain}

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

    async def _process_news_only(self, symbol: str):
        """
        Process news-only refetch for a symbol.

        This is triggered when frontend requests a news refresh.
        Only fetches news, updates hasNews flag, and emits notification.
        Does NOT refetch fundamentals or float shares.
        """
        logger.info(f"Fetching news only for {symbol}")
        timestamp = datetime.now(ZoneInfo("America/New_York"))

        has_news = False
        try:
            articles = await self._fetch_news_merged(symbol)
            if articles:
                await self._cache_news(symbol, articles)
                logger.debug(f"Fetched {len(articles)} news articles for {symbol}")

                # Evaluate hasNews
                has_news = await self._evaluate_has_news(symbol, articles)
        except Exception as e:
            logger.warning(f"Failed to fetch news for {symbol}: {e}")

        # Update summary hasNews field
        summary_key = f"{self.SUMMARY_KEY_PREFIX}:{symbol}"
        self.r.hset(summary_key, "hasNews", "1" if has_news else "0")
        self.r.hset(summary_key, "lastUpdated", timestamp.isoformat())

        # Emit versioned notification for news and summary domains
        domains = ["summary", "news"]
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
                "timestamp": timestamp.isoformat(),
            },
        )
        logger.info(
            f"Emitted versioned news-only update for {symbol}: "
            f"hasNews={has_news}, versions={versions}"
        )

    async def _process_symbol(self, symbol: str):
        """
        Process a single symbol: fetch all static data and cache it.

        Steps:
        1. Fetch fundamentals (company profile)
        2. Fetch float shares
        3. Fetch initial news batch
        4. Write to summary HASH
        5. Write to profile HASH
        6. Write news to ZSET + HASH
        7. Emit notification to stream
        """
        logger.info(f"Fetching static data for {symbol}")
        timestamp = datetime.now(ZoneInfo("America/New_York"))

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

        # 3. Fetch news from multiple providers and evaluate hasNews
        has_news = False
        try:
            articles = await self._fetch_news_merged(symbol)
            if articles:
                await self._cache_news(symbol, articles)
                logger.debug(f"Fetched {len(articles)} news articles for {symbol}")

                # Evaluate hasNews: check for recent, relevant news
                has_news = await self._evaluate_has_news(symbol, articles)

        except Exception as e:
            logger.warning(f"Failed to fetch news for {symbol}: {e}")

        summary_data["hasNews"] = "1" if has_news else "0"
        fields_updated.append("hasNews")

        # 4. Add timestamps
        timestamp_str = timestamp.isoformat()
        summary_data["lastUpdated"] = timestamp_str
        profile_data["lastUpdated"] = timestamp_str

        # 5. Write to Redis
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

        # 6. Emit versioned notification to stream
        # Determine which domains were updated
        domains = []
        if summary_data:
            domains.append("summary")
        if profile_data:
            domains.append("profile")
        if has_news or any(f in fields_updated for f in ["hasNews"]):
            domains.append("news")

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

    async def _fetch_news_merged(self, symbol: str):
        """
        Fetch news articles from multiple providers (momo + benzinga) and merge results.

        Deduplicates by URL and sorts by published_time descending.
        """
        all_articles = []
        seen_urls = set()

        # Fetch from Moomoo
        try:
            momo_articles = self.momo_news.get_news_momo(
                symbol, pageSize=self.news_limit
            )
            if momo_articles:
                for article in momo_articles:
                    if article.url not in seen_urls:
                        all_articles.append(article)
                        seen_urls.add(article.url)
                logger.debug(
                    f"Fetched {len(momo_articles)} articles from Moomoo for {symbol}"
                )
        except Exception as e:
            logger.warning(f"Moomoo news fetch failed for {symbol}: {e}")

        # Fetch from Benzinga
        try:
            benzinga_articles = self.api_news.fetch_news_benzinga(
                symbol, page_size=self.news_limit, display_output="full"
            )
            if benzinga_articles:
                for article in benzinga_articles:
                    if article.url not in seen_urls:
                        all_articles.append(article)
                        seen_urls.add(article.url)
                logger.debug(
                    f"Fetched {len(benzinga_articles)} articles from Benzinga for {symbol}"
                )
        except Exception as e:
            logger.warning(f"Benzinga news fetch failed for {symbol}: {e}")

        # Sort by published_time descending (most recent first)
        all_articles.sort(key=lambda x: x.published_time, reverse=True)

        # Limit total results
        return all_articles[: self.news_limit * 2]  # Keep more since we merged

    def _get_reference_time(self, symbol: str) -> datetime:
        """
        Get the reference time for news recency evaluation.

        - Live mode: current time
        - Replay mode: ticker's first appearance time from movers_subscribed_set ZSET
        """
        if self.run_mode == "live":
            return datetime.now(ZoneInfo("America/New_York"))

        # Replay mode: get ticker's first appearance time from ZSET score
        try:
            score = self.r.zscore(self.SUBSCRIBED_SET_NAME, symbol)
            if score:
                return datetime.fromtimestamp(score, tz=ZoneInfo("America/New_York"))
        except Exception as e:
            logger.warning(f"Failed to get ticker appearance time for {symbol}: {e}")

        # Fallback: use date from db_date
        year = int(self.db_date[:4])
        month = int(self.db_date[4:6])
        day = int(self.db_date[6:8])
        return datetime(year, month, day, 9, 30, 0, tzinfo=ZoneInfo("America/New_York"))

    async def _evaluate_has_news(self, symbol: str, articles) -> bool:
        """
        Evaluate whether ticker has recent, relevant (good) news.

        Criteria:
        1. News must be recent (within news_recency_hours of reference time)
        2. News must be classified as "good" by LLM (positive catalyst for stock movement)

        Returns:
            True if ticker has recent, good news; False otherwise
        """
        if not articles:
            return False

        reference_time = self._get_reference_time(symbol)
        recency_cutoff = reference_time - timedelta(hours=self.news_recency_hours)

        # Filter recent articles
        recent_articles = [
            article for article in articles if article.published_time >= recency_cutoff
        ]

        if not recent_articles:
            logger.debug(f"No recent news for {symbol} (cutoff: {recency_cutoff})")
            return False

        # Classify news using LLM (via NewsClassificator)
        for article in recent_articles[:3]:  # Check top 3 recent articles
            try:
                result, reasoning_content = await self.news_classificator.classify_news(
                    symbol, article
                )
                if result.is_catalyst:
                    logger.info(
                        f"Found good recent news for {symbol}: {article.title}"
                        f" published at:{article.published_time}"
                        f" score:{result.score}"
                        f" explanation:{result.explanation}"
                    )
                    return True
            except Exception as e:
                logger.warning(f"News classification failed for {symbol}: {e}")
                continue

        return False

    async def _cache_news(self, symbol: str, articles):
        """
        Cache news articles in Redis and persist to PostgreSQL.

        Redis structures:
        - news:ticker:{symbol} -> ZSET (news_id sorted by timestamp)
        - news:item:{news_id} -> HASH (article content)

        PostgreSQL:
        - news_events table (persistent storage)
        """
        ticker_key = f"{self.NEWS_TICKER_PREFIX}:{symbol}"

        for article in articles:
            # Generate stable news ID from URL
            news_id = hashlib.md5(article.url.encode()).hexdigest()[:16]
            item_key = f"{self.NEWS_ITEM_PREFIX}:{news_id}"

            # Get timestamp score
            timestamp_score = article.published_time.timestamp()

            # Add to ticker's news ZSET
            self.r.zadd(ticker_key, {news_id: timestamp_score})

            # Store article content
            article_data = {
                "symbol": article.symbol,
                "title": article.title,
                "url": article.url,
                "sources": article.sources,
                "published_time": article.published_time.isoformat(),
            }
            if article.text:
                article_data["text"] = article.text

            self.r.hset(item_key, mapping=article_data)

        # Trim old news (keep last 50 articles per ticker)
        self.r.zremrangebyrank(ticker_key, 0, -51)

        # Persist to PostgreSQL
        try:
            saved_count = self.news_persistence.save_articles(articles)
            if saved_count > 0:
                logger.debug(
                    f"Persisted {saved_count} articles for {symbol} to PostgreSQL"
                )
        except Exception as e:
            logger.warning(f"Failed to persist news to PostgreSQL for {symbol}: {e}")

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

    def get_news(self, symbol: str, limit: int = 10) -> List[Dict[str, str]]:
        """Get cached news for a symbol."""
        ticker_key = f"{self.NEWS_TICKER_PREFIX}:{symbol}"

        # Get news IDs sorted by timestamp (most recent first)
        news_ids = self.r.zrevrange(ticker_key, 0, limit - 1)

        articles = []
        for news_id in news_ids:
            item_key = f"{self.NEWS_ITEM_PREFIX}:{news_id}"
            article = self.r.hgetall(item_key)
            if article:
                articles.append(article)

        return articles

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
        "--news-limit",
        type=int,
        default=5,
        help="News articles per ticker per provider (default: 5)",
    )
    parser.add_argument(
        "--replay-date",
        type=str,
        default=None,
        help="Replay date (YYYYMMDD) for time reference, None for live mode",
    )
    parser.add_argument(
        "--news-recency-hours",
        type=float,
        default=24.0,
        help="Max age of news to be considered recent (default: 24 hours)",
    )

    args = parser.parse_args()

    worker = StaticDataWorker(
        poll_interval=args.poll_interval,
        batch_size=args.batch_size,
        news_limit=args.news_limit,
        replay_date=args.replay_date,
        news_recency_hours=args.news_recency_hours,
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
