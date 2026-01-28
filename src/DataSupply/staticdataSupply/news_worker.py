"""
News Worker - Async Background Service (Refactored)

In the live mode, this worker consumes from the static:pending:news Redis Set and fetches news data
for newly subscribed tickers.
In the replay mode, this worker listen on the latest market_snapshot_processed stream msg to determine the current replay time,
and fetches news from local postgre sql as of that time.

Key Changes from Original:
1. Per-article streaming instead of per-symbol batching
2. Async fetching with rate limiting
3. Retry logic with exponential backoff
4. Article-level deduplication
5. New stream: news_article_stream

Data Flow:
    static:pending:news (SET) -> NewsWorker -> Redis HASHes -> news_article_stream (per article)
                                                            -> static_update_stream (per symbol, legacy)

Redis Schema:
    static:pending:news                  -> SET (symbols awaiting news fetch)
    news:ticker:{symbol}            -> ZSET (news IDs sorted by timestamp)
    news:item:{news_id}             -> HASH (news article content)
    news:seen_articles:{date}       -> SET (deduplication tracking)
    news_article_stream             -> STREAM (per-article notifications)
    static_update_stream            -> STREAM (per-symbol notifications, legacy)

Design Principles:
    - Static data is low-frequency, cacheable, shared across modules
    - Never blocks snapshot pipeline
    - Patch-based updates only
    - Per-article streaming for real-time updates
"""

import asyncio
import hashlib
import json
from datetime import datetime, timedelta
from typing import AsyncGenerator, Dict, List, Optional, Set
from zoneinfo import ZoneInfo

import redis

from DataSupply.staticdataSupply.news_fetch import (
    API_NewsFetchers,
    MoomooStockResolver,
    NewsPersistence,
)
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)


# ============================================================================
# Rate Limiting and Retry Configuration
# ============================================================================


class AsyncRateLimiter:
    """Rate limiter using semaphore with delay between requests."""

    def __init__(self, max_concurrent: int, delay_between: float):
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._delay = delay_between
        self._last_request = 0.0
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        await self._semaphore.acquire()
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait_time = max(0, self._delay - (now - self._last_request))
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._last_request = asyncio.get_event_loop().time()
        return self

    async def __aexit__(self, *args):
        self._semaphore.release()


class RetryConfig:
    """Configuration for retry logic with exponential backoff."""

    MAX_RETRIES = 3
    BASE_DELAY = 0.5  # seconds
    MAX_DELAY = 8.0  # seconds
    EXPONENTIAL_BASE = 2


class NewsWorker:
    """
    Async worker that fetches news data with per-article streaming.

    Key features:
    - Emits to news_article_stream per article (real-time)
    - Emits to static_update_stream per symbol (legacy, backward compatible)
    - Async fetching with rate limiting
    - Retry logic with exponential backoff
    - Article-level deduplication
    """

    # Redis key patterns
    NEWS_PENDING_SET = "static:pending:news"
    NEWS_PROCESSING_SET = "static:processing:news"
    SUMMARY_KEY_PREFIX = "static:ticker:summary"
    NEWS_TICKER_PREFIX = "news:ticker"
    NEWS_ITEM_PREFIX = "news:item"

    # Streams
    UPDATE_STREAM = "static_update_stream"  # Legacy - per-symbol notifications
    ARTICLE_STREAM = "news_article_stream"  # New - per-article notifications

    # Deduplication
    SEEN_ARTICLES_PREFIX = "news:seen_articles"
    SEEN_ARTICLES_TTL = 86400  # 24 hours

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
        # Rate limiting config
        moomoo_concurrent: int = 3,
        moomoo_delay: float = 0.5,
        benzinga_concurrent: int = 5,
        benzinga_delay: float = 0.2,
        content_concurrent: int = 2,
        content_delay: float = 0.3,
    ):
        """
        Initialize the news worker.

        Args:
            redis_host: Redis host
            redis_port: Redis port
            redis_db: Redis database number
            poll_interval: How often to check for pending symbols (seconds)
            batch_size: Max symbols to process per batch
            news_limit: Number of news articles to fetch per ticker
            replay_date: Replay date (YYYYMMDD) for time reference, None for live mode
            news_recency_hours: Max age of news to be considered "recent" (default: 24 hours)
            moomoo_concurrent: Max concurrent Moomoo requests
            moomoo_delay: Delay between Moomoo requests (seconds)
            benzinga_concurrent: Max concurrent Benzinga requests
            benzinga_delay: Delay between Benzinga requests (seconds)
            content_concurrent: Max concurrent content fetch requests
            content_delay: Delay between content fetch requests (seconds)
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

        # Redis stream for get current time
        self.SNAPSHOT_STREAM_NAME = f"market_snapshot_processed:{self.db_date}"

        # Version tracking keys (for monotonic versioning per symbol/domain)
        self.VERSION_KEY_PREFIX = f"static:version:{self.db_date}"

        # Rate limiters
        self.moomoo_limiter = AsyncRateLimiter(moomoo_concurrent, moomoo_delay)
        self.benzinga_limiter = AsyncRateLimiter(benzinga_concurrent, benzinga_delay)
        self.content_limiter = AsyncRateLimiter(content_concurrent, content_delay)

        # Initialize fetchers
        self.momo_news = MoomooStockResolver()
        self.api_news = API_NewsFetchers()
        self.news_persistence = NewsPersistence()

        self._running = False
        self._processed_count = 0
        self._article_count = 0
        self._duplicate_count = 0

        logger.info(
            f"NewsWorker initialized: mode={self.run_mode}, db_date={self.db_date}, "
            f"poll_interval={poll_interval}s, batch_size={batch_size}, "
            f"moomoo_concurrent={moomoo_concurrent}, benzinga_concurrent={benzinga_concurrent}"
        )

    async def start(self):
        """Start the worker loop."""
        self._running = True
        logger.info("NewsWorker starting...")

        # Recovery: move any items stuck in processing set back to pending
        stuck_count = self._recover_stuck_items()
        if stuck_count > 0:
            logger.info(f"Recovered {stuck_count} stuck items from previous run")

        if self.run_mode == "live":
            while self._running:
                try:
                    news_symbols = self._pop_news_pending_batch()

                    if news_symbols:
                        logger.info(
                            f"Processing {len(news_symbols)} symbols: {news_symbols}"
                        )

                        # Process symbols concurrently
                        tasks = [
                            self._process_news_streaming(symbol)
                            for symbol in news_symbols
                        ]
                        results = await asyncio.gather(*tasks, return_exceptions=True)

                        # Handle results and mark completed
                        for symbol, result in zip(news_symbols, results):
                            if isinstance(result, Exception):
                                logger.error(
                                    f"Error processing news for {symbol}: {result}"
                                )
                                self.r.smove(
                                    self.NEWS_PROCESSING_SET,
                                    self.NEWS_PENDING_SET,
                                    symbol,
                                )
                            else:
                                self._mark_news_completed(symbol)
                                self._processed_count += 1

                    await asyncio.sleep(self.poll_interval)

                except asyncio.CancelledError:
                    logger.info("NewsWorker cancelled")
                    break
                except Exception as e:
                    logger.error(f"NewsWorker error: {e}")
                    await asyncio.sleep(5)

            logger.info(
                f"NewsWorker stopped. Processed {self._processed_count} symbols, "
                f"{self._article_count} articles, {self._duplicate_count} duplicates skipped"
            )
        elif self.run_mode == "replay":
            logger.info(
                f"NewsWorker running in replay mode - listen on market_snapshot_processed:{self.db_date}"
            )
            # In replay mode, there is no fetch from pending set.
            # Instead, we listen on market_snapshot_processed stream to get current replay time,
            # and fetch news for tickers as of that time from local persistence psql.
            # but skip for now.
            pass

    def stop(self):
        """Stop the worker loop."""
        self._running = False

    def _recover_stuck_items(self) -> int:
        """Move any items stuck in processing sets back to pending sets."""
        count = 0

        news_stuck = self.r.smembers(self.NEWS_PROCESSING_SET)
        if news_stuck:
            for symbol in news_stuck:
                self.r.smove(self.NEWS_PROCESSING_SET, self.NEWS_PENDING_SET, symbol)
            count += len(news_stuck)

        return count

    def _pop_news_pending_batch(self) -> List[str]:
        """Move a batch of symbols from news-only pending to processing set."""
        symbols = []
        for _ in range(self.batch_size):
            members = self.r.srandmember(self.NEWS_PENDING_SET, 1)
            if not members:
                break
            symbol = members[0]
            if self.r.smove(self.NEWS_PENDING_SET, self.NEWS_PROCESSING_SET, symbol):
                symbols.append(symbol)
        return symbols

    def _mark_news_completed(self, symbol: str):
        """Remove symbol from news processing set after successful completion."""
        self.r.srem(self.NEWS_PROCESSING_SET, symbol)

    def _increment_version(self, symbol: str, domain: str) -> int:
        """
        Atomically increment and return the version for a (symbol, domain) pair.

        Uses Redis INCR for atomic monotonic versioning.
        """
        version_key = f"{self.VERSION_KEY_PREFIX}:{symbol}:{domain}"
        return self.r.incr(version_key)

    def _get_version(self, symbol: str, domain: str) -> int:
        """Get current version for a (symbol, domain) pair without incrementing."""
        version_key = f"{self.VERSION_KEY_PREFIX}:{symbol}:{domain}"
        val = self.r.get(version_key)
        return int(val) if val else 0

    # ============================================================================
    # Per-Article Streaming Methods
    # ============================================================================

    async def _process_news_streaming(self, symbol: str):
        """
        Process news for a symbol with per-article streaming.

        Each article is:
        1. Deduplicated
        2. Cached to Redis
        3. Persisted to PostgreSQL
        4. Emitted to news_article_stream

        After all articles, emits legacy notification to static_update_stream.
        """
        logger.info(f"Fetching news for {symbol}")
        current_timestamp = self._get_current_time(symbol)
        articles_emitted = 0
        articles_to_persist = []

        # Fetch from all providers, streaming articles
        async for article in self._fetch_news_streaming(symbol):
            # Generate article ID
            article_id = hashlib.md5(article.url.encode()).hexdigest()[:16]

            # Check deduplication
            if self._is_duplicate(article_id):
                self._duplicate_count += 1
                logger.debug(f"Skipping duplicate: {article.title[:50]}")
                continue

            # Cache to Redis
            self._cache_single_article(symbol, article, article_id)

            # Emit to article stream
            self._emit_article(symbol, article, article_id, current_timestamp)

            # Mark as seen
            self._mark_seen(article_id)

            # Collect for persistence
            articles_to_persist.append(article)

            articles_emitted += 1
            self._article_count += 1

        # Persist all articles to PostgreSQL
        if articles_to_persist:
            try:
                saved_count = self.news_persistence.save_articles(articles_to_persist)
                if saved_count > 0:
                    logger.debug(
                        f"Persisted {saved_count} articles for {symbol} to PostgreSQL"
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to persist news to PostgreSQL for {symbol}: {e}"
                )

        # Emit legacy notification for backward compatibility
        if articles_emitted > 0:
            version = self._increment_version(symbol, "news")
            self.r.xadd(
                self.UPDATE_STREAM,
                {
                    "symbol": symbol,
                    "update_type": "static",
                    "domains": json.dumps(["news"]),
                    "version": json.dumps({"news": version}),
                    "timestamp": current_timestamp.isoformat(),
                },
            )

        logger.info(f"Emitted {articles_emitted} articles for {symbol}")

    async def _fetch_news_streaming(self, symbol: str) -> AsyncGenerator:
        """
        Fetch news from all providers, yielding articles as they arrive.

        Uses retry logic with exponential backoff.
        Deduplicates by URL within the fetch session.
        """
        seen_urls: Set[str] = set()

        # Fetch from Moomoo with retry
        for attempt in range(RetryConfig.MAX_RETRIES + 1):
            try:
                async for article in self.momo_news.get_news_momo_async(
                    symbol,
                    pageSize=self.news_limit,
                    content_limiter=self.content_limiter,
                ):
                    if article.url not in seen_urls:
                        seen_urls.add(article.url)
                        yield article
                break  # Success, exit retry loop
            except Exception as e:
                if attempt < RetryConfig.MAX_RETRIES:
                    delay = min(
                        RetryConfig.BASE_DELAY
                        * (RetryConfig.EXPONENTIAL_BASE**attempt),
                        RetryConfig.MAX_DELAY,
                    )
                    logger.warning(
                        f"Moomoo retry {attempt + 1}/{RetryConfig.MAX_RETRIES}: {e}, waiting {delay}s"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Moomoo fetch failed for {symbol} after retries: {e}")

        # Fetch from Benzinga with retry
        for attempt in range(RetryConfig.MAX_RETRIES + 1):
            try:
                async for article in self.api_news.fetch_news_benzinga_async(
                    symbol, page_size=self.news_limit
                ):
                    if article.url not in seen_urls:
                        seen_urls.add(article.url)
                        yield article
                break  # Success, exit retry loop
            except Exception as e:
                if attempt < RetryConfig.MAX_RETRIES:
                    delay = min(
                        RetryConfig.BASE_DELAY
                        * (RetryConfig.EXPONENTIAL_BASE**attempt),
                        RetryConfig.MAX_DELAY,
                    )
                    logger.warning(
                        f"Benzinga retry {attempt + 1}/{RetryConfig.MAX_RETRIES}: {e}, waiting {delay}s"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"Benzinga fetch failed for {symbol} after retries: {e}"
                    )

    # ============================================================================
    # Deduplication Methods
    # ============================================================================

    def _is_duplicate(self, article_id: str) -> bool:
        """Check if article was already processed today."""
        key = f"{self.SEEN_ARTICLES_PREFIX}:{self.db_date}"
        return bool(self.r.sismember(key, article_id))

    def _mark_seen(self, article_id: str):
        """Mark article as processed."""
        key = f"{self.SEEN_ARTICLES_PREFIX}:{self.db_date}"
        self.r.sadd(key, article_id)
        self.r.expire(key, self.SEEN_ARTICLES_TTL)

    # ============================================================================
    # Caching and Streaming Methods
    # ============================================================================

    def _cache_single_article(self, symbol: str, article, article_id: str):
        """Cache a single article to Redis."""
        ticker_key = f"{self.NEWS_TICKER_PREFIX}:{symbol}"
        item_key = f"{self.NEWS_ITEM_PREFIX}:{article_id}"

        timestamp_score = article.published_time.timestamp()
        self.r.zadd(ticker_key, {article_id: timestamp_score})

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

    def _emit_article(self, symbol: str, article, article_id: str, timestamp: datetime):
        """Emit single article to news_article_stream."""
        self.r.xadd(
            self.ARTICLE_STREAM,
            {
                "symbol": symbol,
                "news_id": article_id,
                "title": article.title,
                "url": article.url,
                "published_time": article.published_time.isoformat(),
                "sources": article.sources,
                "text": article.text or "",
                "timestamp": timestamp.isoformat(),
            },
        )

    # ============================================================================
    # Time and Utility Methods
    # ============================================================================

    def _get_current_time(self, symbol: str) -> datetime:
        """
        Get the current time for news recency evaluation.

        - Live mode: current time
        - Replay mode: get current replay time from latest market snapshot data
        """
        if self.run_mode == "live":
            return datetime.now(ZoneInfo("America/New_York")).replace(microsecond=0)

        # Replay mode: get current time from latest market snapshot data
        try:
            current_snapshot_data = self.r.xrevrange(self.SNAPSHOT_STREAM_NAME, count=1)

            if current_snapshot_data:
                message_id, message_data = current_snapshot_data[0]
                current_replay_time = message_data.get("timestamp")
                if current_replay_time:
                    return datetime.fromisoformat(current_replay_time).replace(
                        microsecond=0
                    )

        except Exception as e:
            logger.warning(f"Failed to get current time for {symbol}: {e}")

        # Fallback: use date from db_date
        year = int(self.db_date[:4])
        month = int(self.db_date[4:6])
        day = int(self.db_date[6:8])
        return datetime(year, month, day, 9, 30, 0, tzinfo=ZoneInfo("America/New_York"))

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


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="News Worker - Async Background Service"
    )
    parser.add_argument("--redis-host", type=str, default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--redis-db", type=int, default=0)
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--news-limit", type=int, default=5)
    parser.add_argument("--replay-date", type=str, default=None)
    parser.add_argument("--moomoo-concurrent", type=int, default=3)
    parser.add_argument("--benzinga-concurrent", type=int, default=5)
    args = parser.parse_args()

    worker = NewsWorker(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        poll_interval=args.poll_interval,
        batch_size=args.batch_size,
        news_limit=args.news_limit,
        replay_date=args.replay_date,
        moomoo_concurrent=args.moomoo_concurrent,
        benzinga_concurrent=args.benzinga_concurrent,
    )

    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        logger.info("Shutting down NewsWorker...")
        worker.stop()
