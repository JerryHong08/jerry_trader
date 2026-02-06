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
import logging
import os
import re
import string
from datetime import datetime, timedelta
from typing import Any, AsyncGenerator, Dict, List, Optional, Set
from zoneinfo import ZoneInfo

import redis

from DataSupply.staticdataSupply.news_fetch import (
    API_NewsFetchers,
    MoomooStockResolver,
    NewsPersistence,
)
from utils.async_helpers import AsyncRateLimiter, RetryConfig
from utils.logger import setup_logger
from utils.session import db_date_to_date, make_session_id, parse_session_id

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


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

    def __init__(
        self,
        poll_interval: float = 1.0,
        batch_size: int = 5,
        news_limit: int = 5,
        news_recency_hours: float = 24.0,
        session_id: Optional[str] = None,
        # Rate limiting config
        moomoo_concurrent: int = 3,
        moomoo_delay: float = 0.5,
        benzinga_concurrent: int = 5,
        benzinga_delay: float = 0.2,
        content_concurrent: int = 2,
        content_delay: float = 0.3,
        # database config
        redis_config: Optional[Dict[str, Any]] = None,
        postgres_config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the news worker.

        Args:
            poll_interval: How often to check for pending symbols (seconds)
            batch_size: Max symbols to process per batch
            news_limit: Number of news articles to fetch per ticker
            news_recency_hours: Max age of news to be considered "recent" (default: 24 hours)
            session_id: Unified session identifier (e.g. '20260115_replay_v1')
            redis_config: Redis connection config dict with host, port, db keys
            moomoo_concurrent: Max concurrent Moomoo requests
            moomoo_delay: Delay between Moomoo requests (seconds)
            benzinga_concurrent: Max concurrent Benzinga requests
            benzinga_delay: Delay between Benzinga requests (seconds)
            content_concurrent: Max concurrent content fetch requests
            content_delay: Delay between content fetch requests (seconds)
        """
        # Parse redis config (with defaults)
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)
        self.r = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )

        # Parse postgres config
        self.postgres_url = None
        if postgres_config:
            # URL is already built by backend_starter
            self.postgres_url = postgres_config.get("url")
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.news_limit = news_limit
        self.news_recency_hours = news_recency_hours

        # Unified session id — single source of truth for mode & date
        self.session_id = session_id or make_session_id()
        self.db_date, self.run_mode = parse_session_id(self.session_id)

        # Redis key patterns — session-scoped instance attributes
        self.NEWS_PENDING_SET = f"static:pending:news:{self.session_id}"
        self.NEWS_PROCESSING_SET = f"static:processing:news:{self.session_id}"
        self.SUMMARY_KEY_PREFIX = f"static:ticker:summary:{self.session_id}"
        self.NEWS_TICKER_PREFIX = f"news:ticker:{self.session_id}"
        self.NEWS_ITEM_PREFIX = f"news:item:{self.session_id}"

        # Streams — session-scoped
        self.UPDATE_STREAM = f"static_update_stream:{self.session_id}"
        self.ARTICLE_STREAM = f"news_article_stream:{self.session_id}"

        # Deduplication
        self.SEEN_ARTICLES_KEY = f"news:seen_articles:{self.session_id}"
        self.SEEN_TITLES_KEY = f"news:seen_titles:{self.session_id}"
        self.SEEN_ARTICLES_TTL = 86400  # 24 hours

        # Redis stream for get current time
        self.SNAPSHOT_STREAM_NAME = f"market_snapshot_processed:{self.session_id}"

        # Version tracking keys (for monotonic versioning per symbol/domain)
        self.VERSION_KEY_PREFIX = f"static:version:{self.session_id}"

        # Rate limiters
        self.moomoo_limiter = AsyncRateLimiter(moomoo_concurrent, moomoo_delay)
        self.benzinga_limiter = AsyncRateLimiter(benzinga_concurrent, benzinga_delay)
        self.content_limiter = AsyncRateLimiter(content_concurrent, content_delay)

        # Initialize fetchers
        self.momo_news = MoomooStockResolver()
        self.api_news = API_NewsFetchers()
        self.news_persistence = NewsPersistence(self.postgres_url)

        self._running = False
        self._processed_count = 0
        self._article_count = 0
        self._duplicate_count = 0
        self._roundup_count = 0

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
                f"NewsWorker running in replay mode - listen on market_snapshot_processed:{self.session_id}"
            )
            await self._run_replay_loop()

    # ============================================================================
    # Replay Mode
    # ============================================================================

    async def _run_replay_loop(self):
        """
        Replay mode: follow the snapshot stream and inject historical news.

        Algorithm:
        1. Read snapshot messages from market_snapshot_processed via XREAD
        2. For each message, extract current_ts and the list of tickers
        3. Query psql: news_events WHERE symbol IN (...) AND published_at
           BETWEEN last_replay_ts AND current_ts
        4. Push matching articles through the same cache → emit pipeline
        5. Advance last_replay_ts = current_ts
        """
        # Start-of-day as initial lower bound
        d = db_date_to_date(self.db_date)
        last_replay_ts = datetime(
            d.year, d.month, d.day, 0, 0, 0, tzinfo=ZoneInfo("America/New_York")
        )

        # XREAD cursor — start from the beginning of the stream
        last_id = "0-0"

        logger.info(
            f"Replay loop starting, db_date={self.db_date}, "
            f"initial lower bound={last_replay_ts.isoformat()}"
        )

        while self._running:
            try:
                # Block up to 2 s waiting for new snapshot messages
                result = self.r.xread(
                    {self.SNAPSHOT_STREAM_NAME: last_id}, count=1, block=2000
                )

                if not result:
                    continue  # no new messages yet

                for stream_name, messages in result:
                    for msg_id, msg_data in messages:
                        last_id = msg_id

                        # Parse current replay timestamp
                        ts_str = msg_data.get("timestamp")
                        if not ts_str:
                            continue
                        current_ts = datetime.fromisoformat(ts_str)

                        # Extract tickers present in this snapshot
                        tickers = self._extract_tickers_from_snapshot(msg_data)
                        if not tickers:
                            continue

                        # Query psql for articles published in this window
                        articles = self.news_persistence.get_articles_in_window(
                            symbols=tickers,
                            start=last_replay_ts,
                            end=current_ts,
                        )

                        if articles:
                            await self._process_replay_articles(articles, current_ts)

                        last_replay_ts = current_ts

            except asyncio.CancelledError:
                logger.info("Replay loop cancelled")
                break
            except Exception as e:
                logger.error(f"Replay loop error: {e}")
                await asyncio.sleep(2)

        logger.info(
            f"Replay loop stopped. Emitted {self._article_count} articles, "
            f"{self._duplicate_count} duplicates skipped, "
            f"{self._roundup_count} roundups filtered"
        )

    async def _process_replay_articles(
        self, articles: List[Dict], current_ts: datetime
    ):
        """
        Push historical articles from psql through the same emit pipeline.

        Applies the same dedup & roundup filters as live mode.
        """
        for row in articles:
            title = row.get("title", "")
            symbol = row.get("symbol", "")
            url = row.get("url", "")
            news_id = row.get("news_id", "")

            # --- Gate 1: Market roundup filter ---
            if self._is_market_roundup(title):
                self._roundup_count += 1
                continue

            # --- Gate 2: URL dedup ---
            article_id = news_id or hashlib.md5(url.encode()).hexdigest()[:16]
            if self._is_duplicate_url(article_id):
                self._duplicate_count += 1
                continue

            # --- Gate 3: Title fingerprint dedup ---
            title_fp = self._title_fingerprint(title)
            if self._is_duplicate_title(title_fp):
                self._duplicate_count += 1
                continue

            # Parse published_time
            pub_time = row.get("published_time")
            if isinstance(pub_time, str):
                pub_time = datetime.fromisoformat(pub_time)
            elif pub_time is None:
                pub_time = current_ts

            # Build a lightweight article-like dict for caching / emitting
            ticker_key = f"{self.NEWS_TICKER_PREFIX}:{symbol}"
            item_key = f"{self.NEWS_ITEM_PREFIX}:{article_id}"

            article_data = {
                "symbol": symbol,
                "title": title,
                "url": url,
                "sources": row.get("sources", ""),
                "published_time": pub_time.isoformat(),
            }
            text = row.get("text") or row.get("summary") or ""
            if text:
                article_data["text"] = text

            # Cache to Redis
            self.r.zadd(ticker_key, {article_id: pub_time.timestamp()})
            self.r.hset(item_key, mapping=article_data)
            self.r.zremrangebyrank(ticker_key, 0, -51)

            # Emit to article stream
            self.r.xadd(
                self.ARTICLE_STREAM,
                {
                    "symbol": symbol,
                    "news_id": article_id,
                    "title": title,
                    "url": url,
                    "published_time": pub_time.isoformat(),
                    "sources": row.get("sources", ""),
                    "text": text,
                    "timestamp": current_ts.isoformat(),
                },
            )

            self._mark_seen(article_id, title_fp)
            self._article_count += 1

            # Emit legacy static_update_stream per symbol (batch at end)
            version = self._increment_version(symbol, "news")
            self.r.xadd(
                self.UPDATE_STREAM,
                {
                    "symbol": symbol,
                    "update_type": "static",
                    "domains": json.dumps(["news"]),
                    "version": json.dumps({"news": version}),
                    "timestamp": current_ts.isoformat(),
                },
            )

    @staticmethod
    def _extract_tickers_from_snapshot(msg_data: Dict) -> List[str]:
        """Extract ticker symbols from a snapshot stream message."""
        data_json = msg_data.get("data")
        if not data_json:
            return []
        try:
            tickers_data = json.loads(data_json)
            return [item["symbol"] for item in tickers_data if "symbol" in item]
        except (json.JSONDecodeError, KeyError):
            return []

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

        Pipeline per article:
        1. Always persist to PostgreSQL (raw archive, deduped by URL via UPSERT)
        2. Filter out market roundup / junk titles
        3. Deduplicate by URL hash AND title fingerprint
        4. Cache to Redis + emit to news_article_stream

        After all articles, emits legacy notification to static_update_stream.
        """
        logger.info(f"Fetching news for {symbol}")
        current_timestamp = self._get_current_time(symbol)
        articles_emitted = 0
        articles_to_persist = []

        # Fetch from all providers, streaming articles
        async for article in self._fetch_news_streaming(symbol):
            # Always collect for persistence (raw archive)
            articles_to_persist.append(article)

            # --- Gate 1: Market roundup / junk filter ---
            if self._is_market_roundup(article.title):
                self._roundup_count += 1
                logger.debug(f"Filtered roundup: {article.title[:60]}")
                continue

            # --- Gate 2: URL-based dedup ---
            article_id = hashlib.md5(article.url.encode()).hexdigest()[:16]
            if self._is_duplicate_url(article_id):
                self._duplicate_count += 1
                logger.debug(f"Skipping URL duplicate: {article.title[:50]}")
                continue

            # --- Gate 3: Title fingerprint dedup ---
            title_fp = self._title_fingerprint(article.title)
            if self._is_duplicate_title(title_fp):
                self._duplicate_count += 1
                logger.debug(f"Skipping title duplicate: {article.title[:50]}")
                continue

            # Passed all gates — emit
            self._cache_single_article(symbol, article, article_id)
            self._emit_article(symbol, article, article_id, current_timestamp)
            self._mark_seen(article_id, title_fp)

            articles_emitted += 1
            self._article_count += 1

        # Persist ALL articles to PostgreSQL (raw archive, before dedup)
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
    # Title Normalization, Deduplication & Filtering
    # ============================================================================

    # Prefixes that news aggregators prepend to syndicated titles
    _TITLE_STRIP_PREFIXES = [
        "Express News |",
        "Express News|",
        "Breaking |",
        "Breaking|",
        "BREAKING:",
        "UPDATE:",
        "UPDATE ",
        "UPDATED:",
    ]

    # Regex patterns for market-roundup / junk titles that are not ticker-specific.
    # Each pattern is compiled once at class level for speed.
    _ROUNDUP_PATTERNS: List[re.Pattern] = [
        # "12 Communication Services Stocks Moving In Wednesday's After-Market Session"
        re.compile(
            r"^\d+\s+\w+\s+stocks?\s+moving\s+in",
            re.IGNORECASE,
        ),
        # "Trending Stocks Today | Tian Ruixiang Soars 114.81%"
        re.compile(r"^trending\s+stocks?\s+today", re.IGNORECASE),
        # "Top Gainers / Top Losers" roundups
        re.compile(
            r"^(top|biggest)\s+(gainers?|losers?|movers?)",
            re.IGNORECASE,
        ),
        # "Stocks That Hit 52-Week Highs On ..."
        re.compile(
            r"^stocks?\s+that\s+hit\s+\d+-week",
            re.IGNORECASE,
        ),
        # "Mid-Day Gainers / Losers"
        re.compile(
            r"^(mid-?day|pre-?market|after-?hours?)\s+(gainers?|losers?|movers?)",
            re.IGNORECASE,
        ),
    ]

    @staticmethod
    def _normalize_title(title: str) -> str:
        """
        Normalize a title for fingerprinting:
        1. Strip known aggregator prefixes ("Express News | ", "BREAKING:", etc.)
        2. Lowercase
        3. Remove punctuation
        4. Collapse whitespace
        """
        t = title
        for prefix in NewsWorker._TITLE_STRIP_PREFIXES:
            if t.startswith(prefix):
                t = t[len(prefix) :]
                break  # only one prefix at a time
        t = t.lower()
        t = t.translate(str.maketrans("", "", string.punctuation))
        t = " ".join(t.split())  # collapse whitespace
        return t.strip()

    @staticmethod
    def _title_fingerprint(title: str) -> str:
        """
        Generate a stable fingerprint from a title.

        Strips aggregator prefixes, normalizes, then hashes.
        Identical or near-identical headlines from different sources
        (e.g. "Express News | X" vs "X") produce the same fingerprint.
        """
        normalized = NewsWorker._normalize_title(title)
        return hashlib.md5(normalized.encode()).hexdigest()[:16]

    @classmethod
    def _is_market_roundup(cls, title: str) -> bool:
        """
        Return True if the title matches a market-roundup / junk pattern.

        These are multi-stock summary articles that are not ticker-specific
        and would waste LLM tokens in the news_processor.
        """
        return any(pat.search(title) for pat in cls._ROUNDUP_PATTERNS)

    def _is_duplicate_url(self, article_id: str) -> bool:
        """Check if article URL hash was already emitted today."""
        return bool(self.r.sismember(self.SEEN_ARTICLES_KEY, article_id))

    def _is_duplicate_title(self, title_fp: str) -> bool:
        """Check if a title fingerprint was already emitted today."""
        return bool(self.r.sismember(self.SEEN_TITLES_KEY, title_fp))

    def _mark_seen(self, article_id: str, title_fp: str):
        """Mark both article URL hash and title fingerprint as seen."""
        pipe = self.r.pipeline()
        pipe.sadd(self.SEEN_ARTICLES_KEY, article_id)
        pipe.expire(self.SEEN_ARTICLES_KEY, self.SEEN_ARTICLES_TTL)
        pipe.sadd(self.SEEN_TITLES_KEY, title_fp)
        pipe.expire(self.SEEN_TITLES_KEY, self.SEEN_ARTICLES_TTL)
        pipe.execute()

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
        d = db_date_to_date(self.db_date)
        return datetime(
            d.year, d.month, d.day, 9, 30, 0, tzinfo=ZoneInfo("America/New_York")
        )

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

    # Build session_id from CLI args
    session_id = make_session_id(replay_date=args.replay_date)

    worker = NewsWorker(
        redis_config={
            "host": args.redis_host,
            "port": args.redis_port,
            "db": args.redis_db,
        },
        poll_interval=args.poll_interval,
        batch_size=args.batch_size,
        news_limit=args.news_limit,
        session_id=session_id,
        moomoo_concurrent=args.moomoo_concurrent,
        benzinga_concurrent=args.benzinga_concurrent,
    )

    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        logger.info("Shutting down NewsWorker...")
        worker.stop()
