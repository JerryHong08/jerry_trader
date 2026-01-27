"""
NewsProcessor: Classify news articles using DeepSeek LLM API.
listens to static news updates and classifies articles as positive catalysts.

in the long term this can be extended to a full agent
that reads news, classifies, summarizes, and writes back to redis/db.
"""

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import redis
from dotenv import load_dotenv
from openai import OpenAI

from config import load_prompt
from DataUtils.schema import NewsArticle
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)
load_dotenv()


@dataclass
class NewsClassificationResult:
    """Result of news classification by LLM."""

    classification: str  # "YES" or "NO"
    score: str  # e.g., "7/10"
    explanation: str  # Reasoning from LLM

    @property
    def is_catalyst(self) -> bool:
        """Returns True if classified as a catalyst."""
        return self.classification.upper() == "YES"


@dataclass
class NewsTask:
    symbol: str
    version: int
    timestamp: Optional[str] = None


class NewsProcessor:
    """
    Classify news articles using DeepSeek LLM API.

    Determines if news is a positive catalyst that could drive stock movement.
    """

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        worker_count: int = 4,
        queue_maxsize: int = 200,
        article_limit: int = 10,
        monitor_interval: float = 10.0,
    ):
        self.api_key = os.getenv("DEEPSEEK_API_KEY")
        self.base_url = "https://api.deepseek.com/v1/chat/completions"

        if not self.api_key:
            logger.warning(
                "DEEPSEEK_API_KEY not set, news classification will be disabled"
            )

        self.STATIC_UPDATE_STREAM = "static_update_stream"
        self.NEWS_ARTICLE_STREAM = (
            "news_article_stream"  # New - per-article notifications
        )
        self.NEWS_TICKER_PREFIX = "news:ticker"
        self.NEWS_ITEM_PREFIX = "news:item"
        self.STATIC_SUMMARY_PREFIX = "static:ticker:summary"

        self.system_prompt = load_prompt("news_processor_system_prompt.txt")

        self.r = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )

        self.worker_count = worker_count
        self.article_limit = article_limit
        self.monitor_interval = monitor_interval
        self._queue: asyncio.Queue[NewsTask] = asyncio.Queue(maxsize=queue_maxsize)

        self._running = False
        self._listener_task: Optional[asyncio.Task] = None
        self._article_listener_task: Optional[asyncio.Task] = (
            None  # New - article stream listener
        )
        self._monitor_task: Optional[asyncio.Task] = None
        self._worker_tasks: List[asyncio.Task] = []

        self._applied_versions: Dict[str, Dict[str, int]] = {}
        self._queued_versions: Dict[str, Dict[str, int]] = {}
        self._symbol_locks: Dict[str, asyncio.Lock] = {}

        self._active_workers = 0
        self._stats = {
            "enqueued": 0,
            "processed": 0,
            "failed": 0,
            "skipped": 0,
        }

    def _get_symbol_lock(self, symbol: str) -> asyncio.Lock:
        if symbol not in self._symbol_locks:
            self._symbol_locks[symbol] = asyncio.Lock()
        return self._symbol_locks[symbol]

    async def start(self):
        """Start listener, worker pool, and monitor."""
        if self._running:
            return
        self._running = True

        # self._listener_task = asyncio.create_task(self._static_news_stream_listener())
        self._article_listener_task = asyncio.create_task(
            self._article_stream_listener()
        )  # New
        self._worker_tasks = [
            asyncio.create_task(self._worker_loop(i)) for i in range(self.worker_count)
        ]
        self._monitor_task = asyncio.create_task(self._monitor_loop())

        logger.info(
            f"NewsProcessor started: workers={self.worker_count}, "
            f"queue_maxsize={self._queue.maxsize}, article_limit={self.article_limit}"
        )

        try:
            while self._running:
                await asyncio.sleep(0.5)
        finally:
            await self.stop()

    async def stop(self):
        """Stop listener, workers, and monitor."""
        if not self._running:
            return
        self._running = False

        tasks = [
            t
            for t in [
                self._listener_task,
                self._article_listener_task,
                self._monitor_task,
            ]
            if t
        ]
        tasks.extend(self._worker_tasks)

        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._listener_task = None
        self._monitor_task = None
        self._worker_tasks = []

        logger.info("NewsProcessor stopped")

    async def _static_news_stream_listener(self):
        """Listen to static update stream specifically on news and write and notify news analysis (async).

        Static updates are low-frequency, patch-only updates for:
        - News: news articles (for StockDetail news cache)

        Stream message schema (v2 - versioned):
        {
            "symbol": "XPON",
            "update_type": "static",
            "domains": ["news"],
            "version": {"news": 12},
            "timestamp": "2026-01-25T10:30:00-05:00"
        }

        Version rules:
        - NewsProcessor only processes domains with "news" domain and version > last_applied_version

        """
        logger.info("Starting async static:news stream listener...")

        # Create consumer group for NewsProcessor
        try:
            self.r.xgroup_create(
                self.STATIC_UPDATE_STREAM,
                "NewsProcessor_news_consumers",
                id="0",
                mkstream=True,
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        consumer_name = f"NewsProcessor_news_{datetime.now().timestamp()}"

        while self._running:
            try:
                messages = self.r.xreadgroup(
                    "NewsProcessor_news_consumers",
                    consumer_name,
                    {self.STATIC_UPDATE_STREAM: ">"},
                    count=10,
                    block=2000,
                )

                if messages:
                    for _, message_list in messages:
                        for message_id, message_data in message_list:
                            symbol = message_data.get("symbol")
                            if not symbol:
                                self.r.xack(
                                    self.STATIC_UPDATE_STREAM,
                                    "NewsProcessor_news_consumers",
                                    message_id,
                                )
                                continue

                            # Parse versioned schema
                            update_type = message_data.get("update_type", "static")
                            domains_json = message_data.get("domains", "[]")
                            version_json = message_data.get("version", "{}")
                            timestamp = message_data.get("timestamp")

                            try:
                                domains = json.loads(domains_json)
                                versions = json.loads(version_json)
                            except json.JSONDecodeError:
                                # Fallback for legacy format (fields_updated)
                                fields_updated = message_data.get(
                                    "fields_updated", "[]"
                                )
                                try:
                                    fields = json.loads(fields_updated)
                                except json.JSONDecodeError:
                                    fields = []
                                # Infer domains from legacy fields
                                domains = []
                                if "news" in fields:
                                    domains.append("news")
                                versions = {}  # No version tracking for legacy

                            # Only process news domain
                            if "news" not in domains:
                                self.r.xack(
                                    self.STATIC_UPDATE_STREAM,
                                    "NewsProcessor_news_consumers",
                                    message_id,
                                )
                                continue

                            if symbol not in self._applied_versions:
                                self._applied_versions[symbol] = {}
                            if symbol not in self._queued_versions:
                                self._queued_versions[symbol] = {}

                            incoming_version = int(versions.get("news", 0) or 0)
                            last_applied = self._applied_versions[symbol].get("news", 0)
                            last_queued = self._queued_versions[symbol].get("news", 0)

                            if incoming_version <= max(last_applied, last_queued):
                                self._stats["skipped"] += 1
                                logger.debug(
                                    f"Skipping stale/queued news update for {symbol}: "
                                    f"v{incoming_version} <= v{max(last_applied, last_queued)}"
                                )
                                self.r.xack(
                                    self.STATIC_UPDATE_STREAM,
                                    "NewsProcessor_news_consumers",
                                    message_id,
                                )
                                continue

                            await self._enqueue_task(
                                NewsTask(
                                    symbol=symbol,
                                    version=incoming_version,
                                    timestamp=timestamp,
                                )
                            )
                            self._queued_versions[symbol]["news"] = incoming_version

                            # Acknowledge the message
                            self.r.xack(
                                self.STATIC_UPDATE_STREAM,
                                "NewsProcessor_news_consumers",
                                message_id,
                            )

                # Allow other tasks to run
                await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Static stream listener error: {e}")
                await asyncio.sleep(5)

    async def _article_stream_listener(self):
        """
        Listen to news_article_stream for individual article classification.

        Provides real-time classification as articles arrive from NewsWorker.
        Each article is classified immediately upon arrival.
        """
        logger.info("Starting article stream listener...")

        # Create consumer group for NewsProcessor
        try:
            self.r.xgroup_create(
                self.NEWS_ARTICLE_STREAM,
                "NewsProcessor_article_consumers",
                id="0",
                mkstream=True,
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        consumer_name = f"NewsProcessor_article_{datetime.now().timestamp()}"

        while self._running:
            try:
                messages = self.r.xreadgroup(
                    "NewsProcessor_article_consumers",
                    consumer_name,
                    {self.NEWS_ARTICLE_STREAM: ">"},
                    count=5,
                    block=1000,
                )

                if messages:
                    for stream_name, message_list in messages:
                        for message_id, message_data in message_list:
                            symbol = message_data.get("symbol")
                            if not symbol:
                                self.r.xack(
                                    self.NEWS_ARTICLE_STREAM,
                                    "NewsProcessor_article_consumers",
                                    message_id,
                                )
                                continue

                            # Build NewsArticle from stream data
                            try:
                                article = NewsArticle(
                                    symbol=symbol,
                                    title=message_data.get("title", ""),
                                    text=message_data.get("text", ""),
                                    url=message_data.get("url", ""),
                                    published_time=message_data.get(
                                        "published_time", ""
                                    ),
                                    sources=message_data.get("sources", ""),
                                )
                            except Exception as e:
                                logger.warning(
                                    f"Failed to build NewsArticle from stream: {e}"
                                )
                                self.r.xack(
                                    self.NEWS_ARTICLE_STREAM,
                                    "NewsProcessor_article_consumers",
                                    message_id,
                                )
                                continue

                            # Classify immediately (real-time)
                            result, reasoning = await self.classify_news(
                                symbol,
                                article,
                                thinking_mode=False,
                                current_time=message_data.get("timestamp"),
                            )

                            if result:
                                logger.info(
                                    f"Real-time article classification: {symbol} - "
                                    f"{result.classification} score={result.score} "
                                    f"title={article.title}"
                                )
                                if reasoning:
                                    logger.debug(f"Reasoning: {reasoning}")
                            else:
                                logger.info(
                                    f"Real-time classification failed for {symbol}: {article.title}"
                                )

                            # Acknowledge the message
                            self.r.xack(
                                self.NEWS_ARTICLE_STREAM,
                                "NewsProcessor_article_consumers",
                                message_id,
                            )

                # Allow other tasks to run
                await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Article stream listener error: {e}")
                await asyncio.sleep(5)

    async def classify_news(
        self,
        symbol: str,
        article: NewsArticle,
        thinking_mode: bool = False,
        current_time: Optional[str] = None,
    ) -> Tuple[Optional[NewsClassificationResult], str]:
        """
        Classify news article using DeepSeek LLM API.

        Determines if the news is a hot positive catalyst that could drive stock movement.

        Args:
            symbol: Stock ticker symbol
            article: NewsArticle object

        Returns:
            NewsClassificationResult with classification, score, and explanation
            Returns None if classification fails
        """
        return await asyncio.to_thread(
            self._classify_news_sync, symbol, article, thinking_mode, current_time
        )

    def _classify_news_sync(
        self,
        symbol: str,
        article: NewsArticle,
        thinking_mode: bool = False,
        current_time: Optional[str] = None,
    ) -> Tuple[Optional[NewsClassificationResult], str]:
        if not self.api_key:
            logger.debug("LLM api_key missing; skipping classification")
            return None, ""
        client = OpenAI(
            api_key=self.api_key,
            base_url="https://api.deepseek.com",
        )

        user_prompt = (
            f"Ticker: {symbol}\n"
            f"Title: {article.title}\n"
            f"Content: {article.text if article.text else 'N/A'}\n"
            f"Published Time: {article.published_time.isoformat()}\n"
            f"Current Time: {current_time}"
        )

        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            extra_body={"thinking": {"type": "enabled"}} if thinking_mode else None,
        )

        reasoning_content = (
            response.choices[0].message.reasoning_content if thinking_mode else ""
        )
        content = response.choices[0].message.content.strip()

        result_obj = self.parse_response(content)
        return result_obj, reasoning_content

    def parse_response(self, response_text: str) -> Optional[NewsClassificationResult]:
        """
        Parse LLM response text into NewsClassificationResult.

        Args:
            response_text: Raw response text from LLM

        Returns:
            NewsClassificationResult or None if parsing fails
        """
        try:
            parsed = json.loads(response_text)
            classification = parsed.get("Classification", "NO")
            score = parsed.get("Score", "0/10")
            explanation = parsed.get("Explanation", "")

            return NewsClassificationResult(
                classification=classification.upper(),
                score=score,
                explanation=explanation,
            )
        except Exception as e:
            logger.warning(f"Failed to parse LLM response: {e}")
            return None

    async def _enqueue_task(self, task: NewsTask) -> None:
        if self._queue.full():
            logger.warning(
                f"News task queue full. Dropping task: {task.symbol} v{task.version}"
            )
            self._stats["skipped"] += 1
            return
        await self._queue.put(task)
        self._stats["enqueued"] += 1

    async def _worker_loop(self, worker_id: int) -> None:
        logger.info(f"NewsProcessor worker-{worker_id} started")
        while self._running:
            try:
                task = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            try:
                self._active_workers += 1
                await self._process_task(task)
                self._stats["processed"] += 1
            except Exception as e:
                self._stats["failed"] += 1
                logger.error(
                    f"Worker-{worker_id} failed processing {task.symbol} v{task.version}: {e}"
                )
            finally:
                self._active_workers = max(0, self._active_workers - 1)
                self._queue.task_done()

        logger.info(f"NewsProcessor worker-{worker_id} stopped")

    async def _process_task(self, task: NewsTask) -> None:
        lock = self._get_symbol_lock(task.symbol)
        async with lock:
            articles = self._load_latest_articles(task.symbol, self.article_limit)
            if not articles:
                logger.info(f"No articles found for {task.symbol} at v{task.version}")
                self._applied_versions.setdefault(task.symbol, {})[
                    "news"
                ] = task.version
                return

            logger.info(
                f"Processing news for {task.symbol} v{task.version} articles={len(articles)}"
            )

            for article in articles:
                result, reasoning = await self.classify_news(
                    task.symbol,
                    article,
                    thinking_mode=False,
                    current_time=task.timestamp,
                )
                if not result:
                    logger.info(
                        f"Classification failed for {task.symbol}: {article.title}"
                    )
                    continue

                logger.info(
                    f"{task.symbol} news classified: {result.classification} "
                    f"score={result.score} title={article.title}"
                )
                if reasoning:
                    logger.debug(f"Reasoning: {reasoning}")

                # TODO: save result to persistent store
                # Update summary hasNews field
                # summary_key = f"{self.SUMMARY_KEY_PREFIX}:{symbol}"
                # self.r.hset(summary_key, "hasNews", "1" if has_news else "0")
                # self.r.hset(summary_key, "lastUpdated", current_timestamp.isoformat())

            self._applied_versions.setdefault(task.symbol, {})["news"] = task.version

    def _load_latest_articles(self, symbol: str, limit: int) -> List[NewsArticle]:
        news_ticker_key = f"{self.NEWS_TICKER_PREFIX}:{symbol}"
        news_ids = self.r.zrevrange(news_ticker_key, 0, max(0, limit - 1))
        articles: List[NewsArticle] = []

        for news_id in news_ids:
            item_key = f"{self.NEWS_ITEM_PREFIX}:{news_id}"
            article = self.r.hgetall(item_key)
            if not article:
                continue

            try:
                articles.append(
                    NewsArticle(
                        symbol=symbol,
                        title=article.get("title", ""),
                        text=article.get("text", ""),
                        url=article.get("url", ""),
                        published_time=article.get("published_time", ""),
                        sources=article.get("sources", ""),
                    )
                )
            except Exception as e:
                logger.warning(f"Failed to build NewsArticle: {e}")

        return articles

    async def _monitor_loop(self) -> None:
        while self._running:
            try:
                logger.info(
                    "NewsProcessor monitor | "
                    f"queue={self._queue.qsize()} active={self._active_workers} "
                    f"enqueued={self._stats['enqueued']} processed={self._stats['processed']} "
                    f"failed={self._stats['failed']} skipped={self._stats['skipped']}"
                )
                await asyncio.sleep(self.monitor_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                await asyncio.sleep(self.monitor_interval)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="NewsProcessor - Task queue and worker pool"
    )
    parser.add_argument("--redis-host", type=str, default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--redis-db", type=int, default=0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--queue-maxsize", type=int, default=200)
    parser.add_argument("--article-limit", type=int, default=10)
    parser.add_argument("--monitor-interval", type=float, default=10.0)
    args = parser.parse_args()

    news_processor = NewsProcessor(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        worker_count=args.workers,
        queue_maxsize=args.queue_maxsize,
        article_limit=args.article_limit,
        monitor_interval=args.monitor_interval,
    )

    try:
        asyncio.run(news_processor.start())
    except KeyboardInterrupt:
        logger.info("Shutting down NewsProcessor...")
