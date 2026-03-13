"""
Docstring for DataSupply.snapshot_data_supply.news_fetch
News Fetch module to get latest news from different providers.

- Async methods: get_news_momo_async(), fetch_news_fmp(), fetch_news_benzinga() - for streaming
"""

import asyncio
import hashlib
import json
import os
import re
import time
from datetime import datetime
from typing import AsyncGenerator, Dict, List, Optional
from zoneinfo import ZoneInfo

import httpx
import psycopg
from dotenv import load_dotenv

load_dotenv()

from jerry_trader.schema import NewsArticle, NewsFormatter
from jerry_trader.utils.logger import setup_logger
from jerry_trader.utils.momo_token import MoomooQuoteToken

logger = setup_logger(__name__, log_to_file=True)


# ============================================================================
# Proxy Configuration Helper
# ============================================================================


def _get_httpx_client_kwargs() -> Dict:
    """
    Get httpx.AsyncClient kwargs with proxy support.

    Handles SOCKS proxies via HTTP_PROXY environment variable.
    For SOCKS proxies, httpx[socks] must be installed.

    Returns:
        Dict of kwargs to pass to httpx.AsyncClient()
    """
    proxy_url = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    kwargs = {}

    if proxy_url:
        # httpx uses 'proxy' (singular), not 'proxies'
        kwargs["proxy"] = proxy_url
        # logger.debug(f"Using proxy: {proxy_url}")

    return kwargs


# ============================================================================
# News Persistence Layer (PostgreSQL)
# ============================================================================


class NewsPersistence:
    """
    Persist news articles to PostgreSQL database.

    Schema:
        news_events (
            news_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            published_at TIMESTAMPTZ NOT NULL,
            fetched_at TIMESTAMPTZ NOT NULL,
            source TEXT,
            title TEXT NOT NULL,
            summary TEXT,
            url TEXT
        )
    """

    def __init__(self, database_url: Optional[str] = None):

        self.database_url = database_url

        if not self.database_url:
            logger.warning("DATABASE_URL not set, news persistence disabled")
        else:
            # Convert SQLAlchemy URL format to libpq format
            # e.g., "postgresql+psycopg://..." -> "postgresql://..."
            self.database_url = self.database_url.replace(
                "postgresql+psycopg://", "postgresql://"
            )
        self._ensure_table()

    def _get_connection(self):
        """Get a database connection."""
        if not self.database_url:
            return None
        return psycopg.connect(self.database_url)

    def _ensure_table(self):
        """Create news_events table if it doesn't exist."""
        if not self.database_url:
            return

        create_sql = """
        CREATE TABLE IF NOT EXISTS news_events (
            news_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            published_at TIMESTAMPTZ NOT NULL,
            fetched_at TIMESTAMPTZ NOT NULL,
            source TEXT,
            title TEXT NOT NULL,
            summary TEXT,
            url TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_news_events_symbol ON news_events(symbol);
        CREATE INDEX IF NOT EXISTS idx_news_events_published_at ON news_events(published_at DESC);
        CREATE INDEX IF NOT EXISTS idx_news_events_symbol_published ON news_events(symbol, published_at DESC);
        """

        try:
            conn = self._get_connection()
            if conn:
                with conn.cursor() as cur:
                    cur.execute(create_sql)
                conn.commit()
                conn.close()
                logger.info("news_events table ensured")
        except Exception as e:
            logger.error(f"Failed to create news_events table: {e}")

    def save_articles(self, articles: List[NewsArticle]) -> int:
        """
        Save news articles to database.

        Uses UPSERT (INSERT ... ON CONFLICT) to avoid duplicates.

        Args:
            articles: List of NewsArticle objects

        Returns:
            Number of articles saved/updated
        """
        if not self.database_url or not articles:
            return 0

        insert_sql = """
        INSERT INTO news_events (news_id, symbol, published_at, fetched_at, source, title, summary, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (news_id) DO NOTHING
        """

        now = datetime.now(ZoneInfo("America/New_York"))

        rows = []
        for article in articles:
            # Generate stable news ID from URL
            news_id = hashlib.md5(article.url.encode()).hexdigest()[:16]

            rows.append(
                (
                    news_id,
                    article.symbol,
                    article.published_time,
                    now,
                    article.sources,
                    article.title,
                    article.text if article.text else None,
                    article.url,
                )
            )

        try:
            conn = self._get_connection()
            if conn:
                with conn.cursor() as cur:
                    cur.executemany(insert_sql, rows)
                conn.commit()
                conn.close()
                logger.debug(f"Saved {len(rows)} news articles to database")
                return len(rows)
        except Exception as e:
            logger.error(f"Failed to save news articles: {e}")
            return 0

    def get_articles(self, symbol: str, limit: int = 10) -> List[Dict]:
        """
        Get news articles for a symbol from database.

        Args:
            symbol: Stock ticker symbol
            limit: Maximum number of articles to return

        Returns:
            List of article dictionaries
        """
        if not self.database_url:
            return []

        select_sql = """
        SELECT news_id, symbol, published_at, fetched_at, source, title, summary, url
        FROM news_events
        WHERE symbol = %s
        ORDER BY published_at DESC
        LIMIT %s
        """

        try:
            conn = self._get_connection()
            if conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql, (symbol.upper(), limit))
                    rows = cur.fetchall()
                conn.close()

                articles = []
                for row in rows:
                    articles.append(
                        {
                            "news_id": row[0],
                            "symbol": row[1],
                            "published_time": row[2].isoformat() if row[2] else None,
                            "fetched_at": row[3].isoformat() if row[3] else None,
                            "sources": row[4],
                            "title": row[5],
                            "text": row[6],
                            "url": row[7],
                        }
                    )
                return articles
        except Exception as e:
            logger.error(f"Failed to get news articles for {symbol}: {e}")
            return []

    def get_articles_in_window(
        self,
        symbols: List[str],
        start: "datetime",
        end: "datetime",
    ) -> List[Dict]:
        """
        Get articles published between *start* (exclusive) and *end* (inclusive)
        for any of the given symbols.  Used by the replay loop.

        Args:
            symbols: List of ticker symbols.
            start: Lower bound (exclusive) — typically the previous snapshot time.
            end:   Upper bound (inclusive) — the current snapshot time.

        Returns:
            List of article dicts (same shape as get_articles).
        """
        if not self.database_url or not symbols:
            return []

        select_sql = """
        SELECT news_id, symbol, published_at, fetched_at, source, title, summary, url
        FROM news_events
        WHERE symbol = ANY(%s)
          AND published_at > %s
          AND published_at <= %s
        ORDER BY published_at ASC
        """

        try:
            conn = self._get_connection()
            if conn:
                with conn.cursor() as cur:
                    cur.execute(
                        select_sql,
                        ([s.upper() for s in symbols], start, end),
                    )
                    rows = cur.fetchall()
                conn.close()

                articles = []
                for row in rows:
                    articles.append(
                        {
                            "news_id": row[0],
                            "symbol": row[1],
                            "published_time": row[2].isoformat() if row[2] else None,
                            "fetched_at": row[3].isoformat() if row[3] else None,
                            "sources": row[4],
                            "title": row[5],
                            "text": row[6],
                            "url": row[7],
                        }
                    )
                return articles
        except Exception as e:
            logger.error(f"Failed to get articles in window: {e}")
            return []


class MoomooStockResolver:
    """
    Momo Stock Resolver
    """

    def __init__(self):
        self.base_url = "https://www.moomoo.com"
        self.search_api = "/api/headfoot-search"
        self.news_api = "/quote-api/quote-v2/get-news-list"
        self.token_generator = MoomooQuoteToken()
        self.waf_token = os.getenv("MOOMOO_WAF_TOKEN", "")

    async def search_stock(
        self, symbol: str, lang: str = "en-us", site: str = "us"
    ) -> Optional[Dict]:
        """
        Search for stock using header/footer search API.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            lang: Language code
            site: Site code (us, hk, etc.)

        Returns:
            Stock info dict or None if not found
        """
        params = {"keyword": symbol.lower(), "lang": lang, "site": site}

        headers = {
            "referer": f"https://www.moomoo.com/{site}/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }

        try:
            url = self.base_url + self.search_api
            client_kwargs = _get_httpx_client_kwargs()
            async with httpx.AsyncClient(**client_kwargs) as client:
                response = await client.get(
                    url, params=params, headers=headers, timeout=10
                )

            if response.status_code == 200:
                data = response.json()
                if data.get("code") == 0:
                    return data.get("data", {})
                else:
                    logger.error(f"API returned error: {data.get('message')}")
            else:
                logger.error(f"HTTP {response.status_code}: {response.text[:200]}")

        except Exception as e:
            logger.error(f"Search failed for {symbol}: {e}")

        return None

    def extract_stock_id(self, symbol: str, data: Dict) -> Optional[str]:
        """
        Extract stock_id from search response

        Args:
            symbol: Stock symbol to match (uppercase)
            data: Search response data dict

        Returns:
            stock_id string or None
        """
        symbol = symbol.upper()

        # Try to find in quote section first
        for section in ["quote", "stock"]:
            if section in data:
                for item in data[section]:
                    # Match by stockSymbol (most reliable)
                    if item.get("stockSymbol", "").upper() == symbol:
                        stock_id = str(item.get("stockId", ""))
                        if stock_id:
                            logger.info(
                                f"Found {symbol} in {section}: stock_id={stock_id}"
                            )
                            return stock_id

        logger.warning(f"Could not find {symbol} in response")
        return None

    async def get_stock_info(self, symbol: str) -> Optional[Dict]:
        """
        Get complete stock information.

        Returns:
            {
                'symbol': 'AAPL',
                'stock_id': '205189',
                'market': 'us',
                'marketType': 2,
                'stockName': 'Apple',
                'hasOption': True
            }
        """
        data = await self.search_stock(symbol)
        if not data:
            logger.warning(f"No search data returned for {symbol}")
            return None

        stock_id = self.extract_stock_id(symbol, data)
        if not stock_id:
            logger.warning(f"Could not extract stock_id for {symbol}")
            return None

        # Find the matching stock entry to get full info
        for section in ["quote", "stock"]:
            if section in data:
                for item in data[section]:
                    if str(item.get("stockId", "")) == stock_id:
                        return {
                            "symbol": item.get("stockSymbol", ""),
                            "stock_id": stock_id,
                            "market": item.get("market", ""),
                            "marketType": item.get("marketType", 0),
                            "stockName": item.get("stockName", ""),
                            "hasOption": item.get("hasOption", False),
                            "symbol_full": item.get("symbol", ""),
                        }

        return None

    # ============================================================================
    # Async Methods for Streaming
    # ============================================================================

    async def get_news_momo_async(
        self,
        symbol: str,
        pageSize: int = 6,
        content_limiter=None,
        **kwargs,
    ) -> AsyncGenerator[NewsArticle, None]:
        """
        Async generator that yields Moomoo news articles one by one.

        First fetches the news list, then yields each article.
        Content is fetched separately with rate limiting.

        Args:
            symbol: Stock symbol
            pageSize: Number of news items
            content_limiter: Optional AsyncRateLimiter for content fetching
            **kwargs: Additional parameters for news API

        Yields:
            NewsArticle objects one at a time
        """
        # 1. Get stock info (async call, can be cached in future)
        stock_info = await self.get_stock_info(symbol)
        if not stock_info:
            logger.error(f"Could not find stock info for {symbol}")
            return

        # 2. Prepare parameters for news API
        params = {
            "stock_id": stock_info["stock_id"],
            "market_type": stock_info["marketType"],
            "type": kwargs.get("type", 0),
            "subType": kwargs.get("subType", 0),
            "pageSize": pageSize,
        }

        if "_" in kwargs:
            params["_"] = kwargs["_"]
        else:
            params["_"] = int(time.time() * 1000)

        # 3. Generate quote-token
        quote_token = self.token_generator.generate_quote_token(params)

        # 4. Prepare headers
        headers = {
            "quote-token": quote_token,
            "referer": f'https://www.moomoo.com/stock/{symbol.upper()}-{stock_info["market"].upper()}/news',
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
        }

        # 5. Fetch news list asynchronously
        try:
            url = self.base_url + self.news_api
            client_kwargs = _get_httpx_client_kwargs()
            async with httpx.AsyncClient(**client_kwargs) as client:
                response = await client.get(
                    url, params=params, headers=headers, timeout=10
                )

            if response.status_code != 200:
                logger.error(f"HTTP {response.status_code}: {response.text[:200]}")
                return

            data = response.json()
            if data.get("code") != 0:
                logger.error(f"News API error: {data.get('message')}")
                return

            news_data = data.get("data", {})
            news_list = news_data.get("list", [])

            logger.info(f"Fetched {len(news_list)} news items from Moomoo for {symbol}")

            # 6. Yield each article, fetching content individually
            for item in news_list:
                try:
                    article_url = item.get("url", "")

                    # Fetch content with rate limiting if available
                    if article_url:
                        if content_limiter:
                            async with content_limiter:
                                content = await self._fetch_content_async(article_url)
                        else:
                            content = await self._fetch_content_async(article_url)
                        item["text"] = content

                    article = NewsArticle.from_momo_web_response(symbol.upper(), item)
                    yield article

                except Exception as e:
                    logger.error(f"Parse news data error: {e}, raw data: {item}")
                    continue

        except Exception as e:
            logger.error(f"News request failed for {symbol}: {e}")
            raise

    async def _fetch_content_async(self, url: str, timeout: int = 10) -> Optional[str]:
        """
        Async version of fetch_content.

        Args:
            url: Article URL
            timeout: Request timeout in seconds

        Returns:
            Article content text or None if fetch fails
        """
        from lxml import html

        # Handle URL transformation
        if url.startswith("https://news.moomoo.com/flash/"):
            url = url.replace(
                "https://news.moomoo.com/flash/", "https://www.moomoo.com/news/flash/"
            )
            logger.debug(
                f"Transformed URL from news.moomoo.com/flash to www.moomoo.com/news/flash."
            )

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:146.0) Gecko/20100101 Firefox/146.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Sec-GPC": "1",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "no-cache",
            "Cookie": f"wafToken={self.waf_token}; locale=en-us",
        }

        try:
            client_kwargs = _get_httpx_client_kwargs()
            async with httpx.AsyncClient(**client_kwargs) as client:
                resp = await client.get(url, headers=headers, timeout=timeout)
                resp.raise_for_status()

            tree = html.fromstring(resp.text)
            if url.startswith("https://www.moomoo.com/community/feed/"):
                # | pup 'div.feed-detail div.rich-text-wrapper text{}' \
                # | sed 's/&#39;/'\''/g; s/&quot;/"/g; s/&amp;/\&/g' \
                # | tr -s ' ' \
                # | sed '/^[[:space:]]*$/d; s/^[[:space:]]*//; s/[[:space:]]*$//' \
                # | awk 'length($0) > 20 {print $0 "\n"}
                content_divs = tree.xpath(
                    "//div[contains(@class, 'feed-detail')]//div[contains(@class, 'rich-text-wrapper')]"
                )
                full_text = content_divs[0].text_content().strip()

                # Clean up HTML entities
                full_text = full_text.replace("&#39;", "'")
                full_text = full_text.replace("&quot;", '"')
                full_text = full_text.replace("&amp;", "&")

                # Remove extra whitespace and empty lines
                full_text = re.sub(r" +", " ", full_text)  # Collapse multiple spaces
                lines = [line.strip() for line in full_text.split("\n") if line.strip()]

                # Keep only lines longer than 20 characters and join with newlines
                lines = [line for line in lines if len(line) > 20]
                full_text = "\n\n".join(lines)
            else:
                content_divs = tree.xpath(
                    "//div[contains(@class, 'inner') and contains(@class, 'origin_content')]"  # | pup 'div.inner.origin_content * text{}'
                )

                full_text = content_divs[0].text_content().strip()

                if full_text.endswith("Read more"):
                    full_text = full_text[:-9].strip()

            if not full_text:
                logger.warning(f"Could not find content for URL: {url}")
                return None

            logger.info(
                f"_fetch_content_async - Successfully fetched moomoo article content"
            )

            return full_text

        except Exception as e:
            logger.error(f"Failed to fetch content from {url}: {e}")
            return None


class API_NewsFetchers:
    def __init__(self):
        self.FMP_API_KEY = os.getenv("FMP_API_KEY")
        self.FMP_BASE_URL = "https://financialmodelingprep.com/stable/news/stock"

        self.BENZINGA_API_KEY = os.getenv("BENZINGA_API_KEY")
        self.BENZINGA_BASE_URL = "https://api.benzinga.com/api/v2/news"

    async def fetch_news_fmp(
        self,
        symbol: str,
        limit: int = 5,
        timeout: int = 10,
    ) -> AsyncGenerator[NewsArticle, None]:
        """
        Async generator that yields FMP news articles one by one.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            limit: Number of news results
            timeout: Connection timeout in seconds

        Yields:
            NewsArticle objects one at a time
        """
        if not self.FMP_API_KEY:
            logger.error("FMP_API_KEY not found in environment variables")
            raise ValueError("FMP_API_KEY is required")

        params = {"symbols": symbol.upper(), "limit": limit, "apikey": self.FMP_API_KEY}
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; FinancialNewsBot/1.0)",
            "Accept": "application/json",
        }

        try:
            logger.info(
                f"Start fetching {symbol} news data using FMP API (async), limit={limit}"
            )

            client_kwargs = _get_httpx_client_kwargs()
            async with httpx.AsyncClient(**client_kwargs) as client:
                response = await client.get(
                    self.FMP_BASE_URL,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                )
                response.raise_for_status()

            data = response.json()

            if not isinstance(data, list):
                raise ValueError(
                    f"API Response data not matched, want List, got: {type(data)}"
                )

            if len(data) == 0:
                logger.warning(f"No news found for {symbol} from FMP")
                return

            logger.info(f"Fetched {len(data)} news items from FMP for {symbol}")

            for item in data:
                try:
                    article = NewsArticle.from_fmp_api_response(item)
                    yield article
                except Exception as e:
                    logger.error(f"Parse news data error: {e}, raw data: {item}")
                    continue

        except httpx.TimeoutException:
            logger.error(f"FMP request timeout for {symbol} after {timeout}s")
            raise
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching FMP news for {symbol}: {e}")
            raise
        except Exception as e:
            logger.error(f"fetch_news_fmp - Failed to fetch news for {symbol}: {e}")
            raise

    async def fetch_news_benzinga(
        self,
        symbol: str,
        page_size: int = 5,
        display_output: str = "full",
        timeout: int = 10,
    ) -> AsyncGenerator[NewsArticle, None]:
        """
        Fetch News from Benzinga API
        Async generator that yields Benzinga news articles one by one.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            page_size: Number of news items to return (default: 5)
            display_output: 'full' or 'headline' (default: 'full')
            timeout: Connection timeout in seconds


        Yields:
            NewsArticle objects one at a time

        Example (in curl):
            curl --request GET \
            --url 'https://api.benzinga.com/api/v2/news?token={BENZINGA_API_KEY}&pageSize=5&displayOutput=full&tickers=AAPL' \
            --header 'accept: application/json'
        """
        headers = {
            "Accept": "application/json",
        }
        if not self.BENZINGA_API_KEY:
            logger.error("BENZINGA_API_KEY not found in environment variables")
            raise ValueError("BENZINGA_API_KEY is required")

        params = {
            "token": self.BENZINGA_API_KEY,
            "pageSize": page_size,
            "displayOutput": display_output,
            "tickers": symbol.upper(),
        }

        try:
            logger.info(
                f"Start fetching {symbol} news data using Benzinga API (async), page_size={page_size}"
            )

            client_kwargs = _get_httpx_client_kwargs()
            async with httpx.AsyncClient(**client_kwargs) as client:
                response = await client.get(
                    self.BENZINGA_BASE_URL,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                )
                response.raise_for_status()

            data = json.loads(response.text)
            # print(data)

            if not isinstance(data, list):
                logger.error(f"Unexpected API response format: {type(data)}")
                raise ValueError(
                    f"API Response data not matched, want List, got: {type(data)}"
                )

            # Check if any results were returned
            if len(data) == 0:
                logger.warning(f"No news found for {symbol}")
                return

            logger.info(f"Fetched {len(data)} news items from Benzinga for {symbol}")

            # Yield each article
            for item in data:
                try:
                    article = NewsArticle.from_benzinga_api_response(
                        symbol.upper(), item
                    )
                    yield article
                except Exception as e:
                    logger.error(f"Parse news data error: {e}, raw data: {item}")
                    continue

        except httpx.TimeoutException:
            logger.error(f"Request timeout for {symbol} after {timeout}s")
            raise
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching news for {symbol}: {e}")
            if e.response.status_code == 401:
                logger.error(
                    "Invalid Benzinga API token. Please check BENZINGA_API_KEY"
                )
            raise
        except Exception as e:
            logger.error(
                f"fetch_news_benzinga - Failed to fetch news for {symbol}: {e}"
            )
            raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch stock news articles")
    parser.add_argument(
        "--ticker", type=str, required=True, help="Stock ticker symbol (e.g., AAPL)"
    )
    parser.add_argument(
        "--provider",
        type=str,
        choices=["momo", "fmp", "benzinga"],
        default="momo",
        help="News provider to fetch from",
    )
    parser.add_argument(
        "--limit", type=int, default=5, help="Number of news articles to fetch"
    )
    parser.add_argument(
        "--fetch-content", action="store_true", help="Fetch full article content"
    )

    args = parser.parse_args()

    async def main():
        if args.provider == "fmp":
            fetcher = API_NewsFetchers()
            async for article in fetcher.fetch_news_fmp(args.ticker, limit=args.limit):
                print(f"Article: {article.title}")
                print(f"  URL: {article.url}")
                print(f"  Published: {article.published_time}")
                print(f"  Text: {article.text if article.text else 'N/A'}...")
                print("-" * 40)
        elif args.provider == "benzinga":
            fetcher = API_NewsFetchers()
            async for article in fetcher.fetch_news_benzinga(
                args.ticker, page_size=args.limit
            ):
                print(f"Article: {article.title}")
                print(f"  URL: {article.url}")
                print(f"  Published: {article.published_time}")
                print(f"  Text: {article.text if article.text else 'N/A'}...")
                print("-" * 40)
        else:
            fetcher = MoomooStockResolver()
            async for article in fetcher.get_news_momo_async(
                args.ticker, pageSize=args.limit
            ):
                print(f"Article: {article.title}")
                print(f"  URL: {article.url}")
                print(f"  Published: {article.published_time}")
                print(f"  Text: {article.text if article.text else 'N/A'}...")
                print("-" * 40)

    asyncio.run(main())
