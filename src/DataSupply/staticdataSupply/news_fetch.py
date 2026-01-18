"""
Docstring for DataSupply.snapshotDataSupply.news_fetch
News Fetch module to get latest news from different providers.
""" ""
import os
import time
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

from DataUtils.schema import NewsArticle, NewsFormatter
from utils.logger import setup_logger
from utils.momo_token import MoomooQuoteToken

logger = setup_logger(__name__, log_to_file=True)


class MoomooStockResolver:
    """
    Momo Stock Resolver
    """

    def __init__(self):
        self.base_url = "https://www.moomoo.com"
        self.search_api = "/api/headfoot-search"
        self.news_api = "/quote-api/quote-v2/get-news-list"
        self.token_generator = MoomooQuoteToken()

    def search_stock(
        self, symbol: str, lang: str = "en-us", site: str = "us"
    ) -> Optional[Dict]:
        """
        Search for stock using header/footer search API

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
            response = requests.get(url, params=params, headers=headers, timeout=10)

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

    def get_stock_info(self, symbol: str) -> Optional[Dict]:
        """
        Get complete stock information

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
        data = self.search_stock(symbol)
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

    def get_news_momo(
        self, symbol: str, pageSize: int = 6, **kwargs
    ) -> Optional[List[NewsArticle]]:
        """
        Get news for a stock

        Args:
            symbol: Stock symbol
            pageSize: Number of news items
            **kwargs: Additional parameters for news API

        Returns:
            List of NewsArticle objects or None
        """
        # 1. Get stock info
        stock_info = self.get_stock_info(symbol)
        if not stock_info:
            logger.error(f"Could not find stock info for {symbol}")
            return None

        # 2. Prepare parameters for news API
        params = {
            "stock_id": stock_info["stock_id"],
            "market_type": stock_info["marketType"],
            "type": kwargs.get("type", 0),
            "subType": kwargs.get("subType", 0),
            "pageSize": pageSize,
        }

        # Add optional timestamp
        if "_" in kwargs:
            params["_"] = kwargs["_"]
        else:
            params["_"] = int(time.time() * 1000)

        # 3. Generate quote-token
        quote_token = self.token_generator.generate_quote_token(params)
        logger.debug(f"Generated quote-token for {symbol}: {quote_token}")

        # 4. Prepare headers
        headers = {
            "quote-token": quote_token,
            "referer": f'https://www.moomoo.com/stock/{symbol.upper()}-{stock_info["market"].upper()}/news',
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
        }

        # 5. Make the request
        try:
            url = self.base_url + self.news_api
            response = requests.get(url, params=params, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data.get("code") == 0:
                    news_data = data.get("data", {})

                    # Transform into NewsArticle objects
                    articles = []
                    for item in news_data.get("list", []):
                        try:
                            article = NewsArticle.from_momo_web_response(
                                symbol.upper(), item
                            )
                            articles.append(article)
                        except Exception as e:
                            logger.error(
                                f"Parse news data error: {e}, raw data: {item}"
                            )
                            continue

                    logger.info(
                        f"Successfully fetched {len(articles)} news articles from Moomoo"
                    )
                    return articles
                else:
                    logger.error(f"News API error: {data.get('message')}")
            else:
                logger.error(f"HTTP {response.status_code}: {response.text[:200]}")

        except Exception as e:
            logger.error(f"News request failed for {symbol}: {e}")

        return None


class API_NewsFetchers:
    def __init__(self):
        self.FMP_API_KEY = os.getenv("FMP_API_KEY")
        self.FMP_BASE_URL = "https://financialmodelingprep.com/stable/news/stock"

        self.BENZINGA_API_KEY = os.getenv("BENZINGA_API_KEY")
        self.BENZINGA_BASE_URL = "https://api.benzinga.com/api/v2/news"

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; FinancialNewsBot/1.0)",
                "accept": "application/json",
            }
        )

    def fetch_news_fmp(
        self, symbol: str, limit: int = 5, timeout: int = 5
    ) -> List[NewsArticle]:
        """
        Fetch News from FMP API

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            limit: news results numbers
            timeout: connection timeout
        Returns:
            List of NewsArticle objects
        """
        params = {"symbols": symbol.upper(), "limit": limit, "apikey": self.FMP_API_KEY}

        try:
            logger.info(
                f"Start fetching {symbol} news data using FMP API, limit={limit}"
            )
            response = self.session.get(
                self.FMP_BASE_URL, params=params, timeout=timeout
            )
            response.raise_for_status()

            data = response.json()

            # Check return type
            if not isinstance(data, list):
                raise ValueError(
                    f"API Response data not matched, want List, got: {type(data)}"
                )

            # Transform into NewsArticle objects
            articles = []
            for item in data:
                try:
                    article = NewsArticle.from_fmp_api_response(item)
                    articles.append(article)
                except Exception as e:
                    logger.error(f"Parse news data error: {e}, raw data: {item}")
                    continue

            logger.info(f"Successfully fetched {len(articles)} news articles from FMP")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch news for {symbol}: {e}")
            raise

    def fetch_news_benzinga(
        self,
        symbol: str,
        page_size: int = 5,
        display_output: str = "full",
        timeout: int = 10,
    ) -> List[NewsArticle]:
        """
        Fetch News from Benzinga API

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            page_size: Number of news items to return (default: 5)
            display_output: 'full' or 'headline' (default: 'full')
            timeout: Connection timeout in seconds

        Returns:
            List of NewsArticle objects

        Example (in curl):
            curl --request GET \
            --url 'https://api.benzinga.com/api/v2/news?token={BENZINGA_API_KEY}&pageSize=5&displayOutput=full&tickers=AAPL' \
            --header 'accept: application/json'
        """
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
                f"Start fetching {symbol} news data using Benzinga API, page_size={page_size}"
            )

            response = self.session.get(
                self.BENZINGA_BASE_URL, params=params, timeout=timeout
            )
            response.raise_for_status()

            data = response.json()

            # Check if response is a list
            if not isinstance(data, list):
                logger.error(f"Unexpected API response format: {type(data)}")
                raise ValueError(
                    f"API Response data not matched, want List, got: {type(data)}"
                )

            # Check if any results were returned
            if len(data) == 0:
                logger.warning(f"No news found for {symbol}")
                return []

            # Transform into NewsArticle objects
            articles = []
            for item in data:
                try:
                    article = NewsArticle.from_benzinga_api_response(
                        symbol.upper(), item
                    )
                    articles.append(article)
                except Exception as e:
                    logger.error(f"Parse news data error: {e}, raw data: {item}")
                    continue

            logger.info(
                f"Successfully fetched {len(articles)} news articles from Benzinga"
            )
            return articles

        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {symbol} after {timeout}s")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching news for {symbol}: {e}")
            if response.status_code == 401:
                logger.error(
                    "Invalid Benzinga API token. Please check BENZINGA_API_KEY"
                )
            raise
        except Exception as e:
            logger.error(f"Failed to fetch news for {symbol}: {e}")
            raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Stock Latest News Fetcher")
    parser.add_argument(
        "--ticker", default="AAPL", help="ticker you want to fetch its news"
    )
    parser.add_argument(
        "--provider",
        default="momo",
        choices=["momo", "fmp", "benzinga"],
        help="choose provider to fetch news (momo/fmp/benzinga)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of news articles to fetch (default: 5)",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=15,
        help="Request timeout in seconds (default: 15)",
    )

    args = parser.parse_args()
    ticker = args.ticker
    provider = args.provider

    articles = None

    try:
        if provider == "momo":
            fetcher = MoomooStockResolver()
            info = fetcher.get_stock_info(ticker)

            if info:
                logger.info(f"✅ {ticker} stock_id: {info['stock_id']}")
                print(f"✅ {ticker} stock_id: {info['stock_id']}")
                articles = fetcher.get_news_momo(ticker, pageSize=args.limit)
            else:
                logger.error(f"Could not find stock info for {ticker}")
                print(f"❌ Could not find stock info for {ticker}")

        elif provider == "fmp":
            fetcher = API_NewsFetchers()
            articles = fetcher.fetch_news_fmp(
                symbol=ticker, limit=args.limit, timeout=args.timeout
            )

        elif provider == "benzinga":
            fetcher = API_NewsFetchers()
            articles = fetcher.fetch_news_benzinga(
                symbol=ticker, page_size=args.limit, display_output="full"
            )

        if articles:
            print(f"\n📰 Found {len(articles)} articles from {provider.upper()}:\n")
            print(NewsFormatter.format_json(articles))
        else:
            print(f"\n⚠️  No news found for {ticker} on {provider}")

    except Exception as e:
        logger.error(f"Error fetching news: {e}")
        import traceback

        traceback.print_exc()
        exit(1)
