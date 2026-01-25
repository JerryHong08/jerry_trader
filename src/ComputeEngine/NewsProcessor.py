import json
import logging
import os
from dataclasses import dataclass
from typing import Optional
from zoneinfo import ZoneInfo

from openai import OpenAI

from config import load_prompt
from DataSupply.staticdataSupply.news_fetch import NewsPersistence
from DataUtils.schema import NewsArticle, NewsFormatter
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)


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


class NewsClassificator:
    """
    Classify news articles using DeepSeek LLM API.

    Determines if news is a positive catalyst that could drive stock movement.
    """

    def __init__(self):
        self.api_key = os.getenv("DEEPSEEK_API_KEY")
        self.base_url = "https://api.deepseek.com/v1/chat/completions"

        if not self.api_key:
            logger.warning(
                "DEEPSEEK_API_KEY not set, news classification will be disabled"
            )

        self.system_prompt = load_prompt("news_classificator_system_prompt.txt")

    async def classify_news(
        self, symbol: str, article: NewsArticle, thinking_mode: bool = False
    ) -> Optional[NewsClassificationResult]:
        """
        Classify news article using DeepSeek LLM API.

        Determines if the news is a positive catalyst that could drive stock movement.

        Args:
            symbol: Stock ticker symbol
            article: NewsArticle object

        Returns:
            NewsClassificationResult with classification, score, and explanation
            Returns None if classification fails
        """

        client = OpenAI(
            api_key=os.environ.get("DEEPSEEK_API_KEY"),
            base_url="https://api.deepseek.com",
        )

        user_prompt = f"""
        Ticker: {symbol}
        Title: {article.title}
        Content: {article.text if article.text else 'N/A'}
        """

        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            # stream=False,
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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="News Classifier - Fetch and analyze news for a stock ticker"
    )
    parser.add_argument(
        "--ticker",
        type=str,
        required=True,
        help="Stock ticker symbol to analyze news for",
    )
    parser.add_argument(
        "--provider",
        type=str,
        default="benzinga",
        choices=["momo", "fmp", "benzinga"],
        help="News provider to fetch from (default: benzinga)",
    )
    parser.add_argument(
        "--limit", type=int, default=5, help="Number of articles to fetch (default: 5)"
    )
    parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Use cached articles from database instead of fetching fresh",
    )
    parser.add_argument(
        "--thinking-mode",
        action="store_true",
        help="Enable thinking mode for more transparent reasoning steps",
    )
    args = parser.parse_args()

    import asyncio

    from DataSupply.staticdataSupply.news_fetch import (
        API_NewsFetchers,
        MoomooStockResolver,
    )

    symbol = args.ticker.upper()
    news_persistence = NewsPersistence()
    news_classificator = NewsClassificator()

    articles = []

    if args.use_cache:
        # Load from database
        cached = news_persistence.get_articles(symbol=symbol, limit=args.limit)
        if cached:
            logger.info(
                f"Loaded {len(cached)} cached articles for {symbol} from database"
            )
            # Convert dict back to NewsArticle for classification
            for c in cached:
                articles.append(
                    NewsArticle(
                        symbol=c["symbol"],
                        title=c["title"],
                        text=c.get("text"),
                        url=c.get("url", ""),
                        published_time=c.get("published_time"),
                        sources=c.get("sources", ""),
                    )
                )
        else:
            logger.warning(f"No cached articles found for {symbol}")
    else:
        # Fetch fresh news from provider
        try:
            if args.provider == "momo":
                fetcher = MoomooStockResolver()
                articles = fetcher.get_news_momo(symbol, pageSize=args.limit) or []
            elif args.provider == "fmp":
                fetcher = API_NewsFetchers()
                articles = fetcher.fetch_news_fmp(symbol=symbol, limit=args.limit)
            elif args.provider == "benzinga":
                fetcher = API_NewsFetchers()
                articles = fetcher.fetch_news_benzinga(
                    symbol=symbol, page_size=args.limit
                )

            if articles:
                logger.info(
                    f"Fetched {len(articles)} articles for {symbol} from {args.provider}"
                )
                # Save to database for future use
                saved_count = news_persistence.save_articles(articles)
                logger.info(f"Saved {saved_count} articles to database")
            else:
                logger.warning(f"No articles fetched for {symbol} from {args.provider}")
        except Exception as e:
            logger.error(f"Failed to fetch news: {e}")
            articles = []

    if not articles:
        logger.info(f"❌ No news articles found for {symbol}")
        exit(0)

    logger.info(f"\n📰 Analyzing {len(articles)} articles for {symbol}...\n")

    async def classify_all():
        for i, article in enumerate(articles, 1):
            try:
                result, reasoning_content = await news_classificator.classify_news(
                    symbol, article, thinking_mode=args.thinking_mode
                )
                if result:
                    catalyst_emoji = "✅" if result.is_catalyst else "❌"
                    logger.info(
                        "\n=======================================================\n"
                        f"    {catalyst_emoji} Classification: {result.classification}\n"
                        f"    🕛 Published time: {article.published_time.astimezone(ZoneInfo('America/New_York'))}\n"
                        f"    🧾 [{i}] {article.title}\n"
                        f"    📊 Score: {result.score}\n"
                        f"    💡 Explanation: {result.explanation}\n"
                        f"    🔗 URL: {article.url}\n"
                        "======================================================="
                    )
                    if args.thinking_mode:
                        logger.info(f"    🤖 Reasoning Steps:\n{reasoning_content}\n")
                else:
                    logger.info(f"    ⚠️  Classification failed")
            except Exception as e:
                logger.warning(f"News classification failed for article {i}: {e}")

    asyncio.run(classify_all())
