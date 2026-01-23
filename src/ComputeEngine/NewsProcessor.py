import json
import logging
import os
from dataclasses import dataclass
from typing import Optional

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

    async def classify_news(
        self, symbol: str, article: NewsArticle
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
        import httpx

        if not self.api_key:
            logger.warning("DEEPSEEK_API_KEY not set, skipping LLM classification")
            return None

        system_prompt = """
        The user will provide a news json data for one ticker. Please analyze the news article and output them in JSON format.
        here is the analyze instruction:
        remember that my strategy is focus on US pre-market session ticker news momentum and only take day trading, not a typical long term investment analysis for news.
        so sometimes even the news is not good for long term, but enough for a short term day trading hype or catalyst, you should also classify it as YES, but warning me the risk in the Explanation filed of you json respone.
        based on the api fetched result,somes the full content will be N/A you can only analyze the title, which I will show you an example below, analyze the news article and determine if it represents a potential short term catalyst that could drive the stock price up, and response with a JSON format.

        Classification: "YES" or "NO", YES if this is catalyst news,"NO" if this is neutral,irrelevant or even negative news.
        Score: the highest is 10/10, the lowest is 0/10, the higher the score, the more confidence you are that this is a catalyst news.
        Explanation: briefly explain why you classify it as YES or NO, especially if there is any risk for short term trading. any important detail should be mentioned here in your own words.

        EXAMPLE INPUT:

        Title: "Coherus Oncology Publishes Preclinical And Clinical Biomarker Research In Molecular Cancer Therapeutics Describing The High Selectivity, Picomolar Binding Affinity And Significant Effector Mediated Killing Of CCR8+ Cells Of Its Investigational Anti-CCR8 Monoclonal Antibody;"
        Content: ""<p>Coherus Oncology, Inc. (NASDAQ:<a class="ticker" href="https://www.benzinga.com/quote/CHRS">CHRS</a>) today announced compelling six-year overall survival (OS) follow-up results from the Phase 3 JUPITER-02 trial evaluating LOQTORZI (toripalimab-tpzi) plus chemotherapy in recurrent or metastatic nasopharyngeal carcinoma (RM-NPC). The findings reveal a striking and durable survival advantage that underscores the urgent clinical need to incorporate LOQTORZI with chemotherapy as first-line treatment.</p><p>In this exploratory post-hoc analysis, patients receiving LOQTORZI plus gemcitabine and cisplatin achieved a median OS of 64.8 months, nearly double that of chemotherapy alone (33.7 months), representing a 31-month improvement and an observed 38% reduction in risk of death (HR 0.62; 95% CI, 0.45–0.85). These results, presented at ESMO Asia 2025, signal a step change in cancer patient survival, reinforcing LOQTORZI's role in transforming outcomes for people living with RM-NPC.</p><p>JUPITER-02 is a randomized, double-blind, placebo-controlled Phase 3 study evaluating LOQTORZI with chemotherapy in first-line RM-NPC, and this long-term follow-up provides additional context for the previously reported survival outcomes.</p><p><strong>A Meaningful Shift for Patients Who Need It Most</strong></p><p>RM-NPC is an aggressive cancer, and long-term survival with standard chemotherapy can be limited for many patients. The multi-year survival observed in the LOQTORZI arm suggests a potential for meaningful clinical benefit, which may translate into longer survival for patients who typically face a challenging prognosis.</p><p>"The new 6-year overall survival follow up data gives us even greater confidence to use toripalimab in patients with NPC that is recurrent or metastatic," said Victoria Villaflor, MD, Professor and Director, Head and Neck Oncology Program, Division of Hematology-Oncology, Department of Medicine, UC Irvine School of Medicine.</p><p>For many patients, the difference between 33 months and nearly 65 months represents the possibility of more time with family and more milestones. This meaningful extension highlights why oncologists may consider adding LOQTORZI to chemotherapy upfront, as delaying or omitting a therapy associated with improved survival outcomes could reduce a patient's opportunity to achieve longer-term benefit.</p>"

        EXAMPLE JSON OUTPUT:
        {"Classification": "Yes", "Score": "7/10", "Explanation": "good long term survival data, and also can be seen as short term trading catalyst."}

        EXAMPLE INPUT:

        Title: "Nvidia GPU Deal Sends This Stock Soaring Over 114% After-Hours: Here's What Happened"
        Content: "N/A"

        EXAMPLE JSON OUTPUT:
        {"Classification": "No", "Score": "0/10", "Explanation": "it's only a market explanation, no specific catalyst news for the ticker."}

        EXAMPLE INPUT:

        Title: "Nuburu Announced That It Has Secured Operating Control Of Orbit S.r.l., A Revenue-generating Software-as-a-service Company Focused On Operational Resilience, Risk Intelligence, And Mission-critical Decision Support"
        Content: "N/A"

        EXAMPLE JSON OUTPUT:
        {"Classification": "Yes", "Score": "7/10", "Explanation": "good partnership news, but the company is still in early stage with no revenue yet, so there is risk for short term trading."}
        """

        user_prompt = f"""
Title: {article.title}
Content: {article.text if article.text else 'N/A'}
"""

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(
                    self.base_url,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "deepseek-chat",
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                        "response_format": {"type": "json_object"},
                    },
                )

                if response.status_code == 200:
                    result = response.json()
                    content = result["choices"][0]["message"]["content"].strip()

                    # Parse JSON response
                    parsed = json.loads(content)

                    # Handle case-insensitive keys
                    classification = (
                        parsed.get("Classification")
                        or parsed.get("classification")
                        or parsed.get("CLASSIFICATION", "NO")
                    )
                    score = (
                        parsed.get("Score")
                        or parsed.get("score")
                        or parsed.get("SCORE", "0/10")
                    )
                    explanation = (
                        parsed.get("Explanation")
                        or parsed.get("explanation")
                        or parsed.get("EXPLANATION", "")
                    )

                    result_obj = NewsClassificationResult(
                        classification=classification.upper(),
                        score=score,
                        explanation=explanation,
                    )

                    logger.debug(
                        f"News classification for {symbol}: {content} - {article.title[:50]}"
                    )
                    return result_obj
                else:
                    logger.warning(
                        f"DeepSeek API error: {response.status_code} - {response.text[:200]}"
                    )
                    return None

        except Exception as e:
            logger.warning(f"LLM classification request failed: {e}")
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
        print(f"❌ No news articles found for {symbol}")
        exit(0)

    print(f"\n📰 Analyzing {len(articles)} articles for {symbol}...\n")

    async def classify_all():
        for i, article in enumerate(articles, 1):
            try:
                result = await news_classificator.classify_news(symbol, article)
                print(f"[{i}] {article.title[:80]}...")
                if result:
                    catalyst_emoji = "✅" if result.is_catalyst else "❌"
                    print(
                        f"    {catalyst_emoji} Classification: {result.classification}"
                    )
                    print(f"    📊 Score: {result.score}")
                    print(f"    💡 Explanation: {result.explanation}")
                else:
                    print(f"    ⚠️  Classification failed")
                print()
            except Exception as e:
                logger.warning(f"News classification failed for article {i}: {e}")

    asyncio.run(classify_all())
