"""News Article Adapters

Converts external API responses to NewsArticle domain models.
These are adapters - they know about external APIs but produce pure domain objects.
"""

import html
import logging
import re
from datetime import datetime
from typing import Dict, Optional
from zoneinfo import ZoneInfo

from jerry_trader.domain.news.article import NewsArticle

logger = logging.getLogger(__name__)


def _strip_html(text: Optional[str]) -> Optional[str]:
    """Strip HTML tags and decode HTML entities from text."""
    if not text:
        return text
    # Remove HTML tags
    clean = re.sub(r"<[^>]+>", "", text)
    # Decode HTML entities (&#8217; -> ', &amp; -> &, etc.)
    clean = html.unescape(clean)
    # Normalize whitespace
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


class NewsArticleAdapter:
    """Adapters for converting external API responses to NewsArticle domain models"""

    @staticmethod
    def from_fmp_api_response(data: Dict) -> NewsArticle:
        """Create NewsArticle from FMP API response

        Args:
            data: FMP API response dict

        Returns:
            NewsArticle domain model
        """
        try:
            published_time = datetime.strptime(
                data["publishedDate"], "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=ZoneInfo("America/New_York"))
        except (KeyError, ValueError) as e:
            logger.warning(f"FMP date parse error: {e}, using current time")
            published_time = datetime.now().astimezone(ZoneInfo("America/New_York"))

        return NewsArticle(
            symbol=data.get("symbol", ""),
            published_time=published_time,
            title=data.get("title", ""),
            text=data.get("text"),
            url=data.get("url", ""),
            sources=data.get("publisher", "FMP"),
            source_from="fmp",
        )

    @staticmethod
    def from_momo_web_response(symbol: str, data: Dict) -> NewsArticle:
        """Create NewsArticle from Moomoo web response

        Args:
            symbol: Stock ticker symbol
            data: Moomoo web response dict

        Returns:
            NewsArticle domain model
        """
        try:
            published_time = datetime.fromtimestamp(data["time"]).astimezone(
                ZoneInfo("America/New_York")
            )
        except (KeyError, ValueError) as e:
            logger.warning(f"Moomoo timestamp parse error: {e}, using current time")
            published_time = datetime.now().astimezone(ZoneInfo("America/New_York"))

        return NewsArticle(
            symbol=symbol,
            published_time=published_time,
            title=data.get("title", ""),
            text=data.get("text", ""),
            url=data.get("url", ""),
            sources=data.get("sources", "Moomoo"),
            source_from="momo",
        )

    @staticmethod
    def from_benzinga_api_response(symbol: str, data: Dict) -> NewsArticle:
        """Create NewsArticle from Benzinga API response

        Args:
            symbol: Stock ticker symbol
            data: Benzinga API response dict

        Returns:
            NewsArticle domain model
        """
        try:
            # Benzinga format: "Wed, 03 Dec 2025 08:34:03 -0400"
            published_time = datetime.strptime(
                data["created"], "%a, %d %b %Y %H:%M:%S %z"
            ).astimezone(ZoneInfo("America/New_York"))
        except (KeyError, ValueError) as e:
            logger.warning(f"Benzinga date parse error: {e}, using current time")
            published_time = datetime.now().astimezone(ZoneInfo("America/New_York"))

        # Strip HTML tags from body content
        body_text = _strip_html(data.get("body"))

        return NewsArticle(
            symbol=symbol,
            published_time=published_time,
            title=data.get("title", ""),
            text=body_text,
            url=data.get("url", ""),
            sources=data.get("author", "Benzinga"),
            source_from="benzinga",
        )
