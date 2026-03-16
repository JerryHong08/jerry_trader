"""Formatters for Domain Models

Presentation utilities for formatting domain models.
"""

import json
from typing import List

from jerry_trader.domain.news.article import NewsArticle


class NewsFormatter:
    """News Formatter Utility

    Formats NewsArticle domain models for presentation.
    """

    @staticmethod
    def format_json(articles: List[NewsArticle], indent: int = 2) -> str:
        """Format list of NewsArticle to JSON string

        Args:
            articles: List of NewsArticle domain models
            indent: JSON indentation level

        Returns:
            JSON string
        """
        data = [article.model_dump(mode="json") for article in articles]
        return json.dumps(data, indent=indent, ensure_ascii=False)

    @staticmethod
    def format_markdown(articles: List[NewsArticle]) -> str:
        """Format list of NewsArticle to Markdown

        Args:
            articles: List of NewsArticle domain models

        Returns:
            Markdown formatted string
        """
        lines = []
        for i, article in enumerate(articles, 1):
            time_str = article.published_time.strftime("%Y-%m-%d %H:%M")
            lines.append(f"## {i}. {article.title}")
            lines.append(f"**{article.symbol}** | {time_str} | {article.sources}")
            lines.append(f"[Read More]({article.url})")
            if article.text:
                lines.append(f"\n{article.text}...\n")
            lines.append("---\n")
        return "\n".join(lines)
