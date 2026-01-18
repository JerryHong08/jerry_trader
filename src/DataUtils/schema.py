import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import polars as pl
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

logger = logging.getLogger(__name__)


class FloatSourceData(BaseModel):
    source: str = Field(..., description="Data Source")
    float_shares: Optional[float] = None
    short_percent: Optional[float] = None  # 0-1 normalized
    outstanding_shares: Optional[float] = None


class FloatShares(BaseModel):
    ticker: str = Field(..., description="Ticker symbol")
    data: List[FloatSourceData]  # keep all sources
    timestamp: int = Field(
        default_factory=lambda: int(time.time()), description="Last updated time"
    )


class SnapshotMessage(BaseModel):
    ticker: str = Field(..., description="Stock ticker symbol")
    percent_change: float = Field(
        ..., description="Percentage change from previous close"
    )
    accumulated_volume: float = Field(
        ..., description="Total accumulated trading volume"
    )
    current_price: float = Field(..., description="Current stock price")
    prev_close: float = Field(..., description="Previous closing price")
    prev_volume: float = Field(..., description="Previous trading volume")
    timestamp: int = Field(..., description="Timestamp in milliseconds")


def validate_SnapshotMsg_schema(df: pl.DataFrame) -> tuple[bool, str]:
    """
    Fast DataFrame schema validation
    """
    required_schema = {
        "ticker": pl.Utf8,
        "percent_change": pl.Float64,
        "accumulated_volume": pl.Float64,
        "current_price": pl.Float64,
        "prev_close": pl.Float64,
        "prev_volume": pl.Float64,
        "timestamp": pl.Int64,
    }

    # Check columns
    missing = set(required_schema.keys()) - set(df.columns)
    if missing:
        return False, f"Missing columns: {missing}"

    # Check types
    for col, expected_type in required_schema.items():
        if df[col].dtype != expected_type:
            return (
                False,
                f"Column '{col}' type mismatch: {df[col].dtype} vs {expected_type}",
            )

    # Check for nulls
    null_counts = df.null_count()
    null_cols = [col for col in required_schema.keys() if null_counts[col][0] > 0]
    if null_cols:
        return False, f"Null values in columns: {null_cols}"

    return True, ""


def spot_check_SnapshotMsg_with_pydantic(
    df: pl.DataFrame, sample_size: int = 3
) -> bool:
    """
    Deep validation using Pydantic (sample check)
    """
    if len(df) == 0:
        return False

    sample_size = min(sample_size, len(df))
    records = df.head(sample_size).to_dicts()

    for i, record in enumerate(records):
        try:
            SnapshotMessage(**record)
        except ValidationError as e:
            print(f"❌ Pydantic validation failed for row {i}: {e}")
            return False

    return True


class NewsArticle(BaseModel):
    """NewsArticle Schema"""

    symbol: str = Field(
        ..., description="Stock ticker symbol", min_length=1, max_length=10
    )
    published_time: datetime = Field(..., description="News publication time")
    title: str = Field(..., description="News headline", min_length=1)
    text: Optional[str] = Field(None, description="News content body")
    url: str = Field(..., description="News article URL")
    sources: str = Field(..., description="News source/publisher")

    model_config = {"json_encoders": {datetime: lambda v: v.isoformat()}}

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate URL format"""
        if not v.startswith(("http://", "https://")):
            logger.warning(f"Invalid URL format: {v}")
        return v

    @field_validator("published_time", mode="before")
    @classmethod
    def parse_published_time(cls, v):
        """Auto-parse various datetime formats"""
        if isinstance(v, datetime):
            return v
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v).astimezone(ZoneInfo("America/New_York"))
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v)
            except ValueError:
                logger.warning(f"Could not parse datetime: {v}, using current time")
                return datetime.now().astimezone(ZoneInfo("America/New_York"))
        return v

    @classmethod
    def from_fmp_api_response(cls, data: Dict) -> "NewsArticle":
        """Create NewsArticle from FMP API response"""
        try:
            published_time = datetime.strptime(
                data["publishedDate"], "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=ZoneInfo("America/New_York"))
        except (KeyError, ValueError) as e:
            logger.warning(f"FMP date parse error: {e}, using current time")
            published_time = datetime.now().astimezone(ZoneInfo("America/New_York"))

        return cls(
            symbol=data.get("symbol", ""),
            published_time=published_time,
            title=data.get("title", ""),
            text=data.get("text"),
            url=data.get("url", ""),
            sources=data.get("publisher", "FMP"),
        )

    @classmethod
    def from_momo_web_response(cls, symbol: str, data: Dict) -> "NewsArticle":
        """Create NewsArticle from Moomoo web response"""
        try:
            published_time = datetime.fromtimestamp(data["time"]).astimezone(
                ZoneInfo("America/New_York")
            )
        except (KeyError, ValueError) as e:
            logger.warning(f"Moomoo timestamp parse error: {e}, using current time")
            published_time = datetime.now().astimezone(ZoneInfo("America/New_York"))

        return cls(
            symbol=symbol,
            published_time=published_time,
            title=data.get("title", ""),
            text=None,  # Moomoo doesn't provide body
            url=data.get("url", ""),
            sources=data.get("sources", "Moomoo"),
        )

    @classmethod
    def from_benzinga_api_response(cls, symbol: str, data: Dict) -> "NewsArticle":
        """Create NewsArticle from Benzinga API response"""
        try:
            # Benzinga format: "Wed, 03 Dec 2025 08:34:03 -0400"
            published_time = datetime.strptime(
                data["created"], "%a, %d %b %Y %H:%M:%S %z"
            ).astimezone(ZoneInfo("America/New_York"))
        except (KeyError, ValueError) as e:
            logger.warning(f"Benzinga date parse error: {e}, using current time")
            published_time = datetime.now().astimezone(ZoneInfo("America/New_York"))

        return cls(
            symbol=symbol,
            published_time=published_time,
            title=data.get("title", ""),
            text=data.get("body"),
            url=data.get("url", ""),
            sources=data.get("author", "Benzinga"),
        )


class NewsFormatter:
    """News Formatter Utility"""

    @staticmethod
    def format_json(articles: List[NewsArticle], indent: int = 2) -> str:
        """Format list of NewsArticle to JSON string"""
        # ✅ Pydantic provides model_dump() for serialization
        data = [article.model_dump(mode="json") for article in articles]
        return json.dumps(data, indent=indent, ensure_ascii=False)

    @staticmethod
    def format_markdown(articles: List[NewsArticle]) -> str:
        """Format list of NewsArticle to Markdown"""
        lines = []
        for i, article in enumerate(articles, 1):
            time_str = article.published_time.strftime("%Y-%m-%d %H:%M")
            lines.append(f"## {i}. {article.title}")
            lines.append(f"**{article.symbol}** | {time_str} | {article.sources}")
            lines.append(f"[Read More]({article.url})")
            if article.text:
                lines.append(f"\n{article.text[:200]}...\n")
            lines.append("---\n")
        return "\n".join(lines)


class BorrowFee(BaseModel):
    pass
