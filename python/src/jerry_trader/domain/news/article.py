"""News Article Domain Model

Pure domain model with validation rules.
No external API knowledge - adapters handle conversion.
"""

import logging
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


class NewsArticle(BaseModel):
    """News Article Domain Model

    Represents a news article with validation rules.
    Immutable value object for news data.
    """

    symbol: str = Field(
        ..., description="Stock ticker symbol", min_length=1, max_length=10
    )
    published_time: datetime = Field(..., description="News publication time")
    title: str = Field(..., description="News headline", min_length=1)
    text: Optional[str] = Field(None, description="News content body")
    url: str = Field(..., description="News article URL")
    sources: str = Field(..., description="News source/publisher")
    source_from: str = Field(
        default="unknown", description="Data fetch source (momo/benzinga/fmp/etc)"
    )

    model_config = {"json_encoders": {datetime: lambda v: v.isoformat()}}

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate URL format - domain rule"""
        if not v.startswith(("http://", "https://")):
            logger.warning(f"Invalid URL format: {v}")
        return v

    @field_validator("published_time", mode="before")
    @classmethod
    def parse_published_time(cls, v):
        """Auto-parse various datetime formats - domain rule"""
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
