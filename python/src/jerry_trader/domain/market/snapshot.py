"""Market Snapshot Domain Models

Pure domain models for market snapshot data.
"""

from typing import Optional

from pydantic import BaseModel, Field


class SnapshotMessage(BaseModel):
    """Market Snapshot Domain Model

    Represents a single ticker's snapshot data at a point in time.
    """

    ticker: str = Field(..., description="Stock ticker symbol")
    changePercent: float = Field(
        ..., description="Percentage change from previous close"
    )
    volume: float = Field(..., description="Total accumulated trading volume")
    price: float = Field(..., description="Current stock price")
    prev_close: float = Field(..., description="Previous closing price")
    prev_volume: float = Field(..., description="Previous trading volume")
    timestamp: int = Field(..., description="Timestamp in milliseconds")


class FloatSourceData(BaseModel):
    """Float data from a specific source"""

    source: str = Field(..., description="Data Source")
    float_shares: Optional[float] = None
    short_percent: Optional[float] = None  # 0-1 normalized
    outstanding_shares: Optional[float] = None


class FloatShares(BaseModel):
    """Float shares data for a ticker

    Aggregates data from multiple sources.
    """

    ticker: str = Field(..., description="Ticker symbol")
    data: list[FloatSourceData]  # keep all sources
    timestamp: int = Field(
        default_factory=lambda: int(__import__("time").time()),
        description="Last updated time",
    )
