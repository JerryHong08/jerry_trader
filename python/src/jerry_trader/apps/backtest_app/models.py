"""Models for backtest app."""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


@dataclass
class BacktestRequest:
    """Request to start a backtest run."""

    date: str  # YYYY-MM-DD
    events: list[str] = field(default_factory=list)  # Event names to evaluate
    tickers: Optional[list[str]] = None  # Specific tickers (optional)
    hold_duration_minutes: int = 10  # Default 10-min hold


@dataclass
class BacktestProgress:
    """Progress update for WebSocket."""

    type: str  # 'progress', 'signal', 'error', 'complete'
    date: Optional[str] = None
    step: Optional[str] = (
        None  # 'PreFilter', 'DataLoader', 'FactorEngine', 'EventEvaluator'
    )
    percent: Optional[int] = None
    ticker: Optional[str] = None
    entry_time: Optional[int] = None
    entry_price: Optional[float] = None
    message: Optional[str] = None
    experiment_id: Optional[str] = None
    total_signals: Optional[int] = None


@dataclass
class BacktestSignal:
    """Single signal result."""

    experiment_id: str
    date: str
    ticker: str
    event_name: str
    entry_time: datetime
    entry_price: float
    exit_time: datetime
    exit_price: float
    return_pct: float
    exit_reason: str
    factors: dict
    max_price: float
    min_price: float
    time_to_max_ms: int
    time_to_min_ms: int


@dataclass
class BacktestExperiment:
    """Experiment metadata."""

    experiment_id: str
    date: str
    events: list[str]
    status: str  # 'pending', 'running', 'completed', 'failed'
    created_at: datetime
    completed_at: Optional[datetime] = None
    total_signals: int = 0
    avg_return: float = 0.0
    win_rate: float = 0.0


def generate_experiment_id() -> str:
    """Generate unique experiment ID."""
    return str(uuid.uuid4())[:8]  # Short UUID for readability


class FactorsTimelineBatchRequest(BaseModel):
    """Request body for batch factor timeline computation."""

    factors: list[str]
    window_minutes: int = 60
    warmup_minutes: int = 30
    resolution: str = "1m"
