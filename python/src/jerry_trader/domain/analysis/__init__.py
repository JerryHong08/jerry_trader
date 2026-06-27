"""Offline analysis domain models.

Pure value objects following the project convention: ``@dataclass(frozen=True)``
with zero I/O.  All analysis types — classifier, factor, strategy — share these
unifying abstractions.

The central insight: every analysis is *measure forward returns from a trigger
point, grouped by a property of the trigger*.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


# ── Enums ───────────────────────────────────────────────────────────────────


class Horizon(str, Enum):
    """Forward-return horizon measured from the trigger time."""

    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    M60 = "60m"


class Metric(str, Enum):
    """What to measure for each slice group."""

    COUNT = "count"
    AVG_RETURN = "avg_return"
    AVG_MAX_RETURN = "avg_max_return"
    WIN_RATE = "win_rate"
    HIT_RATE_GT_5PCT = "hit_rate_gt_5pct"
    HIT_RATE_GT_10PCT = "hit_rate_gt_10pct"
    HIT_RATE_GT_20PCT = "hit_rate_gt_20pct"
    HIT_RATE_GT_50PCT = "hit_rate_gt_50pct"
    MEAN_MAX_RETURN = "mean_max_return"


class SliceDimension(str, Enum):
    """How to group observations into slices."""

    SCORE_BUCKET = "score_bucket"  # 1-3, 4-5, 6-7, 8-10
    DATE = "date"
    SYMBOL = "symbol"
    SOURCE = "source"
    VERDICT = "verdict"  # catalyst / non-catalyst
    CUSTOM = "custom"  # keyed by ``key_fn`` on observation metadata


# ── Core domain objects ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class Observation:
    """A point in time where something of interest happened.

    This is the universal "trigger" — a classifier decision, a factor crossing
    a threshold, a backtest signal firing.  The *metadata* dict carries
    source-specific tags (score, title, is_catalyst, rule_id, etc.).
    """

    symbol: str
    trigger_time: datetime
    source: str  # "classifier" | "factor_signal" | "backtest_signal" | ...
    trigger_price: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("symbol must not be empty")
        if not self.source:
            raise ValueError("source must not be empty")

    @property
    def date_str(self) -> str:
        """YYYY-MM-DD for grouping / filtering."""
        return self.trigger_time.strftime("%Y-%m-%d")


@dataclass(frozen=True)
class Outcome:
    """An observation enriched with forward returns from snapshot data.

    *returns* and *max_returns* are keyed by :class:`Horizon`.  ``None``
    means the return could not be computed (e.g. no snapshot data at that
    horizon because the system had stopped).
    """

    observation: Observation
    returns: dict[Horizon, float | None] = field(default_factory=dict)
    max_returns: dict[Horizon, float | None] = field(default_factory=dict)

    # Convenience accessors --------------------------------------------------
    @property
    def symbol(self) -> str:
        return self.observation.symbol

    @property
    def trigger_time(self) -> datetime:
        return self.observation.trigger_time

    @property
    def trigger_price(self) -> float | None:
        return self.observation.trigger_price

    def return_at(self, horizon: Horizon) -> float | None:
        return self.returns.get(horizon)

    def max_return_at(self, horizon: Horizon) -> float | None:
        return self.max_returns.get(horizon)


# ── Configuration ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SliceConfig:
    """How to group outcomes for analysis."""

    dimension: SliceDimension = SliceDimension.SCORE_BUCKET
    key_fn: str | None = None  # metadata key for CUSTOM dimension
    bins: list[str] | None = None  # ordered bin labels (e.g. ["8-10","6-7","4-5","1-3"])


@dataclass(frozen=True)
class AnalysisConfig:
    """Complete specification for one analysis run."""

    source: str = "classifier"
    date_range: tuple[str, str] = ("", "")
    horizons: list[Horizon] = field(default_factory=lambda: [Horizon.M5, Horizon.M15])
    slices: list[SliceConfig] = field(default_factory=lambda: [SliceConfig()])
    metrics: list[Metric] = field(
        default_factory=lambda: [
            Metric.COUNT,
            Metric.AVG_MAX_RETURN,
            Metric.HIT_RATE_GT_5PCT,
            Metric.HIT_RATE_GT_10PCT,
            Metric.HIT_RATE_GT_20PCT,
            Metric.HIT_RATE_GT_50PCT,
        ]
    )
    filters: dict[str, Any] = field(default_factory=dict)
    dedup: bool = False  # keep only best-scored observation per symbol


# ── Results ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SliceResult:
    """One group's aggregated metrics."""

    label: str
    outcomes: list[Outcome] = field(default_factory=list)
    metrics: dict[Metric, float] = field(default_factory=dict)

    @property
    def count(self) -> int:
        return len(self.outcomes)


@dataclass(frozen=True)
class AnalysisReport:
    """The complete output of an analysis run."""

    config: AnalysisConfig
    slices: list[SliceResult]
    total_outcomes: int
    generated_at: datetime = field(default_factory=datetime.now)

    def summary(self, horizon: Horizon = Horizon.M15) -> str:
        """Human-readable one-line-per-slice summary (like ExitStrategyReport)."""
        metric_order = [
            Metric.COUNT,
            Metric.AVG_MAX_RETURN,
            Metric.HIT_RATE_GT_5PCT,
            Metric.HIT_RATE_GT_10PCT,
            Metric.HIT_RATE_GT_20PCT,
        ]
        lines = [
            f"{'Label':>8s}  {'n':>5s}  "
            + "  ".join(f"{m.value:>12s}" for m in metric_order)
        ]
        lines.append(f"  {'-'*8}  {'-'*5}  " + "  ".join("-" * 12 for _ in metric_order))
        for sl in self.slices:
            vals = "  ".join(
                _fmt_metric(sl.metrics.get(m)) for m in metric_order
            )
            lines.append(f"  {sl.label:>8s}  {sl.count:>5d}  {vals}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to plain dicts for JSON / API responses."""
        return {
            "config": {
                "source": self.config.source,
                "date_range": self.config.date_range,
                "horizons": [h.value for h in self.config.horizons],
                "dedup": self.config.dedup,
            },
            "slices": [
                {
                    "label": sl.label,
                    "count": sl.count,
                    "metrics": {k.value: v for k, v in sl.metrics.items()},
                    "top_outcomes": [
                        {
                            "symbol": o.symbol,
                            "max_return_15m": o.max_return_at(Horizon.M15),
                            "metadata": o.observation.metadata,
                        }
                        for o in sorted(
                            sl.outcomes,
                            key=lambda o: o.max_return_at(Horizon.M15) or -99,
                            reverse=True,
                        )[:5]
                    ],
                }
                for sl in self.slices
            ],
            "total_outcomes": self.total_outcomes,
            "generated_at": self.generated_at.isoformat(),
        }


# ── Helpers ─────────────────────────────────────────────────────────────────


def _fmt_metric(value: float | None) -> str:
    if value is None:
        return "         N/A"
    if abs(value) < 1:
        return f"{value * 100:>11.1f}%"
    return f"{value:>12.1f}"
