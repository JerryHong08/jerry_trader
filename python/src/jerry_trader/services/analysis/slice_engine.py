"""Slice engine — group outcomes and compute metrics per slice."""

from __future__ import annotations

from typing import Any

from jerry_trader.domain.analysis import (
    Horizon,
    Metric,
    Outcome,
    SliceConfig,
    SliceDimension,
    SliceResult,
)


class SliceEngine:
    """Group :class:`Outcome`\\s by slices and compute metrics per group."""

    # Default score bins (used by SCORE_BUCKET dimension)
    SCORE_BINS = ["8-10", "6-7", "4-5", "1-3"]
    SCORE_BIN_EDGES = [8, 6, 4, 1]  # ≥8 → "8-10", ≥6 → "6-7", etc.

    def compute(
        self,
        outcomes: list[Outcome],
        slices: list[SliceConfig],
        metrics: list[Metric],
        horizon: Horizon = Horizon.M15,
    ) -> list[SliceResult]:
        """Compute slice results for a list of outcomes."""
        results: list[SliceResult] = []
        for sc in slices:
            results.extend(self._compute_slice(outcomes, sc, metrics, horizon))
        return results

    def _compute_slice(
        self,
        outcomes: list[Outcome],
        sc: SliceConfig,
        metrics: list[Metric],
        horizon: Horizon,
    ) -> list[SliceResult]:
        """Compute results for a single SliceConfig."""
        groups = self._group(outcomes, sc)
        results: list[SliceResult] = []
        for label in self._ordered_labels(sc):
            bucket = groups.get(label, [])
            metric_values: dict[Metric, float] = {}
            for m in metrics:
                metric_values[m] = self._compute_metric(bucket, m, horizon)
            results.append(SliceResult(label=label, outcomes=bucket, metrics=metric_values))
        return results

    # ── Grouping ────────────────────────────────────────────────────────

    def _group(
        self, outcomes: list[Outcome], sc: SliceConfig
    ) -> dict[str, list[Outcome]]:
        """Group outcomes by the slice dimension."""
        groups: dict[str, list[Outcome]] = {}

        for o in outcomes:
            key = self._group_key(o, sc)
            groups.setdefault(key, []).append(o)

        return groups

    def _group_key(self, outcome: Outcome, sc: SliceConfig) -> str:
        meta = outcome.observation.metadata

        if sc.dimension == SliceDimension.SCORE_BUCKET:
            score = meta.get("score_num", 0)
            for i, edge in enumerate(self.SCORE_BIN_EDGES):
                if score >= edge:
                    return self.SCORE_BINS[i]
            return self.SCORE_BINS[-1]

        if sc.dimension == SliceDimension.DATE:
            return outcome.trigger_time.strftime("%Y-%m-%d")

        if sc.dimension == SliceDimension.SYMBOL:
            return outcome.symbol

        if sc.dimension == SliceDimension.VERDICT:
            return "catalyst" if meta.get("is_catalyst") else "non-catalyst"

        if sc.dimension == SliceDimension.SOURCE:
            return outcome.observation.source

        if sc.dimension == SliceDimension.CUSTOM:
            key = sc.key_fn or "score_num"
            return str(meta.get(key, "unknown"))

        return "unknown"

    def _ordered_labels(self, sc: SliceConfig) -> list[str]:
        """Return labels in display order."""
        if sc.bins:
            return sc.bins
        if sc.dimension == SliceDimension.SCORE_BUCKET:
            return self.SCORE_BINS
        return []  # will be filled from group keys dynamically; caller handles this

    # ── Metric computation ──────────────────────────────────────────────

    def _compute_metric(
        self,
        outcomes: list[Outcome],
        metric: Metric,
        horizon: Horizon,
    ) -> float:
        """Compute a single metric for a bucket of outcomes."""
        n = len(outcomes)

        if metric == Metric.COUNT:
            return float(n)

        if n == 0:
            return 0.0

        max_vals = [o.max_return_at(horizon) for o in outcomes]
        ret_vals = [o.return_at(horizon) for o in outcomes]

        if metric == Metric.AVG_RETURN:
            valid = [v for v in ret_vals if v is not None]
            return sum(valid) / len(valid) if valid else 0.0

        if metric == Metric.AVG_MAX_RETURN:
            valid = [v for v in max_vals if v is not None]
            return sum(valid) / len(valid) if valid else 0.0

        if metric == Metric.MEAN_MAX_RETURN:
            valid = [v for v in max_vals if v is not None]
            return sum(valid) / len(valid) if valid else 0.0

        if metric == Metric.WIN_RATE:
            valid = [v for v in ret_vals if v is not None]
            return sum(1 for v in valid if v > 0) / len(valid) if valid else 0.0

        if metric == Metric.HIT_RATE_GT_5PCT:
            valid = [v for v in max_vals if v is not None]
            return sum(1 for v in valid if v >= 0.05) / len(valid) if valid else 0.0

        if metric == Metric.HIT_RATE_GT_10PCT:
            valid = [v for v in max_vals if v is not None]
            return sum(1 for v in valid if v >= 0.10) / len(valid) if valid else 0.0

        if metric == Metric.HIT_RATE_GT_20PCT:
            valid = [v for v in max_vals if v is not None]
            return sum(1 for v in valid if v >= 0.20) / len(valid) if valid else 0.0

        if metric == Metric.HIT_RATE_GT_50PCT:
            valid = [v for v in max_vals if v is not None]
            return sum(1 for v in valid if v >= 0.50) / len(valid) if valid else 0.0

        return 0.0
