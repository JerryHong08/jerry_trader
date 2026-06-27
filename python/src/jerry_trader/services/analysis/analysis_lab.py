"""AnalysisLab — orchestrator following the FactorLab / ExitLab pattern.

Chains: load observations → enrich with returns → slice → produce report.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from jerry_trader.domain.analysis import (
    AnalysisConfig,
    AnalysisReport,
    Horizon,
    Observation,
    Outcome,
    SliceConfig,
    SliceDimension,
    SliceResult,
)
from jerry_trader.services.analysis.observation_source import (
    ClassifierObservationSource,
    ObservationSource,
)
from jerry_trader.services.analysis.return_engine import ReturnEngine
from jerry_trader.services.analysis.slice_engine import SliceEngine


class AnalysisLab:
    """Stateful per-run orchestrator.

    Usage::

        config = AnalysisConfig(
            source="classifier",
            date_range=("20260626", "20260626"),
            dedup=True,
        )
        lab = AnalysisLab(config, ch_client)
        report = lab.run()
        print(report.summary())
    """

    def __init__(self, config: AnalysisConfig, ch_client: Any, log_dir: str = "logs/jerry_trader"):
        self.config = config
        self._ch = ch_client
        self._log_dir = log_dir
        self._return_engine = ReturnEngine(ch_client)
        self._slice_engine = SliceEngine()

    # ── Main pipeline ──────────────────────────────────────────────────

    def run(self) -> AnalysisReport:
        """Execute the full pipeline: load → enrich → slice → report."""
        # 1. Load observations
        observations = self._load_observations()

        # 2. Dedup if requested
        if self.config.dedup:
            observations = self._dedup(observations)

        # 3. Enrich with returns
        outcomes = self._return_engine.enrich(observations, self.config.horizons)

        # 4. Slice and compute metrics
        slices = self._slice_engine.compute(
            outcomes,
            self.config.slices,
            self.config.metrics,
        )

        return AnalysisReport(
            config=self.config,
            slices=slices,
            total_outcomes=len(outcomes),
        )

    # ── Observation loading ────────────────────────────────────────────

    def _load_observations(self) -> list[Observation]:
        """Resolve the source name to a concrete loader."""
        source = self._resolve_source()
        observations = source.load(self.config.date_range)

        # Apply filters
        if self.config.filters:
            observations = self._apply_filters(observations)

        return observations

    def _resolve_source(self) -> ObservationSource:
        name = self.config.source
        if name == "classifier":
            if self._ch is None:
                raise ValueError("ClickHouse client required for classifier analysis")
            from jerry_trader.services.analysis.observation_source import (
                ClassifierObservationSource,
            )
            return ClassifierObservationSource(self._ch)
        # Future: BacktestSignalSource, FactorSignalSource, etc.
        raise ValueError(f"Unknown observation source: {name}")

    def _apply_filters(self, observations: list[Observation]) -> list[Observation]:
        """Filter observations by metadata key-value pairs."""
        result = observations
        for key, val in self.config.filters.items():
            result = [o for o in result if o.metadata.get(key) == val]
        return result

    # ── Dedup ──────────────────────────────────────────────────────────

    @staticmethod
    def _dedup(observations: list[Observation]) -> list[Observation]:
        """Keep only the best-scored observation per symbol."""
        best: dict[str, Observation] = {}
        for obs in observations:
            sym = obs.symbol
            if sym not in best:
                best[sym] = obs
                continue
            existing = best[sym]
            r_score = obs.metadata.get("score_num", 0)
            e_score = existing.metadata.get("score_num", 0)
            if r_score > e_score:
                best[sym] = obs
            elif r_score == e_score and obs.trigger_time < existing.trigger_time:
                best[sym] = obs
        return sorted(best.values(), key=lambda o: o.trigger_time)

    # ── Output ─────────────────────────────────────────────────────────

    def to_dataframe(self, report: AnalysisReport | None = None):
        """Return a Polars DataFrame (requires ``polars``)."""
        import polars as pl

        r = report or self.run()
        rows: list[dict[str, Any]] = []
        for sl in r.slices:
            row = {"label": sl.label, "count": sl.count}
            for k, v in sl.metrics.items():
                row[k.value] = v
            rows.append(row)
        return pl.DataFrame(rows)

    def to_dict(self, report: AnalysisReport | None = None) -> dict[str, Any]:
        """Return a JSON-serializable dict."""
        r = report or self.run()
        return r.to_dict()

    def to_html(self, report: AnalysisReport | None = None) -> str:
        """Return a self-contained HTML report string."""
        r = report or self.run()
        from jerry_trader.services.analysis.output import render_html

        return render_html(r)
