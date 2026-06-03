"""Boolean layer pipeline for sequential candidate filtration.

Replaces continuous 0-1 tradability scoring with sequential Boolean gates.
Each layer is pass/fail: a candidate either proceeds to the next layer or is
rejected with a specific reason.  The pipeline measures the marginal
contribution of each layer and supports combinatorial sweep for
experimentation.

Usage:
    lab = ExitLab("2026-03-06")
    lab.load_candidates()

    pipeline = LayerPipeline(lab)
    pipeline.add(DataQualityLayer(min_trade_rate=1.0, max_spread_bps=200))
    pipeline.add(StaticLayer(allowed_countries=["US", "CA"], min_price=1.0))
    pipeline.add(EventPatternLayer(price_direction=(0.3, 0.5), gap_pct=(-0.5, 0.5)))

    report = pipeline.run()       # single run with per-layer stats
    reports = pipeline.sweep()    # all 2^N layer combinations
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from itertools import combinations
from typing import Optional

import numpy as np
import pandas as pd

from jerry_trader.domain.backtest.types import Candidate

# ══════════════════════════════════════════════════════════════════════════
# Candidate context — all data available at entry time for layer decisions
# ══════════════════════════════════════════════════════════════════════════


@dataclass
class CandidateContext:
    """All available data about a candidate at entry time.

    Factor values come from FactorLab snapshots (trade_rate, spread, etc.).
    Static fields come from FMP profiles (country, sector, marketCap, etc.).
    """

    symbol: str
    candidate: Candidate
    # Factor values at entry (None = not computable)
    trade_rate: float | None = None
    bid_ask_spread: float | None = None
    large_trade_ratio: float | None = None
    aggressor_ratio: float | None = None
    n_fwd_trades: int = 0
    # Static / fundamental
    country: str | None = None
    sector: str | None = None
    industry: str | None = None
    market_cap: float | None = None
    exchange: str | None = None
    # Trade intensity — computed from trade stream (not FactorLab)
    n_trades_first_5min: int = 0
    max_trade_gap_sec: float = 0.0
    sustained_activity_active: bool = False
    entry_to_peak_pct: float = 0.0  # max return entry → session end
    n_fwd_trades_total: int = 0  # total forward trades (raw, not FactorLab)
    # Liquidity sub-dimensions
    dollar_vol_5min: float = 0.0  # sum(price * size) in first 5min
    median_gap_sec: float = 0.0
    gap_cv: float = 0.0  # std(gaps) / mean(gaps), burstiness
    # Momentum confirmation sub-dimensions
    big_trade_price_delta: float = 0.0  # top-25% size trades avg price vs entry
    price_range_5min_pct: float = 0.0  # (high - low) / entry in first 5min
    price_uptick_frac: float = 0.5  # frac of consecutive price[t] >= price[t-1]
    trade_accel_slope: float = 0.0  # slope of trade count across 5x 60s windows
    trade_last2_first2_ratio: float = 1.0  # last 2 windows / first 2 windows
    # Rejection tracking
    reject_layer: str | None = None
    reject_reason: str | None = None

    @property
    def entry_price(self) -> float:
        return self.candidate.price_at_entry

    @property
    def entry_ms(self) -> int:
        return self.candidate.first_entry_ms

    @property
    def gain_at_entry(self) -> float:
        return self.candidate.gain_at_entry


# ══════════════════════════════════════════════════════════════════════════
# Boolean layer interface
# ══════════════════════════════════════════════════════════════════════════


class BooleanLayer(ABC):
    """One Boolean gate in the filtration pipeline.

    Each layer receives candidates and returns only those that pass its
    criteria.  Candidates that fail have reject_layer + reject_reason set.
    """

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    def description(self) -> str:
        return ""

    @abstractmethod
    def filter(self, contexts: list[CandidateContext]) -> list[CandidateContext]:
        """Return contexts that pass this layer (may be empty)."""
        ...

    def _reject(self, ctx: CandidateContext, reason: str) -> None:
        ctx.reject_layer = self.name
        ctx.reject_reason = reason


# ══════════════════════════════════════════════════════════════════════════
# Concrete layers
# ══════════════════════════════════════════════════════════════════════════


class DataQualityLayer(BooleanLayer):
    """Filter by trade data quality at entry — is this ticker actually tradable?

    Checks that the ticker has enough trade activity at entry time to compute
    a trade_rate, the spread is below a maximum, and there are forward trades
    to exit into.  This eliminates "zombie" tickers that appear in the top-N
    rank but have no real trading activity.
    """

    name = "data_quality"

    def __init__(
        self,
        min_trade_rate: float = 0.0,
        max_spread_bps: float = float("inf"),
        min_fwd_trades: int = 0,
    ):
        self.min_trade_rate = min_trade_rate
        self.max_spread_bps = max_spread_bps
        self.min_fwd_trades = min_fwd_trades

    @property
    def description(self) -> str:
        parts = []
        if self.min_trade_rate > 0:
            parts.append(f"trade_rate > {self.min_trade_rate}")
        if self.max_spread_bps < float("inf"):
            parts.append(f"spread < {self.max_spread_bps}bps")
        if self.min_fwd_trades > 0:
            parts.append(f"fwd_trades >= {self.min_fwd_trades}")
        return " & ".join(parts) if parts else "trade_rate computable"

    def filter(self, contexts: list[CandidateContext]) -> list[CandidateContext]:
        passed: list[CandidateContext] = []
        for ctx in contexts:
            if ctx.trade_rate is None:
                self._reject(
                    ctx, "trade_rate not computable (insufficient trades at entry)"
                )
                continue
            if self.min_trade_rate > 0 and ctx.trade_rate < self.min_trade_rate:
                self._reject(ctx, f"trade_rate < {self.min_trade_rate}")
                continue
            if (
                ctx.bid_ask_spread is not None
                and ctx.bid_ask_spread > self.max_spread_bps
            ):
                self._reject(ctx, f"spread > {self.max_spread_bps}bps")
                continue
            if self.min_fwd_trades > 0 and ctx.n_fwd_trades < self.min_fwd_trades:
                self._reject(ctx, f"fwd_trades < {self.min_fwd_trades}")
                continue
            passed.append(ctx)
        return passed


class StaticLayer(BooleanLayer):
    """Filter by static / fundamental data — country, sector, price, market cap.

    Uses FMP company profiles (loaded once per date via _load_profiles).
    Float is intentionally NOT a filter here — it belongs in position sizing,
    not in the Boolean gate pipeline.
    """

    name = "static"

    def __init__(
        self,
        allowed_countries: list[str] | None = None,
        blocked_countries: list[str] | None = None,
        min_price: float = 0.0,
        max_price: float = float("inf"),
        min_market_cap: float = 0.0,
        blocked_sectors: list[str] | None = None,
    ):
        self.allowed_countries = allowed_countries
        self.blocked_countries = blocked_countries or ["CN", "HK"]
        self.min_price = min_price
        self.max_price = max_price
        self.min_market_cap = min_market_cap
        self.blocked_sectors = blocked_sectors or []

    @property
    def description(self) -> str:
        parts = []
        if self.allowed_countries:
            parts.append(f"country in {self.allowed_countries}")
        if self.blocked_countries:
            parts.append(f"country not in {self.blocked_countries}")
        if self.min_price > 0:
            parts.append(f"price >= ${self.min_price}")
        if self.max_price < float("inf"):
            parts.append(f"price <= ${self.max_price}")
        if self.min_market_cap > 0:
            parts.append(f"marketCap >= ${self.min_market_cap/1e6:.0f}M")
        return " & ".join(parts) if parts else "no static filters"

    def filter(self, contexts: list[CandidateContext]) -> list[CandidateContext]:
        passed: list[CandidateContext] = []
        for ctx in contexts:
            if self.allowed_countries and ctx.country not in self.allowed_countries:
                self._reject(ctx, f"country not in {self.allowed_countries}")
                continue
            if self.blocked_countries and ctx.country in self.blocked_countries:
                self._reject(ctx, "country is blocked")
                continue
            if self.min_price > 0 and ctx.entry_price < self.min_price:
                self._reject(ctx, f"price < \${self.min_price}")
                continue
            if self.max_price < float("inf") and ctx.entry_price > self.max_price:
                self._reject(ctx, f"price > \${self.max_price}")
                continue
            if self.min_market_cap > 0:
                if ctx.market_cap is None:
                    self._reject(ctx, "marketCap unknown")
                    continue
                if ctx.market_cap < self.min_market_cap:
                    self._reject(ctx, f"marketCap < \${self.min_market_cap/1e6:.0f}M")
                    continue
            passed.append(ctx)
        return passed


class EventPatternLayer(BooleanLayer):
    """Filter by price pattern at entry — reversal, breakout, dip-buy, etc.

    Uses bar factors (price_direction, gap_pct) computed at the candidate's
    entry timestamp.  These factors require building 1m bars per ticker,
    which is moderately expensive — ~200ms per ticker.

    Conditions follow the Signal Framework Design's validated patterns:
      - reversal_entry:  price_direction in [0.3, 0.5], gap_pct in [-0.5, 0.5]
      - dip_buy:          price_direction < 0.3, gap_pct in [-0.5, 0.5]

    Also supports a minimum trade_rate threshold (from DataQuality snapshot).
    """

    name = "event_pattern"

    def __init__(
        self,
        price_direction: tuple[float, float] | None = None,
        gap_pct: tuple[float, float] | None = None,
        min_trade_rate: float = 0.0,
    ):
        self.price_direction = price_direction  # (min, max) or None
        self.gap_pct = gap_pct
        self.min_trade_rate = min_trade_rate

    @property
    def description(self) -> str:
        parts = []
        if self.price_direction:
            parts.append(
                f"price_direction in [{self.price_direction[0]}, {self.price_direction[1]}]"
            )
        if self.gap_pct:
            parts.append(f"gap_pct in [{self.gap_pct[0]}, {self.gap_pct[1]}]")
        if self.min_trade_rate > 0:
            parts.append(f"trade_rate > {self.min_trade_rate}")
        return " & ".join(parts) if parts else "no pattern filter"

    def filter(self, contexts: list[CandidateContext]) -> list[CandidateContext]:
        passed: list[CandidateContext] = []
        for ctx in contexts:
            if self.min_trade_rate > 0:
                if ctx.trade_rate is None or ctx.trade_rate < self.min_trade_rate:
                    self._reject(
                        ctx, f"trade_rate {ctx.trade_rate} < {self.min_trade_rate}"
                    )
                    continue
            # Event pattern checks require bar factors — these are attached
            # to CandidateContext by the pipeline's _compute_event_factors().
            pd_val = getattr(ctx, "price_direction", None)
            gap_val = getattr(ctx, "gap_pct", None)

            if self.price_direction:
                if pd_val is None:
                    self._reject(ctx, "price_direction not available")
                    continue
                lo, hi = self.price_direction
                if not (lo <= pd_val <= hi):
                    self._reject(
                        ctx, f"price_direction {pd_val:.3f} not in [{lo}, {hi}]"
                    )
                    continue

            if self.gap_pct:
                if gap_val is None:
                    self._reject(ctx, "gap_pct not available")
                    continue
                lo, hi = self.gap_pct
                if not (lo <= gap_val <= hi):
                    self._reject(ctx, f"gap_pct {gap_val:.3f} not in [{lo}, {hi}]")
                    continue

            passed.append(ctx)
        return passed


class TradeIntensityLayer(BooleanLayer):
    """Filter by post-entry trade stream intensity — separates liquid momo from ghosts.

    Unlike DataQualityLayer (which uses FactorLab snapshots with limited coverage),
    this layer uses raw trade counts and gaps computed directly from ClickHouse
    trade data, giving 100% coverage on all candidates.

    The core thesis: +50% runners never come from tickers with sparse trading.
    A simple trade-count gate captures 100% of big winners while eliminating 80%+
    of candidates.
    """

    name = "trade_intensity"

    def __init__(
        self,
        min_trades_first_5min: int = 0,
        max_gap_allowed_sec: float = float("inf"),
        require_sustained_activity: bool = False,
    ):
        self.min_trades_first_5min = min_trades_first_5min
        self.max_gap_allowed_sec = max_gap_allowed_sec
        self.require_sustained_activity = require_sustained_activity

    @property
    def description(self) -> str:
        parts = []
        if self.min_trades_first_5min > 0:
            parts.append(f"trades_5min >= {self.min_trades_first_5min}")
        if self.max_gap_allowed_sec < float("inf"):
            parts.append(f"max_gap <= {self.max_gap_allowed_sec:.0f}s")
        if self.require_sustained_activity:
            parts.append("sustained_activity")
        return " & ".join(parts) if parts else "no intensity filter"

    def filter(self, contexts: list[CandidateContext]) -> list[CandidateContext]:
        passed: list[CandidateContext] = []
        for ctx in contexts:
            if self.require_sustained_activity and not ctx.sustained_activity_active:
                self._reject(ctx, "no sustained activity at entry")
                continue
            if (
                self.min_trades_first_5min > 0
                and ctx.n_trades_first_5min < self.min_trades_first_5min
            ):
                self._reject(
                    ctx,
                    f"trades_5min={ctx.n_trades_first_5min} < {self.min_trades_first_5min}",
                )
                continue
            if (
                self.max_gap_allowed_sec < float("inf")
                and ctx.max_trade_gap_sec > self.max_gap_allowed_sec
            ):
                self._reject(
                    ctx,
                    f"max_gap={ctx.max_trade_gap_sec:.0f}s > {self.max_gap_allowed_sec:.0f}s",
                )
                continue
            passed.append(ctx)
        return passed


class LiquidityGate(BooleanLayer):
    """First gate: can we actually trade this ticker?

    Checks basic tradability — enough trades, enough dollar volume, and
    reasonably continuous trade flow (no excessive gaps).  This gate does
    NOT look at price direction or momentum — just whether the ticker is
    liquid enough to enter and exit.

    Uses raw trade-stream features with 100% coverage (no FactorLab dependency).
    """

    name = "liquidity"

    def __init__(
        self,
        min_trades_5min: int = 0,
        min_dollar_vol_5min: float = 0.0,
        max_gap_sec: float = float("inf"),
        require_sustained_activity: bool = False,
    ):
        self.min_trades_5min = min_trades_5min
        self.min_dollar_vol_5min = min_dollar_vol_5min
        self.max_gap_sec = max_gap_sec
        self.require_sustained_activity = require_sustained_activity

    @property
    def description(self) -> str:
        parts = []
        if self.min_trades_5min > 0:
            parts.append(f"trades_5min >= {self.min_trades_5min}")
        if self.min_dollar_vol_5min > 0:
            parts.append(f"dollar_vol >= ${self.min_dollar_vol_5min:,.0f}")
        if self.max_gap_sec < float("inf"):
            parts.append(f"max_gap <= {self.max_gap_sec:.0f}s")
        if self.require_sustained_activity:
            parts.append("sustained_activity")
        return " & ".join(parts) if parts else "no liquidity filter"

    def filter(self, contexts: list[CandidateContext]) -> list[CandidateContext]:
        passed: list[CandidateContext] = []
        for ctx in contexts:
            if (
                self.min_trades_5min > 0
                and ctx.n_trades_first_5min < self.min_trades_5min
            ):
                self._reject(
                    ctx,
                    f"trades_5min={ctx.n_trades_first_5min} < {self.min_trades_5min}",
                )
                continue
            if (
                self.min_dollar_vol_5min > 0
                and ctx.dollar_vol_5min < self.min_dollar_vol_5min
            ):
                self._reject(
                    ctx,
                    f"dollar_vol={ctx.dollar_vol_5min:,.0f} < {self.min_dollar_vol_5min:,.0f}",
                )
                continue
            if (
                self.max_gap_sec < float("inf")
                and ctx.max_trade_gap_sec > self.max_gap_sec
            ):
                self._reject(
                    ctx,
                    f"max_gap={ctx.max_trade_gap_sec:.0f}s > {self.max_gap_sec:.0f}s",
                )
                continue
            if self.require_sustained_activity and not ctx.sustained_activity_active:
                self._reject(ctx, "no sustained activity at entry")
                continue
            passed.append(ctx)
        return passed


class MomentumGate(BooleanLayer):
    """Second gate: does price action confirm momentum?

    Applied AFTER liquidity gate.  Checks that big trades are pushing price
    higher, price range is meaningful, and the tick sequence is directional.

    These features require at least a few minutes of post-entry trading to
    compute reliably.  In live trading this gate would fire at entry+5min
    rather than at the instant of entry.
    """

    name = "momentum"

    def __init__(
        self,
        min_big_trade_delta: float = -float("inf"),
        min_price_range_pct: float = 0.0,
        min_uptick_frac: float = 0.0,
        min_accel_slope: float = -float("inf"),
    ):
        self.min_big_trade_delta = min_big_trade_delta
        self.min_price_range_pct = min_price_range_pct
        self.min_uptick_frac = min_uptick_frac
        self.min_accel_slope = min_accel_slope

    @property
    def description(self) -> str:
        parts = []
        if self.min_big_trade_delta > -float("inf"):
            parts.append(f"big_trade_delta >= ${self.min_big_trade_delta:.2f}")
        if self.min_price_range_pct > 0:
            parts.append(f"price_range >= {self.min_price_range_pct:.1f}%")
        if self.min_uptick_frac > 0:
            parts.append(f"uptick_frac >= {self.min_uptick_frac:.0%}")
        if self.min_accel_slope > -float("inf"):
            parts.append(f"accel >= {self.min_accel_slope:.1f}")
        return " & ".join(parts) if parts else "no momentum filter"

    def filter(self, contexts: list[CandidateContext]) -> list[CandidateContext]:
        passed: list[CandidateContext] = []
        for ctx in contexts:
            if (
                self.min_big_trade_delta > -float("inf")
                and ctx.big_trade_price_delta < self.min_big_trade_delta
            ):
                self._reject(
                    ctx,
                    f"big_trade_delta={ctx.big_trade_price_delta:.2f} < {self.min_big_trade_delta:.2f}",
                )
                continue
            if (
                self.min_price_range_pct > 0
                and ctx.price_range_5min_pct < self.min_price_range_pct
            ):
                self._reject(
                    ctx,
                    f"price_range={ctx.price_range_5min_pct:.1f}% < {self.min_price_range_pct:.1f}%",
                )
                continue
            if (
                self.min_uptick_frac > 0
                and ctx.price_uptick_frac < self.min_uptick_frac
            ):
                self._reject(
                    ctx,
                    f"uptick_frac={ctx.price_uptick_frac:.1%} < {self.min_uptick_frac:.1%}",
                )
                continue
            if (
                self.min_accel_slope > -float("inf")
                and ctx.trade_accel_slope < self.min_accel_slope
            ):
                self._reject(
                    ctx,
                    f"accel={ctx.trade_accel_slope:.1f} < {self.min_accel_slope:.1f}",
                )
                continue
            passed.append(ctx)
        return passed


# ══════════════════════════════════════════════════════════════════════════
# Pipeline report
# ══════════════════════════════════════════════════════════════════════════


@dataclass
class LayerStats:
    """Per-layer filtration statistics."""

    layer_name: str
    description: str
    n_input: int
    n_passed: int
    n_rejected: int
    pass_rate: float
    rejected_tickers: list[str] = field(default_factory=list)
    rejection_reasons: dict[str, int] = field(default_factory=dict)


@dataclass
class PipelineReport:
    """Complete pipeline run report with per-layer stats and performance."""

    layers: list[str]
    layer_stats: list[LayerStats]
    n_input: int
    n_survivors: int
    overall_pass_rate: float
    # Performance of survivors (from ExitLab.run_strategy)
    n_traded_baseline: int  # actually traded candidates in baseline
    n_traded_survivor: int  # actually traded candidates after filtration
    baseline_expectancy: float  # all candidates, no filtration
    survivor_expectancy: float  # candidates that passed all layers
    baseline_win_rate: float
    survivor_win_rate: float
    expectancy_delta: float  # survivor - baseline
    # Right-tail (MOMO) metrics
    baseline_entry_to_peak_median: float
    survivor_entry_to_peak_median: float
    baseline_momo_count: int  # +50% runners in all candidates
    survivor_momo_count: int  # +50% runners among survivors
    momo_retention: float  # survivor_momo / baseline_momo

    def summary(self) -> str:
        lines = [
            "═" * 72,
            f"Pipeline: {' → '.join(self.layers)}",
            f"  Candidates: {self.n_input} → {self.n_survivors} "
            f"({self.overall_pass_rate:.0%} pass rate)",
            f"  Traded:     {self.n_traded_baseline} → {self.n_traded_survivor}",
            "",
            f"  Performance (tp5):",
            f"    all candidates:  expectancy={self.baseline_expectancy:+.2f}%  "
            f"win_rate={self.baseline_win_rate:.1%}  n={self.n_traded_baseline}",
            f"    survivors:       expectancy={self.survivor_expectancy:+.2f}%  "
            f"win_rate={self.survivor_win_rate:.1%}  n={self.n_traded_survivor}",
            f"    Δ:               {self.expectancy_delta:+.2f}%",
            "",
            f"  Right-tail (MOMO ≥+50%):",
            f"    all candidates:  median entry→peak={self.baseline_entry_to_peak_median:+.1f}%  "
            f"momo={self.baseline_momo_count}/{self.n_input}",
            f"    survivors:       median entry→peak={self.survivor_entry_to_peak_median:+.1f}%  "
            f"momo={self.survivor_momo_count}/{self.n_survivors} "
            f"(retention={self.momo_retention:.0%})",
            "",
            "  Per-layer:",
        ]
        for s in self.layer_stats:
            lines.append(
                f"    {s.layer_name:20s}  {s.n_input:>3d} → {s.n_passed:>3d}  "
                f"(-{s.n_rejected}, {s.pass_rate:.0%} pass)"
            )
            if s.rejection_reasons:
                for reason, count in sorted(
                    s.rejection_reasons.items(), key=lambda x: -x[1]
                ):
                    lines.append(f"      {reason}: {count}")
        return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════
# Pipeline orchestrator
# ══════════════════════════════════════════════════════════════════════════


class LayerPipeline:
    """Sequential Boolean layer pipeline for candidate filtration.

    Chains BooleanLayer instances in order.  Each layer receives the
    survivors of the previous layer and applies its pass/fail criteria.
    The pipeline measures per-layer filtration and overall performance
    impact via ExitLab's exit strategy simulation.

    Usage:
        lab = ExitLab("2026-03-06")
        lab.load_candidates()

        pipeline = LayerPipeline(lab)
        pipeline.add(DataQualityLayer(min_trade_rate=1.0))
        pipeline.add(StaticLayer())

        report = pipeline.run()
        print(report.summary())

        # Test all layer combinations
        for report in pipeline.sweep():
            print(report.summary())
    """

    def __init__(self, lab: "ExitLab"):
        self._lab = lab
        self._layers: list[BooleanLayer] = []
        self._snapshots: list[dict] | None = None
        self._contexts: list[CandidateContext] | None = None

    def add(self, layer: BooleanLayer) -> "LayerPipeline":
        """Add a layer to the end of the pipeline.  Returns self for chaining."""
        self._layers.append(layer)
        return self

    def remove(self, layer_name: str) -> "LayerPipeline":
        """Remove a layer by name."""
        self._layers = [l for l in self._layers if l.name != layer_name]
        return self

    @property
    def layers(self) -> list[BooleanLayer]:
        return list(self._layers)

    # ── Data preparation ────────────────────────────────────────────────

    def _ensure_snapshots(self) -> list[dict]:
        """Ensure factor snapshots at entry are computed (cached)."""
        if self._snapshots is None:
            self._snapshots = self._lab._snapshot_factors_at_entry()
        return self._snapshots

    def _build_contexts(self) -> list[CandidateContext]:
        """Build CandidateContext objects from snapshots + profiles.

        Merges factor snapshot data with static profile data for each
        candidate.  Candidates that failed FactorLab.load() (no snapshot
        entry) are still included — they will have trade_rate=None and
        likely be rejected by DataQualityLayer.
        """
        if self._contexts is not None:
            return self._contexts

        snapshots = self._ensure_snapshots()
        snap_by_ticker: dict[str, dict] = {s["ticker"]: s for s in snapshots}
        profiles = self._lab._profiles
        cand_by_ticker: dict[str, Candidate] = {
            c.symbol: c for c in self._lab.candidates
        }

        contexts: list[CandidateContext] = []
        for cand in self._lab.candidates:
            snap = snap_by_ticker.get(cand.symbol, {})
            profile = profiles.get(cand.symbol, {})

            ctx = CandidateContext(
                symbol=cand.symbol,
                candidate=cand,
                trade_rate=snap.get("trade_rate"),
                bid_ask_spread=snap.get("bid_ask_spread"),
                large_trade_ratio=snap.get("large_trade_ratio"),
                aggressor_ratio=snap.get("aggressor_ratio"),
                n_fwd_trades=snap.get("n_fwd_trades", 0),
                country=profile.get("country"),
                sector=profile.get("sector"),
                industry=profile.get("industry"),
                market_cap=profile.get("marketCap"),
                exchange=profile.get("exchange"),
            )
            contexts.append(ctx)

        # Compute trade intensity for all contexts (100% coverage, raw trades)
        self._compute_trade_intensity(contexts)

        self._contexts = contexts
        return contexts

    def _compute_event_factors(self, contexts: list[CandidateContext]) -> None:
        """Compute bar factors (price_direction, gap_pct) for contexts.

        Modifies contexts in-place, attaching price_direction and gap_pct
        attributes.  Skips tickers that already have these attributes set.
        Called lazily — only when a layer requires event pattern data.
        """
        from jerry_trader.services.backtest.factor_lab import FactorLab

        for ctx in contexts:
            if hasattr(ctx, "price_direction") and hasattr(ctx, "gap_pct"):
                continue

            try:
                fl = FactorLab(self._lab.date, eval_start_ms=ctx.entry_ms)
                fl.load(ctx.symbol)
            except Exception:
                ctx.price_direction = None
                ctx.gap_pct = None
                continue

            for fn in ["price_direction", "gap_percent"]:
                try:
                    fl.compute_bar_factor(fn)
                except Exception:
                    pass

            df = fl.factors_df()
            if df.empty or len(df) == 0:
                ctx.price_direction = None
                ctx.gap_pct = None
                continue

            # Snapshot at the first grid point (entry time)
            row = df.iloc[0]
            pd_val = row.get("price_direction")
            gap_val = row.get("gap_percent")
            ctx.price_direction = (
                float(pd_val)
                if pd_val is not None
                and not (isinstance(pd_val, float) and np.isnan(pd_val))
                else None
            )
            ctx.gap_pct = (
                float(gap_val)
                if gap_val is not None
                and not (isinstance(gap_val, float) and np.isnan(gap_val))
                else None
            )

    def _compute_trade_intensity(self, contexts: list[CandidateContext]) -> None:
        """Compute all trade-stream features for all contexts from raw trade data.

        Loads all trades for the date in a single ClickHouse query, then
        computes per-ticker metrics in one pass.

        Liquidity sub-dimensions:
          - n_trades_first_5min, n_fwd_trades_total
          - max_trade_gap_sec, median_gap_sec, gap_cv
          - dollar_vol_5min, sustained_activity_active

        Momentum confirmation sub-dimensions:
          - big_trade_price_delta, price_range_5min_pct
          - price_uptick_frac, trade_accel_slope, trade_last2_first2_ratio
          - entry_to_peak_pct (evaluation only, not a filter feature)

        Modifies contexts in-place.  100% coverage — does not depend on FactorLab.
        """
        from datetime import datetime
        from zoneinfo import ZoneInfo

        from jerry_trader.services.backtest.data_loading import _load_trades_batch_ch

        dt_et = datetime.strptime(self._lab.date, "%Y-%m-%d").replace(
            hour=9, minute=30, tzinfo=ZoneInfo("America/New_York")
        )
        session_end_ms = int(dt_et.timestamp() * 1000)

        tickers = [ctx.symbol for ctx in contexts]
        all_trades = _load_trades_batch_ch(tickers, self._lab.date)

        for ctx in contexts:
            trades = all_trades.get(ctx.symbol)
            if trades is None or trades.empty:
                continue

            entry_price = self._lab.find_entry_price(trades, ctx.entry_ms)
            if entry_price is None:
                continue

            fwd = trades[
                (trades["ts_ms"] > ctx.entry_ms) & (trades["ts_ms"] <= session_end_ms)
            ]
            if fwd.empty:
                continue

            ts = fwd["ts_ms"].values.astype(np.int64)
            prices = fwd["price"].values
            sizes = fwd["size"].values.astype(np.float64)

            first_5min_end = ctx.entry_ms + 300_000
            mask_5min = ts <= first_5min_end
            ts_5m = ts[mask_5min]
            prices_5m = prices[mask_5min]
            sizes_5m = sizes[mask_5min]

            # -- Liquidity: n_trades_first_5min --
            ctx.n_trades_first_5min = len(ts_5m)
            ctx.n_fwd_trades_total = len(ts)

            # -- Liquidity: gap stats --
            if len(ts) >= 2:
                gaps = np.diff(ts) / 1000.0
                ctx.max_trade_gap_sec = float(np.max(gaps))
                ctx.median_gap_sec = float(np.median(gaps))
                ctx.gap_cv = float(np.std(gaps) / max(np.mean(gaps), 0.001))
            else:
                ctx.max_trade_gap_sec = float(session_end_ms - ctx.entry_ms) / 1000.0
                ctx.median_gap_sec = ctx.max_trade_gap_sec
                ctx.gap_cv = 0.0

            # -- Liquidity: dollar_vol_5min --
            ctx.dollar_vol_5min = float(np.sum(prices_5m * sizes_5m))

            # -- Liquidity: sustained_activity_active --
            ctx.sustained_activity_active = self._check_sustained_activity(
                ts, ctx.entry_ms, session_end_ms
            )

            # -- Momentum: big_trade_price_delta --
            if len(prices) >= 5:
                top_q = np.percentile(sizes, 75)
                big_mask = sizes >= top_q
                if big_mask.sum() > 0:
                    ctx.big_trade_price_delta = float(
                        np.mean(prices[big_mask]) - entry_price
                    )
            # else stays 0.0

            # -- Momentum: price_range_5min_pct --
            if len(prices_5m) >= 2:
                ctx.price_range_5min_pct = float(
                    (np.max(prices_5m) - np.min(prices_5m)) / entry_price * 100
                )

            # -- Momentum: price_uptick_frac --
            if len(prices) >= 2:
                ctx.price_uptick_frac = float(np.mean(prices[1:] >= prices[:-1]))

            # -- Momentum: trade acceleration --
            window_counts = []
            for i in range(5):
                ws = ctx.entry_ms + i * 60_000
                window_counts.append(int(((ts >= ws) & (ts < ws + 60_000)).sum()))
            if max(window_counts) > 0:
                x = np.arange(5)
                ctx.trade_accel_slope = float(np.polyfit(x, window_counts, 1)[0])
                ctx.trade_last2_first2_ratio = float(
                    (window_counts[-1] + window_counts[-2])
                    / max(window_counts[0] + window_counts[1], 1)
                )

            # -- Evaluation: entry_to_peak_pct --
            peak = float(np.max(prices))
            ctx.entry_to_peak_pct = (peak - entry_price) / entry_price * 100

    @staticmethod
    def _check_sustained_activity(
        ts: np.ndarray, entry_ms: int, session_end_ms: int
    ) -> bool:
        """True if first 3 consecutive 60s windows from entry_ms have ≥3 trades each."""
        for i in range(3):
            bucket_start = entry_ms + i * 60_000
            count = int(((ts >= bucket_start) & (ts < bucket_start + 60_000)).sum())
            if count < 3:
                return False
        return True

    # ── Pipeline execution ──────────────────────────────────────────────

    def run(self) -> PipelineReport:
        """Run all layers in sequence, returning a PipelineReport.

        Also computes performance deltas: runs tp5 exit strategy on the
        original (unfiltered) candidates and on the survivors to measure
        the filtration's performance impact.
        """
        if not self._layers:
            raise ValueError("No layers added. Use pipeline.add(layer) first.")

        contexts = self._build_contexts()
        original_candidates = self._lab.candidates
        n_input = len(contexts)

        layer_stats: list[LayerStats] = []
        survivors = contexts

        for layer in self._layers:
            n_before = len(survivors)

            # Lazily compute event factors if this layer needs them
            if isinstance(layer, EventPatternLayer):
                self._compute_event_factors(survivors)

            survivors = layer.filter(survivors)
            n_passed = len(survivors)
            n_rejected = n_before - n_passed

            # Collect rejection reasons for this layer
            rejected_ctxs = [
                c
                for c in (
                    survivors
                    if n_rejected == 0
                    else [
                        x
                        for x in (self._contexts or contexts)
                        if x.reject_layer == layer.name
                    ]
                )
            ]
            # Better: scan all contexts for ones this layer rejected
            all_rejected = []
            for ctx in self._contexts or contexts:
                if ctx.reject_layer == layer.name:
                    all_rejected.append(ctx)

            reason_counts: dict[str, int] = {}
            for ctx in all_rejected:
                reason = ctx.reject_reason or "unknown"
                reason_counts[reason] = reason_counts.get(reason, 0) + 1

            layer_stats.append(
                LayerStats(
                    layer_name=layer.name,
                    description=layer.description,
                    n_input=n_before,
                    n_passed=n_passed,
                    n_rejected=n_rejected,
                    pass_rate=n_passed / max(n_before, 1),
                    rejected_tickers=[ctx.symbol for ctx in all_rejected],
                    rejection_reasons=reason_counts,
                )
            )

        # ── Performance measurement ────────────────────────────────────
        survivor_tickers = {ctx.symbol for ctx in survivors}
        all_tickers = {c.symbol for c in original_candidates}

        # Baseline: all candidates
        self._lab.candidates = original_candidates
        baseline = self._lab.run_strategy(tp_pct=5.0)

        # Survivors
        if survivor_tickers:
            self._lab.candidates = [
                c for c in original_candidates if c.symbol in survivor_tickers
            ]
            survivor_report = self._lab.run_strategy(tp_pct=5.0)
        else:
            survivor_report = baseline  # fallback if all rejected

        # Restore
        self._lab.candidates = original_candidates

        # ── Right-tail metrics ──────────────────────────────────────────
        all_peaks = [
            ctx.entry_to_peak_pct for ctx in contexts if ctx.entry_to_peak_pct != 0.0
        ]
        surv_peaks = [
            ctx.entry_to_peak_pct for ctx in survivors if ctx.entry_to_peak_pct != 0.0
        ]
        baseline_momo = sum(1 for p in all_peaks if p >= 50)
        survivor_momo = sum(1 for p in surv_peaks if p >= 50)

        return PipelineReport(
            layers=[l.name for l in self._layers],
            layer_stats=layer_stats,
            n_input=n_input,
            n_survivors=len(survivors),
            overall_pass_rate=len(survivors) / max(n_input, 1),
            n_traded_baseline=baseline.n_trades,
            n_traded_survivor=survivor_report.n_trades,
            baseline_expectancy=baseline.expectancy_pct,
            survivor_expectancy=survivor_report.expectancy_pct,
            baseline_win_rate=baseline.win_rate,
            survivor_win_rate=survivor_report.win_rate,
            expectancy_delta=survivor_report.expectancy_pct - baseline.expectancy_pct,
            baseline_entry_to_peak_median=(
                float(np.median(all_peaks)) if all_peaks else 0.0
            ),
            survivor_entry_to_peak_median=(
                float(np.median(surv_peaks)) if surv_peaks else 0.0
            ),
            baseline_momo_count=baseline_momo,
            survivor_momo_count=survivor_momo,
            momo_retention=survivor_momo / max(baseline_momo, 1),
        )

    def sweep(self) -> list[PipelineReport]:
        """Test all 2^N layer combinations, ordered by survivor expectancy.

        For each combination, runs the full pipeline and measures the
        performance delta vs baseline.  This reveals the marginal
        contribution of each layer and whether specific combinations
        are synergistic or redundant.

        The "empty" combination (no layers) is the baseline.
        """
        if not self._layers:
            raise ValueError("No layers added.")

        # Pre-build contexts so FactorLab isn't called N times
        contexts = self._build_contexts()
        original_candidates = self._lab.candidates

        # Pre-compute event factors if any layer needs them
        has_event = any(isinstance(l, EventPatternLayer) for l in self._layers)
        if has_event:
            print("Pre-computing event pattern factors for sweep...")
            self._compute_event_factors(contexts)

        # Baseline right-tail stats (same for all combinations)
        all_peaks = [
            ctx.entry_to_peak_pct for ctx in contexts if ctx.entry_to_peak_pct != 0.0
        ]
        baseline_entry_to_peak = float(np.median(all_peaks)) if all_peaks else 0.0
        baseline_momo = sum(1 for p in all_peaks if p >= 50)

        reports: list[PipelineReport] = []
        n = len(self._layers)

        for r in range(n + 1):
            for combo_indices in combinations(range(n), r):
                combo_layers = [self._layers[i] for i in combo_indices]

                # Reset all rejection markers for clean slate
                for ctx in contexts:
                    ctx.reject_layer = None
                    ctx.reject_reason = None

                survivors = list(contexts)

                layer_stats: list[LayerStats] = []
                for layer in combo_layers:
                    n_before = len(survivors)
                    survivors = layer.filter(survivors)
                    n_passed = len(survivors)

                    all_rejected = [
                        ctx for ctx in contexts if ctx.reject_layer == layer.name
                    ]
                    reason_counts: dict[str, int] = {}
                    for ctx in all_rejected:
                        reason = ctx.reject_reason or "unknown"
                        reason_counts[reason] = reason_counts.get(reason, 0) + 1

                    layer_stats.append(
                        LayerStats(
                            layer_name=layer.name,
                            description=layer.description,
                            n_input=n_before,
                            n_passed=n_passed,
                            n_rejected=n_before - n_passed,
                            pass_rate=n_passed / max(n_before, 1),
                            rejected_tickers=[ctx.symbol for ctx in all_rejected],
                            rejection_reasons=reason_counts,
                        )
                    )

                # Performance
                survivor_tickers = {ctx.symbol for ctx in survivors}
                if survivor_tickers:
                    self._lab.candidates = [
                        c for c in original_candidates if c.symbol in survivor_tickers
                    ]
                    surv = self._lab.run_strategy(tp_pct=5.0)
                else:
                    surv = self._lab.run_strategy(tp_pct=5.0)

                # Baseline (use cached first time, but re-running is fine)
                self._lab.candidates = original_candidates
                base = self._lab.run_strategy(tp_pct=5.0)

                # Right-tail for this combination
                surv_peaks = [
                    ctx.entry_to_peak_pct
                    for ctx in survivors
                    if ctx.entry_to_peak_pct != 0.0
                ]
                surv_momo = sum(1 for p in surv_peaks if p >= 50)

                reports.append(
                    PipelineReport(
                        layers=[l.name for l in combo_layers],
                        layer_stats=layer_stats,
                        n_input=len(contexts),
                        n_survivors=len(survivors),
                        overall_pass_rate=len(survivors) / max(len(contexts), 1),
                        n_traded_baseline=base.n_trades,
                        n_traded_survivor=surv.n_trades,
                        baseline_expectancy=base.expectancy_pct,
                        survivor_expectancy=surv.expectancy_pct,
                        baseline_win_rate=base.win_rate,
                        survivor_win_rate=surv.win_rate,
                        expectancy_delta=surv.expectancy_pct - base.expectancy_pct,
                        baseline_entry_to_peak_median=baseline_entry_to_peak,
                        survivor_entry_to_peak_median=(
                            float(np.median(surv_peaks)) if surv_peaks else 0.0
                        ),
                        baseline_momo_count=baseline_momo,
                        survivor_momo_count=surv_momo,
                        momo_retention=surv_momo / max(baseline_momo, 1),
                    )
                )

        # Restore
        self._lab.candidates = original_candidates

        # Sort by survivor expectancy descending
        reports.sort(key=lambda r: r.expectancy_delta, reverse=True)
        return reports

    def print_sweep(self) -> None:
        """Print a formatted sweep summary table."""
        reports = self.sweep()
        if not reports:
            print("No reports generated.")
            return

        print("\n" + "=" * 96)
        print("Layer Pipeline Sweep — sorted by MOMO retention, then Δ expectancy")
        print("=" * 96)
        header = (
            f"  {'Layers':<44s} {'Surv':>4s}  "
            f"{'Δ Exp':>8s}  {'MOMO ret':>8s}  "
            f"{'PeakΔ':>7s}  {'WinΔ':>7s}"
        )
        print(header)
        print(f"  {'-'*44} {'-'*4}  {'-'*8}  {'-'*8}  {'-'*7}  {'-'*7}")

        # Sort by momo_retention desc, then expectancy_delta desc
        sorted_reports = sorted(
            reports, key=lambda r: (r.momo_retention, r.expectancy_delta), reverse=True
        )

        for r in sorted_reports:
            layer_str = " + ".join(r.layers) if r.layers else "(none — baseline)"
            win_delta = r.survivor_win_rate - r.baseline_win_rate
            peak_delta = (
                r.survivor_entry_to_peak_median - r.baseline_entry_to_peak_median
            )
            print(
                f"  {layer_str:<44s} {r.n_survivors:>4d}  "
                f"{r.expectancy_delta:>+7.2f}%  "
                f"{r.survivor_momo_count}/{r.baseline_momo_count} ({r.momo_retention:.0%})  "
                f"{peak_delta:>+6.1f}%  "
                f"{win_delta:>+6.1%}"
            )

    # ── Convenience ──────────────────────────────────────────────────────

    @classmethod
    def default(cls, lab: "ExitLab") -> "LayerPipeline":
        """Create a pipeline with sensible default layers.

        LiquidityGate: at least 100 trades in first 5min (tradeable)
        MomentumGate: big trades on average at or above entry price
        Static: exclude CN/HK, require price $1-$30
        """
        return (
            cls(lab)
            .add(LiquidityGate(min_trades_5min=100))
            .add(MomentumGate(min_big_trade_delta=0.0))
            .add(
                StaticLayer(
                    blocked_countries=["CN", "HK"], min_price=1.0, max_price=30.0
                )
            )
        )
