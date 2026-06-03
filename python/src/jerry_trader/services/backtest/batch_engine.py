"""Batch factor engine for backtest — reads FactorSpec from registry, computes via Rust.

All factor computation delegates to Rust PyFactorEngine:
  1. Bar factors — compute_batch_bar()
  2. Trade factors — compute_batch_trade()
  3. Quote factors — compute_batch_quote()

Output: FactorTimeseries = dict[ts_ms, dict[str, float]]
"""

from __future__ import annotations

from bisect import bisect_left
from collections import defaultdict

from jerry_trader._rust import PyBar, PyFactorEngine, PyTrade, forward_fill_factors
from jerry_trader.domain.market import Bar
from jerry_trader.services.backtest.config import TickerData
from jerry_trader.services.factor.factor_registry import (
    FactorRegistry,
    FactorSpec,
    get_factor_registry,
)
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.batch_engine", log_to_file=True)

# Type alias for unified factor timeseries
FactorTimeseries = dict[int, dict[str, float]]


class FactorEngineBatchAdapter:
    """Batch factor computation via Rust PyFactorEngine.

    Reads FactorSpec from the registry and delegates all computation
    to Rust — same code path as live FactorEngine warmup. New factors
    added to factors.yaml + Rust registry automatically work here.
    """

    def __init__(self, registry: FactorRegistry | None = None):
        self._registry = registry or get_factor_registry()

    def compute(
        self,
        ticker_data: TickerData,
    ) -> FactorTimeseries:
        """Compute all factors for a single ticker's data.

        Args:
            ticker_data: Loaded data for one ticker (trades, quotes).

        Returns:
            Unified factor timeseries: {timestamp_ms: {factor_name: value}}
        """
        import time

        symbol = ticker_data.symbol
        ts: FactorTimeseries = defaultdict(dict)

        total_start = time.time()

        # Get factor specs directly (no need to instantiate Python indicators)
        bar_specs = self._registry.get_factors_by_type("bar")
        trade_specs = self._registry.get_factors_by_type("trade")
        quote_specs = self._registry.get_factors_by_type("quote")

        # 1. Build bars from trades via BarBuilder, then feed to bar factors
        bars: list[Bar] = []
        t0 = time.time()
        if bar_specs:
            bars = self._build_bars_from_trades(ticker_data)
            self._compute_bar_factors(bars, bar_specs, ts)
        t1 = time.time()
        bar_elapsed = t1 - t0

        # 2. Trade factors via Rust batch warmup (same as FactorEngine._process_bootstrap_trades)
        t0 = time.time()
        if trade_specs and ticker_data.trades:
            self._compute_trade_factors(ticker_data.trades, trade_specs, ts)
        t1 = time.time()
        tick_elapsed = t1 - t0

        # 3. Quote factors via Rust batch compute
        t0 = time.time()
        if quote_specs and ticker_data.quotes:
            self._compute_quote_factors_rust(ticker_data.quotes, quote_specs, ts)
        t1 = time.time()
        quote_elapsed = t1 - t0

        # 4. Forward-fill to merge factors across timestamps
        t0 = time.time()
        ts = self._forward_fill_factors(ts)
        t1 = time.time()
        ff_elapsed = t1 - t0

        # 4b. Inject entry_gap_pct from candidate
        # This is the TRUE gap at the moment ticker entered top20 (Ross Cameron style)
        # IMPORTANT: This is NOT session_gap_pct (static open vs prev_close),
        # but the dynamic changePercent at entry time!
        entry_gap_pct = 0.0
        if ticker_data.candidate:
            entry_gap_pct = ticker_data.candidate.gain_at_entry
            # Set entry_gap_pct for ALL timestamps (it's a constant for the ticker)
            for t in ts.keys():
                ts[t]["entry_gap_pct"] = entry_gap_pct
            logger.debug(
                f"{symbol}: entry_gap_pct={entry_gap_pct:.2f}% "
                f"(gain_at_entry from candidate)"
            )
        else:
            logger.warning(f"{symbol}: entry_gap_pct not computed (no candidate)")

        # 4c. Inject session_phase for all timestamps
        # session_phase is derived from time of day (mid: 4:00-9:30 NY)
        from jerry_trader.domain.session import get_session_phase_from_epoch_ms

        for t in ts.keys():
            phase = get_session_phase_from_epoch_ms(t)
            ts[t]["session_phase"] = phase.value  # string: "early", "mid", "late"

        total_ts = len(ts)

        # 5. Filter: only output timestamps from first_entry_ms onwards
        # This aligns with live behavior — we don't compute signals for a ticker
        # before it enters the subscription set (top 20).
        # Full data is still used for factor warmup (e.g., EMA, relative_volume),
        # but we only expose the post-subscription window to the event evaluator.
        if ticker_data.candidate and ticker_data.candidate.first_entry_ms > 0:
            entry_ms = ticker_data.candidate.first_entry_ms
            ts = {t: v for t, v in ts.items() if t >= entry_ms}

        total_elapsed = time.time() - total_start

        logger.debug(
            f"BatchEngine: {symbol} — "
            f"{len(ts)} factor timestamps (filtered from {total_ts}), "
            f"{len(bar_specs)} bar, {len(trade_specs)} trade, "
            f"{len(quote_specs)} quote | "
            f"timing: bar={bar_elapsed:.2f}s, tick={tick_elapsed:.2f}s, "
            f"quote={quote_elapsed:.2f}s, "
            f"ff={ff_elapsed:.2f}s, total={total_elapsed:.2f}s"
        )

        return dict(ts)

    def _build_bars_from_trades(self, ticker_data: TickerData) -> list[Bar]:
        """Build bars from trades using Rust BarBuilder.

        Reuses the same BarBuilder that BarsBuilderService uses in live mode.
        Uses ingest_trades_batch for efficient bulk ingestion.
        """
        from jerry_trader._rust import BarBuilder as RustBarBuilder

        builder = RustBarBuilder(["1m"])
        # Tight watermark for batch: no late arrival window, close immediately
        builder.configure_watermark(late_arrival_ms=0, idle_close_ms=1)

        # ingest_trades_batch expects [(ts_ms, price, size_float)]
        batch_trades = [
            (ts, price, float(size)) for ts, price, size in ticker_data.trades
        ]
        builder.ingest_trades_batch(ticker_data.symbol, batch_trades)

        # Flush any remaining incomplete bars
        builder.flush()

        completed = builder.drain_completed()
        bars: list[Bar] = []
        for raw in completed:
            try:
                bars.append(Bar.from_rust_dict(raw))
            except (ValueError, KeyError) as e:
                logger.warning(f"Skipping malformed bar: {e}")

        return bars

    @staticmethod
    def _compute_bar_factors(
        bars: list[Bar],
        specs: list[FactorSpec],
        ts: FactorTimeseries,
    ) -> None:
        """Compute bar factors via Rust PyFactorEngine.compute_batch_bar()."""
        if not bars or not specs:
            return

        engine = PyFactorEngine()
        py_bars = [
            PyBar(b.bar_start, b.open, b.high, b.low, b.close, int(b.volume))
            for b in bars
        ]

        for spec in specs:
            rust_params = {k: float(v) for k, v in spec.params.items()}
            values = engine.compute_batch_bar(spec.rust_key, py_bars, rust_params)
            for bar, val in zip(bars, values):
                if val is not None:
                    ts.setdefault(bar.bar_end, {})[spec.id] = val

    @staticmethod
    def _compute_trade_factors(
        trades: list[tuple[int, float, int]],
        specs: list[FactorSpec],
        ts: FactorTimeseries,
    ) -> None:
        """Batch trade factor warmup via Rust PyFactorEngine.compute_batch_trade()."""
        if not trades or not specs:
            return

        sorted_trades = sorted(trades, key=lambda x: x[0])
        t0 = sorted_trades[0][0]
        t_end = sorted_trades[-1][0]
        first_compute = t0 - (t0 % 1000) + 1000
        compute_ts = list(range(first_compute, t_end + 1000, 1000))
        if not compute_ts:
            return

        engine = PyFactorEngine()
        py_trades = [PyTrade(t, p, s) for t, p, s in sorted_trades]

        for spec in specs:
            rust_params = {k: float(v) for k, v in spec.params.items()}
            values = engine.compute_batch_trade(
                spec.rust_key,
                py_trades,
                compute_ts,
                rust_params,
            )
            for ct, val in zip(compute_ts, values):
                if val is not None:
                    ts.setdefault(ct, {})[spec.id] = val

    @staticmethod
    def _compute_quote_factors_rust(
        quotes: list[tuple[int, float, float, int, int]],
        specs: list[FactorSpec],
        ts: FactorTimeseries,
    ) -> None:
        """Batch compute quote factors via Rust PyFactorEngine.compute_batch_quote()."""
        if not quotes or not specs:
            return

        engine = PyFactorEngine()

        for spec in specs:
            rust_params = {k: float(v) for k, v in spec.params.items()}
            results = engine.compute_batch_quote(spec.rust_key, quotes, rust_params)
            for ts_ms, val in results:
                if val is not None:
                    ts.setdefault(ts_ms, {})[spec.id] = val

    @staticmethod
    def _forward_fill_factors(ts: FactorTimeseries) -> FactorTimeseries:
        """Forward-fill missing factors from most recent previous timestamp.

        Different factor types are computed at different timestamps:
        - Bar factors at bar_end (every 60s)
        - Tick factors at trade timestamps (~1s intervals)
        - Quote factors at quote timestamps (~1s intervals)

        This merges them so each timestamp has all available factors.
        Uses Rust implementation for speed.
        """
        if not ts:
            return ts

        # Convert to list of (ts_ms, dict) for Rust
        ts_list = [(t, factors) for t, factors in ts.items()]

        # Call Rust forward-fill
        result = forward_fill_factors(ts_list)

        # Convert back to dict
        merged: dict[int, dict[str, float]] = {}
        for ts_ms, factor_list in result.merged:
            merged[ts_ms] = dict(factor_list)

        return merged


def get_factors_at(ts: FactorTimeseries, target_ms: int) -> dict[str, float]:
    """Get the most recent factor values at or before target_ms.

    Uses the unified timeseries to find the latest snapshot
    whose timestamp <= target_ms.

    Returns:
        Merged dict of all factor values available at that time.
        Empty dict if no factors exist before target_ms.
    """
    if not ts:
        return {}

    sorted_times = sorted(ts.keys())
    idx = bisect_left(sorted_times, target_ms)

    # Exact match
    if idx < len(sorted_times) and sorted_times[idx] == target_ms:
        return dict(ts[sorted_times[idx]])

    # Just before target
    if idx > 0:
        return dict(ts[sorted_times[idx - 1]])

    return {}
