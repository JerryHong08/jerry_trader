"""Batch factor engine for backtest — reuses live FactorRegistry indicators.

Key principle: backtest and live share the *exact same* indicator instances
(created from FactorRegistry). Any new indicator added to the registry
automatically works in backtest — no parallel computation path.

Three indicator types:
  1. Bar indicators (EMA, etc.) — walk bars chronologically, call update()
  2. Tick indicators (TradeRate) — Rust bootstrap_trade_rate() for batch warmup
  3. Quote indicators (spread, etc.) — walk quotes chronologically

Output: FactorTimeseries = dict[ts_ms, dict[str, float]]
"""

from __future__ import annotations

from bisect import bisect_left
from collections import defaultdict

from jerry_trader._rust import bootstrap_trade_rate
from jerry_trader.domain.market import Bar
from jerry_trader.services.backtest.config import TickerData
from jerry_trader.services.factor.factor_registry import (
    FactorRegistry,
    get_factor_registry,
)
from jerry_trader.services.factor.indicators import (
    BarIndicator,
    QuoteIndicator,
    TickIndicator,
)
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.batch_engine", log_to_file=True)

# Type alias for unified factor timeseries
FactorTimeseries = dict[int, dict[str, float]]


class FactorEngineBatchAdapter:
    """Reuses live FactorRegistry indicators for batch backtest.

    Creates fresh indicator instances from the same FactorRegistry
    that live FactorEngine uses. Feeds data in chronological order
    and collects all factor outputs into an in-memory FactorTimeseries.

    This guarantees backtest factors are identical to live factors —
    new indicators added to FactorRegistry automatically work here.
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
        symbol = ticker_data.symbol
        ts: FactorTimeseries = defaultdict(dict)

        # Create fresh indicator instances — same source as live FactorEngine
        all_indicators = self._registry.create_indicators_for_type("bar")
        bar_indicators: list[BarIndicator] = [
            i for i in all_indicators if isinstance(i, BarIndicator)
        ]

        all_tick = self._registry.create_indicators_for_type("trade")
        tick_indicators: list[TickIndicator] = [
            i for i in all_tick if isinstance(i, TickIndicator)
        ]

        all_quote = self._registry.create_indicators_for_type("quote")
        quote_indicators: list[QuoteIndicator] = [
            i for i in all_quote if isinstance(i, QuoteIndicator)
        ]

        # 1. Build bars from trades via BarBuilder, then feed to bar indicators
        if bar_indicators:
            bars = self._build_bars_from_trades(ticker_data)
            self._compute_bar_factors(bars, bar_indicators, ts)

        # 2. Tick factors via Rust batch warmup (same as FactorEngine._process_bootstrap_trades)
        if tick_indicators and ticker_data.trades:
            self._compute_tick_factors(ticker_data.trades, tick_indicators, ts)

        # 3. Quote factors (if any quote indicators registered)
        if quote_indicators and ticker_data.quotes:
            self._compute_quote_factors(ticker_data.quotes, quote_indicators, ts)

        logger.debug(
            f"BatchEngine: {symbol} — "
            f"{len(ts)} factor timestamps, "
            f"{len(bar_indicators)} bar ind, {len(tick_indicators)} tick ind, "
            f"{len(quote_indicators)} quote ind"
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
        indicators: list[BarIndicator],
        ts: FactorTimeseries,
    ) -> None:
        """Walk bars chronologically, update bar indicators.

        Same loop as FactorEngine._on_bar() / _bootstrap_bars_for_timeframe().
        """
        for bar in bars:
            factors: dict[str, float] = {}
            for ind in indicators:
                value = ind.update(bar)
                if value is not None:
                    factors[ind.name] = value

            if factors:
                ts.setdefault(bar.bar_end, {}).update(factors)

    @staticmethod
    def _compute_tick_factors(
        trades: list[tuple[int, float, int]],
        indicators: list[TickIndicator],
        ts: FactorTimeseries,
    ) -> None:
        """Batch tick indicator warmup via Rust.

        Same approach as FactorEngine._process_bootstrap_trades():
        uses Rust bootstrap_trade_rate for windowed tick indicators.
        """
        timestamps = [int(t) for t, _, _ in trades]
        if not timestamps:
            return

        for ind in indicators:
            if hasattr(ind, "window_ms"):
                result = bootstrap_trade_rate(
                    timestamps,
                    window_ms=getattr(ind, "window_ms", 20_000),
                    min_trades=getattr(ind, "min_trades", 5),
                    compute_interval_ms=1000,
                )
                for ts_ms, rate in result.snapshots:
                    ts.setdefault(ts_ms, {})[ind.name] = rate
            else:
                # Fallback: Python loop warmup for non-windowed tick indicators
                last_compute_ms = 0
                for ts_ms, price, size in trades:
                    ts_ms = int(ts_ms)
                    ind.on_tick(ts_ms, float(price), int(size))
                    if ts_ms - last_compute_ms >= 1000:
                        value = ind.compute(ts_ms)
                        if value is not None:
                            ts.setdefault(ts_ms, {})[ind.name] = value
                        last_compute_ms = ts_ms

    @staticmethod
    def _compute_quote_factors(
        quotes: list[tuple[int, float, float, int, int]],
        indicators: list[QuoteIndicator],
        ts: FactorTimeseries,
    ) -> None:
        """Walk quotes chronologically, update quote indicators.

        Same pattern as FactorEngine._on_quote() + _compute_loop().
        Compute every 1 second.
        """
        last_compute_ms = 0
        for ts_ms, bid, ask, bid_size, ask_size in quotes:
            ts_ms = int(ts_ms)
            for ind in indicators:
                ind.on_quote(
                    ts_ms, float(bid), float(ask), int(bid_size), int(ask_size)
                )

            if ts_ms - last_compute_ms >= 1000:
                for ind in indicators:
                    value = ind.compute(ts_ms)
                    if value is not None:
                        ts.setdefault(ts_ms, {})[ind.name] = value
                last_compute_ms = ts_ms


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
