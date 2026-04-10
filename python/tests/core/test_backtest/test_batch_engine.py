"""Tests for batch_engine — FactorEngineBatchAdapter."""

from unittest.mock import MagicMock

from jerry_trader.domain.backtest.types import Candidate
from jerry_trader.domain.market import Bar
from jerry_trader.services.backtest.batch_engine import (
    FactorEngineBatchAdapter,
    FactorTimeseries,
    get_factors_at,
)
from jerry_trader.services.backtest.config import TickerData


def _make_candidate(symbol: str = "TEST") -> Candidate:
    return Candidate(
        symbol=symbol,
        first_entry_ms=1000,
        gain_at_entry=5.0,
        price_at_entry=10.0,
        prev_close=9.5,
        volume_at_entry=1000.0,
        relative_volume=2.0,
        max_gain=10.0,
    )


def _make_bar(symbol: str, bar_start: int, close: float) -> Bar:
    return Bar(
        symbol=symbol,
        timeframe="1m",
        open=close - 0.1,
        high=close + 0.2,
        low=close - 0.2,
        close=close,
        volume=1000.0,
        trade_count=50,
        vwap=close,
        bar_start=bar_start,
        bar_end=bar_start + 60_000,
        session="premarket",
    )


class TestBuildBarsFromTrades:
    """Test bar building from trades via Rust BarBuilder."""

    def test_bars_built_from_trades(self):
        """Trades should produce bars via ingest_trades_batch."""
        # Create trades spanning 2 minutes at 10s intervals
        base_ts = 9 * 3600 * 1000  # 9:00 AM ET in ms
        trades = [(base_ts + i * 10_000, 10.0 + i * 0.01, 100) for i in range(15)]
        td = TickerData(symbol="TEST", trades=trades, candidate=_make_candidate())

        adapter = FactorEngineBatchAdapter.__new__(FactorEngineBatchAdapter)
        bars = adapter._build_bars_from_trades(td)

        # Should produce at least 1 bar from these trades
        assert len(bars) >= 1
        assert all(isinstance(b, Bar) for b in bars)

    def test_no_trades_produces_no_bars(self):
        td = TickerData(symbol="TEST", candidate=_make_candidate())
        adapter = FactorEngineBatchAdapter.__new__(FactorEngineBatchAdapter)
        bars = adapter._build_bars_from_trades(td)
        assert len(bars) == 0


class TestComputeBarFactors:
    """Test bar factor computation via live indicator instances."""

    def test_bar_factors_computed(self):
        """Bar indicators should produce factor values after warmup."""
        bars = [_make_bar("TEST", i * 60_000, 10.0 + i * 0.5) for i in range(25)]
        ts: FactorTimeseries = {}

        # Use the static method directly — it just walks bars and updates indicators
        from jerry_trader.services.factor.factor_registry import get_factor_registry

        registry = get_factor_registry()
        all_indicators = registry.create_indicators_for_type("bar")
        bar_indicators = [i for i in all_indicators if hasattr(i, "update")]

        FactorEngineBatchAdapter._compute_bar_factors(bars, bar_indicators, ts)

        # Should have entries for bars after warmup
        assert len(ts) > 0


class TestComputeTickFactors:
    """Test trade_rate factor via Rust bootstrap_trade_rate."""

    def test_trade_rate_computed(self):
        trades = [(i * 1000, 10.0 + i * 0.01, 100) for i in range(100)]
        ts: FactorTimeseries = {}

        # Create a mock tick indicator with window_ms
        mock_ind = MagicMock()
        mock_ind.name = "trade_rate"
        mock_ind.window_ms = 20_000
        mock_ind.min_trades = 5

        FactorEngineBatchAdapter._compute_tick_factors(trades, [mock_ind], ts)

        trade_rate_keys = [k for k, v in ts.items() if "trade_rate" in v]
        assert len(trade_rate_keys) > 0

    def test_no_trades_skips(self):
        ts: FactorTimeseries = {}
        mock_ind = MagicMock()
        mock_ind.name = "trade_rate"

        FactorEngineBatchAdapter._compute_tick_factors([], [mock_ind], ts)
        assert len(ts) == 0


class TestComputeQuoteFactors:
    """Test quote factor computation via live indicator instances."""

    def test_quote_factors_computed(self):
        quotes = [
            (1000, 10.0, 10.5, 100, 200),
            (2000, 10.1, 10.6, 150, 250),
            (3000, 10.2, 10.7, 200, 300),
        ]
        ts: FactorTimeseries = {}

        # Use live QuoteIndicator instances from registry
        from jerry_trader.services.factor.factor_registry import get_factor_registry

        registry = get_factor_registry()
        all_quote = registry.create_indicators_for_type("quote")
        from jerry_trader.services.factor.indicators import QuoteIndicator

        quote_indicators = [i for i in all_quote if isinstance(i, QuoteIndicator)]

        if quote_indicators:
            FactorEngineBatchAdapter._compute_quote_factors(
                quotes, quote_indicators, ts
            )
            # Should have factor entries
            assert len(ts) > 0


class TestGetFactorsAt:
    """Test get_factors_at lookup."""

    def test_exact_match(self):
        ts: FactorTimeseries = {
            1000: {"momentum": 0.05},
            2000: {"momentum": 0.10},
        }
        result = get_factors_at(ts, 2000)
        assert result["momentum"] == 0.10

    def test_before_target(self):
        ts: FactorTimeseries = {
            1000: {"momentum": 0.05},
            2000: {"momentum": 0.10},
        }
        result = get_factors_at(ts, 1500)
        assert result["momentum"] == 0.05

    def test_before_all(self):
        ts: FactorTimeseries = {1000: {"momentum": 0.05}}
        result = get_factors_at(ts, 500)
        assert result == {}

    def test_empty_ts(self):
        result = get_factors_at({}, 1000)
        assert result == {}

    def test_merged_sources(self):
        """Factors from different sources at the same timestamp are merged."""
        ts: FactorTimeseries = {
            1000: {"momentum": 0.05, "trade_rate": 3.0, "spread_pct": 0.01},
        }
        result = get_factors_at(ts, 1000)
        assert result["momentum"] == 0.05
        assert result["trade_rate"] == 3.0
        assert result["spread_pct"] == 0.01
