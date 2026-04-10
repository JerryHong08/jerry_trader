"""Tests for metrics — slippage, returns, MFE/MAE computation."""

from jerry_trader.domain.backtest.types import Candidate
from jerry_trader.services.backtest.batch_engine import FactorTimeseries
from jerry_trader.services.backtest.config import TickerData
from jerry_trader.services.backtest.evaluator import TriggerPoint
from jerry_trader.services.backtest.metrics import (
    compute_batch_metrics,
    compute_entry_price,
    compute_returns_and_metrics,
    find_ask_at_time,
    get_price_at,
)


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


class TestSlippage:
    def test_entry_price_with_ask(self):
        entry, slip = compute_entry_price(10.0, ask_price=10.05, slippage_buffer=0.001)
        assert abs(entry - 10.06005) < 1e-6
        assert abs(slip - 0.006005) < 1e-6

    def test_entry_price_no_ask_uses_default(self):
        entry, slip = compute_entry_price(10.0, ask_price=None, default_slippage=0.002)
        assert abs(entry - 10.02) < 1e-6
        assert abs(slip - 0.002) < 1e-6

    def test_entry_price_zero_ask_uses_default(self):
        entry, slip = compute_entry_price(10.0, ask_price=0.0, default_slippage=0.002)
        assert abs(entry - 10.02) < 1e-6


class TestFindAskAtTime:
    def test_exact_match(self):
        quotes = [(1000, 10.0, 10.5, 100, 200), (2000, 10.1, 10.6, 150, 250)]
        assert find_ask_at_time(quotes, 2000) == 10.6

    def test_before_match(self):
        quotes = [(1000, 10.0, 10.5, 100, 200), (2000, 10.1, 10.6, 150, 250)]
        assert find_ask_at_time(quotes, 1500) == 10.5

    def test_before_all(self):
        quotes = [(1000, 10.0, 10.5, 100, 200)]
        assert find_ask_at_time(quotes, 500) is None

    def test_empty_quotes(self):
        assert find_ask_at_time([], 1000) is None


class TestGetPriceAt:
    def test_exact_match(self):
        ts = [1000, 2000, 3000]
        prices = [10.0, 10.5, 11.0]
        assert get_price_at(ts, prices, 2000) == 10.5

    def test_before_match(self):
        ts = [1000, 2000, 3000]
        prices = [10.0, 10.5, 11.0]
        assert get_price_at(ts, prices, 2500) == 10.5

    def test_before_all(self):
        ts = [1000, 2000]
        prices = [10.0, 10.5]
        assert get_price_at(ts, prices, 500) is None

    def test_empty(self):
        assert get_price_at([], [], 1000) is None


class TestComputeReturnsAndMetrics:
    def _make_trigger(
        self, symbol: str = "TEST", trigger_ms: int = 1000, price: float = 10.0
    ) -> TriggerPoint:
        return TriggerPoint(
            rule_id="test_rule",
            symbol=symbol,
            trigger_time_ms=trigger_ms,
            trigger_price=price,
            factors={"trade_rate": 5.0},
        )

    def test_basic_return_computation(self):
        # Price goes from 10.0 → 10.5 after 1 minute
        trades = [
            (1000, 10.0, 100),
            (61_000, 10.5, 100),  # 1 minute later
            (121_000, 10.2, 100),  # 2 minutes later
        ]
        quotes = [(999, 10.0, 10.01, 100, 100)]
        td = TickerData(
            symbol="TEST", trades=trades, quotes=quotes, candidate=_make_candidate()
        )
        ts: FactorTimeseries = {1000: {"trade_rate": 5.0}}

        trigger = self._make_trigger(trigger_ms=1000, price=10.0)
        result = compute_returns_and_metrics(
            trigger,
            td,
            ts,
            horizons_ms=[60_000, 120_000],
            slippage_buffer=0.001,
        )

        assert result.trigger_price == 10.0
        # Entry price = ask * (1+buffer) = 10.01 * 1.001 = 10.02001
        assert abs(result.entry_price - 10.02001) < 1e-4
        assert result.rule_id == "test_rule"
        assert "1m" in result.returns
        assert "2m" in result.returns
        # 1m return = (10.5 - entry) / entry ≈ positive
        assert result.returns["1m"] > 0

    def test_mfe_mae(self):
        # Price fluctuates after trigger
        trades = [
            (1000, 10.0, 100),  # trigger time
            (30_000, 10.8, 100),  # high point → MFE
            (60_000, 9.5, 100),  # low point → MAE
            (90_000, 10.2, 100),
        ]
        quotes = [(999, 10.0, 10.01, 100, 100)]
        td = TickerData(
            symbol="TEST", trades=trades, quotes=quotes, candidate=_make_candidate()
        )
        ts: FactorTimeseries = {}

        trigger = self._make_trigger(trigger_ms=1000, price=10.0)
        result = compute_returns_and_metrics(
            trigger, td, ts, horizons_ms=[60_000], slippage_buffer=0.001
        )

        # MFE should be positive (from the 10.8 peak)
        assert result.mfe is not None and result.mfe > 0
        # MAE should be negative (from the 9.5 dip)
        assert result.mae is not None and result.mae < 0
        # Time to peak should be around 30_000
        assert result.time_to_peak_ms == 30_000

    def test_no_trades_after_trigger(self):
        # No trades at all after trigger — empty trade list
        td = TickerData(
            symbol="TEST", trades=[], quotes=[], candidate=_make_candidate()
        )
        ts: FactorTimeseries = {}

        trigger = self._make_trigger(trigger_ms=1000, price=10.0)
        result = compute_returns_and_metrics(
            trigger, td, ts, horizons_ms=[60_000], slippage_buffer=0.001
        )

        assert result.mfe is None
        assert result.mae is None
        assert result.time_to_peak_ms is None
        assert len(result.returns) == 0

    def test_slippage_when_no_quotes(self):
        trades = [
            (1000, 10.0, 100),
            (61_000, 10.5, 100),
        ]
        td = TickerData(
            symbol="TEST", trades=trades, quotes=[], candidate=_make_candidate()
        )

        trigger = self._make_trigger(trigger_ms=1000, price=10.0)
        result = compute_returns_and_metrics(
            trigger,
            td,
            {},
            horizons_ms=[60_000],
            slippage_buffer=0.001,
            default_slippage=0.002,
        )

        # entry = 10.0 * (1 + 0.002) = 10.02
        assert abs(result.entry_price - 10.02) < 1e-6
        assert abs(result.slippage_pct - 0.002) < 1e-6


class TestComputeBatchMetrics:
    def test_multiple_triggers(self):
        trades = [
            (1000, 10.0, 100),
            (2000, 11.0, 100),  # trigger 2
            (61_000, 10.5, 100),
            (62_000, 11.5, 100),
        ]
        td = TickerData(
            symbol="TEST",
            trades=trades,
            quotes=[],
            candidate=_make_candidate(),
        )

        triggers = [
            TriggerPoint("r1", "TEST", 1000, 10.0, {"trade_rate": 5.0}),
            TriggerPoint("r1", "TEST", 2000, 11.0, {"trade_rate": 6.0}),
        ]

        results = compute_batch_metrics(
            triggers, td, {}, horizons_ms=[60_000], slippage_buffer=0.001
        )

        assert len(results) == 2
        assert results[0].trigger_time_ms == 1000
        assert results[1].trigger_time_ms == 2000
