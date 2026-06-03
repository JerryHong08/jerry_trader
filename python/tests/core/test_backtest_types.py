"""Tests for domain/backtest/types.py.

Covers:
  - Candidate: construction, default peak_volume, immutability, hashability
  - SignalResult: construction, validation, trigger_time_ms, is_winner
  - BacktestResult: construction, default fields, nested signals
"""

from __future__ import annotations

import pytest

from jerry_trader.domain.backtest.types import BacktestResult, Candidate, SignalResult

# ══════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════


def _candidate(**overrides) -> Candidate:
    defaults = dict(
        symbol="AAPL",
        first_entry_ms=1700000000000,
        gain_at_entry=5.2,
        price_at_entry=150.0,
        prev_close=142.5,
        volume_at_entry=50000.0,
        relative_volume=2.3,
        max_gain=7.1,
    )
    defaults.update(overrides)
    return Candidate(**defaults)


def _signal(**overrides) -> SignalResult:
    defaults = dict(
        rule_id="RULE_001",
        symbol="AAPL",
        trigger_time_ns=1_715_000_000_000_000_000,
        trigger_price=150.0,
        entry_price=150.15,
        slippage_pct=0.001,
        factors={"ema_20": 149.0, "rv_20": 2.1},
    )
    defaults.update(overrides)
    return SignalResult(**defaults)


# ══════════════════════════════════════════════════════════════════════
# Candidate
# ══════════════════════════════════════════════════════════════════════


class TestCandidate:
    def test_construction(self):
        c = _candidate()
        assert c.symbol == "AAPL"
        assert c.first_entry_ms == 1700000000000
        assert c.gain_at_entry == 5.2
        assert c.price_at_entry == 150.0
        assert c.prev_close == 142.5
        assert c.volume_at_entry == 50000.0
        assert c.relative_volume == 2.3
        assert c.max_gain == 7.1

    def test_default_peak_volume(self):
        c = _candidate()
        assert c.peak_volume == 0.0

    def test_custom_peak_volume(self):
        c = _candidate(peak_volume=75000.0)
        assert c.peak_volume == 75000.0

    def test_immutable(self):
        c = _candidate()
        with pytest.raises(Exception):
            c.symbol = "MSFT"  # type: ignore[misc]

    def test_hashable(self):
        c = _candidate()
        assert hash(c) is not None

    def test_equality(self):
        a = _candidate()
        b = _candidate()
        assert a == b
        assert hash(a) == hash(b)

    def test_different_symbol_not_equal(self):
        a = _candidate(symbol="AAPL")
        b = _candidate(symbol="TSLA")
        assert a != b

    def test_repr_includes_symbol(self):
        c = _candidate(symbol="AAPL")
        assert "AAPL" in repr(c)


# ══════════════════════════════════════════════════════════════════════
# SignalResult
# ══════════════════════════════════════════════════════════════════════


class TestSignalResultConstruction:
    def test_minimal_construction(self):
        s = _signal()
        assert s.rule_id == "RULE_001"
        assert s.symbol == "AAPL"
        assert s.trigger_time_ns == 1_715_000_000_000_000_000
        assert s.trigger_price == 150.0
        assert s.entry_price == 150.15
        assert s.slippage_pct == 0.001
        assert s.factors == {"ema_20": 149.0, "rv_20": 2.1}

    def test_default_optional_fields(self):
        s = _signal()
        assert s.returns == {}
        assert s.mfe is None
        assert s.mae is None
        assert s.time_to_peak_ms is None
        assert s.ask_price is None

    def test_with_returns(self):
        s = _signal(returns={"1m": 0.023, "5m": -0.015})
        assert s.returns == {"1m": 0.023, "5m": -0.015}

    def test_with_mfe_mae(self):
        s = _signal(mfe=0.05, mae=-0.02)
        assert s.mfe == 0.05
        assert s.mae == -0.02

    def test_with_time_to_peak(self):
        s = _signal(time_to_peak_ms=1715000030000)
        assert s.time_to_peak_ms == 1715000030000

    def test_with_ask_price(self):
        s = _signal(ask_price=150.10)
        assert s.ask_price == 150.10


class TestSignalResultValidation:
    def test_zero_entry_price_raises(self):
        with pytest.raises(ValueError, match="entry_price must be positive"):
            _signal(entry_price=0.0)

    def test_negative_entry_price_raises(self):
        with pytest.raises(ValueError, match="entry_price must be positive"):
            _signal(entry_price=-0.01)

    def test_zero_trigger_price_raises(self):
        with pytest.raises(ValueError, match="trigger_price must be positive"):
            _signal(trigger_price=0.0)

    def test_negative_trigger_price_raises(self):
        with pytest.raises(ValueError, match="trigger_price must be positive"):
            _signal(trigger_price=-150.0)


class TestSignalResultProperties:
    def test_trigger_time_ms(self):
        s = _signal(trigger_time_ns=1_715_000_000_123_456_789)
        assert s.trigger_time_ms == 1_715_000_000_123

    def test_trigger_time_ms_exact(self):
        s = _signal(trigger_time_ns=1_715_000_000_000_000_000)
        assert s.trigger_time_ms == 1_715_000_000_000

    def test_is_winner_true(self):
        s = _signal(returns={"1m": 0.01, "5m": -0.02})
        assert s.is_winner is True  # At least one positive

    def test_is_winner_all_negative(self):
        s = _signal(returns={"1m": -0.01, "5m": -0.02})
        assert s.is_winner is False

    def test_is_winner_empty_returns(self):
        s = _signal(returns={})
        assert s.is_winner is False  # any([]) → False

    def test_is_winner_zero_return(self):
        """Zero return is not > 0, so not a winner."""
        s = _signal(returns={"1m": 0.0})
        assert s.is_winner is False


class TestSignalResultImmutability:
    def test_frozen(self):
        s = _signal()
        with pytest.raises(Exception):
            s.entry_price = 999.0  # type: ignore[misc]

    def test_not_hashable_due_to_factors_dict(self):
        """SignalResult is frozen but contains a dict (unhashable)."""
        s = _signal()
        with pytest.raises(TypeError):
            hash(s)

    def test_equal_signals(self):
        a = _signal()
        b = _signal()
        assert a == b

    def test_factors_dict_equality(self):
        """Signals with same factors dict are equal."""
        a = _signal(factors={"a": 1.0})
        b = _signal(factors={"a": 1.0})
        assert a == b


# ══════════════════════════════════════════════════════════════════════
# BacktestResult
# ══════════════════════════════════════════════════════════════════════


class TestBacktestResult:
    def test_minimal_construction(self):
        result = BacktestResult(
            date="2026-03-06",
            rules_tested=["RULE_001"],
            total_signals=0,
            signals=[],
        )
        assert result.date == "2026-03-06"
        assert result.rules_tested == ["RULE_001"]
        assert result.total_signals == 0
        assert result.signals == []

    def test_default_fields(self):
        result = BacktestResult(
            date="2026-03-06",
            rules_tested=["RULE_001"],
            total_signals=0,
            signals=[],
        )
        assert result.run_id is None
        assert result.win_rate == {}
        assert result.avg_return == {}
        assert result.profit_factor == 0.0
        assert result.avg_slippage == 0.0
        assert result.avg_mfe == 0.0
        assert result.avg_mae == 0.0
        assert result.avg_time_to_peak_ms == 0.0

    def test_with_signals(self):
        sig = _signal(returns={"1m": 0.02})
        result = BacktestResult(
            date="2026-03-06",
            run_id="RUN-001",
            rules_tested=["RULE_001", "RULE_002"],
            total_signals=1,
            signals=[sig],
            win_rate={"1m": 0.65},
            avg_return={"1m": 0.012},
            profit_factor=2.5,
        )
        assert result.run_id == "RUN-001"
        assert result.rules_tested == ["RULE_001", "RULE_002"]
        assert result.total_signals == 1
        assert len(result.signals) == 1
        assert result.signals[0].rule_id == "RULE_001"
        assert result.win_rate == {"1m": 0.65}
        assert result.avg_return == {"1m": 0.012}
        assert result.profit_factor == 2.5

    def test_multiple_signals(self):
        sigs = [
            _signal(rule_id="R1", symbol="AAPL"),
            _signal(rule_id="R2", symbol="TSLA"),
        ]
        result = BacktestResult(
            date="2026-03-06",
            rules_tested=["R1", "R2"],
            total_signals=2,
            signals=sigs,
        )
        assert result.total_signals == 2
        assert result.signals[0].symbol == "AAPL"
        assert result.signals[1].symbol == "TSLA"

    def test_immutable(self):
        result = BacktestResult(
            date="2026-03-06",
            rules_tested=["RULE_001"],
            total_signals=0,
            signals=[],
        )
        with pytest.raises(Exception):
            result.date = "2026-03-07"  # type: ignore[misc]

    def test_not_hashable_due_to_list_fields(self):
        """BacktestResult is frozen but contains lists (unhashable)."""
        result = BacktestResult(
            date="2026-03-06",
            rules_tested=["RULE_001"],
            total_signals=0,
            signals=[],
        )
        with pytest.raises(TypeError):
            hash(result)
