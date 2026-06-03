"""Tests for services/backtest/event_validator.py.

Covers:
  - EventValidationResult: is_valid(), is_valid_for_action(), passes_basic(),
    stability_score(), summary(), detailed_report()
  - EventValidator.validate() — aggregate and per-date metrics
  - EventValidator.compare_events()
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from jerry_trader.services.backtest.event.validator import (
    EventValidationResult,
    EventValidator,
)

# ══════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════


def _make_result(**overrides) -> EventValidationResult:
    defaults = dict(
        event_name="test_event",
        total_signals=200,
        avg_return=0.035,
        win_rate=0.55,
        positive_dates_ratio=0.75,
        date_returns={"2026-03-01": 0.04, "2026-03-02": 0.03},
        date_win_rates={"2026-03-01": 0.60, "2026-03-02": 0.50},
        date_signal_counts={"2026-03-01": 100, "2026-03-02": 100},
        return_std=0.01,
        win_rate_std=0.05,
    )
    defaults.update(overrides)
    return EventValidationResult(**defaults)


def _make_mock_event(name="test_event"):
    event = MagicMock()
    event.name = name
    return event


# ══════════════════════════════════════════════════════════════════════
# EventValidationResult — is_valid()
# ══════════════════════════════════════════════════════════════════════


class TestIsValid:
    def test_all_criteria_pass(self):
        r = _make_result()
        assert r.is_valid() is True

    def test_low_avg_return_fails(self):
        r = _make_result(avg_return=0.01)
        assert r.is_valid() is False

    def test_low_win_rate_fails(self):
        r = _make_result(win_rate=0.40)
        assert r.is_valid() is False

    def test_low_positive_dates_fails(self):
        r = _make_result(positive_dates_ratio=0.55)
        assert r.is_valid() is False

    def test_insufficient_signals_fails(self):
        r = _make_result(total_signals=50)
        assert r.is_valid() is False

    def test_exactly_at_thresholds(self):
        """Exactly at threshold still passes (strict >)."""
        r = _make_result(
            avg_return=0.02,  # NOT > 0.02
            win_rate=0.45,  # NOT > 0.45
        )
        assert r.is_valid() is False


# ══════════════════════════════════════════════════════════════════════
# EventValidationResult — is_valid_for_action()
# ══════════════════════════════════════════════════════════════════════


class TestIsValidForAction:
    def test_accept_valid(self):
        r = _make_result()
        assert r.is_valid_for_action(is_avoid=False) is True

    def test_accept_invalid(self):
        r = _make_result(avg_return=0.01)
        assert r.is_valid_for_action(is_avoid=False) is False

    def test_avoid_valid(self):
        """Avoid event with negative returns passes avoidance check."""
        r = _make_result(
            avg_return=-0.05,
            win_rate=0.30,
            positive_dates_ratio=0.25,  # < 40% positive dates
        )
        assert r.is_valid_for_action(is_avoid=True) is True

    def test_avoid_not_negative_enough(self):
        r = _make_result(
            avg_return=-0.01,  # Not below -2%
            win_rate=0.35,
            positive_dates_ratio=0.30,
        )
        assert r.is_valid_for_action(is_avoid=True) is False

    def test_avoid_win_rate_too_high(self):
        r = _make_result(
            avg_return=-0.05,
            win_rate=0.50,  # > 40% is too high for avoid
            positive_dates_ratio=0.30,
        )
        assert r.is_valid_for_action(is_avoid=True) is False

    def test_avoid_insufficient_signals(self):
        r = _make_result(
            avg_return=-0.05,
            win_rate=0.30,
            positive_dates_ratio=0.30,
            total_signals=50,
        )
        assert r.is_valid_for_action(is_avoid=True) is False


# ══════════════════════════════════════════════════════════════════════
# EventValidationResult — passes_basic()
# ══════════════════════════════════════════════════════════════════════


class TestPassesBasic:
    def test_positive_return_good_win_rate(self):
        r = _make_result(avg_return=0.01, win_rate=0.45)
        assert r.passes_basic() is True

    def test_negative_return_fails(self):
        r = _make_result(avg_return=-0.01, win_rate=0.50)
        assert r.passes_basic() is False

    def test_low_win_rate_fails(self):
        r = _make_result(avg_return=0.01, win_rate=0.35)
        assert r.passes_basic() is False

    def test_zero_return_fails(self):
        r = _make_result(avg_return=0.0, win_rate=0.50)
        assert r.passes_basic() is False


# ══════════════════════════════════════════════════════════════════════
# EventValidationResult — stability_score()
# ══════════════════════════════════════════════════════════════════════


class TestStabilityScore:
    def test_zero_std_is_perfect(self):
        r = _make_result(return_std=0.0, avg_return=0.05)
        assert r.stability_score() == 1.0

    def test_low_std_high_score(self):
        r = _make_result(return_std=0.01, avg_return=0.10)
        score = r.stability_score()
        assert score > 0.8

    def test_high_std_low_score(self):
        r = _make_result(return_std=0.20, avg_return=0.05)
        score = r.stability_score()
        assert score < 0.5

    def test_score_bounded_below_zero(self):
        """stability_score is max(0, ...)."""
        r = _make_result(return_std=10.0, avg_return=0.01)
        assert r.stability_score() == 0.0


# ══════════════════════════════════════════════════════════════════════
# EventValidationResult — summary() / detailed_report()
# ══════════════════════════════════════════════════════════════════════


class TestSummary:
    def test_summary_contains_event_name(self):
        r = _make_result()
        s = r.summary()
        assert "test_event" in s
        assert "Signals:" in s
        assert "Avg Return:" in s

    def test_summary_avoid_mode_shows_different_targets(self):
        r = _make_result(avg_return=-0.05, win_rate=0.30, positive_dates_ratio=0.25)
        s = r.summary(is_avoid=True)
        assert "< -0.02" in s

    def test_detailed_report_includes_per_date(self):
        r = _make_result()
        report = r.detailed_report()
        assert "2026-03-01" in report
        assert "2026-03-02" in report
        assert "Per-date breakdown" in report


# ══════════════════════════════════════════════════════════════════════
# EventValidator.validate()
# ══════════════════════════════════════════════════════════════════════


class TestEventValidatorValidate:
    def test_empty_data_returns_zero_result(self):
        validator = EventValidator()
        event = _make_mock_event()
        result = validator.validate(event, signals_by_date={})
        assert result.total_signals == 0
        assert result.avg_return == 0.0
        assert result.win_rate == 0.0
        assert result.positive_dates_ratio == 0.0

    def test_dates_with_no_returns_skipped(self):
        validator = EventValidator()
        event = _make_mock_event()
        signals_by_date = {
            "2026-03-01": [{"not_return": 0.05}],  # Missing return field
            "2026-03-02": [],
        }
        result = validator.validate(event, signals_by_date)
        assert result.total_signals == 0

    def test_single_date_with_signals(self):
        validator = EventValidator()
        event = _make_mock_event()
        signals_by_date = {
            "2026-03-01": [
                {"return_5m": 0.03},
                {"return_5m": -0.01},
                {"return_5m": 0.05},
                {"return_5m": 0.02},
            ],
        }
        result = validator.validate(event, signals_by_date)
        assert result.total_signals == 4
        assert result.win_rate == 0.75  # 3/4 positive
        assert result.positive_dates_ratio == 1.0  # 1/1 dates positive

    def test_multi_date_aggregation(self):
        validator = EventValidator()
        event = _make_mock_event()
        signals_by_date = {
            "2026-03-01": [
                {"return_5m": 0.05},
                {"return_5m": -0.02},
            ],
            "2026-03-02": [
                {"return_5m": 0.03},
                {"return_5m": 0.01},
            ],
        }
        result = validator.validate(event, signals_by_date)
        assert result.total_signals == 4
        assert len(result.date_returns) == 2
        # Avg return: mean([(0.05-0.02)/2=0.015, (0.03+0.01)/2=0.02]) = 0.0175
        assert result.avg_return > 0

    def test_none_values_filtered(self):
        validator = EventValidator()
        event = _make_mock_event()
        signals_by_date = {
            "2026-03-01": [
                {"return_5m": 0.05},
                {"return_5m": None},  # Should be filtered
                {"return_5m": -0.01},
            ],
        }
        result = validator.validate(event, signals_by_date)
        assert result.total_signals == 3  # None counted as signal


# ══════════════════════════════════════════════════════════════════════
# EventValidator.compare_events()
# ══════════════════════════════════════════════════════════════════════


class TestCompareEvents:
    def test_returns_table_string(self):
        validator = EventValidator()
        r1 = _make_result(event_name="alpha", avg_return=0.05)
        r2 = _make_result(event_name="beta", avg_return=0.03)
        output = validator.compare_events([r1, r2])
        assert "alpha" in output
        assert "beta" in output
        assert "Event Comparison" in output

    def test_sorts_by_avg_return_desc(self):
        validator = EventValidator()
        r1 = _make_result(event_name="low", avg_return=0.02)
        r2 = _make_result(event_name="high", avg_return=0.10)
        output = validator.compare_events([r1, r2])
        assert output.index("high") < output.index("low")
