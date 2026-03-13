"""
Tests for jerry_trader.core.factors.compute

Covers:
    - z_score: rolling z-score calculation
    - price_accel: direction-aware price acceleration
    - compute_trade_rate: trades per second in a time window
"""

import math
from collections import deque

import pytest

from jerry_trader.core.factors.compute import compute_trade_rate, price_accel, z_score

# =========================================================================
# z_score
# =========================================================================


class TestZScore:
    """Tests for z_score."""

    def test_insufficient_data_returns_none(self):
        """Need at least 2 samples in history."""
        assert z_score(5.0, deque()) is None
        assert z_score(5.0, deque([3.0])) is None

    def test_two_samples(self):
        """Minimum valid case: 2 samples → meaningful z-score."""
        history = deque([4.0, 6.0])  # mean=5, std=1
        result = z_score(7.0, history)
        assert result is not None
        assert result == pytest.approx(2.0)

    def test_value_equals_mean(self):
        """Value at the mean → z = 0."""
        history = deque([2.0, 4.0, 6.0, 8.0])  # mean=5
        result = z_score(5.0, history)
        assert result == pytest.approx(0.0)

    def test_negative_z_score(self):
        """Value below mean → negative z."""
        history = deque([10.0, 10.0, 10.0, 10.0, 10.0])  # mean=10, std=0 → but...
        # Need some variance
        history = deque([8.0, 10.0, 12.0])  # mean=10, var=2.667, std≈1.633
        result = z_score(7.0, history)
        assert result is not None
        assert result < 0

    def test_zero_std_returns_zero(self):
        """All identical values → std ≈ 0 → returns 0.0."""
        history = deque([5.0, 5.0, 5.0, 5.0])
        result = z_score(5.0, history)
        assert result == 0.0

    def test_known_values(self):
        """Verify against hand-calculated z-score."""
        # history: [2, 4, 6, 8, 10]
        # mean = 6, var = (16+4+0+4+16)/5 = 8, std = sqrt(8) ≈ 2.828
        # z(12) = (12-6)/2.828 ≈ 2.121
        history = deque([2.0, 4.0, 6.0, 8.0, 10.0])
        result = z_score(12.0, history)
        expected = (12.0 - 6.0) / math.sqrt(8.0)
        assert result == pytest.approx(expected, rel=1e-6)

    def test_large_history(self):
        """Should handle large deques without issue."""
        history = deque(range(1, 1001))  # 1..1000
        result = z_score(1500.0, history)
        assert result is not None
        assert result > 0


# =========================================================================
# price_accel
# =========================================================================


class TestPriceAccel:
    """Tests for price_accel."""

    def test_empty_inputs(self):
        """Empty lists → 0.0."""
        assert price_accel([], []) == 0.0

    def test_single_trade_each(self):
        """Single trade in each half → 0.0 (can't compute rate from 1 point)."""
        recent = [(1000, 100.0)]
        older = [(500, 99.0)]
        assert price_accel(recent, older) == 0.0

    def test_accelerating_upward(self):
        """Price accelerating upward → positive accel."""
        # Older half: slow rise — 100 → 101 over 1 second
        older = [(0, 100.0), (1000, 101.0)]
        # Recent half: fast rise — 101 → 104 over 1 second
        recent = [(1000, 101.0), (2000, 104.0)]

        result = price_accel(recent, older)
        assert result > 0, f"Expected positive accel, got {result}"

    def test_decelerating(self):
        """Price decelerating → negative accel."""
        # Older half: fast rise — 100 → 105 over 1 second
        older = [(0, 100.0), (1000, 105.0)]
        # Recent half: slow rise — 105 → 106 over 1 second
        recent = [(1000, 105.0), (2000, 106.0)]

        result = price_accel(recent, older)
        assert result < 0, f"Expected negative accel, got {result}"

    def test_constant_speed(self):
        """Same return rate in both halves → accel ≈ 0."""
        # Both halves: 100 → 101 over 1 second
        older = [(0, 100.0), (1000, 101.0)]
        recent = [(1000, 101.0), (2000, 102.01)]  # same % rise

        result = price_accel(recent, older)
        assert result == pytest.approx(0.0, abs=1e-6)

    def test_reversal(self):
        """Price going from up to down → strongly negative accel."""
        # Older: rising
        older = [(0, 100.0), (1000, 105.0)]
        # Recent: falling
        recent = [(1000, 105.0), (2000, 100.0)]

        result = price_accel(recent, older)
        assert result < 0

    def test_zero_dt_in_recent(self):
        """Same timestamp → dt=0 → rate=0, no crash."""
        older = [(0, 100.0), (1000, 101.0)]
        recent = [(1000, 101.0), (1000, 102.0)]  # dt=0
        result = price_accel(recent, older)
        assert isinstance(result, float)

    def test_zero_price_in_older(self):
        """Zero price → rate=0, no crash."""
        older = [(0, 0.0), (1000, 100.0)]
        recent = [(1000, 100.0), (2000, 105.0)]
        result = price_accel(recent, older)
        assert isinstance(result, float)


# =========================================================================
# compute_trade_rate
# =========================================================================


class TestComputeTradeRate:
    """Tests for compute_trade_rate."""

    def test_basic(self):
        """100 trades in 1000ms → 100 trades/sec."""
        assert compute_trade_rate(100, 1000) == pytest.approx(100.0)

    def test_zero_trades(self):
        assert compute_trade_rate(0, 1000) == 0.0

    def test_zero_window(self):
        """Zero window → 0.0 (avoid division by zero)."""
        assert compute_trade_rate(100, 0) == 0.0

    def test_negative_window(self):
        """Negative window → 0.0."""
        assert compute_trade_rate(100, -500) == 0.0

    def test_small_window(self):
        """10 trades in 100ms → 100 trades/sec."""
        assert compute_trade_rate(10, 100) == pytest.approx(100.0)

    def test_large_window(self):
        """5000 trades in 60000ms (1 min) → 83.33 trades/sec."""
        assert compute_trade_rate(5000, 60000) == pytest.approx(5000.0 / 60.0)
