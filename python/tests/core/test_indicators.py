"""Tests for indicator classes (EMA, TradeRate, etc.).

Covers:
    - Warmup: indicator returns None until enough data
    - Edge cases: empty data, zero values, rapid/sparse ticks
    - Premarket scenarios: zero bars, delayed data
    - Typical scenarios: normal market conditions
"""

import pytest

from jerry_trader.domain.market import Bar
from jerry_trader.services.factor.indicators.base import BarIndicator, TickIndicator
from jerry_trader.services.factor.indicators.ema import EMA
from jerry_trader.services.factor.indicators.trade_rate import TradeRate


def make_bar(
    close: float,
    bar_start: int,
    volume: float = 1000.0,
    symbol: str = "TEST",
    open: float | None = None,
    high: float | None = None,
    low: float | None = None,
) -> Bar:
    """Helper to create test bars with defaults."""
    if open is None:
        open = close - 0.5
    if high is None:
        high = close + 1.0
    if low is None:
        low = close - 1.0

    return Bar(
        symbol=symbol,
        timeframe="1m",
        open=open,
        high=high,
        low=low,
        close=close,
        volume=volume,
        trade_count=int(volume / 100),  # Approximate
        vwap=close,  # Simplified VWAP
        bar_start=bar_start,
        bar_end=bar_start + 60_000,
        session="premarket",
    )


# =========================================================================
# EMA Indicator Tests
# =========================================================================


class TestEMAWarmup:
    """Tests for EMA warmup behavior."""

    def test_returns_none_until_period_bars(self):
        """EMA needs period bars to be ready."""
        ema = EMA(period=20)

        # Feed 19 bars - should return None each time
        for i in range(19):
            bar = make_bar(close=100.0 + i, bar_start=i * 60_000)
            result = ema.update(bar)
            assert result is None, f"Expected None at bar {i}, got {result}"
            assert not ema.ready

        # 20th bar - should return value
        bar = make_bar(close=119.0, bar_start=19 * 60_000)
        result = ema.update(bar)
        assert result is not None, "Expected value after 20 bars"
        assert ema.ready

    def test_ema_10_warmup(self):
        """EMA with period=10 needs 10 bars."""
        ema = EMA(period=10)

        for i in range(9):
            bar = make_bar(close=100.0 + i, bar_start=i * 60_000)
            assert ema.update(bar) is None

        # 10th bar
        bar = make_bar(close=109.0, bar_start=9 * 60_000)
        assert ema.update(bar) is not None

    def test_value_property_returns_none_if_not_ready(self):
        """EMA.value returns None if not ready."""
        ema = EMA(period=20)
        assert ema.value is None

        # Feed 5 bars
        for i in range(5):
            bar = make_bar(close=100.0, bar_start=i * 60_000)
            ema.update(bar)

        assert ema.value is None


class TestEMAEdgeCases:
    """Tests for EMA edge cases."""

    def test_empty_data_reset(self):
        """Reset clears all state."""
        ema = EMA(period=20)

        # Warm up completely
        for i in range(20):
            bar = make_bar(close=100.0 + i * 0.5, bar_start=i * 60_000)
            ema.update(bar)

        assert ema.ready
        assert ema.value is not None

        # Reset
        ema.reset()
        assert not ema.ready
        assert ema.value is None
        assert ema._count == 0
        assert ema._value is None

    def test_zero_price_bars(self):
        """EMA handles zero price bars."""
        ema = EMA(period=5)

        result = None
        for i in range(5):
            bar = make_bar(close=0.0, bar_start=i * 60_000, volume=0.0)
            result = ema.update(bar)

        assert ema.ready
        assert result == 0.0

    def test_negative_prices(self):
        """EMA handles negative prices (unlikely but possible)."""
        ema = EMA(period=5)

        result = None
        for i in range(5):
            bar = make_bar(close=-100.0 + i, bar_start=i * 60_000)
            result = ema.update(bar)

        assert ema.ready
        assert result is not None
        assert result < 0  # Should be negative

    def test_very_large_prices(self):
        """EMA handles large prices (e.g., Berkshire Hathaway)."""
        ema = EMA(period=20)

        for i in range(20):
            bar = make_bar(close=600_000.0 + i * 100, bar_start=i * 60_000, volume=10.0)
            ema.update(bar)

        assert ema.ready
        assert ema.value > 600_000.0

    def test_constant_prices(self):
        """EMA of constant prices equals that price."""
        ema = EMA(period=20)

        for i in range(20):
            bar = make_bar(close=100.0, bar_start=i * 60_000)
            ema.update(bar)

        # EMA of constant values should be that value
        assert ema.value == pytest.approx(100.0, rel=1e-6)


class TestEMATypicalScenarios:
    """Tests for typical market scenarios."""

    def test_ema_follows_price_trend(self):
        """EMA should follow upward trend."""
        ema = EMA(period=10)

        # Upward trend: prices 100 → 110
        for i in range(10):
            bar = make_bar(close=100.0 + i, bar_start=i * 60_000)
            ema.update(bar)

        # EMA should be between first and last price
        assert ema.value > 100.0  # Higher than first
        assert ema.value < 110.0  # Lower than last (lagging)

    def test_ema_smothers_volatility(self):
        """EMA smooths out price volatility."""
        ema = EMA(period=20)

        # Alternating prices: 100, 110, 100, 110, ...
        for i in range(20):
            price = 100.0 if i % 2 == 0 else 110.0
            bar = make_bar(close=price, bar_start=i * 60_000)
            ema.update(bar)

        # EMA should be around 105 (average of alternation)
        assert ema.value == pytest.approx(105.0, abs=1.0)

    def test_ema_reacts_to_price_change(self):
        """EMA should react to sudden price jump."""
        ema = EMA(period=10)

        # Steady at 100 for 10 bars
        for i in range(10):
            bar = make_bar(close=100.0, bar_start=i * 60_000)
            ema.update(bar)

        initial_ema = ema.value

        # Jump to 120 for 10 more bars
        for i in range(10, 20):
            bar = make_bar(close=120.0, bar_start=i * 60_000)
            ema.update(bar)

        # EMA should have moved toward 120
        assert ema.value > initial_ema
        assert ema.value < 120.0  # Still lagging


# =========================================================================
# TradeRate Indicator Tests
# =========================================================================


class TestTradeRateWarmup:
    """Tests for TradeRate warmup behavior."""

    def test_returns_none_until_min_trades(self):
        """TradeRate needs min_trades to compute."""
        tr = TradeRate(window_ms=20_000, min_trades=5)

        # 4 ticks - not enough
        for i in range(4):
            tr.on_tick(ts_ms=i * 100, price=100.0, size=100)

        assert tr.compute(ts_ms=400) is None
        assert not tr.ready

        # 5th tick - enough
        tr.on_tick(ts_ms=500, price=100.0, size=100)
        result = tr.compute(ts_ms=600)
        assert result is not None
        assert tr.ready

    def test_value_property_returns_none_if_not_ready(self):
        """TradeRate.value returns None if not ready."""
        tr = TradeRate(min_trades=5)
        assert tr.value is None

        # 3 ticks
        for i in range(3):
            tr.on_tick(ts_ms=i * 100, price=100.0, size=100)
        tr.compute(ts_ms=300)

        assert tr.value is None


class TestTradeRateEdgeCases:
    """Tests for TradeRate edge cases."""

    def test_empty_data_reset(self):
        """Reset clears all timestamps."""
        tr = TradeRate(min_trades=5)

        # Warm up
        for i in range(10):
            tr.on_tick(ts_ms=i * 100, price=100.0, size=100)
        tr.compute(ts_ms=1000)

        assert tr.ready
        assert tr.value is not None

        # Reset
        tr.reset()
        assert not tr.ready
        assert tr.value is None
        assert len(tr._timestamps) == 0

    def test_rapid_ticks(self):
        """High-frequency ticks (1ms apart)."""
        tr = TradeRate(window_ms=1000, min_trades=3)

        # 100 ticks in 100ms
        for i in range(100):
            tr.on_tick(ts_ms=i, price=100.0, size=100)

        result = tr.compute(ts_ms=100)
        assert result is not None
        # 100 trades in window, rate depends on window calculation
        # Just verify it's a high rate
        assert result >= 100  # Should be at least 100

    def test_sparse_ticks(self):
        """Ticks spaced far apart (10s apart)."""
        tr = TradeRate(window_ms=20_000, min_trades=3)

        # 3 ticks, 10s apart
        tr.on_tick(ts_ms=0, price=100.0, size=100)
        tr.on_tick(ts_ms=10_000, price=100.0, size=100)
        tr.on_tick(ts_ms=20_000, price=100.0, size=100)

        result = tr.compute(ts_ms=21_000)
        # Window at 21s looks back 20s → [1s, 21s]
        # Contains ticks at 10s and 20s only (tick at 0 is out)
        # So only 2 ticks in window, below min_trades=3
        assert result is None or result < 1.0

    def test_time_gap(self):
        """Large time gap between tick bursts."""
        tr = TradeRate(window_ms=10_000, min_trades=5)

        # Burst 1: 10 ticks at t=0-100ms
        for i in range(10):
            tr.on_tick(ts_ms=i * 10, price=100.0, size=100)

        # Gap: 5 minutes
        # Burst 2: 10 ticks at t=300000-300100ms
        for i in range(10):
            tr.on_tick(ts_ms=300_000 + i * 10, price=100.0, size=100)

        # Compute at end of burst 2
        result = tr.compute(ts_ms=300_200)

        # Only burst 2 trades in window (last 10s)
        # Window [290200, 300200] contains burst 2 trades only
        assert result is not None
        # 10 trades in ~100ms span within 10s window
        # Actual rate calculation depends on Rust impl
        assert result >= 1.0

    def test_prune_old_timestamps(self):
        """Prune removes old timestamps."""
        tr = TradeRate(min_trades=5)

        # Add 100 ticks
        for i in range(100):
            tr.on_tick(ts_ms=i * 100, price=100.0, size=100)

        assert len(tr._timestamps) == 100

        # Prune older than 5000ms
        tr.prune(cutoff_ms=5000)

        # Should keep only timestamps >= 5000 (i >= 50)
        assert len(tr._timestamps) == 50

    def test_window_boundary(self):
        """Ticks exactly at window boundary."""
        tr = TradeRate(window_ms=1000, min_trades=3)

        # Ticks at t=0, 500, 1000 (window boundary)
        tr.on_tick(ts_ms=0, price=100.0, size=100)
        tr.on_tick(ts_ms=500, price=100.0, size=100)
        tr.on_tick(ts_ms=1000, price=100.0, size=100)

        # Compute at t=1000
        result = tr.compute(ts_ms=1000)
        assert result is not None
        # All 3 trades in window [0, 1000]
        # 3 trades / 1s = 3 trades/sec
        assert result == pytest.approx(3.0, rel=0.1)


class TestTradeRateTypicalScenarios:
    """Tests for typical market scenarios."""

    def test_morning_spike(self):
        """Simulate morning news spike - rapid trades."""
        tr = TradeRate(window_ms=20_000, min_trades=5)

        # Normal period: 10 trades over 1 minute
        for i in range(10):
            tr.on_tick(ts_ms=i * 6000, price=100.0, size=100)

        # Spike: 50 trades in 2 seconds
        spike_start = 60_000
        for i in range(50):
            tr.on_tick(ts_ms=spike_start + i * 40, price=105.0, size=200)

        # Compute during spike
        result = tr.compute(ts_ms=spike_start + 2000)
        assert result is not None
        # Verify it's a valid rate (actual value depends on Rust impl)
        assert result > 0  # Some rate during spike

    def test_steady_trading(self):
        """Steady trading - consistent rate."""
        tr = TradeRate(window_ms=10_000, min_trades=5)

        # 100 ticks at steady 100ms intervals
        for i in range(100):
            tr.on_tick(ts_ms=i * 100, price=100.0, size=100)

        # Compute after steady period
        result = tr.compute(ts_ms=10_000)
        assert result is not None
        # 10 trades per second (100ms interval)
        assert result == pytest.approx(10.0, rel=0.5)


# =========================================================================
# Premarket Scenarios
# =========================================================================


class TestPremarketScenarios:
    """Tests for premarket edge cases."""

    def test_zero_bars_premarket(self):
        """What happens when there are zero bars in premarket."""
        ema = EMA(period=20)

        # Simulate premarket: feed zero-volume bars with same price
        for i in range(20):
            bar = make_bar(close=100.0, bar_start=i * 60_000, volume=0.0)
            ema.update(bar)

        # EMA should still work (price-based, not volume-based)
        assert ema.ready
        assert ema.value == pytest.approx(100.0)

    def test_delayed_first_bar(self):
        """First bar arrives late (after premarket)."""
        ema = EMA(period=10)

        # No bars for first 30 minutes
        # Then bars start at t=30min
        start_time = 30 * 60_000

        for i in range(10):
            bar = make_bar(close=100.0 + i, bar_start=start_time + i * 60_000)
            ema.update(bar)

        assert ema.ready
        assert ema.value is not None
        assert ema.value > 100.0

    def test_premarket_no_trades(self):
        """TradeRate during premarket with no trades."""
        tr = TradeRate(window_ms=20_000, min_trades=5)

        # No ticks for 30 minutes
        # Compute should return None
        result = tr.compute(ts_ms=30 * 60_000)
        assert result is None
        assert not tr.ready

    def test_first_tick_at_open(self):
        """First trade arrives exactly at market open."""
        tr = TradeRate(window_ms=20_000, min_trades=5)

        # Market open at 09:30 ET = 34200000 ms from midnight
        # Assume first 5 trades at exactly 09:30
        open_time = 34_200_000

        for i in range(5):
            tr.on_tick(ts_ms=open_time + i * 10, price=100.0, size=100)

        result = tr.compute(ts_ms=open_time + 100)
        assert result is not None
        # 5 trades in 100ms → rate depends on window calculation
        assert result > 0  # Some rate


# =========================================================================
# Indicator Registration (Task 6.17 preparation)
# =========================================================================


class TestIndicatorRegistration:
    """Tests for indicator auto-registration (future Task 6.17)."""

    def test_ema_name_matches_pattern(self):
        """EMA name follows ema_{period} pattern."""
        ema20 = EMA(period=20)
        assert ema20.name == "ema_20"

        ema10 = EMA(period=10)
        assert ema10.name == "ema_10"

    def test_trade_rate_name(self):
        """TradeRate name is 'trade_rate'."""
        tr = TradeRate()
        assert tr.name == "trade_rate"

    def test_ema_is_bar_indicator(self):
        """EMA inherits from BarIndicator."""
        ema = EMA()
        assert isinstance(ema, BarIndicator)

    def test_trade_rate_is_tick_indicator(self):
        """TradeRate inherits from TickIndicator."""
        tr = TradeRate()
        assert isinstance(tr, TickIndicator)


# =========================================================================
# Warmup from ClickHouse (future integration test)
# =========================================================================


class TestWarmupIntegration:
    """Integration tests for indicator warmup (requires ClickHouse).

    These tests are marked as integration and skipped in unit test runs.
    """

    @pytest.mark.integration
    def test_ema_warmup_from_clickhouse(self):
        """Warmup EMA from historical bars in ClickHouse."""
        # This would require ClickHouse connection
        # Placeholder for future implementation
        pass

    @pytest.mark.integration
    def test_trade_rate_warmup_from_clickhouse(self):
        """Warmup TradeRate from historical trades in ClickHouse."""
        # Placeholder for future implementation
        pass
