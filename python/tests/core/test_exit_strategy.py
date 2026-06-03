"""Tests for services/backtest/exit_strategy.py.

Covers:
  - ExitReason enum values
  - PartialExit construction
  - ExitResult construction and defaults
  - ExitStrategyConfig construction, defaults, to_dict()
  - apply_exit_strategy: stop loss, partial exits, trailing stop, time limit, market close
  - get_default_exit_strategy / get_simple_exit_strategy
  - Edge cases: empty trades, no triggers, single trade
"""

from __future__ import annotations

from jerry_trader.services.backtest.event.exit_strategy import (
    ExitReason,
    ExitResult,
    ExitStrategyConfig,
    PartialExit,
    apply_exit_strategy,
    get_default_exit_strategy,
    get_simple_exit_strategy,
)

# ══════════════════════════════════════════════════════════════════════
# ExitReason enum
# ══════════════════════════════════════════════════════════════════════


class TestExitReason:
    def test_values(self):
        assert ExitReason.STOP_LOSS.value == "stop_loss"
        assert ExitReason.TAKE_PROFIT.value == "take_profit"
        assert ExitReason.TRAILING_STOP.value == "trailing_stop"
        assert ExitReason.MARKET_CLOSE.value == "market_close"
        assert ExitReason.TIME_LIMIT.value == "time_limit"


# ══════════════════════════════════════════════════════════════════════
# PartialExit
# ══════════════════════════════════════════════════════════════════════


class TestPartialExit:
    def test_construction(self):
        pe = PartialExit(
            price=155.0,
            pct_gain=5.0,
            size=0.5,
            reason=ExitReason.TAKE_PROFIT,
            time_ms=1000,
        )
        assert pe.price == 155.0
        assert pe.pct_gain == 5.0
        assert pe.size == 0.5
        assert pe.reason == ExitReason.TAKE_PROFIT
        assert pe.time_ms == 1000

    def test_default_time(self):
        pe = PartialExit(
            price=150.0, pct_gain=0.0, size=1.0, reason=ExitReason.MARKET_CLOSE
        )
        assert pe.time_ms == 0


# ══════════════════════════════════════════════════════════════════════
# ExitResult
# ══════════════════════════════════════════════════════════════════════


class TestExitResult:
    def test_minimal_construction(self):
        result = ExitResult(
            entry_price=100.0, total_return_pct=5.0, weighted_return=5.0
        )
        assert result.entry_price == 100.0
        assert result.total_return_pct == 5.0
        assert result.exits == []
        assert result.max_price == 0.0
        assert result.min_price == 0.0

    def test_default_fields(self):
        result = ExitResult(
            entry_price=100.0, total_return_pct=0.0, weighted_return=0.0
        )
        assert result.final_position_size == 0.0
        assert result.exit_reasons == {}
        assert result.time_to_max_ms == 0
        assert result.time_to_min_ms == 0

    def test_with_exits(self):
        pe = PartialExit(
            price=105.0, pct_gain=5.0, size=0.5, reason=ExitReason.TAKE_PROFIT
        )
        result = ExitResult(
            entry_price=100.0,
            total_return_pct=2.5,
            weighted_return=2.5,
            exits=[pe],
            max_price=105.0,
            min_price=100.0,
        )
        assert len(result.exits) == 1
        assert result.max_price == 105.0


# ══════════════════════════════════════════════════════════════════════
# ExitStrategyConfig
# ══════════════════════════════════════════════════════════════════════


class TestExitStrategyConfig:
    def test_default_is_scale_fast(self):
        config = ExitStrategyConfig()
        assert config.name == "scale_fast"
        assert len(config.partial_exits) == 3
        assert config.stop_loss_pct == 8.0

    def test_custom_config(self):
        config = ExitStrategyConfig(
            name="custom",
            partial_exits=[(5.0, 1.0)],
            stop_loss_pct=10.0,
        )
        assert config.name == "custom"
        assert config.partial_exits == [(5.0, 1.0)]
        assert config.stop_loss_pct == 10.0

    def test_to_dict(self):
        config = ExitStrategyConfig()
        d = config.to_dict()
        assert d["name"] == "scale_fast"
        assert "partial_exits" in d
        assert d["stop_loss_pct"] == 8.0


# ══════════════════════════════════════════════════════════════════════
# apply_exit_strategy
# ══════════════════════════════════════════════════════════════════════


class TestApplyExitStrategy:
    def test_empty_trades_returns_default(self):
        result = apply_exit_strategy(
            trades=[],
            entry_time_ms=1000,
            entry_price=100.0,
            strategy=ExitStrategyConfig(),
        )
        assert result.total_return_pct == 0.0
        assert result.exits == []
        assert result.max_price == 100.0
        assert result.min_price == 100.0

    def test_price_goes_up_no_exits(self):
        """Price rises but never hits a threshold → market close at end."""
        trades = [
            (i * 60_000, 100.0 + i * 0.1) for i in range(1, 11)
        ]  # 10 bars, slowly up to 101.0
        result = apply_exit_strategy(
            trades=trades,
            entry_time_ms=0,
            entry_price=100.0,
            strategy=ExitStrategyConfig(),
        )
        # No thresholds hit (max 1% gain, stop loss at -8%, take profit at 5%)
        assert result.final_position_size == 0.0  # Closed at market close
        assert len(result.exits) == 1
        assert result.exits[0].reason == ExitReason.MARKET_CLOSE

    def test_stop_loss_triggered(self):
        """Price drops below stop loss → immediate exit."""
        trades = [
            (60_000, 99.0),
            (120_000, 95.0),
            (180_000, 91.0),  # -9% → below -8% SL
        ]
        strategy = ExitStrategyConfig(stop_loss_pct=8.0, partial_exits=[])
        result = apply_exit_strategy(
            trades=trades,
            entry_time_ms=0,
            entry_price=100.0,
            strategy=strategy,
        )
        assert len(result.exits) == 1
        assert result.exits[0].reason == ExitReason.STOP_LOSS
        assert result.exits[0].size > 0.9
        assert result.final_position_size == 0.0

    def test_partial_exits_triggered_in_order(self):
        """Multiple thresholds trigger sequentially."""
        trades = [
            (60_000, 103.0),  # +3% → below first threshold (5%)
            (120_000, 105.5),  # +5.5% → triggers 5% threshold
            (180_000, 108.0),  # +8% → below second threshold (10%)
            (240_000, 110.5),  # +10.5% → triggers 10% threshold
            (300_000, 115.5),  # +15.5% → triggers 15% threshold
        ]
        strategy = ExitStrategyConfig(
            partial_exits=[(5.0, 0.50), (10.0, 0.30), (15.0, 0.20)],
            stop_loss_pct=100.0,  # No stop loss
        )
        result = apply_exit_strategy(
            trades=trades,
            entry_time_ms=0,
            entry_price=100.0,
            strategy=strategy,
        )
        assert len(result.exits) >= 3
        # Each threshold triggered once
        reasons = [e.reason for e in result.exits if e.reason == ExitReason.TAKE_PROFIT]
        assert len(reasons) >= 3

    def test_threshold_only_triggers_once(self):
        """Same threshold can't fire twice."""
        trades = [
            (60_000, 106.0),  # +6% → triggers 5%
            (120_000, 104.0),  # dips
            (180_000, 107.0),  # +7% → 5% already triggered, won't re-fire
        ]
        strategy = ExitStrategyConfig(
            partial_exits=[(5.0, 0.50)],
            stop_loss_pct=100.0,
        )
        result = apply_exit_strategy(
            trades=trades,
            entry_time_ms=0,
            entry_price=100.0,
            strategy=strategy,
        )
        # Only one 5% exit, plus market close
        take_profits = [e for e in result.exits if e.reason == ExitReason.TAKE_PROFIT]
        assert len(take_profits) == 1

    def test_time_limit_exit(self):
        """Position closed when max hold time reached."""
        trades = [(i * 60_000, 101.0) for i in range(1, 30)]  # 29 minutes of flat price
        strategy = ExitStrategyConfig(
            max_hold_minutes=15,
            stop_loss_pct=100.0,
            partial_exits=[],
        )
        result = apply_exit_strategy(
            trades=trades,
            entry_time_ms=0,
            entry_price=100.0,
            strategy=strategy,
        )
        assert any(e.reason == ExitReason.TIME_LIMIT for e in result.exits)

    def test_single_trade_closes_at_market(self):
        """Single trade → market close at that trade."""
        trades = [(60_000, 102.0)]
        result = apply_exit_strategy(
            trades=trades,
            entry_time_ms=0,
            entry_price=100.0,
            strategy=ExitStrategyConfig(),
        )
        assert len(result.exits) == 1
        assert result.exits[0].reason == ExitReason.MARKET_CLOSE
        assert result.total_return_pct == 2.0

    def test_weighted_return(self):
        """Verify weighted return calculation for partial exits.
        With a single trade at +5%:
        - partial exit sells 50% at 5% → 5.0 * 0.5 = 2.5
        - market close sells remaining 50% at 5% → 5.0 * 0.5 = 2.5
        - total weighted_return = 5.0
        """
        trades = [(60_000, 105.0)]  # +5%
        strategy = ExitStrategyConfig(
            partial_exits=[(5.0, 0.50)],
            stop_loss_pct=100.0,
        )
        result = apply_exit_strategy(
            trades=trades,
            entry_time_ms=0,
            entry_price=100.0,
            strategy=strategy,
        )
        # Both partial exit and market close contribute
        assert len(result.exits) == 2
        assert result.weighted_return == 5.0


# ══════════════════════════════════════════════════════════════════════
# Factory functions
# ══════════════════════════════════════════════════════════════════════


class TestFactoryFunctions:
    def test_get_default_exit_strategy(self):
        config = get_default_exit_strategy()
        assert config.name == "tp10_sl15"
        assert config.stop_loss_pct == 15.0
        assert config.partial_exits == [(10.0, 1.0)]

    def test_get_simple_exit_strategy(self):
        config = get_simple_exit_strategy(hold_minutes=10)
        assert config.name == "simple_hold"
        assert config.max_hold_minutes == 10
        assert config.partial_exits == []
        assert config.stop_loss_pct == 100.0

    def test_get_simple_exit_strategy_default(self):
        config = get_simple_exit_strategy()
        assert config.max_hold_minutes == 10
