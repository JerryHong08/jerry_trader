"""Exit Strategy Module - Dynamic exit logic for backtest.

Based on experimental findings:
- 81.2% of losing trades had profit at some point
- Best strategy: scale_fast (72.9% win rate, 4.1% avg return)
- Partial exits: 50% at 5%, 30% at 10%, 20% runs
- Stop loss: -8%
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ExitReason(Enum):
    """Reason for exiting a position."""

    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"  # Generic take profit
    TRAILING_STOP = "trailing_stop"
    MARKET_CLOSE = "market_close"
    TIME_LIMIT = "time_limit"
    HOLD_DURATION = "hold_duration"  # Legacy: fixed hold time


def _get_take_profit_reason(threshold: float) -> str:
    """Get take profit reason string for a threshold."""
    return f"take_profit_{int(threshold)}"


@dataclass
class PartialExit:
    """A partial exit event."""

    price: float
    pct_gain: float  # Gain at exit time
    size: float  # Position size sold (0-1)
    reason: ExitReason
    time_ms: int = 0


@dataclass
class ExitResult:
    """Result of applying exit strategy to a trade."""

    entry_price: float
    total_return_pct: float  # Weighted average return
    weighted_return: float  # Position-weighted return sum
    exits: list[PartialExit] = field(default_factory=list)
    max_price: float = 0.0
    min_price: float = 0.0
    time_to_max_ms: int = 0
    time_to_min_ms: int = 0
    final_position_size: float = 0.0  # Remaining position at end
    exit_reasons: dict[str, float] = field(default_factory=dict)  # reason -> size


@dataclass
class ExitStrategyConfig:
    """Configuration for exit strategy.

    Based on experimental results:
    - scale_fast: 50% at 5%, 30% at 10%, 20% runs → 72.9% win rate
    - stop_loss: -8% hard stop
    """

    name: str = "scale_fast"
    description: str = "分批止盈：涨5%卖50%，涨10%卖30%，涨15%清仓"

    # Partial exits (priority order: earlier threshold first)
    # (gain_threshold_pct, position_size_to_sell)
    partial_exits: list[tuple[float, float]] = field(
        default_factory=lambda: [
            (5.0, 0.50),  # 涨5%卖50%
            (10.0, 0.30),  # 涨10%卖30%
            (15.0, 0.20),  # 涨15%清仓（剩余20%）
        ]
    )

    # Hard stop loss
    stop_loss_pct: float = 8.0  # -8%止损

    # Trailing stop (optional)
    trailing_stop_trigger_pct: float = 0.0  # Start trailing after this gain
    trailing_stop_pct: float = 0.0  # Trailing distance

    # Time limit (optional)
    max_hold_minutes: int = 0  # 0 = no limit (hold until market close)

    def to_dict(self) -> dict:
        """Convert to dict for logging."""
        return {
            "name": self.name,
            "description": self.description,
            "partial_exits": self.partial_exits,
            "stop_loss_pct": self.stop_loss_pct,
            "trailing_stop_trigger_pct": self.trailing_stop_trigger_pct,
            "trailing_stop_pct": self.trailing_stop_pct,
            "max_hold_minutes": self.max_hold_minutes,
        }


def apply_exit_strategy(
    trades: list[tuple[int, float]],
    entry_time_ms: int,
    entry_price: float,
    strategy: ExitStrategyConfig,
    session_end_ms: int | None = None,
) -> ExitResult:
    """Apply exit strategy to a trade trajectory.

    Args:
        trades: List of (ts_ms, price) tuples after entry (sorted)
        entry_time_ms: Entry timestamp
        entry_price: Entry price
        strategy: Exit strategy configuration
        session_end_ms: Session end (market close), None = use last trade

    Returns:
        ExitResult with weighted returns and exit details
    """
    if not trades:
        return ExitResult(
            entry_price=entry_price,
            total_return_pct=0.0,
            weighted_return=0.0,
            exits=[],
            max_price=entry_price,
            min_price=entry_price,
        )

    # Track position state
    remaining_position = 1.0  # Start with 100% position
    weighted_return = 0.0  # Sum of (return * position_sold)
    exits: list[PartialExit] = []

    # Track price extremes
    max_price = entry_price
    min_price = entry_price
    time_to_max_ms = entry_time_ms
    time_to_min_ms = entry_time_ms

    # Track thresholds already triggered (避免重复触发)
    triggered_thresholds: set[float] = set()

    # Trailing stop state
    highest_price_after_trigger = entry_price
    trailing_active = False

    # Iterate through trades
    for ts_ms, price in trades:
        # Update extremes
        if price > max_price:
            max_price = price
            time_to_max_ms = ts_ms
        if price < min_price:
            min_price = price
            time_to_min_ms = ts_ms

        # Calculate current gain
        pct_gain = (price - entry_price) / entry_price * 100

        # 1. Check stop loss first (hard stop)
        if pct_gain <= -strategy.stop_loss_pct:
            # Stop loss triggered - sell remaining position
            exits.append(
                PartialExit(
                    price=price,
                    pct_gain=pct_gain,
                    size=remaining_position,
                    reason=ExitReason.STOP_LOSS,
                    time_ms=ts_ms,
                )
            )
            weighted_return += pct_gain * remaining_position
            remaining_position = 0.0
            break  # Exit immediately

        # 2. Check trailing stop (if active)
        if trailing_active and strategy.trailing_stop_pct > 0:
            # Update highest price
            if price > highest_price_after_trigger:
                highest_price_after_trigger = price

            # Check trailing stop
            trailing_threshold = (
                highest_price_after_trigger - entry_price
            ) / entry_price * 100 - strategy.trailing_stop_pct
            if pct_gain <= trailing_threshold:
                exits.append(
                    PartialExit(
                        price=price,
                        pct_gain=pct_gain,
                        size=remaining_position,
                        reason=ExitReason.TRAILING_STOP,
                        time_ms=ts_ms,
                    )
                )
                weighted_return += pct_gain * remaining_position
                remaining_position = 0.0
                break

        # 3. Check partial exits (in threshold order)
        for threshold, sell_size in strategy.partial_exits:
            if threshold in triggered_thresholds:
                continue  # Already triggered

            if pct_gain >= threshold and remaining_position > 0:
                # Partial exit triggered
                actual_sell = min(sell_size, remaining_position)
                exits.append(
                    PartialExit(
                        price=price,
                        pct_gain=pct_gain,
                        size=actual_sell,
                        reason=ExitReason.TAKE_PROFIT,  # Use generic reason
                        time_ms=ts_ms,
                    )
                )
                weighted_return += pct_gain * actual_sell
                remaining_position -= actual_sell
                triggered_thresholds.add(threshold)

                # Activate trailing stop if configured
                if (
                    strategy.trailing_stop_trigger_pct > 0
                    and pct_gain >= strategy.trailing_stop_trigger_pct
                    and not trailing_active
                ):
                    trailing_active = True
                    highest_price_after_trigger = price

        # 4. Check time limit (if configured)
        if strategy.max_hold_minutes > 0:
            elapsed_minutes = (ts_ms - entry_time_ms) / 60000
            if elapsed_minutes >= strategy.max_hold_minutes and remaining_position > 0:
                exits.append(
                    PartialExit(
                        price=price,
                        pct_gain=pct_gain,
                        size=remaining_position,
                        reason=ExitReason.TIME_LIMIT,
                        time_ms=ts_ms,
                    )
                )
                weighted_return += pct_gain * remaining_position
                remaining_position = 0.0
                break

    # 5. Market close: sell remaining position at last price
    if remaining_position > 0 and trades:
        last_price = trades[-1][1]
        last_time = trades[-1][0]
        pct_gain = (last_price - entry_price) / entry_price * 100

        exits.append(
            PartialExit(
                price=last_price,
                pct_gain=pct_gain,
                size=remaining_position,
                reason=ExitReason.MARKET_CLOSE,
                time_ms=last_time,
            )
        )
        weighted_return += pct_gain * remaining_position
        remaining_position = 0.0

    # Calculate total return (weighted average)
    total_return_pct = weighted_return  # Already weighted by position sizes

    # Build exit reasons summary
    exit_reasons: dict[str, float] = {}
    for exit_ in exits:
        reason = exit_.reason.value
        exit_reasons[reason] = exit_reasons.get(reason, 0.0) + exit_.size

    return ExitResult(
        entry_price=entry_price,
        total_return_pct=round(total_return_pct, 2),
        weighted_return=weighted_return,
        exits=exits,
        max_price=max_price,
        min_price=min_price,
        time_to_max_ms=time_to_max_ms,
        time_to_min_ms=time_to_min_ms,
        final_position_size=remaining_position,
        exit_reasons=exit_reasons,
    )


def get_default_exit_strategy() -> ExitStrategyConfig:
    """Get default exit strategy based on multi-date streaming backtest (May 2026).

    Multi-date test (38 signals, 5 dates): tp10_sl15 delivers -0.10% EV
    vs tp15_sl20 at -2.30% EV. Tighter TP (10% vs 15%) captures profits before
    reversals; wider SL (15% vs 10%) reduces premature stop-outs.

    tp15_sl20 was tested at -2.30% EV (38 signals, 5 dates) — REPLACED.
    Previous scale_fast (-8% SL): 25% win rate, -3.4% avg exit — REPLACED.
    """
    return ExitStrategyConfig(
        name="tp10_sl15",
        description="止盈+10%，止损-15%（多日期验证 -0.10% EV）",
        partial_exits=[(10.0, 1.0)],
        stop_loss_pct=15.0,
    )


def get_simple_exit_strategy(hold_minutes: int = 10) -> ExitStrategyConfig:
    """Get simple fixed-hold exit strategy (backward compatibility).

    Args:
        hold_minutes: Fixed hold duration in minutes

    Returns simple strategy that holds for fixed time then exits.
    """
    return ExitStrategyConfig(
        name="simple_hold",
        description=f"固定持有 {hold_minutes} 分钟后卖出",
        partial_exits=[],  # No partial exits
        stop_loss_pct=100.0,  # No stop loss (very wide)
        max_hold_minutes=hold_minutes,
    )
