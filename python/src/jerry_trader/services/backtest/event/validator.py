"""Event Validator - validate events using Avg Return + Win Rate.

Based on Event Framework Validation findings:
- IC is unstable (std=0.29), use Avg Return + Win Rate instead
- Cross-date stability is critical (positive dates > 60%)
"""

from dataclasses import dataclass
from typing import Optional

import numpy as np

from jerry_trader.domain.event import Event


@dataclass
class EventValidationResult:
    """Event validation result using Avg Return + Win Rate metrics.

    Validation thresholds (based on validated events):
    - avg_return > 2% (baseline is -0.23%)
    - win_rate > 45% (baseline is 38.89%)
    - positive_dates_ratio > 60% (stability requirement)
    - total_signals >= 100 (sample size)
    """

    event_name: str
    total_signals: int

    # Primary metrics (Avg Return + Win Rate)
    avg_return: float
    win_rate: float

    # Stability metrics (cross-date)
    positive_dates_ratio: float
    date_returns: dict[str, float]
    date_win_rates: dict[str, float]
    date_signal_counts: dict[str, int]

    # Variance metrics
    return_std: float  # Std across dates
    win_rate_std: float

    def is_valid(self) -> bool:
        """Check if event passes validation thresholds."""
        # Note: This method doesn't distinguish AVOID vs ACCEPT events
        # Use is_valid_for_action() for proper validation
        return (
            self.avg_return > 0.02  # > 2% avg return
            and self.win_rate > 0.45  # > 45% win rate
            and self.positive_dates_ratio > 0.6  # > 60% dates positive
            and self.total_signals >= 100  # minimum sample
        )

    def is_valid_for_action(self, is_avoid: bool = False) -> bool:
        """Check if event passes validation thresholds based on action type.

        Args:
            is_avoid: True for AVOID events, False for ACCEPT events

        For ACCEPT events:
            - avg_return > 2% (signals are profitable)
            - win_rate > 45% (signals win more than half)
            - positive_dates > 60% (stable across dates)

        For AVOID events:
            - avg_return < -2% (signals are losers, correctly avoided)
            - win_rate < 40% (signals lose more than half)
            - negative_dates > 60% (stable across dates)
        """
        if is_avoid:
            # AVOID event: we want signals that LOSE money
            return (
                self.avg_return < -0.02  # signals lose > 2%
                and self.win_rate < 0.40  # signals win < 40%
                and self.positive_dates_ratio
                < 0.4  # < 40% dates positive (mostly negative)
                and self.total_signals >= 100  # minimum sample
            )
        else:
            # ACCEPT event: we want signals that MAKE money
            return self.is_valid()

    def passes_basic(self) -> bool:
        """Check basic threshold (avg_return > 0 and win_rate > 40%)."""
        return self.avg_return > 0 and self.win_rate > 0.4

    def stability_score(self) -> float:
        """Compute stability score (lower variance = more stable)."""
        if self.return_std == 0:
            return 1.0
        # Score based on std relative to mean
        # Lower is better: std < 0.5 * abs(mean) is acceptable
        return max(0, 1 - self.return_std / (abs(self.avg_return) + 0.01))

    def summary(self, is_avoid: bool = False) -> str:
        """Return validation summary string."""
        status = "PASS" if self.is_valid_for_action(is_avoid) else "FAIL"
        basic = "✓" if self.passes_basic() else "✗"

        if is_avoid:
            target_ret = "< -0.02"
            target_wr = "< 40%"
        else:
            target_ret = "> 0.02"
            target_wr = "> 45%"

        return (
            f"{self.event_name}: {status} (basic: {basic})\n"
            f"  Signals: {self.total_signals} (min 100)\n"
            f"  Avg Return: {self.avg_return:.4f} (target {target_ret})\n"
            f"  Win Rate: {self.win_rate:.2%} (target {target_wr})\n"
            f"  Positive Dates: {self.positive_dates_ratio:.2%} (target > 60%)\n"
            f"  Return Std: {self.return_std:.4f} (lower is better)\n"
            f"  Stability Score: {self.stability_score():.2f}"
        )

    def detailed_report(self) -> str:
        """Return detailed validation report with per-date breakdown."""
        lines = [self.summary(), "", "Per-date breakdown:"]

        for date, ret in sorted(self.date_returns.items()):
            wr = self.date_win_rates.get(date, 0)
            count = self.date_signal_counts.get(date, 0)
            status = "✓" if ret > 0 else "✗"
            lines.append(f"  {date}: {status} ret={ret:.4f}, win={wr:.2%}, n={count}")

        return "\n".join(lines)


class EventValidator:
    """Validate events across multiple dates.

    Uses Avg Return + Win Rate metrics (not IC).
    Enforces cross-date stability requirements.
    """

    def validate(
        self,
        event: Event,
        signals_by_date: dict[str, list[dict]],
        return_field: str = "return_5m",
    ) -> EventValidationResult:
        """Validate event across multiple dates.

        Args:
            event: Event to validate
            signals_by_date: Dict mapping date -> list of matched signals
            return_field: Field name for return calculation

        Returns:
            EventValidationResult with metrics
        """
        date_returns: dict[str, float] = {}
        date_win_rates: dict[str, float] = {}
        date_signal_counts: dict[str, int] = {}
        total_signals = 0

        all_returns: list[float] = []

        for date, signals in signals_by_date.items():
            returns = [
                s[return_field]
                for s in signals
                if return_field in s and s[return_field] is not None
            ]
            if not returns:
                continue

            avg_ret = float(np.mean(returns))
            wins = sum(1 for r in returns if r > 0)
            win_rate = wins / len(returns)

            date_returns[date] = avg_ret
            date_win_rates[date] = win_rate
            date_signal_counts[date] = len(signals)
            total_signals += len(signals)
            all_returns.extend(returns)

        # Compute aggregate metrics
        if not date_returns:
            # No data
            return EventValidationResult(
                event_name=event.name,
                total_signals=0,
                avg_return=0.0,
                win_rate=0.0,
                positive_dates_ratio=0.0,
                date_returns={},
                date_win_rates={},
                date_signal_counts={},
                return_std=0.0,
                win_rate_std=0.0,
            )

        avg_return = float(np.mean(list(date_returns.values())))
        avg_win_rate = float(np.mean(list(date_win_rates.values())))

        # Stability: % of dates with positive return
        positive_dates = sum(1 for r in date_returns.values() if r > 0)
        positive_dates_ratio = positive_dates / len(date_returns)

        # Variance across dates
        return_std = float(np.std(list(date_returns.values())))
        win_rate_std = float(np.std(list(date_win_rates.values())))

        return EventValidationResult(
            event_name=event.name,
            total_signals=total_signals,
            avg_return=avg_return,
            win_rate=avg_win_rate,
            positive_dates_ratio=positive_dates_ratio,
            date_returns=date_returns,
            date_win_rates=date_win_rates,
            date_signal_counts=date_signal_counts,
            return_std=return_std,
            win_rate_std=win_rate_std,
        )

    def compare_events(
        self,
        results: list[EventValidationResult],
    ) -> str:
        """Compare multiple event validation results.

        Args:
            results: List of EventValidationResult

        Returns:
            Comparison table string
        """
        lines = [
            "Event Comparison:",
            "-" * 80,
            f"{'Event':<30} {'Signals':>10} {'AvgRet':>10} {'WinRate':>10} {'PosDates':>10}",
            "-" * 80,
        ]

        for r in sorted(results, key=lambda x: x.avg_return, reverse=True):
            status = "✓" if r.is_valid() else "✗"
            lines.append(
                f"{r.event_name:<30} {r.total_signals:>10} "
                f"{r.avg_return:>10.4f} {r.win_rate:>9.2%} "
                f"{r.positive_dates_ratio:>9.2%} {status}"
            )

        lines.append("-" * 80)
        return "\n".join(lines)


__all__ = ["EventValidationResult", "EventValidator"]
