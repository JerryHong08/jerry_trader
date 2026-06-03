"""Exit strategy result types — pure dataclasses for simulation output."""

from __future__ import annotations

from dataclasses import dataclass, field

# ══════════════════════════════════════════════════════════════════════════
# Exit Lab — exit timing research (no factors, pure price-path analysis)
# ══════════════════════════════════════════════════════════════════════════


@dataclass
class ExitResult:
    """Single ticker exit simulation result."""

    ticker: str
    entry_ms: int
    entry_price: float
    exit_ms: int
    exit_price: float
    exit_reason: str  # take_profit | stop_loss | trailing_stop | session_end
    pnl_pct: float
    mfe_pct: float
    mae_pct: float
    duration_min: float


@dataclass
class ExitStrategyReport:
    """Aggregated exit strategy results across all tickers."""

    strategy_name: str
    params: dict
    n_trades: int
    n_profitable: int
    win_rate: float
    avg_win_pct: float
    avg_loss_pct: float
    expectancy_pct: float
    avg_mfe_pct: float
    avg_duration_min: float
    per_ticker: list[ExitResult] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"  {self.strategy_name}: "
            f"expectancy={self.expectancy_pct:+.2f}% | "
            f"win_rate={self.win_rate:.1%} ({self.n_profitable}/{self.n_trades}) | "
            f"avg_win={self.avg_win_pct:+.2f}% avg_loss={self.avg_loss_pct:+.2f}% | "
            f"avg_dur={self.avg_duration_min:.1f}min | "
            f"avg_MFE={self.avg_mfe_pct:+.2f}%"
        )
