"""Strategy Domain Models

Pure domain models for trading strategy concepts.
Immutable value objects with no I/O dependencies.
"""

from dataclasses import dataclass
from enum import Enum


class SignalType(Enum):
    """Trading signal types"""

    ENTRY_LONG = "entry_long"
    EXIT_LONG = "exit_long"
    ENTRY_SHORT = "entry_short"
    EXIT_SHORT = "exit_short"


@dataclass(frozen=True)
class Signal:
    """Trading signal value object

    Represents a trading decision with confidence and reasoning.
    Immutable to ensure signal integrity.
    """

    symbol: str
    timestamp_ns: int
    signal_type: SignalType
    confidence: float  # 0.0 to 1.0
    reason: str
    metadata: dict  # Additional context (factors, prices, etc.)

    def __post_init__(self):
        """Validate signal data"""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(
                f"Confidence must be between 0.0 and 1.0, got {self.confidence}"
            )

        if not self.reason:
            raise ValueError("Signal must have a reason")

        # Make metadata immutable by converting to frozenset of items
        # This prevents accidental mutation
        object.__setattr__(self, "metadata", dict(self.metadata))

    @property
    def is_entry(self) -> bool:
        """Check if signal is an entry signal"""
        return self.signal_type in (SignalType.ENTRY_LONG, SignalType.ENTRY_SHORT)

    @property
    def is_exit(self) -> bool:
        """Check if signal is an exit signal"""
        return self.signal_type in (SignalType.EXIT_LONG, SignalType.EXIT_SHORT)

    @property
    def is_long(self) -> bool:
        """Check if signal is for long position"""
        return self.signal_type in (SignalType.ENTRY_LONG, SignalType.EXIT_LONG)

    @property
    def is_short(self) -> bool:
        """Check if signal is for short position"""
        return self.signal_type in (SignalType.ENTRY_SHORT, SignalType.EXIT_SHORT)


@dataclass(frozen=True)
class RiskLimits:
    """Risk management limits

    Defines maximum risk parameters for trading.
    Immutable to prevent accidental modification.
    """

    max_position_size: float  # USD
    max_positions: int
    max_daily_loss: float  # USD
    max_drawdown_pct: float  # 0.0 to 1.0

    def __post_init__(self):
        """Validate risk limits"""
        if self.max_position_size <= 0:
            raise ValueError(
                f"Max position size must be positive, got {self.max_position_size}"
            )

        if self.max_positions <= 0:
            raise ValueError(
                f"Max positions must be positive, got {self.max_positions}"
            )

        if self.max_daily_loss <= 0:
            raise ValueError(
                f"Max daily loss must be positive, got {self.max_daily_loss}"
            )

        if not 0.0 < self.max_drawdown_pct <= 1.0:
            raise ValueError(
                f"Max drawdown must be between 0.0 and 1.0, got {self.max_drawdown_pct}"
            )


@dataclass
class RiskState:
    """Current risk state (mutable)

    Tracks current risk metrics. Mutable because it changes frequently.
    """

    current_positions: int
    daily_pnl: float
    peak_equity: float
    current_equity: float

    def __post_init__(self):
        """Validate risk state"""
        if self.current_positions < 0:
            raise ValueError(
                f"Current positions cannot be negative, got {self.current_positions}"
            )

        if self.peak_equity < 0:
            raise ValueError(f"Peak equity cannot be negative, got {self.peak_equity}")

        if self.current_equity < 0:
            raise ValueError(
                f"Current equity cannot be negative, got {self.current_equity}"
            )

    @property
    def current_drawdown_pct(self) -> float:
        """Calculate current drawdown percentage"""
        if self.peak_equity == 0:
            return 0.0
        return (self.peak_equity - self.current_equity) / self.peak_equity

    @property
    def is_in_profit(self) -> bool:
        """Check if currently in profit"""
        return self.daily_pnl > 0

    @property
    def is_in_loss(self) -> bool:
        """Check if currently in loss"""
        return self.daily_pnl < 0
