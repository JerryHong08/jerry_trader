"""Factor Domain Models

Pure domain models for factor computation results.
Immutable value objects with no I/O dependencies.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Factor:
    """Factor value object

    Represents a single computed factor value at a point in time.
    Immutable to ensure factor integrity.
    """

    symbol: str
    timestamp_ns: int
    name: str  # "z_score", "price_accel", "volume_surge", etc.
    value: float

    def __post_init__(self):
        """Validate factor data"""
        if not self.name:
            raise ValueError("Factor name cannot be empty")


@dataclass(frozen=True)
class FactorSnapshot:
    """All factors for a symbol at a point in time

    Represents a complete set of factor values.
    Immutable to ensure snapshot integrity.
    """

    symbol: str
    timestamp_ns: int
    factors: dict[str, float]  # name -> value

    def __post_init__(self):
        """Validate factor snapshot"""
        if not self.factors:
            raise ValueError("Factor snapshot must contain at least one factor")

        # Make factors dict immutable by converting to new dict
        object.__setattr__(self, "factors", dict(self.factors))

    def get_factor(self, name: str, default: float = 0.0) -> float:
        """Get factor value by name with default"""
        return self.factors.get(name, default)

    def has_factor(self, name: str) -> bool:
        """Check if factor exists"""
        return name in self.factors

    @property
    def factor_names(self) -> list[str]:
        """Get list of factor names"""
        return list(self.factors.keys())

    @property
    def factor_count(self) -> int:
        """Get number of factors"""
        return len(self.factors)
