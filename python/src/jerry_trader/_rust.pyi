"""Type stubs for the Rust extension module (jerry_trader._rust)."""

from typing import Optional

def sum_as_string(a: int, b: int) -> str:
    """Formats the sum of two numbers as a string."""
    ...

def z_score(value: float, history: list[float]) -> Optional[float]:
    """Return z-score of value relative to history, or None if < 2 samples.

    z = (value - mean) / std  (population std)
    """
    ...

def price_accel(
    recent: list[tuple[int, float]], older: list[tuple[int, float]]
) -> float:
    """Direction-aware price acceleration.

    Returns rate_recent - rate_older where rate is fractional return per second.
    """
    ...
