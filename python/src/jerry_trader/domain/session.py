"""Session phase detection for pre-market signals.

Based on Event Framework Validation findings:
- Mid phase (07:00-09:00 ET): avg_return=+0.20%, only effective window
- Early phase (04:00-07:00 ET): avg_return=-2.33%, noise
- Late phase (09:00-09:30 ET): avg_return=-1.06%, pre-open chaos
"""

from datetime import datetime
from enum import Enum


class SessionPhase(Enum):
    """Pre-market session phase classification."""

    EARLY = "early"  # 04:00-07:00 ET — noise, avoid
    MID = "mid"  # 07:00-09:00 ET — effective window
    LATE = "late"  # 09:00-09:30 ET — pre-open chaos, avoid


def get_session_phase(timestamp_et: datetime) -> SessionPhase:
    """Classify session phase based on ET time.

    Args:
        timestamp_et: Timestamp in America/New_York timezone

    Returns:
        SessionPhase enum value
    """
    hour = timestamp_et.hour

    if hour < 7:
        return SessionPhase.EARLY
    elif hour < 9:
        return SessionPhase.MID
    else:
        return SessionPhase.LATE


def get_session_phase_from_ns(trigger_time_ns: int) -> SessionPhase:
    """Classify session phase from nanosecond UTC timestamp.

    Args:
        trigger_time_ns: Nanosecond UTC timestamp

    Returns:
        SessionPhase enum value
    """
    from datetime import timezone

    import pytz

    utc_time = datetime.fromtimestamp(trigger_time_ns / 1e9, tz=timezone.utc)
    et_time = utc_time.astimezone(pytz.timezone("America/New_York"))
    return get_session_phase(et_time)


def get_session_phase_from_epoch_ms(epoch_ms: int) -> SessionPhase:
    """Classify session phase from millisecond UTC timestamp.

    Args:
        epoch_ms: Millisecond UTC timestamp

    Returns:
        SessionPhase enum value
    """
    # Convert ms to ns (multiply by 1e6)
    return get_session_phase_from_ns(epoch_ms * 1_000_000)


def is_effective_window(timestamp_et: datetime) -> bool:
    """Check if timestamp is in the effective signal window (mid phase)."""
    return get_session_phase(timestamp_et) == SessionPhase.MID


__all__ = [
    "SessionPhase",
    "get_session_phase",
    "get_session_phase_from_ns",
    "get_session_phase_from_epoch_ms",
    "is_effective_window",
]
