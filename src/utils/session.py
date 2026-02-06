"""
Unified Session ID for Redis keys and InfluxDB tags.

Every Redis key and InfluxDB tag in the system uses a session_id to identify
the trading day + mode. This replaces the scattered run_mode/db_date/db_id logic.

Session ID format:
    - Live mode:                "{YYYYMMDD}_live"
    - Replay mode (no suffix):  "{YYYYMMDD}_replay"
    - Replay mode (with suffix):"{YYYYMMDD}_replay_{suffix_id}"

Examples:
    make_session_id()                                       -> "20260206_live"
    make_session_id(replay_date="20260120")                 -> "20260120_replay"
    make_session_id(replay_date="20260120", suffix_id="v1") -> "20260120_replay_v1"
"""

from datetime import date, datetime
from typing import Tuple
from zoneinfo import ZoneInfo


def make_session_id(
    replay_date: str | None = None,
    suffix_id: str | None = None,
) -> str:
    """
    Build a unified session identifier.

    Args:
        replay_date: Replay date in YYYYMMDD format. None = live mode.
        suffix_id: Optional suffix for replay sessions (e.g., "test", "v2").

    Returns:
        Session ID string used as suffix for every Redis key and InfluxDB tag.
    """
    if replay_date:
        if suffix_id:
            return f"{replay_date}_replay_{suffix_id}"
        return f"{replay_date}_replay"
    else:
        live_date = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
        return f"{live_date}_live"


def parse_session_id(session_id: str) -> Tuple[str, str]:
    """
    Extract db_date and run_mode from a session_id string.

    Args:
        session_id: Session ID (e.g. "20260120_live", "20260120_replay",
                    "20260120_replay_v1").

    Returns:
        (db_date, run_mode) tuple where db_date is "YYYYMMDD" and
        run_mode is "live" or "replay".
    """
    db_date = session_id[:8]
    run_mode = "replay" if "_replay" in session_id else "live"
    return db_date, run_mode


def db_date_to_date(db_date: str) -> date:
    """
    Parse a YYYYMMDD string into a :class:`datetime.date`.

    This eliminates the fragile ``db_date[:4]`` / ``[4:6]`` / ``[6:8]``
    slicing that was copy-pasted across the codebase.

    Args:
        db_date: Date string in YYYYMMDD format (e.g. "20260120").

    Returns:
        A ``datetime.date`` object.
    """
    return date(int(db_date[:4]), int(db_date[4:6]), int(db_date[6:8]))
