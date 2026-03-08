"""Unified clock for jerry_trader — single source of truth for "what time is it?"

In **live mode** (default): every call falls through to ``time.time()`` with zero
overhead — the Rust ``ReplayClock`` is never instantiated.

In **replay mode**: all calls are served by a Rust-backed ``ReplayClock`` that uses
``std::time::Instant`` (monotonic) for drift-free timekeeping (<2 ms/hr).

Usage::

    from jerry_trader import clock

    # ── In any module that needs the current time ──
    ts_ms = clock.now_ms()                  # epoch ms
    dt    = clock.now_datetime()            # datetime in ET
    ts_s  = clock.now_s()                   # epoch seconds (float)

    # ── At startup (backend_starter.py) ──
    # Live mode (default — nothing to do, or explicitly):
    clock.set_live_mode()

    # Replay mode:
    clock.init_replay(data_start_ts_ns=..., speed=1.0)

    # ── Control (replay only) ──
    clock.jump_to(target_ts_ns)
    clock.set_speed(2.0)
    clock.pause()
    clock.resume()
"""

from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo

from jerry_trader._rust import ReplayClock, TickDataReplayer

ET = ZoneInfo("America/New_York")

# ── Module-level singleton ───────────────────────────────────────────

_clock: ReplayClock | None = None


# ── Lifecycle ────────────────────────────────────────────────────────


def init_replay(data_start_ts_ns: int, speed: float = 1.0) -> ReplayClock:
    """Activate replay mode by creating a Rust ``ReplayClock``.

    Args:
        data_start_ts_ns: The market-data epoch nanosecond timestamp that
            marks the beginning of the replay.
        speed: Replay speed multiplier (1.0 = real-time).

    Returns:
        The ``ReplayClock`` instance (also stored as module singleton).
    """
    global _clock
    _clock = ReplayClock(data_start_ts_ns, speed)
    return _clock


def set_live_mode() -> None:
    """Switch to live mode — all ``now_*()`` calls fall back to ``time.time()``."""
    global _clock
    _clock = None


def is_replay() -> bool:
    """Return ``True`` if a ``ReplayClock`` is active (replay mode)."""
    return _clock is not None


def get_clock() -> ReplayClock | None:
    """Return the active ``ReplayClock``, or ``None`` in live mode.

    Useful when other Rust components (e.g. ``TickDataReplayer``) need a
    direct reference to the clock.
    """
    return _clock


# ── Time queries ─────────────────────────────────────────────────────


def now_ms() -> int:
    """Current time as epoch milliseconds.

    In live mode: ``int(time.time() * 1000)``.
    In replay mode: ``ReplayClock.now_ms()``.
    """
    if _clock is None:
        return int(time.time() * 1000)
    return _clock.now_ms()


def now_ns() -> int:
    """Current time as epoch nanoseconds."""
    if _clock is None:
        return int(time.time() * 1_000_000_000)
    return _clock.now_ns()


def now_s() -> float:
    """Current time as epoch seconds (float, like ``time.time()``)."""
    if _clock is None:
        return time.time()
    return _clock.now_ms() / 1000.0


def now_datetime() -> datetime:
    """Current time as a timezone-aware ``datetime`` in US/Eastern."""
    return datetime.fromtimestamp(now_ms() / 1000.0, tz=ET)


# ── Control (replay only) ───────────────────────────────────────────


def jump_to(target_ts_ns: int) -> None:
    """Seek to an arbitrary point in market-data time.

    Raises ``RuntimeError`` if no ``ReplayClock`` is active.
    """
    if _clock is None:
        raise RuntimeError("jump_to() requires replay mode (call init_replay first)")
    _clock.jump_to(target_ts_ns)


def set_speed(speed: float) -> None:
    """Change replay speed (re-anchors current position).

    Raises ``RuntimeError`` if no ``ReplayClock`` is active.
    """
    if _clock is None:
        raise RuntimeError("set_speed() requires replay mode (call init_replay first)")
    _clock.set_speed(speed)


def pause() -> None:
    """Freeze the replay clock.

    Raises ``RuntimeError`` if no ``ReplayClock`` is active.
    """
    if _clock is None:
        raise RuntimeError("pause() requires replay mode (call init_replay first)")
    _clock.pause()


def resume() -> None:
    """Resume the replay clock from where it was frozen.

    Raises ``RuntimeError`` if no ``ReplayClock`` is active.
    """
    if _clock is None:
        raise RuntimeError("resume() requires replay mode (call init_replay first)")
    _clock.resume()


# ── TickDataReplayer factory ─────────────────────────────────────────


def create_tick_replayer(
    replay_date: str,
    lake_data_dir: str,
    *,
    start_time: str | None = None,
    max_gap_ms: int | None = None,
) -> TickDataReplayer:
    """Create a ``TickDataReplayer`` that inherits the active clock's params.

    Convenience wrapper: reads ``data_start_ts_ns`` and ``speed`` from the
    module-level ``ReplayClock`` so the replayer's internal timeline is
    automatically in sync.

    Raises ``RuntimeError`` if no ``ReplayClock`` is active (live mode).
    """
    if _clock is None:
        raise RuntimeError(
            "create_tick_replayer() requires replay mode (call init_replay first)"
        )
    return TickDataReplayer(
        replay_date=replay_date,
        lake_data_dir=lake_data_dir,
        data_start_ts_ns=_clock.data_start_ts_ns,
        speed=_clock.speed,
        start_time=start_time,
        max_gap_ms=max_gap_ms,
    )
