"""RemoteClockFollower — tracks a ReplayClock on a remote machine via Redis pub/sub.

On the **clock-master** machine (e.g. wsl2), ``clock.start_heartbeat_publisher()``
publishes the current virtual time to ``clock:heartbeat:{session_id}`` every ~100 ms.

On any **remote** machine (e.g. mibuntu running MarketSnapshotReplayer), create a
``RemoteClockFollower`` and call ``start_listening()``.  It subscribes to that same
channel in a background daemon thread and interpolates virtual time between heartbeats
using the local monotonic clock — so ``now_ms()`` / ``now_ns()`` are always smooth,
never coarse-grained to the 100 ms heartbeat interval.

Typical usage::

    from jerry_trader.utils.remote_clock import RemoteClockFollower
    import redis

    r = redis.Redis(host="wsl2-host", port=6379, decode_responses=True)
    follower = RemoteClockFollower(r, session_id="20260313_replay_v1")
    follower.start_listening()

    # … later, in the replay loop …
    while follower.now_ns() < target_ts_ns:
        await asyncio.sleep(0.05)

Design notes
~~~~~~~~~~~~
* ``wall_ns`` in the heartbeat is the *sender's* ``time.time_ns()`` at the moment of
  publish.  We record our local ``time.time_ns()`` at reception (``_rx_wall_ns``) and
  use *that* for interpolation; this removes the one-way network latency from the
  virtual-time estimate.

* If the clock is paused, ``now_ns()`` freezes at the last reported ``ts_ns``.

* While ``has_sync`` is ``False`` (no heartbeat received yet), ``now_ns()`` returns
  local wall time so callers don't block forever on startup.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Optional


class RemoteClockFollower:
    """Subscribe to ReplayClock heartbeats and interpolate virtual time.

    Args:
        redis_client: A synchronous ``redis.Redis`` instance whose connection
            points to the *clock-master* machine's Redis.  A second dedicated
            connection is opened internally for blocking pub/sub.
        session_id: Session identifier that scopes the heartbeat channel.
    """

    def __init__(self, redis_client, session_id: str) -> None:
        from jerry_trader.utils.redis_keys import clock_heartbeat_channel

        self._redis = redis_client
        self._channel = clock_heartbeat_channel(session_id)

        # Latest heartbeat state — protected by _lock
        self._ts_ns: int = 0  # virtual time reported by master at last heartbeat
        self._speed: float = 1.0
        self._is_paused: bool = False
        self._rx_wall_ns: int = 0  # our local time.time_ns() at heartbeat reception

        self._has_sync: bool = False
        self._lock = threading.Lock()

        self._thread: Optional[threading.Thread] = None

    # ── Public API ────────────────────────────────────────────────────

    def start_listening(self) -> None:
        """Start a background daemon thread that subscribes to the heartbeat channel.

        Safe to call multiple times — only starts one thread.
        """
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._listen_loop,
            daemon=True,
            name=f"RemoteClockFollower:{self._channel}",
        )
        self._thread.start()

    def now_ns(self) -> int:
        """Current virtual epoch nanoseconds, interpolated since last heartbeat.

        Falls back to ``time.time_ns()`` before the first heartbeat is received
        so callers don't block on startup.
        """
        with self._lock:
            if not self._has_sync:
                return time.time_ns()
            if self._is_paused:
                return self._ts_ns
            elapsed_ns = time.time_ns() - self._rx_wall_ns
            return self._ts_ns + int(elapsed_ns * self._speed)

    def now_ms(self) -> int:
        """Current virtual epoch milliseconds."""
        return self.now_ns() // 1_000_000

    @property
    def has_sync(self) -> bool:
        """``True`` after at least one heartbeat has been received."""
        return self._has_sync

    @property
    def is_paused(self) -> bool:
        """Mirror of the master clock's paused state."""
        with self._lock:
            return self._is_paused

    @property
    def speed(self) -> float:
        """Mirror of the master clock's speed multiplier."""
        with self._lock:
            return self._speed

    # ── Internal ─────────────────────────────────────────────────────

    def _listen_loop(self) -> None:
        """Background thread: subscribe to heartbeats and update state."""
        # Create a dedicated Redis connection for blocking subscribe
        try:
            conn_kwargs = self._redis.connection_pool.connection_kwargs
            import redis as _redis_mod

            sub_client = _redis_mod.Redis(
                host=conn_kwargs.get("host", "127.0.0.1"),
                port=conn_kwargs.get("port", 6379),
                db=conn_kwargs.get("db", 0),
                decode_responses=True,
            )
        except Exception as exc:
            import logging

            logging.getLogger(__name__).error(
                f"RemoteClockFollower: failed to create subscriber connection — {exc}"
            )
            return

        ps = sub_client.pubsub()
        ps.subscribe(self._channel)

        for msg in ps.listen():
            if msg["type"] != "message":
                continue
            try:
                data = json.loads(msg["data"])
                rx = time.time_ns()
                with self._lock:
                    self._ts_ns = int(data["ts_ns"])
                    self._speed = float(data["speed"])
                    self._is_paused = bool(data["is_paused"])
                    self._rx_wall_ns = rx
                    self._has_sync = True
            except Exception:
                pass  # malformed message — skip
