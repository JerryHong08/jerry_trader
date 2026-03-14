"""
synced_replayer_manager.py

In-process tick data replay manager using the Rust TickDataReplayer.

Drop-in replacement for ReplayerWebSocketManager (same interface) but
eliminates the WebSocket hop — the Rust engine delivers ticks via a
Python callback directly into the asyncio fan-out queues.

Usage:
    from jerry_trader._rust import TickDataReplayer
    from jerry_trader.data_supply.tick_data_supply.synced_replayer_manager import (
        SyncedReplayerManager,
    )

    replayer = TickDataReplayer(...)
    manager = SyncedReplayerManager(replayer)

    # Same interface as ReplayerWebSocketManager from here on:
    await manager.subscribe(client, ["AAPL", "NVDA"], events=["Q", "T"])
    await manager.stream_forever()
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from jerry_trader.shared.utils.logger import setup_logger

if TYPE_CHECKING:
    from jerry_trader._rust import TickDataReplayer

logger = setup_logger(__name__, log_to_file=True)


class SyncedReplayerManager:
    """
    In-process replayer manager backed by the Rust ``TickDataReplayer``.

    The Rust engine thread delivers ``(symbol, payload_dict)`` via a
    Python callback.  This class converts the Polygon wire-format dict
    into the normalised payload expected by downstream consumers and
    fans it out to per-client ``asyncio.Queue`` objects — exactly the
    same data flow as :class:`ReplayerWebSocketManager`, minus the
    WebSocket serialisation round-trip.
    """

    def __init__(self, replayer: TickDataReplayer) -> None:
        self._replayer = replayer
        self.connected: bool = False

        # Per-client fan-out queues: { stream_key: { client: asyncio.Queue } }
        self._client_queues: Dict[str, Dict[str, asyncio.Queue]] = {}

        # client -> set of stream_keys
        self.connections: Dict[str, Set[str]] = {}

        # Track which (symbol, ev) combos have been subscribed to the Rust replayer
        self.subscribed_streams: Set[str] = set()

        # Event loop for thread-safe queue insertion from the Rust callback
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ── Backward-compatible flat queue view ──────────────────────────

    @property
    def queues(self) -> Dict[str, asyncio.Queue]:
        """Backward-compatible view: {stream_key: first_client_queue}."""
        flat: Dict[str, asyncio.Queue] = {}
        for sk, client_map in self._client_queues.items():
            if client_map:
                flat[sk] = next(iter(client_map.values()))
        return flat

    def get_client_queue(
        self, client: object, stream_key: str
    ) -> Optional[asyncio.Queue]:
        """Get a specific client's queue for *stream_key*."""
        return self._client_queues.get(stream_key, {}).get(client)

    # ── Rust callback (runs on engine thread) ────────────────────────

    def _on_tick(self, symbol: str, payload: dict) -> None:  # noqa: C901
        """Called from the Rust engine thread with the GIL held.

        Converts the Polygon wire-format dict (``ev``, ``sym``, ``bp``, …)
        into the normalised format used by *ReplayerWebSocketManager*
        (``event_type``, ``symbol``, ``bid``, …) and fans out to all
        client queues via ``loop.call_soon_threadsafe``.
        """
        ev = payload.get("ev")
        if ev == "Q":
            converted = {
                "event_type": "Q",
                "symbol": symbol,
                "bid": payload.get("bp"),
                "ask": payload.get("ap"),
                "bid_size": payload.get("bs"),
                "ask_size": payload.get("as"),
                "timestamp": payload.get("t"),
                "bid_exchange": payload.get("bx", ""),
                "ask_exchange": payload.get("ax", ""),
                "conditions": payload.get("c", []),
                "tape": payload.get("z", 0),
            }
            stream_key = f"Q.{symbol}"
        elif ev == "T":
            converted = {
                "event_type": "T",
                "symbol": symbol,
                "price": payload.get("p"),
                "size": payload.get("s"),
                "exchange": payload.get("x"),
                "timestamp": payload.get("t"),
                "conditions": payload.get("c", []),
                "tape": payload.get("z", 0),
            }
            stream_key = f"T.{symbol}"
        else:
            return

        loop = self._loop
        if loop is None or loop.is_closed():
            return

        client_map = self._client_queues.get(stream_key)
        if not client_map:
            return

        for q in client_map.values():
            try:
                loop.call_soon_threadsafe(q.put_nowait, converted)
            except RuntimeError:
                # Event loop closed between check and call — safe to ignore.
                pass

    # ── Subscribe / Unsubscribe ──────────────────────────────────────

    async def subscribe(
        self,
        websocket_client: object,
        symbols: List[str],
        events: Optional[List[str]] = None,
    ) -> None:
        """Subscribe *websocket_client* to *symbols* / *events*.

        Creates the per-client asyncio queues first, then calls the Rust
        ``TickDataReplayer.subscribe()`` (blocking, GIL-released) in a
        thread-pool executor so the event loop is not blocked.
        """
        if events is None:
            events = ["Q"]

        if websocket_client not in self.connections:
            self.connections[websocket_client] = set()

        loop = asyncio.get_running_loop()

        for sym in symbols:
            # ── 1. Set up per-client queues ──────────────────────────
            for ev in events:
                stream_key = f"{ev}.{sym}"

                if stream_key not in self._client_queues:
                    self._client_queues[stream_key] = {}
                if websocket_client not in self._client_queues[stream_key]:
                    self._client_queues[stream_key][websocket_client] = asyncio.Queue()

                self.connections[websocket_client].add(stream_key)

            # ── 2. Subscribe to Rust replayer (once per symbol+events) ──
            # Collect events not yet subscribed for this symbol:
            needed = [
                ev for ev in events if f"{ev}.{sym}" not in self.subscribed_streams
            ]
            if not needed:
                logger.debug(
                    "%s already subscribed for events %s, skipping Rust subscribe",
                    sym,
                    events,
                )
                continue

            try:
                # subscribe() blocks (releases GIL internally) — run in executor
                await loop.run_in_executor(
                    None,
                    self._replayer.subscribe,
                    sym,
                    needed,
                    self._on_tick,
                )
                for ev in needed:
                    self.subscribed_streams.add(f"{ev}.{sym}")
                logger.info("📡 Synced-replayer subscribed: %s events=%s", sym, needed)
            except Exception:
                logger.exception("❌ Failed to subscribe %s via Rust replayer", sym)
                raise

    async def unsubscribe(
        self,
        websocket_client: object,
        symbol: str,
        events: Optional[List[str]] = None,
    ) -> None:
        """Unsubscribe *websocket_client* from *symbol*."""
        if events is None:
            events = ["Q"]

        if websocket_client in self.connections:
            for ev in events:
                self.connections[websocket_client].discard(f"{ev}.{symbol}")

        for ev in events:
            stream_key = f"{ev}.{symbol}"

            if stream_key in self._client_queues:
                self._client_queues[stream_key].pop(websocket_client, None)

            still_needed = any(stream_key in syms for syms in self.connections.values())

            if not still_needed:
                # No clients left — tell Rust to stop replaying this symbol.
                if stream_key in self.subscribed_streams:
                    try:
                        self._replayer.unsubscribe(symbol)
                    except Exception:
                        logger.exception(
                            "❌ Failed to unsubscribe %s from Rust replayer", symbol
                        )
                    self.subscribed_streams.discard(stream_key)
                    logger.info("❌ Synced-replayer unsubscribed: %s", stream_key)

                self._client_queues.pop(stream_key, None)

    async def disconnect(self, websocket_client: object) -> None:
        """Disconnect *websocket_client* and clean up its queues."""
        client_streams = self.connections.pop(websocket_client, set())

        for stream_key in list(client_streams):
            if stream_key in self._client_queues:
                self._client_queues[stream_key].pop(websocket_client, None)

            still_needed = any(stream_key in syms for syms in self.connections.values())

            if not still_needed and stream_key in self.subscribed_streams:
                ev, symbol = stream_key.split(".", 1)
                try:
                    self._replayer.unsubscribe(symbol)
                except Exception:
                    logger.exception(
                        "❌ Failed to auto-unsubscribe %s from Rust replayer", symbol
                    )
                self.subscribed_streams.discard(stream_key)
                self._client_queues.pop(stream_key, None)
                logger.info("❌ Auto-unsubscribed %s (no more clients)", stream_key)

        logger.info("🔌 Client disconnected from synced-replayer")

    # ── Stream loop ──────────────────────────────────────────────────

    async def stream_forever(self) -> None:
        """Keep the manager alive.

        Unlike :class:`ReplayerWebSocketManager` there is no WebSocket to
        receive from — the Rust engine thread pushes ticks via the
        ``_on_tick`` callback.  This coroutine simply captures the event
        loop reference and keeps the task alive so ``run_forever()`` on
        the shared loop doesn't exit.
        """
        self._loop = asyncio.get_running_loop()
        self.connected = True
        logger.info("✅ Synced-replayer manager started (in-process Rust engine)")

        # Stay alive forever — cancellation will stop us.
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            logger.info("🔌 Synced-replayer stream_forever cancelled")
            self.connected = False

    async def close(self) -> None:
        """Shut down the underlying Rust replayer."""
        try:
            self._replayer.shutdown()
        except Exception:
            logger.exception("Error shutting down Rust replayer")
        self.connected = False
        logger.info("🔌 Synced-replayer closed")
