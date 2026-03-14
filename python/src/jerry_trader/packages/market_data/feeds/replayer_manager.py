#!/usr/bin/env python3
"""
replayer_manager.py

A drop-in replacement for polygon_manager.py that connects to the local Rust
replay WebSocket server instead of Polygon.io. Provides the same interface
and functionality for seamless integration.

Usage:
    # Replace PolygonWebSocketManager with ReplayerWebSocketManager
    from replayer_manager import ReplayerWebSocketManager

    manager = ReplayerWebSocketManager(replay_url="ws://127.0.0.1:8765")
    await manager.connect()
    await manager.subscribe(client, ["AAPL", "NVDA"], events=["Q", "T"])
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Set

import websockets

from jerry_trader.shared.utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)


class ReplayerWebSocketManager:
    """
    WebSocket manager for local Rust replay server.

    Drop-in replacement for PolygonWebSocketManager with identical interface.
    Connects to local replay server instead of Polygon.io.
    """

    def __init__(self, replay_url: str = "ws://127.0.0.1:8765"):
        """
        Initialize the replayer manager.

        Args:
            replay_url: WebSocket URL of the Rust replay server
        """
        self.replay_url = replay_url
        self.ws = None
        self.connected = False

        # Per-client fan-out queues: { stream_key: { client: asyncio.Queue } }
        self._client_queues: Dict[str, Dict[str, asyncio.Queue]] = {}

        # Connections map client -> set of stream_keys
        self.connections: Dict[str, Set[str]] = {}

        # Track which stream_keys have been subscribed to replay server
        self.subscribed_streams: Set[str] = set()

        # Track server connection info
        self._server_info = None

    @property
    def queues(self) -> Dict[str, asyncio.Queue]:
        """Backward-compatible view: returns {stream_key: first_client_queue}.
        For explicit per-client access, use get_client_queue()."""
        flat = {}
        for sk, client_map in self._client_queues.items():
            if client_map:
                flat[sk] = next(iter(client_map.values()))
        return flat

    def get_client_queue(self, client, stream_key: str) -> Optional[asyncio.Queue]:
        """Get a specific client's queue for a stream_key."""
        return self._client_queues.get(stream_key, {}).get(client)

    async def connect(self):
        """Connect to the Rust replay WebSocket server."""
        try:
            self.ws = await websockets.connect(self.replay_url)

            # Wait for initial connection message from server
            initial_msg = await asyncio.wait_for(self.ws.recv(), timeout=5.0)
            try:
                data = json.loads(initial_msg)
                if data.get("status") == "connected":
                    self._server_info = data.get("message", "")
                    print(f"✅ Connected to replay server: {self._server_info}")
                    self.connected = True
                    logger.info(f"🔐 Replay WebSocket connected: {self._server_info}")
                else:
                    logger.warning(f"Unexpected initial message: {data}")
                    self.connected = True  # Continue anyway
            except json.JSONDecodeError:
                logger.warning(f"Non-JSON initial message: {initial_msg}")
                self.connected = True  # Continue anyway

        except Exception as e:
            logger.error(f"❌ Failed to connect to replay server: {e}")
            self.connected = False
            self.ws = None
            raise

    async def subscribe(
        self, websocket_client: str, symbols: List[str], events: List[str] = ["Q"]
    ):
        """
        Subscribe to symbols with specific event types.

        Args:
            websocket_client: Client identifier
            symbols: List of symbols to subscribe (e.g., ["AAPL", "NVDA"])
            events: List of event types (e.g., ["Q"] for quotes, ["T"] for trades, ["Q", "T"] for both)
        """
        if not self.connected:
            await self.connect()

        print(f"Debug: Subscribing to symbols: {symbols} with events: {events}")

        # Ensure client mapping exists
        if websocket_client not in self.connections:
            self.connections[websocket_client] = set()

        for sym in symbols:
            for ev in events:
                stream_key = f"{ev}.{sym}"

                # Ensure per-client queue exists for this stream_key
                if stream_key not in self._client_queues:
                    self._client_queues[stream_key] = {}
                if websocket_client not in self._client_queues[stream_key]:
                    self._client_queues[stream_key][websocket_client] = asyncio.Queue()

                # Add to client's subscriptions
                self.connections[websocket_client].add(stream_key)

                # Incrementally subscribe to replay server only once per stream_key
                if stream_key not in self.subscribed_streams:
                    try:
                        # Send subscription request to Rust server
                        subscribe_msg = {
                            "action": "subscribe",
                            "symbols": [sym],
                            "events": [ev],
                        }
                        await self.ws.send(json.dumps(subscribe_msg))
                        self.subscribed_streams.add(stream_key)
                        logger.info(f"📡 Subscribed to replay server: {stream_key}")
                        print(f"📡 Successfully subscribed to {stream_key}")

                        # Wait for subscription confirmation
                        # try:
                        #     response = await asyncio.wait_for(self.ws.recv(), timeout=2.0)
                        #     resp_data = json.loads(response)
                        #     if resp_data.get("status") == "subscribed":
                        #         logger.info(f"✅ Confirmed: {resp_data.get('message')}")
                        # except asyncio.TimeoutError:
                        #     logger.warning(f"No confirmation received for {stream_key}")

                    except Exception as e:
                        logger.error(f"❌ Failed to subscribe to {stream_key}: {e}")
                        self.connected = False
                        self.ws = None
                        raise
                else:
                    # Already subscribed globally
                    print(f"ℹ️ {stream_key} already subscribed to replay server")

    async def unsubscribe(
        self, websocket_client: str, symbol: str, events: List[str] = ["Q"]
    ):
        """
        Unsubscribe from a symbol's event streams.

        Args:
            websocket_client: Client identifier
            symbol: Symbol to unsubscribe
            events: Event types to unsubscribe
        """
        # Remove stream_keys for this symbol from this client
        if websocket_client in self.connections:
            for ev in events:
                sk = f"{ev}.{symbol}"
                self.connections[websocket_client].discard(sk)

        # For each stream_key, remove per-client queue and check if still needed
        for ev in events:
            stream_key = f"{ev}.{symbol}"

            # Remove this client's queue
            if stream_key in self._client_queues:
                self._client_queues[stream_key].pop(websocket_client, None)

            still_needed = any(stream_key in syms for syms in self.connections.values())

            if not still_needed and self.connected:
                if stream_key in self.subscribed_streams:
                    try:
                        unsubscribe_msg = {
                            "action": "unsubscribe",
                            "symbols": [symbol],
                            "events": [ev],
                        }
                        await self.ws.send(json.dumps(unsubscribe_msg))
                        self.subscribed_streams.discard(stream_key)
                        logger.info(f"❌ Unsubscribed from replay server: {stream_key}")
                        print(f"❌ Unsubscribed from {stream_key}")
                    except Exception as e:
                        logger.error(f"❌ Failed to unsubscribe from {stream_key}: {e}")
                        self.connected = False
                        self.ws = None
                # Remove all queues for this stream_key
                self._client_queues.pop(stream_key, None)
            else:
                print(f"ℹ️ {stream_key} still needed by other clients or not connected")

    async def disconnect(self, websocket_client: str):
        """
        Disconnect a client and clean up its subscriptions.

        Args:
            websocket_client: Client identifier to disconnect
        """
        client_streams = self.connections.pop(websocket_client, set())

        for stream_key in list(client_streams):
            # Remove this client's queue
            if stream_key in self._client_queues:
                self._client_queues[stream_key].pop(websocket_client, None)

            still_needed = any(stream_key in syms for syms in self.connections.values())

            if (
                not still_needed
                and self.connected
                and stream_key in self.subscribed_streams
            ):
                try:
                    # Parse stream_key back to symbol and event
                    ev, symbol = stream_key.split(".", 1)
                    unsubscribe_msg = {
                        "action": "unsubscribe",
                        "symbols": [symbol],
                        "events": [ev],
                    }
                    await self.ws.send(json.dumps(unsubscribe_msg))
                    self.subscribed_streams.discard(stream_key)
                    self._client_queues.pop(stream_key, None)
                    logger.info(
                        f"❌ Auto-unsubscribed from {stream_key} (no more clients)"
                    )
                except Exception as e:
                    logger.error(
                        f"❌ Failed to auto-unsubscribe from {stream_key}: {e}"
                    )

        logger.info("🔌 Client disconnected")

    async def stream_forever(self):
        """
        Main streaming loop - receives data from replay server and distributes to queues.

        This method runs forever, automatically reconnecting on connection loss.
        """
        while True:
            try:
                await self.connect()

                async for msg in self.ws:
                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse message: {msg}")
                        continue

                    # Handle status messages
                    if "status" in data:
                        status = data.get("status")
                        if status == "subscribed":
                            logger.info(f"📊 Status: {status} - {data.get('message')}")
                        elif status == "error":
                            logger.error(f"❌ Server error: {data.get('message')}")
                        continue

                    # Handle Quote messages (ev: "Q")
                    if data.get("ev") == "Q":
                        symbol = data.get("sym")
                        if not symbol:
                            continue

                        payload = {
                            "event_type": "Q",
                            "symbol": symbol,
                            "bid": data.get("bp"),
                            "ask": data.get("ap"),
                            "bid_size": data.get("bs"),
                            "ask_size": data.get("as"),
                            "timestamp": data.get("t"),
                            "bid_exchange": data.get("bx", ""),
                            "ask_exchange": data.get("ax", ""),
                            "conditions": data.get("c", []),
                            "tape": data.get("z", 0),
                        }

                        stream_key = f"Q.{symbol}"
                        # Fan out to all client queues
                        for q in self._client_queues.get(stream_key, {}).values():
                            await q.put(payload)

                    # Handle Trade messages (ev: "T")
                    elif data.get("ev") == "T":
                        symbol = data.get("sym")
                        if not symbol:
                            continue

                        payload = {
                            "event_type": "T",
                            "symbol": symbol,
                            "price": data.get("p"),
                            "size": data.get("s"),
                            "exchange": data.get("x"),
                            "timestamp": data.get("t"),
                            "conditions": data.get("c", []),
                            "tape": data.get("z", 0),
                        }

                        stream_key = f"T.{symbol}"
                        # Fan out to all client queues
                        for q in self._client_queues.get(stream_key, {}).values():
                            await q.put(payload)

                    else:
                        # Unknown event type, log but continue
                        ev = data.get("ev", "unknown")
                        if ev not in ["status", "connected"]:
                            logger.debug(f"Unknown event type: {ev}")

            except websockets.exceptions.ConnectionClosed:
                logger.warning("🔌 Replay server connection closed, reconnecting...")
                self.connected = False
                self.ws = None
                self.subscribed_streams.clear()
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"❌ Error in stream_forever: {e}")
                self.connected = False
                self.ws = None
                self.subscribed_streams.clear()
                await asyncio.sleep(5)

    async def close(self):
        """Close the WebSocket connection."""
        if self.ws:
            await self.ws.close()
            self.ws = None
            self.connected = False
            logger.info("🔌 WebSocket connection closed")


# Example usage
async def example_usage():
    """Example demonstrating how to use ReplayerWebSocketManager."""

    # Create manager (default connects to localhost:8765)
    manager = ReplayerWebSocketManager()

    # Connect
    await manager.connect()

    # Subscribe to quotes
    await manager.subscribe(
        websocket_client="example_client",
        symbols=["AAPL", "NVDA"],
        events=["Q"],  # Quotes only
    )

    # Subscribe to trades
    await manager.subscribe(
        websocket_client="example_client", symbols=["TSLA"], events=["T"]  # Trades only
    )

    # Subscribe to both quotes and trades
    await manager.subscribe(
        websocket_client="example_client", symbols=["MSFT"], events=["Q", "T"]  # Both
    )

    # Start streaming in background
    stream_task = asyncio.create_task(manager.stream_forever())

    # Process messages from queues
    try:
        aapl_quote_queue = manager.queues.get("Q.AAPL")
        if aapl_quote_queue:
            # Get messages with timeout
            while True:
                try:
                    msg = await asyncio.wait_for(aapl_quote_queue.get(), timeout=1.0)
                    print(f"AAPL Quote: bid={msg['bid']}, ask={msg['ask']}")
                except asyncio.TimeoutError:
                    continue
                except KeyboardInterrupt:
                    break
    finally:
        # Cleanup
        await manager.disconnect("example_client")
        await manager.close()
        stream_task.cancel()


if __name__ == "__main__":
    # Run example
    asyncio.run(example_usage())
