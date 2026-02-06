# src/DataSupply/polygon_manager.py
import asyncio
import json
import logging

import websockets

from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)


class PolygonWebSocketManager:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.ws = None
        self.connected = False
        # Per-client fan-out queues: { stream_key: { client: asyncio.Queue } }
        self._client_queues = {}  # { stream_key: { client: Queue } }
        # connections map client -> set of stream_keys
        self.connections = {}  # { websocket_client: set(stream_keys) }
        # track which stream_keys have been subscribed to Polygon
        self.subscribed_streams = set()

    @property
    def queues(self):
        """Backward-compatible view: returns {stream_key: first_client_queue}.
        For explicit per-client access, use get_client_queue()."""
        flat = {}
        for sk, client_map in self._client_queues.items():
            if client_map:
                flat[sk] = next(iter(client_map.values()))
        return flat

    def get_client_queue(self, client, stream_key: str):
        """Get a specific client's queue for a stream_key."""
        return self._client_queues.get(stream_key, {}).get(client)

    async def connect(self):
        try:
            url = "wss://socket.polygon.io/stocks"
            self.ws = await websockets.connect(url)

            # send authentication request
            await self.ws.send(json.dumps({"action": "auth", "params": self.api_key}))

            print("✅ Authenticated to Polygon")
            if self.subscribed_streams:
                # re-subscribe to existing streams
                for stream_key in self.subscribed_streams:
                    await self.ws.send(
                        json.dumps({"action": "subscribe", "params": stream_key})
                    )
                logger.info(
                    f"🔄 Re-subscribed to existing streams: {self.subscribed_streams}"
                )

            self.connected = True
            logger.info("🔐 Polygon WebSocket connected & authenticated")

        except Exception as e:
            logger.error(f"❌ Failed to connect: {e}")
            self.connected = False
            self.ws = None
            raise

    async def subscribe(self, websocket_client, symbols, events=["Q"]):
        """
        subscribe new symbols, supporting multiple event types.
        (e.g., T for Trades, Q for Quotes)
        """
        if not self.connected:
            await self.connect()

        print(f"Debug: Subscribing to symbols: {symbols}")

        # ensure client mapping exists
        if websocket_client not in self.connections:
            self.connections[websocket_client] = set()

        for sym in symbols:
            for ev in events:
                stream_key = f"{ev}.{sym}"

                # ensure per-client queue exists for this stream_key
                if stream_key not in self._client_queues:
                    self._client_queues[stream_key] = {}
                if websocket_client not in self._client_queues[stream_key]:
                    self._client_queues[stream_key][websocket_client] = asyncio.Queue()

                # add to client's subscriptions
                self.connections[websocket_client].add(stream_key)

                # incrementally subscribe to Polygon only once per stream_key
                if stream_key not in self.subscribed_streams:
                    try:
                        await self.ws.send(
                            json.dumps({"action": "subscribe", "params": stream_key})
                        )
                        self.subscribed_streams.add(stream_key)
                        logger.info(f"📡 Subscribed to Polygon: {stream_key}")
                        print(f"📡 Successfully subscribed to {stream_key}")
                    except Exception as e:
                        logger.error(f"❌ Failed to subscribe to {stream_key}: {e}")
                        self.connected = False
                        self.ws = None
                else:
                    # already subscribed globally
                    print(f"ℹ️ {stream_key} already subscribed to Polygon")

    async def unsubscribe(self, websocket_client, symbol, events=["Q"]):
        """
        unsubscribe symbol
        """
        # remove stream_keys for this symbol from this client
        if websocket_client in self.connections:
            for ev in events:
                sk = f"{ev}.{symbol}"
                self.connections[websocket_client].discard(sk)

        # remove per-client queues and check if stream_key still needed
        for ev in events:
            stream_key = f"{ev}.{symbol}"

            # remove this client's queue
            if stream_key in self._client_queues:
                self._client_queues[stream_key].pop(websocket_client, None)

            still_needed = any(stream_key in syms for syms in self.connections.values())

            if not still_needed and self.connected:
                if stream_key in self.subscribed_streams:
                    try:
                        await self.ws.send(
                            json.dumps({"action": "unsubscribe", "params": stream_key})
                        )
                        self.subscribed_streams.discard(stream_key)
                        logger.info(f"❌ Unsubscribed from Polygon: {stream_key}")
                        print(f"❌ Unsubscribed for {stream_key}")
                    except Exception as e:
                        logger.error(f"❌ Failed to unsubscribe from {stream_key}: {e}")
                        self.connected = False
                        self.ws = None
                # remove all queues for this stream_key
                self._client_queues.pop(stream_key, None)
            else:
                print(f"ℹ️ {stream_key} still needed by other clients or not connected")

    async def disconnect(self, websocket_client):
        """client disconnect"""
        client_streams = self.connections.pop(websocket_client, set())

        for stream_key in list(client_streams):
            # remove this client's queue
            if stream_key in self._client_queues:
                self._client_queues[stream_key].pop(websocket_client, None)

            still_needed = any(stream_key in syms for syms in self.connections.values())

            if (
                not still_needed
                and self.connected
                and stream_key in self.subscribed_streams
            ):
                try:
                    await self.ws.send(
                        json.dumps({"action": "unsubscribe", "params": stream_key})
                    )
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
        """Polygon WebSocket Forever"""
        while True:
            try:
                await self.connect()

                async for msg in self.ws:
                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        continue

                    if not isinstance(data, list):
                        continue

                    for item in data:
                        ev = item.get("ev")
                        if ev == "Q":
                            symbol = item["sym"]
                            payload = {
                                "event_type": "Q",
                                "symbol": symbol,
                                "bid": item.get("bp"),
                                "ask": item.get("ap"),
                                "bid_size": item.get("bs"),
                                "ask_size": item.get("as"),
                                "timestamp": item.get("t"),
                            }
                            stream_key = f"Q.{symbol}"
                            # Fan out to all client queues
                            for q in self._client_queues.get(stream_key, {}).values():
                                await q.put(payload)

                        elif ev == "T":  # Trade
                            symbol = item["sym"]
                            payload = {
                                "event_type": "T",
                                "symbol": symbol,
                                "price": item.get("p"),
                                "size": item.get("s"),
                                "tape": item.get("z"),
                                "sequence_number": item.get("i"),
                                "timestamp": item.get("t"),
                                "trtf": item.get("trf_ts"),
                            }
                            stream_key = f"T.{symbol}"
                            # Fan out to all client queues
                            for q in self._client_queues.get(stream_key, {}).values():
                                await q.put(payload)

            except websockets.exceptions.ConnectionClosed:
                logger.warning(
                    "🔌 Polygon WebSocket connection closed, reconnecting..."
                )
                self.connected = False
                # self.ws = None
                # self.subscribed_streams.clear()
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"❌ Error in stream_forever: {e}")
                self.connected = False
                self.ws = None
                self.subscribed_streams.clear()
                await asyncio.sleep(5)
