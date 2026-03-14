# src/DataSupply/thetadata_manager.py

import asyncio
import datetime as dt
import json
import logging
import os
import traceback
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import pytz
import websockets

from jerry_trader.shared.utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class ThetaDataManager:
    """
    ThetaData WebSocket Manager with architecture similar to PolygonWebSocketManager.

    Stream Key Format: "{sec_type}.{req_type}.{identifier}"
    Examples:
        - "STOCK.QUOTE.AAPL"
        - "STOCK.TRADE.AAPL"
        - "OPTION.QUOTE.QQQ_20250428_462000_P"
        - "OPTION.TRADE.QQQ_20250428_462000_P"
        - "INDEX.QUOTE.SPX"
    """

    def __init__(self, uri: str = "ws://127.0.0.1:25520/v1/events"):
        self.uri = uri
        self.ws = None
        self.connected = False

        # Per-client fan-out queues: { stream_key: { client: asyncio.Queue } }
        self._client_queues: Dict[str, Dict[object, asyncio.Queue]] = {}

        # connections map client -> set of stream_keys
        self.connections: Dict[object, Set[str]] = {}

        # track which stream_keys have been subscribed to ThetaData
        self.subscribed_streams: Set[str] = set()

        # subscription ID counter
        self.next_id = 0

    @property
    def queues(self) -> Dict[str, asyncio.Queue]:
        """Backward-compatible view: returns {stream_key: first_client_queue}."""
        flat = {}
        for sk, client_map in self._client_queues.items():
            if client_map:
                flat[sk] = next(iter(client_map.values()))
        return flat

    def get_client_queue(self, client, stream_key: str) -> Optional[asyncio.Queue]:
        """Get a specific client's queue for a stream_key."""
        return self._client_queues.get(stream_key, {}).get(client)

    def _generate_stream_key(self, sec_type: str, req_type: str, contract: dict) -> str:
        """Generate a unique stream key for subscription tracking."""
        root = contract.get("root", "")

        if sec_type in ("STOCK", "INDEX"):
            identifier = root
        elif sec_type == "OPTION":
            # Format: ROOT_EXPIRATION_STRIKE_RIGHT
            expiration = contract.get("expiration", "")
            strike = contract.get("strike", "")
            right = contract.get("right", "")
            identifier = f"{root}_{expiration}_{strike}_{right}"
        else:
            identifier = root

        return f"{sec_type}.{req_type}.{identifier}"

    def _parse_stream_key(self, stream_key: str) -> Tuple[str, str, dict]:
        """Parse a stream key back into its components."""
        parts = stream_key.split(".", 2)
        if len(parts) != 3:
            raise ValueError(f"Invalid stream key format: {stream_key}")

        sec_type, req_type, identifier = parts

        if sec_type in ("STOCK", "INDEX"):
            contract = {"root": identifier}
        elif sec_type == "OPTION":
            option_parts = identifier.split("_")
            if len(option_parts) != 4:
                raise ValueError(f"Invalid option identifier: {identifier}")
            root, expiration, strike, right = option_parts
            contract = {
                "root": root,
                "expiration": expiration,
                "strike": strike,
                "right": right,
            }
        else:
            contract = {"root": identifier}

        return sec_type, req_type, contract

    async def connect(self):
        """Connect to ThetaData local terminal."""
        try:
            self.ws = await websockets.connect(self.uri)
            self.connected = True
            logger.info(f"🔌 ThetaData WebSocket connected to {self.uri}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to ThetaData: {e}")
            self.connected = False
            self.ws = None
            raise

    async def subscribe(self, websocket_client, subscriptions: List[dict]):
        """
        Subscribe to multiple streams.

        Args:
            subscriptions: List of subscription dicts with format:
                {
                    "sec_type": "STOCK" | "OPTION" | "INDEX",
                    "req_types": ["QUOTE", "TRADE"],
                    "contract": {"root": "AAPL", ...}
                }
        """
        if not self.connected:
            await self.connect()

        logger.debug(f"📡 Subscribing to {len(subscriptions)} streams")

        if websocket_client not in self.connections:
            self.connections[websocket_client] = set()

        for sub in subscriptions:
            sec_type = sub.get("sec_type", "STOCK").upper()
            req_types = sub.get("req_types", ["QUOTE"])
            contract = sub.get("contract", {})

            for req_type in req_types:
                req_type = req_type.upper()
                stream_key = self._generate_stream_key(sec_type, req_type, contract)

                if stream_key not in self._client_queues:
                    self._client_queues[stream_key] = {}
                if websocket_client not in self._client_queues[stream_key]:
                    self._client_queues[stream_key][websocket_client] = asyncio.Queue()

                self.connections[websocket_client].add(stream_key)

                if stream_key not in self.subscribed_streams:
                    try:
                        req = {
                            "msg_type": "STREAM",
                            "sec_type": sec_type,
                            "req_type": req_type,
                            "add": True,
                            "id": self.next_id,
                            "contract": contract,
                        }
                        self.next_id += 1

                        logger.debug(
                            f"📤 Sending subscription request: {json.dumps(req)}"
                        )
                        await self.ws.send(json.dumps(req))
                        self.subscribed_streams.add(stream_key)
                        logger.info(f"📡 Subscribed to ThetaData: {stream_key}")
                    except Exception as e:
                        logger.error(f"❌ Failed to subscribe to {stream_key}: {e}")
                        self.connected = False
                        self.ws = None
                else:
                    logger.debug(f"ℹ️ {stream_key} already subscribed")

    async def unsubscribe(
        self, websocket_client, sec_type: str, req_types: List[str], contract: dict
    ):
        """Unsubscribe from streams."""
        sec_type = sec_type.upper()

        for req_type in req_types:
            req_type = req_type.upper()
            stream_key = self._generate_stream_key(sec_type, req_type, contract)

            if websocket_client in self.connections:
                self.connections[websocket_client].discard(stream_key)

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
                    req = {
                        "msg_type": "STREAM",
                        "sec_type": sec_type,
                        "req_type": req_type,
                        "add": False,
                        "id": self.next_id,
                        "contract": contract,
                    }
                    self.next_id += 1

                    await self.ws.send(json.dumps(req))
                    self.subscribed_streams.discard(stream_key)
                    self._client_queues.pop(stream_key, None)
                    logger.info(f"❌ Unsubscribed from ThetaData: {stream_key}")
                except Exception as e:
                    logger.error(f"❌ Failed to unsubscribe from {stream_key}: {e}")
                    self.connected = False
                    self.ws = None

    async def disconnect(self, websocket_client):
        """Clean up when a client disconnects."""
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
                    sec_type, req_type, contract = self._parse_stream_key(stream_key)

                    req = {
                        "msg_type": "STREAM",
                        "sec_type": sec_type,
                        "req_type": req_type,
                        "add": False,
                        "id": self.next_id,
                        "contract": contract,
                    }
                    self.next_id += 1

                    await self.ws.send(json.dumps(req))
                    self.subscribed_streams.discard(stream_key)
                    self._client_queues.pop(stream_key, None)
                    logger.info(
                        f"❌ Auto-unsubscribed from {stream_key} (no more clients)"
                    )
                except Exception as e:
                    logger.error(
                        f"❌ Failed to auto-unsubscribe from {stream_key}: {e}"
                    )

        logger.info("🔌 Client disconnected from ThetaData")

    async def stream_forever(self):
        """Maintain ThetaData WebSocket connection and route messages to queues."""
        while True:
            try:
                await self.connect()

                async for msg in self.ws:
                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        logger.warning("⚠️ Received non-JSON message")
                        continue

                    header = data.get("header", {})
                    req_type = header.get("type")  # "QUOTE", "TRADE", "OHLC", etc.
                    status = header.get("status")

                    # Skip connection confirmation messages
                    # if status == "CONNECTED":
                    #     logger.debug(f"✅ Stream connected confirmation: {req_type}")
                    # continue

                    # Only process QUOTE and TRADE events (ignore OHLC and others)
                    if req_type not in ("QUOTE", "TRADE"):
                        # logger.debug(f"⏭️ Skipping unsupported event type: {req_type}")
                        continue

                    contract = data.get("contract", {})
                    if not contract:
                        logger.debug(f"⚠️ No contract in message, skipping")
                        continue

                    # Skip messages that don't have actual data (connection confirmations)
                    # Data messages will have "quote" or "trade" fields
                    has_data = data.get("quote") or data.get("trade")
                    if not has_data:
                        logger.debug(
                            f"✅ Stream connected confirmation: {req_type} (no data payload)"
                        )
                        continue

                    sec_type = contract.get("security_type", "STOCK")
                    stream_key = self._generate_stream_key(sec_type, req_type, contract)

                    # Get the appropriate data field
                    if req_type == "QUOTE":
                        event_data = data.get("quote", {})
                        if event_data:
                            payload = {
                                "event_type": "QUOTE",
                                "sec_type": sec_type,
                                "symbol": contract.get("root"),
                                "contract": contract,
                                "bid": event_data.get("bid"),
                                "ask": event_data.get("ask"),
                                "bid_size": event_data.get("bid_size"),
                                "ask_size": event_data.get("ask_size"),
                                "bid_exchange": event_data.get("bid_exchange"),
                                "ask_exchange": event_data.get("ask_exchange"),
                                "ms_of_day": event_data.get("ms_of_day"),
                                "date": event_data.get("date"),
                                "timestamp": self._convert_timestamp(
                                    event_data.get("date"), event_data.get("ms_of_day")
                                ),
                            }

                    elif req_type == "TRADE":
                        event_data = data.get("trade", {})
                        if event_data:
                            payload = {
                                "event_type": "TRADE",
                                "sec_type": sec_type,
                                "symbol": contract.get("root"),
                                "contract": contract,
                                "price": event_data.get("price"),
                                "size": event_data.get("size"),
                                "exchange": event_data.get("exchange"),
                                "condition": event_data.get("condition"),
                                "sequence": event_data.get("sequence"),
                                "ms_of_day": event_data.get("ms_of_day"),
                                "date": event_data.get("date"),
                                "timestamp": self._convert_timestamp(
                                    event_data.get("date"), event_data.get("ms_of_day")
                                ),
                            }
                    else:
                        continue

                    # Put payload into the appropriate queues (fan out)
                    client_queues = self._client_queues.get(stream_key, {})
                    if client_queues and event_data:
                        logger.debug(f"👁️ sent payload: {payload}")
                        for q in client_queues.values():
                            await q.put(payload)
                        logger.debug(f"📨 Routed {req_type} for {stream_key}")

            except websockets.exceptions.ConnectionClosed:
                logger.warning(
                    "🔌 ThetaData WebSocket connection closed, reconnecting..."
                )
                self.connected = False
                self.ws = None
                self.subscribed_streams.clear()
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"❌ Error in stream_forever: {e}")
                logger.debug(traceback.format_exc())
                self.connected = False
                self.ws = None
                self.subscribed_streams.clear()
                await asyncio.sleep(5)

    def _convert_timestamp(self, date: int, ms_of_day: int) -> int:
        """Convert ThetaData date and ms_of_day to Unix timestamp in milliseconds."""
        if not date or ms_of_day is None:
            return None

        try:
            date_str = str(date)
            dt_obj = dt.datetime.strptime(date_str, "%Y%m%d")
            dt_obj = dt_obj + dt.timedelta(milliseconds=ms_of_day)
            eastern = pytz.timezone("US/Eastern")
            dt_obj = eastern.localize(dt_obj)
            return int(dt_obj.timestamp() * 1000)
        except Exception as e:
            logger.warning(
                f"⚠️ Failed to convert timestamp: date={date}, ms_of_day={ms_of_day}, error={e}"
            )
            return None
