"""
TickDataServer - Backend for OrderTrader frontend tick data

A FastAPI + WebSocket server that serves real-time tick data to the OrderTrader
frontend. Can run standalone or as a role within backend_starter.py.

When integrated with backend_starter, shares a UnifiedTickManager instance with
FactorEngine so both consume from the same provider connection via fan-out queues.

On subscribe/unsubscribe, also publishes to the factor_tasks Redis stream so
FactorEngine can react to ticker changes (even when running on a different machine).

Usage:
    # Standalone (for development)
    uvicorn src.DataManager.tickdata_server:create_standalone_app --factory --port 8000

    # As a role in backend_starter (production)
    # Configured via config.yaml TickDataServer role
"""

import asyncio
import json
import os
from typing import Any, Dict, Optional, Set

import redis
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from DataSupply.tickDataSupply.unified_tick_manager import UnifiedTickManager
from utils.logger import setup_logger
from utils.redis_keys import factor_tasks_stream
from utils.session import make_session_id

logger = setup_logger(__name__, log_to_file=True)

load_dotenv()


class TickDataServer:
    """
    Backend for OrderTrader frontend tick data visualization.

    Serves real-time tick data via WebSocket and bridges frontend subscriptions
    to FactorEngine via Redis streams.

    Args:
        host: Bind host
        port: Bind port
        session_id: Unified session identifier
        ws_manager: Shared UnifiedTickManager instance (if None, creates its own)
        redis_config: Redis connection config dict
        manager_type: Data provider type (only used if ws_manager is None)
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8000,
        session_id: Optional[str] = None,
        ws_manager: Optional[UnifiedTickManager] = None,
        redis_config: Optional[Dict[str, Any]] = None,
        manager_type: Optional[str] = None,
    ):
        self.host = host
        self.port = port

        # Session ID for Redis stream scoping
        self.session_id = session_id or make_session_id()

        # Tick data manager - shared or own
        if ws_manager:
            self.manager = ws_manager
            self._owns_manager = False
        else:
            self.manager = UnifiedTickManager(provider=manager_type)
            self._owns_manager = True

        logger.info(
            f"🚀 TickDataServer using {self.manager.provider.upper()} data manager"
        )

        # Redis for factor_tasks stream
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)
        self.r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        self.FACTOR_TASKS_STREAM = factor_tasks_stream(self.session_id)

        # Build FastAPI app
        self.app = FastAPI(title="TickDataServer", version="1.0.0")

        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        self._setup_routes()

        logger.info(
            f"TickDataServer initialized: host={host}, port={port}, "
            f"session_id={self.session_id}, provider={self.manager.provider}, "
            f"shared_manager={not self._owns_manager}"
        )

    def _setup_routes(self):
        """Setup FastAPI routes and WebSocket endpoints."""

        manager = self.manager
        r = self.r
        FACTOR_TASKS_STREAM = self.FACTOR_TASKS_STREAM

        # --- Startup event (only start stream_forever if we own the manager) ---
        if self._owns_manager:

            @self.app.on_event("startup")
            async def startup_event():
                asyncio.create_task(manager.stream_forever())

        # --- Debug endpoints ---

        @self.app.get("/debug/status")
        async def debug_status():
            """Check all connected clients status."""
            all_client_symbols = {}
            for client, stream_keys in manager.connections.items():
                client_id = (
                    f"client_{id(client)}"
                    if hasattr(client, "__hash__")
                    else str(client)
                )
                all_client_symbols[client_id] = list(stream_keys)

            return {
                "connected": manager.connected,
                "subscribed_streams": list(manager.subscribed_streams),
                "active_connections": len(manager.connections),
                "client_subscriptions": all_client_symbols,
                "queue_lengths": {
                    stream: queue.qsize() for stream, queue in manager.queues.items()
                },
                "session_id": self.session_id,
                "shared_manager": not self._owns_manager,
            }

        @self.app.get("/debug/latest/{symbol}")
        async def get_latest_quote(symbol: str, event: str = "Q"):
            """Get symbol latest event data (default Q=quote). Use ?event=T for trades."""
            symbol = symbol.upper()
            stream_key = f"{event}.{symbol}"
            queue = manager.queues.get(stream_key)

            if not queue:
                return {"error": f"Stream {stream_key} not subscribed"}

            if queue.empty():
                return {"message": f"No data available for {stream_key}"}

            try:
                latest_data = None
                while not queue.empty():
                    latest_data = await asyncio.wait_for(queue.get(), timeout=0.1)
                return latest_data or {"message": "No data"}
            except asyncio.TimeoutError:
                return {"message": "No recent data"}

        @self.app.get("/debug", response_class=HTMLResponse)
        async def debug_page():
            """Debug Page."""
            return _DEBUG_HTML

        # --- Main WebSocket endpoint ---

        @self.app.websocket("/ws/tickdata")
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            consumer_tasks = {}
            current_streams: Set[str] = set()

            try:
                while True:
                    msg = await websocket.receive_text()
                    try:
                        data = json.loads(msg)

                        # ==================================================
                        # UNSUBSCRIBE HANDLING
                        # ==================================================
                        if data.get("action") == "unsubscribe":
                            subscriptions = data.get("subscriptions", [])

                            # Backward compatibility: single symbol unsubscribe
                            if not subscriptions and "symbol" in data:
                                subscriptions = [
                                    {
                                        "symbol": data.get("symbol"),
                                        "events": data.get("events", ["Q"]),
                                        "sec_type": data.get("sec_type", "STOCK"),
                                        "contract": data.get("contract", {}),
                                    }
                                ]

                            logger.info(f"📤 Unsubscribe request: {subscriptions}")

                            # Unsubscribe using unified interface
                            await manager.unsubscribe(
                                websocket, subscriptions=subscriptions
                            )

                            # Cancel consumer tasks for unsubscribed streams
                            unsubscribed_keys = manager.generate_stream_keys(
                                subscriptions=subscriptions
                            )
                            for stream_key in unsubscribed_keys:
                                if stream_key in current_streams:
                                    consumer_tasks[stream_key].cancel()
                                    del consumer_tasks[stream_key]
                                    current_streams.discard(stream_key)

                            # Notify FactorEngine via Redis stream
                            for sub in subscriptions:
                                sym = sub.get("symbol", "").upper()
                                if sym:
                                    try:
                                        r.xadd(
                                            FACTOR_TASKS_STREAM,
                                            {"action": "remove", "ticker": sym},
                                        )
                                        logger.debug(
                                            f"📤 XADD factor_tasks: remove {sym}"
                                        )
                                    except Exception as e:
                                        logger.warning(
                                            f"⚠️ Failed to XADD remove {sym}: {e}"
                                        )

                            continue

                        # ==================================================
                        # SUBSCRIBE HANDLING
                        # ==================================================
                        subscriptions = data.get("subscriptions", [])

                        # Backward compatibility: legacy polygon format
                        if not subscriptions:
                            symbols = data.get("symbols", [])
                            events = data.get("events", ["Q"])
                            if isinstance(symbols, str):
                                symbols = [s.strip() for s in symbols.split(",")]

                            subscriptions = [
                                {"symbol": sym, "events": events, "sec_type": "STOCK"}
                                for sym in symbols
                                if sym
                            ]

                        if not subscriptions:
                            logger.warning("⚠️ No subscriptions found in message")
                            continue

                        logger.info(f"📥 Subscribe request: {subscriptions}")

                        # Subscribe using unified interface
                        await manager.subscribe(websocket, subscriptions=subscriptions)

                        # Generate stream keys and create consumer tasks
                        new_streams = manager.generate_stream_keys(
                            subscriptions=subscriptions
                        )
                        to_add = new_streams - current_streams

                        for sk in to_add:
                            task = asyncio.create_task(
                                _consume_stream(websocket, sk, manager)
                            )
                            consumer_tasks[sk] = task
                            logger.debug(f"📡 Created consumer task for {sk}")

                        current_streams.update(to_add)

                        # Notify FactorEngine via Redis stream
                        for sub in subscriptions:
                            sym = sub.get("symbol", "").upper()
                            if sym:
                                try:
                                    r.xadd(
                                        FACTOR_TASKS_STREAM,
                                        {"action": "add", "ticker": sym},
                                    )
                                    logger.debug(f"📥 XADD factor_tasks: add {sym}")
                                except Exception as e:
                                    logger.warning(f"⚠️ Failed to XADD add {sym}: {e}")

                    except json.JSONDecodeError as e:
                        logger.error(f"⚠️ JSON decode error: {e}")

            except WebSocketDisconnect:
                logger.info("🔌 WebSocket disconnected")
                await manager.disconnect(websocket)
                for task in consumer_tasks.values():
                    task.cancel()

    def run(self, debug: bool = False):
        """Run the TickDataServer (blocking). Used by backend_starter."""
        import uvicorn

        logger.info("=" * 60)
        logger.info(f"Starting TickDataServer on {self.host}:{self.port}")
        logger.info("=" * 60)

        uvicorn.run(
            self.app,
            host=self.host,
            port=self.port,
            log_level="debug" if debug else "info",
        )

    def cleanup(self):
        """Clean up resources."""
        logger.info("TickDataServer resources cleaned up")


async def _consume_stream(
    websocket: WebSocket, stream_key: str, manager: UnifiedTickManager
):
    """Consume a specific stream_key queue (per-client) and push to frontend."""
    q = manager.get_client_queue(websocket, stream_key)
    if q is None:
        logger.warning(f"⚠️ No queue found for stream_key: {stream_key}")
        return

    logger.debug(f"🎧 Consumer started for {stream_key}")

    try:
        while True:
            data = await q.get()

            # Normalize data format for frontend
            normalized_data = manager.normalize_data(data)

            await websocket.send_json(normalized_data)
    except asyncio.CancelledError:
        logger.debug(f"🛑 Consumer cancelled for {stream_key}")
    except Exception as e:
        logger.error(f"❌ Error in consumer for {stream_key}: {e}")


# =============================================================================
# Debug HTML page
# =============================================================================
_DEBUG_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>TickData WebSocket Debug</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .container { max-width: 800px; }
        .section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; }
        button { margin: 5px; padding: 8px 16px; cursor: pointer; }
        #output { background: #f5f5f5; padding: 10px; height: 200px; overflow-y: auto; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔍 TickData WebSocket Debug</h1>
        <div class="section">
            <h3>📊 Status</h3>
            <button onclick="checkStatus()">Check Status</button>
            <div id="status"></div>
        </div>
        <div class="section">
            <h3>📋 Output</h3>
            <div id="output"></div>
            <button onclick="clearOutput()">Clear</button>
        </div>
    </div>
    <script>
        function log(message) {
            const output = document.getElementById('output');
            output.innerHTML += '<div>' + new Date().toLocaleTimeString() + ': ' + message + '</div>';
            output.scrollTop = output.scrollHeight;
        }
        async function checkStatus() {
            try {
                const response = await fetch('/debug/status');
                const data = await response.json();
                document.getElementById('status').innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
                log('✅ Status checked');
            } catch (error) {
                log('❌ Error checking status: ' + error);
            }
        }
        function clearOutput() {
            document.getElementById('output').innerHTML = '';
        }
    </script>
</body>
</html>
"""


# =============================================================================
# Standalone factory (for development / uvicorn --factory)
# =============================================================================


def create_standalone_app() -> FastAPI:
    """
    Factory function for running TickDataServer standalone with uvicorn.

    Usage:
        uvicorn src.DataManager.tickdata_server:create_standalone_app --factory --port 8000
    """
    session_id = make_session_id(
        replay_date=os.getenv("REPLAY_DATE"),
        suffix_id=os.getenv("SUFFIX_ID"),
    )
    server = TickDataServer(session_id=session_id)

    # For standalone mode, start stream_forever on startup
    @server.app.on_event("startup")
    async def startup():
        asyncio.create_task(server.manager.stream_forever())

    return server.app
