"""
AgentBFF - News Processor Results BFF for JerryTrader

FastAPI + WebSocket server for real-time news processor results.
Listens to news_processor_results_stream and broadcasts to frontend clients.

This BFF is designed to run on a separate machine from the main BFF.

Usage:
    python -m jerry_trader.BackendForFrontend.openclaw --session-id replay_20260115_test
"""

import argparse
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, Optional

from dotenv import load_dotenv

load_dotenv()

import redis
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from jerry_trader.utils.logger import setup_logger
from jerry_trader.utils.redis_keys import news_processor_results_stream
from jerry_trader.utils.session import make_session_id

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


# ============ Pydantic Models ============


class HealthResponse(BaseModel):
    status: str
    redis: str
    connected_clients: int
    session_id: Optional[str]


# ============ WebSocket Connection Manager ============


class ConnectionManager:
    """Manages WebSocket connections."""

    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, client_id: str):
        """Accept WebSocket connection."""
        await websocket.accept()
        self.active_connections[client_id] = websocket
        logger.info(
            f"Client {client_id} connected. Total: {len(self.active_connections)}"
        )

    def disconnect(self, client_id: str):
        """Remove WebSocket connection."""
        if client_id in self.active_connections:
            del self.active_connections[client_id]
            logger.info(
                f"Client {client_id} disconnected. Total: {len(self.active_connections)}"
            )

    async def send_personal_message(self, message: dict, client_id: str):
        """Send message to specific client."""
        if client_id in self.active_connections:
            try:
                await self.active_connections[client_id].send_json(message)
            except Exception as e:
                logger.error(f"Failed to send message to {client_id}: {e}")
                self.disconnect(client_id)

    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients."""
        disconnected = []
        for client_id, websocket in self.active_connections.items():
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.error(f"Failed to broadcast to {client_id}: {e}")
                disconnected.append(client_id)

        for client_id in disconnected:
            self.disconnect(client_id)


# ============ AgentBFF ============


class AgentBFF:
    """
    Agent BFF for News Processor Results.

    Listens to Redis stream for news processor classification results
    and broadcasts them to connected WebSocket clients in real-time.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 5003,
        session_id: Optional[str] = None,
        redis_config: Optional[Dict[str, Any]] = None,
    ):
        self.host = host
        self.port = port
        self.session_id = session_id or make_session_id()

        # Parse redis config (with defaults)
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)

        # Connection manager for WebSocket
        self.manager = ConnectionManager()

        # Redis connection
        self.r = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )

        # Redis stream key
        self.NEWS_PROCESSOR_RESULTS_STREAM = news_processor_results_stream(
            self.session_id
        )

        # Background tasks
        self._running = False
        self._listener_task = None

        # Create FastAPI app with lifespan
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            logger.info("Starting AgentBFF...")
            self._running = True

            # Start background listener
            self._listener_task = asyncio.create_task(
                self._news_processor_results_listener()
            )

            yield

            # Shutdown
            logger.info("Shutting down AgentBFF...")
            self._running = False
            if self._listener_task:
                self._listener_task.cancel()
            self.cleanup()

        self.app = FastAPI(
            title="AgentBFF - News Processor Results",
            description="Backend For Frontend for news processor results",
            version="1.0.0",
            lifespan=lifespan,
        )

        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Register routes
        self._register_routes()

        logger.info(f"AgentBFF initialized for session: {self.session_id}")

    def _register_routes(self):
        """Register HTTP and WebSocket routes."""

        @self.app.get("/health", response_model=HealthResponse)
        async def health():
            """Health check endpoint."""
            try:
                self.r.ping()
                redis_status = "connected"
            except Exception as e:
                logger.error(f"Redis health check failed: {e}")
                redis_status = "disconnected"

            return HealthResponse(
                status="ok",
                redis=redis_status,
                connected_clients=len(self.manager.active_connections),
                session_id=self.session_id,
            )

        @self.app.websocket("/ws/{client_id}")
        async def websocket_endpoint(websocket: WebSocket, client_id: str):
            """WebSocket endpoint for real-time updates."""
            await self.manager.connect(websocket, client_id)

            try:
                # Send initial connection confirmation
                await websocket.send_json(
                    {
                        "type": "connection_established",
                        "client_id": client_id,
                        "session_id": self.session_id,
                        "timestamp": datetime.now().isoformat(),
                    }
                )

                # Keep connection alive and handle messages
                while True:
                    data = await websocket.receive_text()
                    message = json.loads(data)

                    # Handle ping/pong
                    if message.get("type") == "ping":
                        await websocket.send_json({"type": "pong"})
                    else:
                        logger.debug(f"Received message from {client_id}: {message}")

            except WebSocketDisconnect:
                self.manager.disconnect(client_id)
            except Exception as e:
                logger.error(f"WebSocket error for {client_id}: {e}")
                self.manager.disconnect(client_id)

    async def _news_processor_results_listener(self):
        """
        Listen to news_processor_results_stream for LLM classification results.

        Each classification result is broadcast to all connected WebSocket clients
        for the NewsRoom component to display in real-time.
        """
        logger.info(
            f"Starting news processor results listener for stream: {self.NEWS_PROCESSOR_RESULTS_STREAM}"
        )

        # Create consumer group
        try:
            self.r.xgroup_create(
                self.NEWS_PROCESSOR_RESULTS_STREAM,
                "AgentBFF_news_processor_consumers",
                id="0",
                mkstream=True,
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        consumer_name = f"AgentBFF_news_proc_{datetime.now().timestamp()}"

        while self._running:
            try:
                messages = self.r.xreadgroup(
                    "AgentBFF_news_processor_consumers",
                    consumer_name,
                    {self.NEWS_PROCESSOR_RESULTS_STREAM: ">"},
                    count=10,
                    block=1000,
                )

                if messages:
                    for stream_name, message_list in messages:
                        for message_id, message_data in message_list:
                            symbol = message_data.get("symbol")
                            if not symbol:
                                self.r.xack(
                                    self.NEWS_PROCESSOR_RESULTS_STREAM,
                                    "AgentBFF_news_processor_consumers",
                                    message_id,
                                )
                                continue

                            # Parse explanation JSON if present
                            explanation = {}
                            try:
                                explanation = json.loads(
                                    message_data.get("explanation", "{}")
                                )
                            except Exception:
                                explanation = {
                                    "raw": message_data.get("explanation", "")
                                }

                            # Build news processor result message
                            news_result = {
                                "type": "news_processor_result",
                                "model": message_data.get("model", ""),
                                "symbol": symbol,
                                "is_catalyst": message_data.get("is_catalyst")
                                == "true",
                                "classification": message_data.get(
                                    "classification", "NO"
                                ),
                                "score": message_data.get("score", "0/10"),
                                "title": message_data.get("title", ""),
                                "published_time": message_data.get(
                                    "published_time", ""
                                ),
                                "current_time": message_data.get("current_time", ""),
                                "explanation": explanation,
                                "url": message_data.get("url", ""),
                                "content_preview": message_data.get(
                                    "content_preview", ""
                                ),
                                "sources": message_data.get("sources", "[]"),
                                "source_from": message_data.get("source_from", ""),
                                "timestamp": message_id,  # Use stream ID as timestamp
                            }

                            logger.info(
                                f"Broadcasting news processor result: {symbol} - "
                                f"{'✅' if news_result['is_catalyst'] else '❌'} {news_result['score']}"
                            )

                            # Broadcast to all connected clients
                            await self.manager.broadcast(news_result)

                            # Acknowledge
                            self.r.xack(
                                self.NEWS_PROCESSOR_RESULTS_STREAM,
                                "AgentBFF_news_processor_consumers",
                                message_id,
                            )

                await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"News processor results listener error: {e}")
                await asyncio.sleep(5)

    def cleanup(self):
        """Cleanup resources."""
        logger.info("Cleaning up AgentBFF resources...")
        # Close all WebSocket connections
        for client_id in list(self.manager.active_connections.keys()):
            self.manager.disconnect(client_id)

    def run(self, debug: bool = False):
        """Run the AgentBFF server."""
        logger.info("=" * 60)
        logger.info(f"Starting AgentBFF on {self.host}:{self.port}")
        logger.info(f"Session ID: {self.session_id}")
        logger.info("=" * 60)

        uvicorn.run(
            self.app,
            host=self.host,
            port=self.port,
            log_level="debug" if debug else "info",
        )


# ============ CLI ============


def main():
    """CLI entry point for AgentBFF."""
    parser = argparse.ArgumentParser(
        description="AgentBFF - News Processor Results BFF"
    )
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=5003, help="Port to bind to")
    parser.add_argument(
        "--session-id", help="Session ID (auto-generated if not provided)"
    )
    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis database")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")

    args = parser.parse_args()

    redis_config = {
        "host": args.redis_host,
        "port": args.redis_port,
        "db": args.redis_db,
    }

    bff = AgentBFF(
        host=args.host,
        port=args.port,
        session_id=args.session_id,
        redis_config=redis_config,
    )

    bff.run(debug=args.debug)


if __name__ == "__main__":
    main()
