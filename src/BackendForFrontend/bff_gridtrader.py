"""
BFF (Backend For Frontend) for GridTrader

FastAPI + WebSocket server for real-time visualization connecting React frontend
with Python backend services via Redis streams.

This version is designed for the GridTrader frontend which uses:
- RankList: Column-based data display with multiple columns
- OverviewChartModule: Segmented line chart with state-colored segments

Usage:
    python -m src.BackendForFrontend.bff_gridtrader --replay-date 20260115 --suffix-id test

Note: Run backendStarter.py first to start the backend services.
"""

import argparse
import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from threading import Thread
from typing import Any, Dict, List, Optional, Set

import redis
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from ChartdataManager.overviewchartdataManager import GridTraderChartDataManager
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


# ============ Pydantic Models ============


class HealthResponse(BaseModel):
    status: str
    redis: str
    connected_clients: int
    replay_date: Optional[str]
    suffix_id: Optional[str]
    subscriptions: Dict[str, int]


class RankListResponse(BaseModel):
    data: List[Dict[str, Any]]
    timestamp: str


class StockDetailRequest(BaseModel):
    ticker: str


class TickerRequest(BaseModel):
    ticker: str


# ============ WebSocket Connection Manager ============


class ConnectionManager:
    """Manages WebSocket connections and subscriptions."""

    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.subscriptions: Dict[str, Set[str]] = {
            "market_snapshot": set(),
            "stock_detail": set(),
            "orders": set(),
            "portfolio": set(),
        }

    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        self.active_connections[client_id] = websocket
        logger.info(f"Client connected: {client_id}")

    def disconnect(self, client_id: str):
        self.active_connections.pop(client_id, None)
        for domain in self.subscriptions:
            self.subscriptions[domain].discard(client_id)
        logger.info(f"Client disconnected: {client_id}")

    def subscribe(self, client_id: str, domain: str):
        if domain in self.subscriptions:
            self.subscriptions[domain].add(client_id)
            logger.debug(f"Client {client_id} subscribed to {domain}")

    def unsubscribe(self, client_id: str, domain: str):
        if domain in self.subscriptions:
            self.subscriptions[domain].discard(client_id)
            logger.debug(f"Client {client_id} unsubscribed from {domain}")

    async def send_personal_message(self, message: dict, client_id: str):
        if client_id in self.active_connections:
            try:
                await self.active_connections[client_id].send_json(message)
            except Exception as e:
                logger.error(f"Error sending to {client_id}: {e}")

    async def broadcast_to_subscribers(self, message: dict, domain: str):
        """Send message to all clients subscribed to a domain."""
        for client_id in self.subscriptions.get(domain, set()).copy():
            await self.send_personal_message(message, client_id)

    def get_stats(self) -> Dict[str, int]:
        return {k: len(v) for k, v in self.subscriptions.items()}


# ============ GridTrader BFF Class ============


class GridTraderBFF:
    """
    Backend For Frontend for GridTrader React application using FastAPI.

    Connects to backend services via Redis streams:
    - market_snapshot_processed:{date} - For rank list and chart updates
    - movers_state:{date} - For state change notifications

    WebSocket message types:
    - rank_list_update: Updates to the RankList component
    - overview_chart_update: Updates to the OverviewChartModule
    - state_change: Real-time state transition notifications
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5001,
        replay_date: Optional[str] = None,
        suffix_id: Optional[str] = None,
        use_callback: bool = False,
    ):
        self.host = host
        self.port = port
        self.replay_date = replay_date
        self.suffix_id = suffix_id
        self.use_callback = use_callback

        # Connection manager for WebSocket
        self.manager = ConnectionManager()

        # Redis connection
        self.r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

        # Determine date suffix for stream names
        if replay_date:
            self.date_suffix = replay_date
        else:
            self.date_suffix = datetime.now().strftime("%Y%m%d")

        self.SNAPSHOT_STREAM_NAME = f"market_snapshot_processed:{self.date_suffix}"
        self.STATE_STREAM_NAME = f"movers_state:{self.date_suffix}"

        # Initialize chart data manager
        self.chart_manager = GridTraderChartDataManager(
            replay_date=replay_date,
            suffix_id=suffix_id,
        )

        # Background tasks
        self._running = False
        self._snapshot_task = None
        self._state_task = None

        # Create FastAPI app with lifespan
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            logger.info("Starting GridTrader BFF...")
            self._running = True

            # Start background listeners
            if not self.use_callback:
                self._snapshot_task = asyncio.create_task(
                    self._snapshot_stream_listener()
                )
            self._state_task = asyncio.create_task(self._state_stream_listener())

            yield

            # Shutdown
            logger.info("Shutting down GridTrader BFF...")
            self._running = False
            if self._snapshot_task:
                self._snapshot_task.cancel()
            if self._state_task:
                self._state_task.cancel()
            self.cleanup()

        self.app = FastAPI(
            title="GridTrader BFF",
            description="Backend For Frontend for GridTrader trading application",
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

        # Setup routes
        self._setup_routes()

        logger.info(
            f"GridTraderBFF initialized: host={host}, port={port}, "
            f"replay_date={replay_date}, suffix_id={suffix_id}"
        )

    def _setup_routes(self):
        """Setup FastAPI routes."""

        @self.app.get("/")
        async def index():
            return {
                "service": "GridTrader BFF",
                "status": "running",
                "version": "1.0.0",
            }

        @self.app.get("/health", response_model=HealthResponse)
        async def health():
            """Health check endpoint."""
            try:
                self.r.ping()
                redis_status = "connected"
            except Exception:
                redis_status = "disconnected"

            return HealthResponse(
                status="ok",
                redis=redis_status,
                connected_clients=len(self.manager.active_connections),
                replay_date=self.replay_date,
                suffix_id=self.suffix_id,
                subscriptions=self.manager.get_stats(),
            )

        @self.app.get("/api/rank-list")
        async def get_rank_list():
            """API endpoint to get current rank list data."""
            rank_data, snapshot_timestamp = self.chart_manager.get_latest_rank_data()
            return {
                "data": rank_data,
                "timestamp": snapshot_timestamp,
            }

        @self.app.get("/api/overview-chart")
        async def get_overview_chart():
            """API endpoint to get overview chart data."""
            chart_data = self.chart_manager.get_overview_chart_data()
            return chart_data

        @self.app.get("/api/stock/{ticker}")
        async def get_stock_detail(ticker: str):
            """API endpoint to get detailed stock information."""
            history = self.chart_manager.get_ticker_history(ticker, limit=100)
            state_history = self.chart_manager.get_ticker_state_history(ticker)

            if history or state_history:
                return {
                    "ticker": ticker,
                    "history": [
                        {
                            "timestamp": h["timestamp"].isoformat(),
                            "changePercent": h["changePercent"],
                            "price": h["price"],
                            "rank": h["rank"],
                        }
                        for h in history
                    ],
                    "stateHistory": [
                        {
                            "timestamp": sh["timestamp"].isoformat(),
                            "state": sh["state"],
                        }
                        for sh in state_history
                    ],
                }
            else:
                return {"error": "Stock not found"}

        @self.app.get("/api/subscribed")
        async def get_subscribed_tickers():
            """API endpoint to get all subscribed tickers."""
            tickers = self.chart_manager.get_subscribed_tickers()
            return {"tickers": tickers, "count": len(tickers)}

        @self.app.get("/api/test-data")
        async def test_data():
            """Test endpoint to check current data."""
            chart_data = self.chart_manager.get_overview_chart_data()
            rank_data = self.chart_manager.get_latest_rank_data()
            subscribed = self.chart_manager.get_subscribed_tickers()
            return {
                "data_points_count": len(chart_data.get("data", [])),
                "tickers_count": len(chart_data.get("segmentInfo", {})),
                "rank_list_count": len(rank_data),
                "subscribed_count": len(subscribed),
            }

        # ============ WebSocket Endpoint ============

        @self.app.websocket("/ws/{client_id}")
        async def websocket_endpoint(websocket: WebSocket, client_id: str):
            await self.manager.connect(websocket, client_id)

            # Auto-subscribe to market snapshot and send initial data
            self.manager.subscribe(client_id, "market_snapshot")
            await self._send_initial_data(client_id)

            try:
                while True:
                    data = await websocket.receive_json()
                    await self._handle_websocket_message(client_id, data)
            except WebSocketDisconnect:
                self.manager.disconnect(client_id)
            except Exception as e:
                logger.error(f"WebSocket error for {client_id}: {e}")
                self.manager.disconnect(client_id)

    async def _handle_websocket_message(self, client_id: str, data: dict):
        """Handle incoming WebSocket messages."""
        msg_type = data.get("type", "")
        payload = data.get("payload", {})

        if msg_type == "subscribe_market_snapshot":
            self.manager.subscribe(client_id, "market_snapshot")
            await self._emit_rank_list_update(client_id)
            await self._emit_overview_chart_update(client_id)

        elif msg_type == "unsubscribe_market_snapshot":
            self.manager.unsubscribe(client_id, "market_snapshot")

        elif msg_type == "request_stock_detail":
            ticker = payload.get("ticker")
            if ticker:
                await self._send_stock_detail(client_id, ticker)
            else:
                await self.manager.send_personal_message(
                    {"type": "error", "message": "No ticker specified"}, client_id
                )

        elif msg_type == "request_news":
            ticker = payload.get("ticker")
            await self.manager.send_personal_message(
                {
                    "type": "news_result",
                    "ticker": ticker,
                    "news": [],
                    "timestamp": datetime.now().isoformat(),
                },
                client_id,
            )

        elif msg_type == "request_fundamental":
            ticker = payload.get("ticker")
            await self.manager.send_personal_message(
                {
                    "type": "fundamental_result",
                    "ticker": ticker,
                    "fundamentals": {},
                    "timestamp": datetime.now().isoformat(),
                },
                client_id,
            )

        elif msg_type == "refresh_chart":
            chart_data = self.chart_manager.get_overview_chart_data(force_refresh=True)
            await self.manager.send_personal_message(
                {"type": "overview_chart_update", **chart_data}, client_id
            )

        elif msg_type == "refresh_rank_list":
            rank_data, snapshot_timestamp = self.chart_manager.get_latest_rank_data()
            await self.manager.send_personal_message(
                {
                    "type": "rank_list_update",
                    "data": rank_data,
                    "timestamp": snapshot_timestamp,
                },
                client_id,
            )

    async def _send_initial_data(self, client_id: str):
        """Send initial data to a newly connected client."""
        await self._emit_rank_list_update(client_id)
        await self._emit_overview_chart_update(client_id)

    async def _send_stock_detail(self, client_id: str, ticker: str):
        """Send stock detail to a client."""
        history = self.chart_manager.get_ticker_history(ticker, limit=100)
        state_history = self.chart_manager.get_ticker_state_history(ticker)

        if history or state_history:
            await self.manager.send_personal_message(
                {
                    "type": "stock_detail",
                    "ticker": ticker,
                    "history": [
                        {
                            "timestamp": h["timestamp"].isoformat(),
                            "changePercent": h["changePercent"],
                            "price": h["price"],
                        }
                        for h in history[-50:]
                    ],
                    "stateHistory": [
                        {
                            "timestamp": sh["timestamp"].isoformat(),
                            "state": sh["state"],
                        }
                        for sh in state_history
                    ],
                },
                client_id,
            )
        else:
            await self.manager.send_personal_message(
                {"type": "error", "message": f"Stock {ticker} not found"}, client_id
            )

    async def _emit_rank_list_update(self, client_id: Optional[str] = None):
        """Emit rank list update to specific client or all subscribed clients."""
        rank_data, snapshot_timestamp = self.chart_manager.get_latest_rank_data()
        payload = {
            "type": "rank_list_update",
            "data": rank_data,
            "timestamp": snapshot_timestamp,  # Use actual snapshot timestamp, not machine time
        }

        if client_id:
            await self.manager.send_personal_message(payload, client_id)
        else:
            await self.manager.broadcast_to_subscribers(payload, "market_snapshot")

    async def _emit_overview_chart_update(self, client_id: Optional[str] = None):
        """Emit overview chart update to specific client or all subscribed clients."""
        chart_data = self.chart_manager.get_overview_chart_data()
        payload = {"type": "overview_chart_update", **chart_data}

        if client_id:
            await self.manager.send_personal_message(payload, client_id)
        else:
            await self.manager.broadcast_to_subscribers(payload, "market_snapshot")

    async def _snapshot_stream_listener(self):
        """Listen to processed snapshot stream for chart updates (async)."""
        logger.info("Starting async snapshot stream listener...")

        # Create consumer group for BFF
        try:
            self.r.xgroup_create(
                self.SNAPSHOT_STREAM_NAME,
                "gridtrader_bff_consumers",
                id="0",
                mkstream=True,
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        consumer_name = f"gridtrader_bff_{datetime.now().timestamp()}"

        while self._running:
            try:
                # Use blocking read with timeout
                messages = self.r.xreadgroup(
                    "gridtrader_bff_consumers",
                    consumer_name,
                    {self.SNAPSHOT_STREAM_NAME: ">"},
                    count=1,
                    block=2000,
                )

                if messages:
                    for stream_name, message_list in messages:
                        for message_id, message_data in message_list:
                            # Refresh chart data
                            self.chart_manager.mark_dirty()

                            # Only emit if there are subscribed clients
                            if self.manager.subscriptions["market_snapshot"]:
                                timestamp = message_data.get("timestamp", "unknown")
                                logger.debug(f"Snapshot update: timestamp={timestamp}")

                                # Emit to all subscribed clients
                                await self._emit_rank_list_update()
                                await self._emit_overview_chart_update()

                            # Acknowledge the message
                            self.r.xack(
                                self.SNAPSHOT_STREAM_NAME,
                                "gridtrader_bff_consumers",
                                message_id,
                            )

                # Allow other tasks to run
                await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Snapshot stream listener error: {e}")
                await asyncio.sleep(5)

    async def _state_stream_listener(self):
        """Listen to state stream and broadcast state changes (async)."""
        logger.info("Starting async state stream listener...")

        # Create consumer group for BFF
        try:
            self.r.xgroup_create(
                self.STATE_STREAM_NAME,
                "gridtrader_bff_state_consumers",
                id="0",
                mkstream=True,
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        consumer_name = f"gridtrader_state_{datetime.now().timestamp()}"

        while self._running:
            try:
                messages = self.r.xreadgroup(
                    "gridtrader_bff_state_consumers",
                    consumer_name,
                    {self.STATE_STREAM_NAME: ">"},
                    count=10,
                    block=1000,
                )

                if messages:
                    for stream_name, message_list in messages:
                        for message_id, message_data in message_list:
                            # Map backend state to frontend state
                            from_state = message_data.get("from", "stable")
                            to_state = message_data.get("to", "stable")

                            state_mapping = self.chart_manager.STATE_MAPPING
                            frontend_from = state_mapping.get(from_state, "OnWatch")
                            frontend_to = state_mapping.get(to_state, "OnWatch")

                            state_change = {
                                "type": "state_change",
                                "symbol": message_data.get("symbol"),
                                "from": frontend_from,
                                "to": frontend_to,
                                "timestamp": message_data.get("timestamp"),
                            }

                            logger.info(
                                f"State change: {state_change['symbol']} "
                                f"{frontend_from} -> {frontend_to}"
                            )

                            # Broadcast to all connected clients
                            for client_id in self.manager.active_connections:
                                await self.manager.send_personal_message(
                                    state_change, client_id
                                )

                            # Acknowledge the message
                            self.r.xack(
                                self.STATE_STREAM_NAME,
                                "gridtrader_bff_state_consumers",
                                message_id,
                            )

                # Allow other tasks to run
                await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"State stream listener error: {e}")
                await asyncio.sleep(5)

    def run(self, debug: bool = False):
        """Run the GridTrader BFF server."""
        logger.info("=" * 60)
        logger.info(f"Starting GridTrader BFF on {self.host}:{self.port}")
        logger.info("=" * 60)

        if self.replay_date:
            logger.info(f"Mode: REPLAY (date={self.replay_date}, id={self.suffix_id})")
        else:
            logger.info("Mode: LIVE")

        logger.info(f"API available at http://{self.host}:{self.port}")
        logger.info(
            f"WebSocket available at ws://{self.host}:{self.port}/ws/{{client_id}}"
        )
        logger.info("=" * 60)

        # Run uvicorn server
        uvicorn.run(
            self.app,
            host=self.host,
            port=self.port,
            log_level="debug" if debug else "info",
        )

    def cleanup(self):
        """Clean up resources on shutdown."""
        if self.chart_manager:
            self.chart_manager.close()
        logger.info("GridTrader BFF resources cleaned up")

    # ============ Callback Methods for Integration ============

    async def on_snapshot_processed_async(self, result: dict, is_historical: bool):
        """
        Async callback invoked after each snapshot is processed.
        Used when running with backendStarter in callback mode.
        """
        self.chart_manager.mark_dirty()

        if self.manager.subscriptions["market_snapshot"]:
            logger.debug("on_snapshot_processed_async: emitting updates")
            await self._emit_rank_list_update()
            await self._emit_overview_chart_update()

    def on_snapshot_processed(self, result: dict, is_historical: bool):
        """
        Sync callback wrapper for compatibility with backendStarter.
        Creates an event loop if needed to run the async callback.
        """
        self.chart_manager.mark_dirty()

        # For sync context, we can't easily broadcast to websockets
        # This is mainly used to mark data dirty; actual updates happen via stream listener
        logger.debug(f"on_snapshot_processed (sync): marking data dirty")


def main():
    """Main entry point for GridTrader BFF."""
    parser = argparse.ArgumentParser(
        description="GridTrader BFF (Backend For Frontend)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Live mode
    python -m src.BackendForFrontend.bff_gridtrader

    # Replay mode
    python -m src.BackendForFrontend.bff_gridtrader --replay-date 20260115 --suffix-id test

    # Custom host/port
    python -m src.BackendForFrontend.bff_gridtrader --host 0.0.0.0 --port 8080
        """,
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Host to bind to (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5001,
        help="Port to bind to (default: 5001)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )
    parser.add_argument(
        "--replay-date",
        help="Receive specific replay date data (YYYYMMDD)",
    )
    parser.add_argument(
        "--suffix-id",
        help="Custom replay identifier for InfluxDB tagging",
    )

    args = parser.parse_args()

    # Create and run BFF
    bff = GridTraderBFF(
        host=args.host,
        port=args.port,
        replay_date=args.replay_date,
        suffix_id=args.suffix_id,
    )

    try:
        bff.run(debug=args.debug)
    except Exception as e:
        logger.error(f"GridTrader BFF error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        bff.cleanup()


if __name__ == "__main__":
    main()
