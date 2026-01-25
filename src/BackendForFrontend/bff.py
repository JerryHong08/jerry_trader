"""
BFF (Backend For Frontend) for GridTrader

FastAPI + WebSocket server for real-time visualization connecting React frontend
with Python backend services via Redis streams.

This version is designed for the GridTrader frontend which uses:
- RankList: Column-based data display with multiple columns
- OverviewChartModule: Segmented line chart with state-colored segments

Usage:
    python -m src.BackendForFrontend.bff --replay-date 20260115 --suffix-id test

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

from DataManager.overviewchartdataManager import GridTraderChartDataManager
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
        self.STATIC_UPDATE_STREAM = "static_update_stream"
        self.STATIC_SUMMARY_PREFIX = "static:ticker:summary"
        self.STATIC_PROFILE_PREFIX = "static:ticker:profile"
        self.NEWS_TICKER_PREFIX = "news:ticker"
        self.NEWS_ITEM_PREFIX = "news:item"

        # Pending sets for static data worker
        self.STATIC_PENDING_SET = "static:pending"
        self.NEWS_PENDING_SET = "static:pending:news"  # News-only refetch

        # Version tracking for static updates (symbol -> domain -> last_applied_version)
        # Used to ignore stale updates with version <= last_applied_version
        self._applied_versions: Dict[str, Dict[str, int]] = {}
        self.VERSION_KEY_PREFIX = "static:version"  # Must match static_data_worker

        # Initialize chart data manager
        self.chart_manager = GridTraderChartDataManager(
            replay_date=replay_date,
            suffix_id=suffix_id,
        )

        # Chart settings (persistent for session)
        self.chart_top_n = 20  # Default top N tickers to send

        # Background tasks
        self._running = False
        self._snapshot_task = None
        self._state_task = None
        self._static_task = None

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
            self._static_task = asyncio.create_task(self._static_stream_listener())

            yield

            # Shutdown
            logger.info("Shutting down GridTrader BFF...")
            self._running = False
            if self._snapshot_task:
                self._snapshot_task.cancel()
            if self._state_task:
                self._state_task.cancel()
            if self._static_task:
                self._static_task.cancel()
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
            chart_data = self.chart_manager.get_overview_chart_data_lw(
                top_n=self.chart_top_n
            )
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

        @self.app.get("/api/stock/{ticker}/profile")
        async def get_stock_profile(ticker: str):
            """API endpoint to get stock profile (static data).

            Pull-based endpoint for StockDetail page.
            Reads from static:ticker:profile:{symbol} HASH.
            If not found, queues ticker for static data fetch.
            """
            ticker_upper = ticker.upper()
            profile_key = f"{self.STATIC_PROFILE_PREFIX}:{ticker_upper}"
            profile = self.r.hgetall(profile_key)
            logger.debug(f"Fetched profile for {ticker}: {profile}")

            if profile:
                # Parse numeric fields
                for field in [
                    "marketCap",
                    "float",
                    "averageVolume",
                    "fullTimeEmployees",
                ]:
                    if field in profile:
                        try:
                            profile[field] = float(profile[field])
                        except (ValueError, TypeError):
                            pass
                return {
                    "ticker": ticker_upper,
                    "profile": profile,
                    "timestamp": datetime.now().isoformat(),
                }
            else:
                # Queue for static data fetch (will be processed by StaticDataWorker)
                self.r.sadd("static:pending", ticker_upper)
                logger.info(
                    f"Profile not found for {ticker_upper}, queued for static fetch"
                )
                return {
                    "error": f"Profile not found for {ticker_upper}",
                    "ticker": ticker_upper,
                    "queued": True,  # Indicates client can retry later
                }

        @self.app.get("/api/stock/{ticker}/news")
        async def get_stock_news(ticker: str, limit: int = 10, refresh: bool = False):
            """API endpoint to get stock news.

            Pull-based endpoint for StockDetail page.
            Reads from news:ticker:{symbol} ZSET + news:item:{id} HASH.

            Args:
                ticker: Stock ticker symbol
                limit: Maximum number of news articles to return
                refresh: If True, queue ticker for news-only refetch

            If no news found or refresh=True, queues ticker for news fetch.
            """
            ticker_upper = ticker.upper()
            ticker_key = f"{self.NEWS_TICKER_PREFIX}:{ticker_upper}"

            # If refresh requested, queue for news-only refetch
            if refresh:
                self.r.sadd(self.NEWS_PENDING_SET, ticker_upper)
                logger.info(
                    f"News refresh requested for {ticker_upper}, queued for news fetch"
                )

            # Get news IDs sorted by timestamp (most recent first)
            news_ids = self.r.zrevrange(ticker_key, 0, limit - 1)

            articles = []
            for news_id in news_ids:
                item_key = f"{self.NEWS_ITEM_PREFIX}:{news_id}"
                article = self.r.hgetall(item_key)
                if article:
                    articles.append(article)

            # If no news found, queue for news-only fetch (not full static)
            if not articles and not refresh:
                self.r.sadd(self.NEWS_PENDING_SET, ticker_upper)
                logger.info(f"News not found for {ticker_upper}, queued for news fetch")

            return {
                "ticker": ticker_upper,
                "news": articles,
                "count": len(articles),
                "queued": refresh
                or len(articles) == 0,  # Indicates if fetch is pending
                "timestamp": datetime.now().isoformat(),
            }

        @self.app.get("/api/subscribed")
        async def get_subscribed_tickers():
            """API endpoint to get all subscribed tickers."""
            tickers = self.chart_manager.get_subscribed_tickers()
            return {"tickers": tickers, "count": len(tickers)}

        @self.app.get("/api/test-data")
        async def test_data():
            """Test endpoint to check current data."""
            chart_data = self.chart_manager.get_overview_chart_data_lw(
                top_n=self.chart_top_n
            )
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
            top_n = payload.get("top_n")
            if top_n is not None:
                self.chart_top_n = top_n  # Store for future requests
            chart_data = self.chart_manager.get_overview_chart_data_lw(
                force_refresh=True,
                top_n=self.chart_top_n,
            )
            await self.manager.send_personal_message(
                {"type": "overview_chart_update", **chart_data}, client_id
            )

        elif msg_type == "set_top_n":
            # Persist top_n setting and broadcast new chart data to all subscribers
            top_n = payload.get("top_n", 20)
            self.chart_top_n = top_n
            logger.info(f"Chart top_n set to {top_n}")
            # Broadcast updated chart data to all subscribers
            await self._emit_overview_chart_update()

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

    def _get_static_summaries(self, symbols: List[str]) -> Dict[str, Dict]:
        """Batch fetch static summaries for multiple symbols.

        Returns dict mapping symbol -> summary data.
        Missing symbols will have empty dict as value.
        """
        if not symbols:
            return {}

        # Use pipeline for efficient batch fetch
        pipe = self.r.pipeline()
        for symbol in symbols:
            key = f"{self.STATIC_SUMMARY_PREFIX}:{symbol}"
            pipe.hgetall(key)

        results = pipe.execute()

        summaries = {}
        for i, symbol in enumerate(symbols):
            summary = results[i] if results[i] else {}
            # Parse numeric fields
            parsed = {}
            for field, value in summary.items():
                if field in ["marketCap", "float"]:
                    try:
                        parsed[field] = float(value)
                    except (ValueError, TypeError):
                        parsed[field] = None
                elif field == "hasNews":
                    parsed[field] = value == "1"
                else:
                    parsed[field] = value
            summaries[symbol] = parsed

        return summaries

    async def _emit_rank_list_update(self, client_id: Optional[str] = None):
        """Emit rank list update to specific client or all subscribed clients.

        Pure snapshot data only. Static data (marketCap, float, hasNews) is sent
        separately via static_update messages from _static_stream_listener.
        """
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
        """Emit overview chart update to specific client or all subscribed clients.

        Uses Lightweight Charts format for optimal frontend performance.
        """
        chart_data = self.chart_manager.get_overview_chart_data_lw(
            top_n=self.chart_top_n
        )
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

                            # Note: REENTER_TOP20 events are handled directly by SnapshotProcessor
                            # STATE_BAD events are handled directly by StateEngine
                            # Both write to the same Redis Set. Frontend syncs on connect.

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
                            from_state = message_data.get("from", "OnWatch")
                            to_state = message_data.get("to", "OnWatch")
                            state_reason = message_data.get("stateReason", "")

                            state_change = {
                                "type": "state_change",
                                "symbol": message_data.get("symbol"),
                                "from": from_state,
                                "to": to_state,
                                "stateReason": state_reason,
                                "timestamp": message_data.get("ts"),
                            }

                            logger.info(
                                f"State change: {state_change['symbol']} "
                                f"{from_state} -> {to_state}"
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

    async def _static_stream_listener(self):
        """Listen to static update stream and broadcast static data patches (async).

        Static updates are low-frequency, patch-only updates for:
        - Summary: marketCap, float, country, sector, hasNews (for RankList)
        - Profile: full company profile (for StockDetail caching)
        - News: news articles (for StockDetail news cache)

        Stream message schema (v2 - versioned):
        {
            "symbol": "XPON",
            "update_type": "static",
            "domains": ["summary", "profile", "news"],
            "version": {"summary": 3, "profile": 2, "news": 12},
            "timestamp": "2026-01-25T10:30:00-05:00"
        }

        Version rules:
        - BFF ignores updates with version <= last_applied_version for each domain
        - Frontend receives version alongside data to cache with version
        """
        logger.info("Starting async static stream listener...")

        # Create consumer group for BFF
        try:
            self.r.xgroup_create(
                self.STATIC_UPDATE_STREAM,
                "gridtrader_bff_static_consumers",
                id="0",
                mkstream=True,
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        consumer_name = f"gridtrader_static_{datetime.now().timestamp()}"

        while self._running:
            try:
                messages = self.r.xreadgroup(
                    "gridtrader_bff_static_consumers",
                    consumer_name,
                    {self.STATIC_UPDATE_STREAM: ">"},
                    count=10,
                    block=2000,
                )

                if messages:
                    for stream_name, message_list in messages:
                        for message_id, message_data in message_list:
                            symbol = message_data.get("symbol")
                            if not symbol:
                                self.r.xack(
                                    self.STATIC_UPDATE_STREAM,
                                    "gridtrader_bff_static_consumers",
                                    message_id,
                                )
                                continue

                            # Parse versioned schema
                            update_type = message_data.get("update_type", "static")
                            domains_json = message_data.get("domains", "[]")
                            version_json = message_data.get("version", "{}")
                            timestamp = message_data.get("timestamp")

                            try:
                                domains = json.loads(domains_json)
                                versions = json.loads(version_json)
                            except json.JSONDecodeError:
                                # Fallback for legacy format (fields_updated)
                                fields_updated = message_data.get(
                                    "fields_updated", "[]"
                                )
                                try:
                                    fields = json.loads(fields_updated)
                                except json.JSONDecodeError:
                                    fields = []
                                # Infer domains from legacy fields
                                domains = []
                                if any(
                                    f in fields
                                    for f in [
                                        "marketCap",
                                        "float",
                                        "country",
                                        "sector",
                                        "hasNews",
                                    ]
                                ):
                                    domains.append("summary")
                                if "news" in fields or "hasNews" in fields:
                                    domains.append("news")
                                versions = {}  # No version tracking for legacy

                            # Check version freshness - skip stale updates
                            if symbol not in self._applied_versions:
                                self._applied_versions[symbol] = {}

                            domains_to_process = []
                            for domain in domains:
                                incoming_version = versions.get(domain, 0)
                                last_applied = self._applied_versions[symbol].get(
                                    domain, 0
                                )

                                if incoming_version > last_applied:
                                    domains_to_process.append(domain)
                                    self._applied_versions[symbol][
                                        domain
                                    ] = incoming_version
                                else:
                                    logger.debug(
                                        f"Skipping stale {domain} update for {symbol}: "
                                        f"v{incoming_version} <= v{last_applied}"
                                    )

                            if not domains_to_process:
                                # All domains are stale, skip this message
                                self.r.xack(
                                    self.STATIC_UPDATE_STREAM,
                                    "gridtrader_bff_static_consumers",
                                    message_id,
                                )
                                continue

                            # Read data for non-stale domains
                            summary_parsed = {}
                            profile_parsed = {}
                            news_articles = []

                            if "summary" in domains_to_process:
                                summary_key = f"{self.STATIC_SUMMARY_PREFIX}:{symbol}"
                                summary_data = self.r.hgetall(summary_key)
                                for field, value in (summary_data or {}).items():
                                    if field in ["marketCap", "float"]:
                                        try:
                                            summary_parsed[field] = float(value)
                                        except (ValueError, TypeError):
                                            summary_parsed[field] = value
                                    elif field == "hasNews":
                                        summary_parsed[field] = value == "1"
                                    else:
                                        summary_parsed[field] = value

                            if "profile" in domains_to_process:
                                profile_key = f"{self.STATIC_PROFILE_PREFIX}:{symbol}"
                                profile_data = self.r.hgetall(profile_key)
                                for field, value in (profile_data or {}).items():
                                    if field in [
                                        "marketCap",
                                        "float",
                                        "averageVolume",
                                        "fullTimeEmployees",
                                    ]:
                                        try:
                                            profile_parsed[field] = float(value)
                                        except (ValueError, TypeError):
                                            profile_parsed[field] = value
                                    else:
                                        profile_parsed[field] = value

                            if "news" in domains_to_process:
                                news_ticker_key = f"{self.NEWS_TICKER_PREFIX}:{symbol}"
                                news_ids = self.r.zrevrange(news_ticker_key, 0, 9)
                                for idx, news_id in enumerate(news_ids):
                                    item_key = f"{self.NEWS_ITEM_PREFIX}:{news_id}"
                                    article = self.r.hgetall(item_key)
                                    if article:
                                        news_articles.append(
                                            {
                                                "id": f"{symbol}-news-{idx}",
                                                "title": article.get("title", ""),
                                                "source": article.get("sources", ""),
                                                "publishedAt": article.get(
                                                    "published_time", ""
                                                ),
                                                "url": article.get("url", ""),
                                                "summary": (
                                                    article.get("text", "")[:500]
                                                    if article.get("text")
                                                    else ""
                                                ),
                                                "isNew": False,
                                            }
                                        )

                            if summary_parsed or profile_parsed or news_articles:
                                # Build version info for processed domains only
                                processed_versions = {
                                    domain: versions.get(domain, 0)
                                    for domain in domains_to_process
                                }

                                # Broadcast to all connected clients
                                static_update = {
                                    "type": "static_update",
                                    "symbol": symbol,
                                    "domains": domains_to_process,
                                    "version": processed_versions,
                                    "summary": (
                                        summary_parsed
                                        if "summary" in domains_to_process
                                        else None
                                    ),
                                    "profile": (
                                        profile_parsed
                                        if "profile" in domains_to_process
                                        else None
                                    ),
                                    "news": (
                                        news_articles
                                        if "news" in domains_to_process
                                        else None
                                    ),
                                    "timestamp": timestamp,
                                }

                                logger.info(
                                    f"Static update: {symbol} - domains={domains_to_process}, "
                                    f"versions={processed_versions}"
                                )

                                for client_id in self.manager.active_connections:
                                    await self.manager.send_personal_message(
                                        static_update, client_id
                                    )

                            # Acknowledge the message
                            self.r.xack(
                                self.STATIC_UPDATE_STREAM,
                                "gridtrader_bff_static_consumers",
                                message_id,
                            )

                # Allow other tasks to run
                await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Static stream listener error: {e}")
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
    python -m src.BackendForFrontend.bff

    # Replay mode
    python -m src.BackendForFrontend.bff --replay-date 20260115 --suffix-id test

    # Custom host/port
    python -m src.BackendForFrontend.bff --host 0.0.0.0 --port 8080
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
