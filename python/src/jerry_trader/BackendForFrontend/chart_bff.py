"""
Chart Data BFF (Backend For Frontend) for JerryTrader

Standalone FastAPI server dedicated to OHLCV bar data for the ChartModule.
Designed to run on the same machine as BarsBuilderService + ClickHouse
so bar queries are local and low-latency.

Endpoints:
  REST:
    GET /api/chart/bars/{ticker}   – Historical bars (ClickHouse → Polygon fallback)
    GET /api/chart/timeframes      – Available timeframes with metadata

  WebSocket:
    /ws/{client_id}
      subscribe_bars   – Real-time bar updates via Redis pub/sub
      unsubscribe_bars – Stop receiving bar updates

Usage:
    python -m jerry_trader.BackendForFrontend.chart_bff
    python -m jerry_trader.BackendForFrontend.chart_bff --port 5002
"""

import argparse
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Set

from dotenv import load_dotenv

load_dotenv()

import clickhouse_connect
import redis
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from jerry_trader.DataManager.chart_data_service import ChartDataService
from jerry_trader.utils.logger import setup_logger
from jerry_trader.utils.session import make_session_id, parse_session_id

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


# ============ Pydantic Models ============


class HealthResponse(BaseModel):
    status: str
    clickhouse: str
    connected_clients: int
    session_id: Optional[str]
    run_mode: str
    bar_subscriptions: int


# ============ WebSocket Connection Manager ============


class ConnectionManager:
    """Manages WebSocket connections for chart bar subscriptions."""

    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        self.active_connections[client_id] = websocket
        logger.info(f"Chart client connected: {client_id}")

    def disconnect(self, client_id: str):
        self.active_connections.pop(client_id, None)
        logger.info(f"Chart client disconnected: {client_id}")

    async def send_personal_message(self, message: dict, client_id: str):
        if client_id in self.active_connections:
            try:
                await self.active_connections[client_id].send_json(message)
            except Exception as e:
                logger.error(f"Error sending to {client_id}: {e}")


# ============ Chart Data BFF Class ============


class ChartDataBFF:
    """
    Chart Data Backend For Frontend for JerryTrader React application.

    Serves OHLCV bar data from ClickHouse (primary) with Polygon fallback.
    Real-time bar updates relayed from BarsBuilder via Redis pub/sub.

    Co-located with BarsBuilderService + ClickHouse for minimal latency.
    """

    # Timeframes served by BarBuilder/ClickHouse (lowercase)
    BARS_BUILDER_TIMEFRAMES = {"10s", "1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"}

    # Map frontend timeframe names → BarBuilder names
    _TF_TO_BUILDER: Dict[str, str] = {
        "10s": "10s",
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "4h": "4h",
        "1D": "1d",
        "1W": "1w",
        "1d": "1d",
        "1w": "1w",
    }

    _TF_DURATION_SEC: Dict[str, int] = {
        "10s": 10,
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
        "1w": 604800,
    }

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 5002,
        session_id: Optional[str] = None,
        redis_config: Optional[Dict[str, Any]] = None,
        clickhouse_config: Optional[Dict[str, Any]] = None,
    ):
        self.host = host
        self.port = port

        # Unified session id — single source of truth for mode & date
        self.session_id = session_id or make_session_id()
        self.db_date, self.run_mode = parse_session_id(self.session_id)

        # Parse redis config (with defaults)
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)

        # Connection manager for WebSocket
        self.manager = ConnectionManager()

        # Redis connection (used for pub/sub relay only)
        self.r = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )

        # Initialize chart data service (Polygon fallback for non-BarBuilder TFs)
        self.chart_data_service = ChartDataService(
            redis_config=redis_config,
            session_id=self.session_id,
        )

        # ── ClickHouse (bar queries) ─────────────────────────────────
        ch_cfg = clickhouse_config or {}
        ch_host = ch_cfg.get("host", "localhost")
        ch_port = ch_cfg.get("port", 8123)
        ch_user = ch_cfg.get("user", "default")
        ch_db = ch_cfg.get("database", "jerry_trader")
        password_env = ch_cfg.get("password_env", "CLICKHOUSE_PASSWORD")
        ch_password = os.getenv(password_env, "")

        self.ch_client = None
        if ch_password:
            try:
                self.ch_client = clickhouse_connect.get_client(
                    host=ch_host,
                    port=ch_port,
                    username=ch_user,
                    password=ch_password,
                    database=ch_db,
                )
                self.ch_client.command("SELECT 1")  # connectivity check
                logger.info(f"ClickHouse connected: {ch_host}:{ch_port}/{ch_db}")
            except Exception as exc:
                logger.warning(
                    f"ClickHouse unavailable, falling back to ChartDataService: {exc}"
                )
                self.ch_client = None
        else:
            logger.info(
                "No ClickHouse password — bar queries fall back to ChartDataService"
            )

        # ── Bar subscriptions (WS clients → ticker:timeframe) ────────
        # key = "TICKER:builder_tf", value = set of client_ids
        self._bar_subscriptions: Dict[str, Set[str]] = {}
        self._bars_pubsub_task = None

        # Background task tracking
        self._running = False

        # Create FastAPI app with lifespan
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            logger.info("Starting Chart Data BFF...")
            self._running = True

            # Start Redis pub/sub listener for completed bar broadcasts
            self._bars_pubsub_task = asyncio.create_task(self._bars_pubsub_listener())

            yield

            # Shutdown
            logger.info("Shutting down Chart Data BFF...")
            self._running = False
            if self._bars_pubsub_task:
                self._bars_pubsub_task.cancel()
            self.cleanup()

        self.app = FastAPI(
            title="JerryTrader Chart Data BFF",
            description="Chart data service for JerryTrader — OHLCV bars from ClickHouse",
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
            f"ChartDataBFF initialized: host={host}, port={port}, "
            f"session_id={self.session_id}, run_mode={self.run_mode}, "
            f"redis={redis_host}:{redis_port}/{redis_db}, "
            f"clickhouse={'connected' if self.ch_client else 'unavailable'}"
        )

    def _setup_routes(self):
        """Setup FastAPI routes."""

        @self.app.get("/")
        async def index():
            return {
                "service": "JerryTrader Chart Data BFF",
                "status": "running",
                "version": "1.0.0",
            }

        @self.app.get("/health", response_model=HealthResponse)
        async def health():
            """Health check endpoint."""
            ch_status = "disconnected"
            if self.ch_client:
                try:
                    self.ch_client.command("SELECT 1")
                    ch_status = "connected"
                except Exception:
                    ch_status = "error"

            return HealthResponse(
                status="ok",
                clickhouse=ch_status,
                connected_clients=len(self.manager.active_connections),
                session_id=self.session_id,
                run_mode=self.run_mode,
                bar_subscriptions=sum(len(v) for v in self._bar_subscriptions.values()),
            )

        # ============ Chart Bars API (for ChartModule) ============

        @self.app.get("/api/chart/bars/{ticker}")
        async def get_chart_bars(
            ticker: str,
            timeframe: str = "1D",
            from_date: Optional[str] = None,
            to_date: Optional[str] = None,
            limit: int = 5000,
            request_id: Optional[str] = None,
        ):
            """Fetch OHLCV bars for the ChartModule.

            Queries ClickHouse for BarBuilder timeframes (10s–1w),
            falls back to ChartDataService (Polygon) for others (1M)
            or when ClickHouse has no data.

            Args:
                ticker: Stock ticker symbol
                timeframe: Bar timeframe (10s, 1m, 5m, 15m, 30m, 1h, 4h, 1D, 1W, 1M)
                from_date: Start date YYYY-MM-DD (default: auto from timeframe)
                to_date: End date YYYY-MM-DD (default: today)
                limit: Maximum bars to return
                request_id: Opaque ID echoed back so the frontend can discard
                            stale responses from superseded requests.

            Returns:
                JSON with bars array formatted for lightweight-charts CandlestickSeries
            """
            builder_tf = self._TF_TO_BUILDER.get(timeframe)
            ticker_upper = ticker.upper()

            # ── ClickHouse path (BarBuilder timeframes) ───────────────
            if builder_tf and self.ch_client:
                ch_result = self._query_bars_clickhouse(
                    ticker_upper,
                    builder_tf,
                    from_date,
                    to_date,
                    limit,
                )
                ch_bars = ch_result["bars"] if ch_result else []

                # If ClickHouse is missing historical coverage, backfill
                # from Polygon and persist into ClickHouse.
                if self._needs_historical_backfill(
                    ch_bars,
                    builder_tf,
                    from_date,
                    to_date,
                ):
                    n = self._backfill_to_clickhouse(
                        ticker_upper,
                        timeframe,
                        builder_tf,
                        from_date,
                        to_date,
                        limit,
                    )
                    if n > 0:
                        # Re-query for merged result
                        ch_result = self._query_bars_clickhouse(
                            ticker_upper,
                            builder_tf,
                            from_date,
                            to_date,
                            limit,
                        )

                if ch_result and ch_result["barCount"] > 0:
                    ch_result["timeframe"] = timeframe
                    if request_id:
                        ch_result["requestId"] = request_id
                    return ch_result

            # ── Fallback for non-BarBuilder timeframes (1M, …) ───
            result = self.chart_data_service.get_bars(
                ticker=ticker,
                timeframe=timeframe,
                from_date=from_date,
                to_date=to_date,
                limit=limit,
            )
            if result:
                if request_id:
                    result["requestId"] = request_id
                return result
            return {
                "ticker": ticker_upper,
                "timeframe": timeframe,
                "bars": [],
                "barCount": 0,
                "error": "No data available",
                **(({"requestId": request_id}) if request_id else {}),
            }

        @self.app.get("/api/chart/timeframes")
        async def get_chart_timeframes():
            """Return available chart timeframes with metadata."""
            base_tfs = self.chart_data_service.get_timeframes()
            # Inject 10s if ClickHouse is connected
            if self.ch_client:
                has_10s = any(tf.get("value") == "10s" for tf in base_tfs)
                if not has_10s:
                    base_tfs.insert(
                        0,
                        {
                            "value": "10s",
                            "label": "10 Seconds",
                            "barDurationSec": 10,
                            "source": "clickhouse",
                        },
                    )
            return {"timeframes": base_tfs}

        # ============ Clock API (for TimelineClock sync) ============

        @self.app.get("/api/clock")
        async def get_clock():
            """Return the current clock state for frontend TimelineClock sync.

            Response:
                mode: "replay" | "live"
                now_ms: current epoch ms (replay-aware)
                speed: replay speed multiplier (1.0 in live)
                paused: whether replay clock is paused
                data_start_ts_ns: replay start epoch ns (null in live)
                session_id: current session ID
            """
            from jerry_trader import clock

            if clock.is_replay():
                rc = clock.get_clock()
                return {
                    "mode": "replay",
                    "now_ms": clock.now_ms(),
                    "speed": rc.speed if rc else 1.0,
                    "paused": rc.is_paused if rc else False,
                    "data_start_ts_ns": rc.data_start_ts_ns if rc else None,
                    "session_id": self.session_id,
                }
            else:
                return {
                    "mode": "live",
                    "now_ms": clock.now_ms(),
                    "speed": 1.0,
                    "paused": False,
                    "data_start_ts_ns": None,
                    "session_id": self.session_id,
                }

        # ============ WebSocket Endpoint ============

        @self.app.websocket("/ws/{client_id}")
        async def websocket_endpoint(websocket: WebSocket, client_id: str):
            await self.manager.connect(websocket, client_id)
            try:
                while True:
                    data = await websocket.receive_json()
                    await self._handle_websocket_message(client_id, data)
            except WebSocketDisconnect:
                self._remove_bar_subscriptions(client_id)
                self.manager.disconnect(client_id)
            except Exception as e:
                logger.error(f"Chart WebSocket error for {client_id}: {e}")
                self._remove_bar_subscriptions(client_id)
                self.manager.disconnect(client_id)

    async def _handle_websocket_message(self, client_id: str, data: dict):
        """Handle incoming WebSocket messages (bar subscriptions only)."""
        msg_type = data.get("type", "")
        payload = data.get("payload", {})

        if msg_type == "subscribe_bars":
            # Subscribe this client to real-time bar updates for a ticker+timeframe
            ticker = (payload.get("ticker") or "").upper()
            tf = payload.get("timeframe", "")
            builder_tf = self._TF_TO_BUILDER.get(tf, tf)
            if ticker and builder_tf in self.BARS_BUILDER_TIMEFRAMES:
                key = f"{ticker}:{builder_tf}"
                self._bar_subscriptions.setdefault(key, set()).add(client_id)
                logger.debug(f"Client {client_id} subscribed to bars {key}")

        elif msg_type == "unsubscribe_bars":
            ticker = (payload.get("ticker") or "").upper()
            tf = payload.get("timeframe", "")
            builder_tf = self._TF_TO_BUILDER.get(tf, tf)
            key = f"{ticker}:{builder_tf}"
            subs = self._bar_subscriptions.get(key)
            if subs:
                subs.discard(client_id)
                if not subs:
                    del self._bar_subscriptions[key]
                logger.debug(f"Client {client_id} unsubscribed from bars {key}")

    # ════════════════════════════════════════════════════════════════════════
    # ClickHouse bar query
    # ════════════════════════════════════════════════════════════════════════

    def _query_bars_clickhouse(
        self,
        ticker: str,
        builder_tf: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        limit: int = 5000,
    ) -> Optional[Dict]:
        """Query completed bars from ClickHouse.

        Returns a response dict matching ChartDataService format, or None on error.
        """
        if not self.ch_client:
            return None

        # Default date range based on timeframe
        from jerry_trader import clock

        if to_date:
            end_dt = datetime.strptime(to_date, "%Y-%m-%d").date()
        else:
            end_dt = clock.now_datetime().date()

        if from_date:
            start_dt = datetime.strptime(from_date, "%Y-%m-%d").date()
        else:
            # Auto-range: short TFs get fewer days, long TFs get more
            dur = self._TF_DURATION_SEC.get(builder_tf, 60)
            if dur <= 60:  # 10s, 1m
                days_back = 2
            elif dur <= 900:  # 5m, 15m
                days_back = 5
            elif dur <= 14400:  # 1h, 4h
                days_back = 30
            else:  # 1d, 1w
                days_back = 365
            start_dt = end_dt - timedelta(days=days_back)

        start_ms = int(
            datetime.combine(start_dt, datetime.min.time()).timestamp() * 1000
        )
        end_ms = int(
            datetime.combine(
                end_dt + timedelta(days=1), datetime.min.time()
            ).timestamp()
            * 1000
        )

        try:
            query = """
                SELECT bar_start, bar_end, open, high, low, close, volume,
                       trade_count, vwap, session
                FROM ohlcv_bars FINAL
                WHERE ticker = {ticker:String}
                  AND timeframe = {timeframe:String}
                  AND bar_start >= {start_ms:Int64}
                  AND bar_start < {end_ms:Int64}
                ORDER BY bar_start ASC
                LIMIT {limit:UInt32}
            """
            result = self.ch_client.query(
                query,
                parameters={
                    "ticker": ticker,
                    "timeframe": builder_tf,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "limit": limit,
                },
            )

            bars = []
            for row in result.result_rows:
                bar_start, bar_end, o, h, l, c, vol, tc, vwap, session = row
                # lightweight-charts expects `time` in seconds (UTC)
                bars.append(
                    {
                        "time": bar_start // 1000,
                        "open": o,
                        "high": h,
                        "low": l,
                        "close": c,
                        "volume": vol,
                    }
                )

            dur_sec = self._TF_DURATION_SEC.get(builder_tf, 60)
            return {
                "ticker": ticker,
                "timeframe": builder_tf,
                "bars": bars,
                "barCount": len(bars),
                "barDurationSec": dur_sec,
                "source": "clickhouse",
                "from": str(start_dt),
                "to": str(end_dt),
            }
        except Exception as e:
            logger.error(f"ClickHouse bar query failed for {ticker}/{builder_tf}: {e}")
            return None

    # ════════════════════════════════════════════════════════════════════════
    # Historical backfill: Polygon → ClickHouse
    # ════════════════════════════════════════════════════════════════════════

    def _needs_historical_backfill(
        self,
        ch_bars: list,
        builder_tf: str,
        from_date: Optional[str],
        to_date: Optional[str],
    ) -> bool:
        """Return True when ClickHouse bars are empty or miss historical coverage.

        Compares the earliest bar timestamp against the requested start date.
        If the gap exceeds a few bar durations, historical data is missing
        (e.g. cold-start scenario where BarsBuilder only has recent ticks).
        """
        if not ch_bars:
            return True

        dur = self._TF_DURATION_SEC.get(builder_tf, 60)

        from jerry_trader import clock

        end_dt = (
            datetime.strptime(to_date, "%Y-%m-%d").date()
            if to_date
            else clock.now_datetime().date()
        )
        if from_date:
            start_dt = datetime.strptime(from_date, "%Y-%m-%d").date()
        else:
            # Mirror the default lookback in _query_bars_clickhouse
            if dur <= 60:
                days_back = 2
            elif dur <= 900:
                days_back = 5
            elif dur <= 14400:
                days_back = 30
            else:
                days_back = 365
            start_dt = end_dt - timedelta(days=days_back)

        requested_start = int(
            datetime.combine(start_dt, datetime.min.time()).timestamp()
        )
        earliest_bar = ch_bars[0]["time"]  # epoch seconds

        # Gap > 3 bar-durations (min 5 min) ⇒ need historical backfill
        gap_threshold = max(dur * 3, 300)
        return (earliest_bar - requested_start) > gap_threshold

    def _backfill_to_clickhouse(
        self,
        ticker: str,
        frontend_tf: str,
        builder_tf: str,
        from_date: Optional[str],
        to_date: Optional[str],
        limit: int,
    ) -> int:
        """Fetch historical bars from Polygon via ChartDataService and
        persist them into ClickHouse so future queries hit a single
        source of truth.

        Returns the number of bars written.
        """
        result = self.chart_data_service.get_bars(
            ticker=ticker,
            timeframe=frontend_tf,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
        )
        if not result or not result.get("bars"):
            return 0

        bars = result["bars"]
        dur_sec = self._TF_DURATION_SEC.get(builder_tf, 60)

        columns = [
            "ticker",
            "timeframe",
            "bar_start",
            "bar_end",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
            "vwap",
            "session",
        ]
        rows = []
        for bar in bars:
            bar_start_ms = bar["time"] * 1000
            bar_end_ms = bar_start_ms + dur_sec * 1000
            rows.append(
                [
                    ticker,
                    builder_tf,
                    bar_start_ms,
                    bar_end_ms,
                    bar["open"],
                    bar["high"],
                    bar["low"],
                    bar["close"],
                    bar.get("volume", 0),
                    0,
                    0.0,
                    "polygon_backfill",
                ]
            )

        try:
            self.ch_client.insert(
                "ohlcv_bars",
                data=rows,
                column_names=columns,
            )
            logger.info(
                f"Backfilled {len(rows)} {builder_tf} bars for {ticker} "
                f"to ClickHouse (source={result.get('source', '?')})"
            )
            return len(rows)
        except Exception as e:
            logger.error(
                f"ClickHouse backfill insert failed for {ticker}/{builder_tf}: {e}"
            )
            return 0

    # ════════════════════════════════════════════════════════════════════════
    # Redis pub/sub: relay completed bars to WebSocket clients
    # ════════════════════════════════════════════════════════════════════════

    async def _bars_pubsub_listener(self):
        """Subscribe to bars:* Redis pub/sub and relay to interested WS clients."""
        logger.info("Starting bars pub/sub listener...")

        # Create a dedicated Redis connection for pub/sub (blocking subscriber)
        try:
            redis_host = self.r.connection_pool.connection_kwargs.get(
                "host", "127.0.0.1"
            )
            redis_port = self.r.connection_pool.connection_kwargs.get("port", 6379)
            redis_db = self.r.connection_pool.connection_kwargs.get("db", 0)
            pubsub_redis = redis.Redis(
                host=redis_host, port=redis_port, db=redis_db, decode_responses=True
            )
            ps = pubsub_redis.pubsub()
            ps.psubscribe("bars:*")
            logger.info("Subscribed to Redis bars:* pattern")
        except Exception as e:
            logger.error(f"Failed to subscribe to bars pub/sub: {e}")
            return

        try:
            while self._running:
                try:
                    msg = ps.get_message(timeout=0.5)
                    if msg and msg["type"] == "pmessage":
                        # Channel: bars:{ticker}:{timeframe}
                        channel = msg["channel"]
                        parts = channel.split(":", 2)
                        if len(parts) == 3:
                            _, ticker, builder_tf = parts
                            key = f"{ticker}:{builder_tf}"
                            client_ids = self._bar_subscriptions.get(key, set())
                            if client_ids:
                                bar_data = json.loads(msg["data"])
                                # Convert bar_start ms → time seconds for frontend
                                ws_msg = {
                                    "type": "bar_update",
                                    "ticker": ticker,
                                    "timeframe": builder_tf,
                                    "bar": {
                                        "time": bar_data["bar_start"] // 1000,
                                        "open": bar_data["open"],
                                        "high": bar_data["high"],
                                        "low": bar_data["low"],
                                        "close": bar_data["close"],
                                        "volume": bar_data["volume"],
                                    },
                                }
                                for cid in client_ids.copy():
                                    await self.manager.send_personal_message(
                                        ws_msg, cid
                                    )
                    else:
                        # No message — yield to event loop
                        await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Bars pub/sub error: {e}")
                    await asyncio.sleep(1)
        finally:
            try:
                ps.punsubscribe("bars:*")
                ps.close()
                pubsub_redis.close()
            except Exception:
                pass

    def _remove_bar_subscriptions(self, client_id: str):
        """Remove a client from all bar subscriptions (called on disconnect)."""
        empty_keys = []
        for key, clients in self._bar_subscriptions.items():
            clients.discard(client_id)
            if not clients:
                empty_keys.append(key)
        for key in empty_keys:
            del self._bar_subscriptions[key]

    def run(self, debug: bool = False):
        """Run the Chart Data BFF server."""
        logger.info("=" * 60)
        logger.info(f"Starting Chart Data BFF on {self.host}:{self.port}")
        logger.info("=" * 60)

        if self.run_mode == "replay":
            logger.info(f"Mode: REPLAY (session_id={self.session_id})")
        else:
            logger.info(f"Mode: LIVE (session_id={self.session_id})")

        logger.info(f"REST API: http://{self.host}:{self.port}")
        logger.info(f"WebSocket: ws://{self.host}:{self.port}/ws/{{client_id}}")
        logger.info("=" * 60)

        uvicorn.run(
            self.app,
            host=self.host,
            port=self.port,
            log_level="debug" if debug else "info",
        )

    def cleanup(self):
        """Clean up resources on shutdown."""
        if self.ch_client:
            try:
                self.ch_client.close()
            except Exception:
                pass
        logger.info("Chart Data BFF resources cleaned up")


def main():
    """Main entry point for Chart Data BFF."""
    parser = argparse.ArgumentParser(
        description="JerryTrader Chart Data BFF",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Default (port 5002)
    python -m jerry_trader.BackendForFrontend.chart_bff

    # Custom host/port
    python -m jerry_trader.BackendForFrontend.chart_bff --host 0.0.0.0 --port 5002

    # Replay mode
    python -m jerry_trader.BackendForFrontend.chart_bff --replay-date 20260115 --suffix-id test
        """,
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5002,
        help="Port to bind to (default: 5002)",
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

    # Build session_id from CLI args
    session_id = make_session_id(
        replay_date=args.replay_date,
        suffix_id=args.suffix_id,
    )

    # Create and run Chart Data BFF
    bff = ChartDataBFF(
        host=args.host,
        port=args.port,
        session_id=session_id,
    )

    try:
        bff.run(debug=args.debug)
    except Exception as e:
        logger.error(f"Chart Data BFF error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        bff.cleanup()


if __name__ == "__main__":
    main()
