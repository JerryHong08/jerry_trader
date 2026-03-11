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

import clickhouse_connect
import redis
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from jerry_trader.DataManager.clickhouse_client import ClickHouseClient
from jerry_trader.DataSupply.tickDataSupply.unified_tick_manager import (
    UnifiedTickManager,
)
from jerry_trader.utils.logger import setup_logger
from jerry_trader.utils.redis_keys import factor_tasks_stream
from jerry_trader.utils.session import make_session_id
from jerry_trader.utils.timezone import ms_to_readable

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

    # Timeframes with live partial-bar updates from Rust BarBuilder.
    # Only these have pending/partial bar state at runtime.
    BARS_BUILDER_TIMEFRAMES = {"10s", "1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"}

    # Map frontend timeframe names → canonical ClickHouse timeframe keys.
    # All timeframes go through ClickHouse (with Polygon backfill).
    _TF_TO_CH: Dict[str, str] = {
        "10s": "10s",
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "4h": "4h",
        "1D": "1d",
        "1W": "1w",
        "1M": "1M",
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
        "1M": 2592000,
    }

    _ms_to_readable = staticmethod(ms_to_readable)

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8000,
        session_id: Optional[str] = None,
        ws_manager: Optional[UnifiedTickManager] = None,
        redis_config: Optional[Dict[str, Any]] = None,
        clickhouse_config: Optional[Dict[str, Any]] = None,
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

        # Track subscribed tickers for backfill decisions
        self.subscribed_tickers: Set[str] = set()

        # ── ClickHouse (bar queries) ─────────────────────────────────
        self.ch_client = ClickHouseClient(
            session_id=self.session_id,
            redis_config=redis_config,
            clickhouse_config=clickhouse_config,
        )

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

            All timeframes go through ClickHouse as the single source
            of truth. Missing historical data is backfilled from Polygon
            on first request.

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
            ch_tf = self._TF_TO_CH.get(timeframe)
            ticker_upper = ticker.upper()

            if not ch_tf:
                return {
                    "ticker": ticker_upper,
                    "timeframe": timeframe,
                    "bars": [],
                    "barCount": 0,
                    "error": f"Unknown timeframe: {timeframe}",
                    **({"requestId": request_id} if request_id else {}),
                }

            if not self.ch_client:
                return {
                    "ticker": ticker_upper,
                    "timeframe": timeframe,
                    "bars": [],
                    "barCount": 0,
                    "error": "ClickHouse unavailable",
                    **({"requestId": request_id} if request_id else {}),
                }

            # ── Query ClickHouse ──────────────────────────────────────
            ch_result = self.ch_client._query_bars_clickhouse(
                ticker_upper,
                ch_tf,
                from_date,
                to_date,
                limit,
            )
            ch_bars = ch_result["bars"] if ch_result else []

            # If ClickHouse is missing historical coverage, backfill
            # from Polygon and persist into ClickHouse.
            # Skip backfill if BarsBuilder is actively building (ticker is subscribed).
            # Only backfill when bars_builder is NOT ingesting tick data.
            if self._needs_historical_backfill(
                ticker_upper,
                ch_bars,
                ch_tf,
            ):
                n = self.ch_client.custom_bar_backfill(
                    ticker_upper,
                    timeframe,
                    ch_tf,
                    from_date,
                    to_date,
                    limit,
                )
                if n > 0:
                    ch_result = self.ch_client._query_bars_clickhouse(
                        ticker_upper,
                        ch_tf,
                        from_date,
                        to_date,
                        limit,
                    )

            if ch_result and ch_result["barCount"] > 0:
                # Append pending + partial bar for BarBuilder timeframes
                if ch_tf in self.BARS_BUILDER_TIMEFRAMES:
                    self.ch_client._append_partial_bar(ch_result, ticker_upper, ch_tf)
                ch_result["timeframe"] = timeframe
                if request_id:
                    ch_result["requestId"] = request_id
                return ch_result

            return {
                "ticker": ticker_upper,
                "timeframe": timeframe,
                "bars": [],
                "barCount": 0,
                "error": "No data available",
                **({"requestId": request_id} if request_id else {}),
            }

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

                            # Notify FactorEngine via Redis stream and update subscribed_tickers
                            for sub in subscriptions:
                                sym = sub.get("symbol", "").upper()
                                if sym:
                                    # Remove from subscribed tickers set
                                    self.subscribed_tickers.discard(sym)
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

                        # Notify FactorEngine via Redis stream and update subscribed_tickers
                        for sub in subscriptions:
                            sym = sub.get("symbol", "").upper()
                            if sym:
                                # Add to subscribed tickers set
                                self.subscribed_tickers.add(sym)
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

    # ════════════════════════════════════════════════════════════════════════
    # Historical backfill: Polygon → ClickHouse
    # ════════════════════════════════════════════════════════════════════════

    def _needs_historical_backfill(
        self,
        ticker: str,
        ch_bars: list,
        builder_tf: str,
    ) -> bool:
        """Return True when ClickHouse needs historical bar data from Polygon.

        Since custom_bar_backfill now handles ONLY historical data (yesterday and
        before for intraday TFs), and trades_backfill handles today's data, we need
        to orchestrate them properly:

        Scenarios:
        1. No bars + not subscribed → custom_bar_backfill (historical)
        2. No bars + subscribed → custom_bar_backfill (historical) + trades_backfill (today)
        3. Has bars + not subscribed → custom_bar_backfill (fill gaps)
        4. Has bars + subscribed → skip (trades_backfill handles today, historical already exists)

        The key insight: Always run custom_bar_backfill on FIRST request (empty ClickHouse)
        because it only fetches historical data now. Subsequent requests can skip if
        ticker is subscribed (meaning trades_backfill is handling today).
        """
        # ALWAYS backfill if ClickHouse is empty (first request)
        # custom_bar_backfill will fetch historical, trades_backfill handles today
        if not ch_bars:
            logger.debug(
                f"_needs_historical_backfill - {ticker}/{builder_tf}: "
                f"ClickHouse empty, need custom_bar_backfill for historical data"
            )
            return True

        # Has bars + subscribed → skip (historical exists, today handled by trades_backfill)
        if ticker in self.subscribed_tickers:
            logger.debug(
                f"_needs_historical_backfill - {ticker}/{builder_tf}: "
                f"has bars + ticker subscribed, skip backfill (trades_backfill handles today)"
            )
            return False

        # Has bars + NOT subscribed → backfill to fill any gaps
        logger.debug(
            f"_needs_historical_backfill - {ticker}/{builder_tf}: "
            f"has bars but ticker not subscribed, need backfill to fill gaps"
        )
        return True

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
