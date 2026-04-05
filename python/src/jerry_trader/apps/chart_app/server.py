"""
ChartBFF - Backend for OrderTrader frontend tick data

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
    # Configured via config.yaml ChartBFF role
"""

import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional, Set

import clickhouse_connect
import redis
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from jerry_trader.platform.config.session import make_session_id
from jerry_trader.services.bar_builder.bar_query_service import ClickHouseClient
from jerry_trader.services.market_data.feeds.unified_tick_manager import (
    UnifiedTickManager,
)
from jerry_trader.services.orchestration.bootstrap_coordinator import (
    BOOTSTRAP_TIMEFRAMES,
)
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.time.timezone import ms_to_readable

logger = setup_logger(__name__, log_to_file=True)

load_dotenv()


class ChartBFF:
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
        coordinator=None,
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

        logger.info(f"🚀 ChartBFF using {self.manager.provider.upper()} data manager")

        # Redis for pub/sub (factor updates)
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)
        self.r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

        # Track subscribed tickers for backfill decisions
        self.subscribed_tickers: Set[str] = set()
        # Per-ticker backfill tracking: prevents duplicate custom_bar_backfill
        self._backfill_in_progress: Set[str] = set()
        self._backfill_done: Set[str] = set()
        # Per-ticker-per-TF tracking: avoid repeated futile on-demand backfill
        # attempts when data genuinely doesn't exist (e.g., no day_aggs in replay)
        self._backfill_attempted: Dict[str, Set[str]] = {}  # ticker → {tf1, tf2, ...}
        # Thread pool for background backfill (non-blocking)
        self._backfill_executor = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="custom_bar_backfill"
        )

        # ── ClickHouse (bar queries) ─────────────────────────────────
        self.ch_client = ClickHouseClient(
            session_id=self.session_id,
            redis_config=redis_config,
            clickhouse_config=clickhouse_config,
        )

        # Bootstrap _backfill_done from ClickHouse: tickers already backfilled
        # today skip redundant custom_bar_backfill_all on re-subscribe after restart
        self._backfill_done.update(self.ch_client.tickers_with_bars_today())

        # Build FastAPI app
        self.app = FastAPI(title="ChartBFF", version="1.0.0")

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
            f"ChartBFF initialized: host={host}, port={port}, "
            f"session_id={self.session_id}, provider={self.manager.provider}, "
            f"shared_manager={not self._owns_manager}"
        )

        # ── FactorStorage (for factor queries) ───────────────────────────
        from jerry_trader.services.factor.factor_storage import FactorStorage

        self.factor_storage = FactorStorage(
            session_id=self.session_id,
            clickhouse_config=clickhouse_config,
        )

        # Optional reference to FactorEngine (same process).
        # Used to wait for factor bootstrap before serving factor REST responses.
        self._factor_engine = None

        # BootstrapCoordinator for unified orchestration
        self._coordinator = coordinator
        logger.info(
            f"ChartBFF __init__: coordinator={'set' if coordinator else 'None'}"
        )

    def set_factor_engine(self, engine) -> None:
        """Set FactorEngine reference for bootstrap wait."""
        self._factor_engine = engine

    def set_coordinator(self, coordinator) -> None:
        """Set BootstrapCoordinator for unified bootstrap orchestration."""
        self._coordinator = coordinator
        logger.info(f"ChartBFF: coordinator {'set' if coordinator else 'cleared'}")

    def set_bars_builder(self, bars_builder) -> None:
        """Set BarsBuilder reference for partial bar appends."""
        self.ch_client._bars_builder = bars_builder
        logger.info(f"ChartBFF: BarsBuilder {'set' if bars_builder else 'cleared'}")

    def _setup_routes(self):
        """Setup FastAPI routes and WebSocket endpoints."""

        manager = self.manager
        r = self.r

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

        # ============ Factor Specs API (for FactorChartModule) ============

        @self.app.get("/api/factors/specs")
        async def get_factor_specs():
            """Get all factor specifications for frontend configuration.

            Returns factor metadata including display name, color, priceScale,
            and applicable timeframes. Frontend uses this to dynamically
            configure factor charts instead of hardcoding.

            Returns:
                JSON array of factor specs:
                [{id, type, display: {name, description, color, priceScale}, timeframes}]
            """
            from jerry_trader.services.factor.factor_registry import get_factor_registry

            registry = get_factor_registry()
            specs = registry.get_specs_for_api()
            return {"factors": specs, "count": len(specs)}

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
            # If trades_backfill is running for this ticker (e.g., after
            # subscribe/re-subscribe), wait briefly so the gap-fill bars
            # land in ClickHouse before we serve the REST response.
            # Use BootstrapCoordinator if available, otherwise fall back to legacy.
            if ch_tf in self.BARS_BUILDER_TIMEFRAMES:
                if self._coordinator is not None:
                    ready = self._coordinator.wait_for_ticker_ready(
                        ticker_upper, timeout=30.0
                    )
                    if not ready:
                        logger.warning(
                            f"get_chart_bars - {ticker_upper}/{ch_tf}: "
                            f"bootstrap not ready after 30s, serving available data"
                        )

            ch_result = self.ch_client._query_bars_clickhouse(
                ticker_upper,
                ch_tf,
                from_date,
                to_date,
                limit,
            )
            ch_bars = ch_result["bars"] if ch_result else []

            # If ClickHouse is missing historical coverage, backfill
            # from Polygon/local data and persist into ClickHouse.
            # Applies to both subscribed and unsubscribed tickers.
            # Skip 10s - it can only be built from trades, not fetched from Polygon.
            if ch_tf != "10s" and self._needs_historical_backfill(
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
                # Track attempt so we don't retry this TF on every request
                self._backfill_attempted.setdefault(ticker_upper, set()).add(ch_tf)
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

        @self.app.get("/api/factors/{ticker}")
        async def get_factors(
            ticker: str,
            from_ms: Optional[int] = None,
            to_ms: Optional[int] = None,
            factors: Optional[str] = None,
            timeframe: Optional[str] = None,
        ):
            """Fetch historical factors for bootstrap.

            Args:
                ticker: Stock ticker symbol
                from_ms: Start timestamp in milliseconds (default: 1 hour ago)
                to_ms: End timestamp in milliseconds (default: now)
                factors: Comma-separated factor names to filter (default: all)
                timeframe: Timeframe filter (e.g., 'trade', '1m', '5m'). Default: all timeframes.

            Returns:
                JSON with factors dict: {factor_name: [{time, value}, ...]}
            """
            ticker_upper = ticker.upper()

            if not self.factor_storage:
                return JSONResponse(
                    {"error": "FactorStorage not available", "ticker": ticker_upper},
                    status_code=503,
                )

            # Pre-register bootstrap if FactorEngine exists (ensures wait_for_bootstrap
            # has an event to wait on even if this REST call happens before WebSocket
            # subscription triggers FactorEngine to start processing)
            # Pass the timeframe so bootstrap waits for the correct timeframe's data
            if self._factor_engine is not None:
                self._factor_engine.pre_register_bootstrap(ticker_upper, timeframe)

            # Wait for factor bootstrap before querying
            # Pass the timeframe to wait for the specific timeframe's bootstrap
            if self._factor_engine is not None:
                self._factor_engine.wait_for_bootstrap(
                    ticker_upper, timeframe, timeout=60.0
                )

            factor_names = factors.split(",") if factors else None

            # Convert ms to ns if provided, otherwise query all data (no time filter)
            # This supports replay mode where data is from historical dates
            start_ns = from_ms * 1_000_000 if from_ms else None
            end_ns = to_ms * 1_000_000 if to_ms else None

            try:
                results = self.factor_storage.query_factors(
                    ticker=ticker_upper,
                    start_ns=start_ns,
                    end_ns=end_ns,
                    factor_names=factor_names,
                    timeframe=timeframe,
                )

                # Transform to frontend format: {factor_name: [{time, value}, ...]}
                # Deduplicate by (factor_name, time_in_seconds) — keep last value
                # Multiple nanosecond rows can map to the same second.
                factors_dict: dict[str, list[dict]] = {}
                for row in results:
                    name = row["factor_name"]
                    time_sec = row["timestamp_ns"] // 1_000_000_000
                    if name not in factors_dict:
                        factors_dict[name] = []
                    # Check for duplicate timestamp — replace if same second
                    points = factors_dict[name]
                    if points and points[-1]["time"] == time_sec:
                        points[-1] = {"time": time_sec, "value": row["factor_value"]}
                    else:
                        points.append({"time": time_sec, "value": row["factor_value"]})

                return {
                    "ticker": ticker_upper,
                    "from_ms": from_ms,
                    "to_ms": to_ms,
                    "factors": factors_dict,
                    "count": len(results),
                }

            except Exception as e:
                logger.error(f"get_factors - {ticker_upper}: {e}")
                return JSONResponse(
                    {"error": str(e), "ticker": ticker_upper},
                    status_code=500,
                )

        # --- Main WebSocket endpoint ---

        @self.app.websocket("/ws/tickdata")
        async def websocket_endpoint(websocket: WebSocket):
            """WebSocket endpoint for real-time tick data streaming.

            Handles subscribe/unsubscribe messages and manages consumer tasks
            for each stream. Notifies FactorEngine via Redis streams.
            """
            await websocket.accept()
            consumer_tasks = {}
            current_streams: Set[str] = set()
            factor_subscriptions: Set[str] = set()  # Track factor subscriptions

            try:
                while True:
                    msg = await websocket.receive_text()
                    try:
                        data = json.loads(msg)

                        # Route message to appropriate handler
                        if data.get("action") == "unsubscribe":
                            await self._handle_unsubscribe(
                                websocket,
                                data,
                                manager,
                                consumer_tasks,
                                current_streams,
                                factor_subscriptions,
                            )
                        elif data.get("action") == "subscribe_factors":
                            await self._handle_factor_subscribe(
                                websocket,
                                data,
                                consumer_tasks,
                                factor_subscriptions,
                            )
                        elif data.get("action") == "unsubscribe_factors":
                            await self._handle_factor_unsubscribe(
                                websocket,
                                data,
                                consumer_tasks,
                                factor_subscriptions,
                            )
                        else:
                            # Default to subscribe for backward compatibility
                            await self._handle_subscribe(
                                websocket,
                                data,
                                manager,
                                consumer_tasks,
                                current_streams,
                                factor_subscriptions,
                            )

                    except json.JSONDecodeError as e:
                        logger.error(f"⚠️ JSON decode error: {e}")

            except WebSocketDisconnect:
                logger.info("🔌 WebSocket disconnected")
                await manager.disconnect(websocket)
                for task in consumer_tasks.values():
                    task.cancel()

    # ════════════════════════════════════════════════════════════════════════
    # WebSocket message handlers
    # ════════════════════════════════════════════════════════════════════════

    async def _handle_unsubscribe(
        self,
        websocket: WebSocket,
        data: dict,
        manager: UnifiedTickManager,
        consumer_tasks: dict,
        current_streams: Set[str],
        factor_subscriptions: Set[str],
    ) -> None:
        """Handle unsubscribe requests from WebSocket clients.

        Args:
            websocket: WebSocket connection
            data: Parsed JSON message from client
            manager: UnifiedTickManager instance
            consumer_tasks: Dict of stream_key -> asyncio.Task
            current_streams: Set of currently active stream keys
            factor_subscriptions: Set of currently subscribed factor symbols
        """
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
        await manager.unsubscribe(websocket, subscriptions=subscriptions)

        # Cancel consumer tasks for unsubscribed streams
        unsubscribed_keys = manager.generate_stream_keys(subscriptions=subscriptions)
        for stream_key in unsubscribed_keys:
            if stream_key in current_streams:
                consumer_tasks[stream_key].cancel()
                del consumer_tasks[stream_key]
                current_streams.discard(stream_key)

        # Notify FactorEngine via Redis stream and update subscribed_tickers
        for sub in subscriptions:
            sym = sub.get("symbol", "").upper()
            if sym:
                # Remove from subscribed tickers set.
                # Keep _backfill_done and _backfill_attempted
                # so re-subscribe skips redundant Polygon/local
                # data fetch (data hasn't changed).
                self.subscribed_tickers.discard(sym)

                # Clean up coordinator state on unsubscribe
                # This ensures fresh bootstrap on re-subscribe
                if self._coordinator is not None:
                    try:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, self._coordinator.cleanup, sym)
                        logger.debug(f"📤 Coordinator cleanup for {sym}")
                    except Exception as e:
                        logger.warning(
                            f"⚠️ Failed to cleanup coordinator for {sym}: {e}"
                        )

                # Clean up factor subscriptions and consumer tasks for this symbol
                # This ensures re-subscribe will create fresh consumers
                for sub_key in list(factor_subscriptions):
                    if sub_key.startswith(f"{sym}:"):
                        factor_subscriptions.discard(sub_key)
                        task_key = f"factors:{sub_key}"
                        if task_key in consumer_tasks:
                            consumer_tasks[task_key].cancel()
                            del consumer_tasks[task_key]
                            logger.debug(f"📤 Cancelled factor consumer for {sub_key}")

    async def _handle_subscribe(
        self,
        websocket: WebSocket,
        data: dict,
        manager: UnifiedTickManager,
        consumer_tasks: dict,
        current_streams: Set[str],
        factor_subscriptions: Set[str],
    ) -> None:
        """Handle subscribe requests from WebSocket clients.

        Args:
            websocket: WebSocket connection
            data: Parsed JSON message from client
            manager: UnifiedTickManager instance
            consumer_tasks: Dict of stream_key -> asyncio.Task
            current_streams: Set of currently active stream keys
            factor_subscriptions: Set of currently subscribed factor symbols
        """
        subscriptions = data.get("subscriptions", [])

        # Frontend format: { type: 'subscribe_bars', payload: { ticker, timeframe } }
        if not subscriptions and "payload" in data:
            payload = data.get("payload", {})
            ticker = payload.get("ticker")
            timeframe = payload.get("timeframe", "")
            if ticker:
                subscriptions = [
                    {
                        "symbol": ticker,
                        "timeframe": timeframe,
                        "events": ["Q"],
                        "sec_type": "STOCK",
                    }
                ]

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
            return

        logger.info(f"📥 Subscribe request: {subscriptions}")

        # Extract symbols and start bootstrap FIRST (before manager.subscribe which may block)
        symbols_to_bootstrap = []
        for sub in subscriptions:
            sym = sub.get("symbol", "").upper()
            if sym:
                symbols_to_bootstrap.append((sym, sub.get("timeframe", "")))
                self.subscribed_tickers.add(sym)

        # Start bootstrap coordination for each symbol (in background to not block)
        async def run_bootstrap_for_symbol(symbol: str, timeframe: str):
            """Run bootstrap for a symbol in background."""
            if self._coordinator is None:
                logger.warning(
                    f"📥 Subscribe: coordinator not ready yet, skipping bootstrap for {symbol}"
                )
                await websocket.send_json(
                    {
                        "type": "error",
                        "message": "Server initializing, please retry",
                        "code": "NOT_READY",
                    }
                )
                return

            logger.debug(f"📥 Subscribe: starting bootstrap for {symbol}")
            all_bootstrap_tfs = list(BOOTSTRAP_TIMEFRAMES)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self._coordinator.start_bootstrap,
                symbol,
                all_bootstrap_tfs,
            )
            logger.info(f"📥 Subscribe: started bootstrap for {symbol} via coordinator")

            # Wait for bootstrap completion
            ready = await loop.run_in_executor(
                None,
                self._coordinator.wait_for_ticker_ready,
                symbol,
                60.0,
            )
            if ready:
                logger.info(f"✅ Subscribe: {symbol} bootstrap ready")
            else:
                logger.warning(f"⏱️ Subscribe: {symbol} bootstrap timeout")

            # Auto-subscribe to factors
            actual_timeframe = timeframe if timeframe else "trade"
            await self._auto_subscribe_factors(
                websocket,
                symbol,
                actual_timeframe,
                consumer_tasks,
                factor_subscriptions,
            )

        # Start bootstrap tasks in background (don't block on manager.subscribe)
        # Pre-create events so REST calls have something to wait on
        if self._coordinator is not None:
            for sym, _ in symbols_to_bootstrap:
                self._coordinator.ensure_event(sym)

        for sym, tf in symbols_to_bootstrap:
            asyncio.create_task(run_bootstrap_for_symbol(sym, tf))

        # Subscribe using unified interface (this may block, but bootstrap is already running)
        logger.info(f"🔍 DEBUG: Before manager.subscribe")
        await manager.subscribe(websocket, subscriptions=subscriptions)
        logger.info(f"🔍 DEBUG: After manager.subscribe")

        # Generate stream keys and create consumer tasks
        new_streams = manager.generate_stream_keys(subscriptions=subscriptions)
        to_add = new_streams - current_streams

        for sk in to_add:
            task = asyncio.create_task(_consume_stream(websocket, sk, manager))
            consumer_tasks[sk] = task
            logger.debug(f"📡 Created consumer task for {sk}")

        current_streams.update(to_add)
        logger.info(f"🔍 DEBUG: After stream update, streams={current_streams}")

    async def _handle_factor_subscribe(
        self,
        websocket: WebSocket,
        data: dict,
        consumer_tasks: dict,
        factor_subscriptions: Set[str],
    ) -> None:
        """Handle factor subscription requests.

        Args:
            websocket: WebSocket connection
            data: Parsed JSON message {
                "action": "subscribe_factors",
                "symbols": ["AAPL", "TSLA"],
                "timeframe": "5m"  // optional, defaults to trade-based factors
            }
            consumer_tasks: Dict of task_key -> asyncio.Task
            factor_subscriptions: Set of currently subscribed symbols (with timeframe)
        """
        symbols = data.get("symbols", [])
        if isinstance(symbols, str):
            symbols = [s.strip().upper() for s in symbols.split(",")]
        else:
            symbols = [s.upper() for s in symbols if s]

        # Timeframe for bar-based factors (e.g., "1m", "5m")
        # Use "trade" for trade-based factors (TradeRate, etc.)
        timeframe = data.get("timeframe", "trade")

        if not symbols:
            logger.warning("⚠️ No symbols in factor subscribe request")
            return

        logger.info(f"📊 Factor subscribe request: {symbols} (timeframe={timeframe})")

        for symbol in symbols:
            # Subscription key includes timeframe: "AAPL:5m"
            sub_key = f"{symbol}:{timeframe}"
            if sub_key in factor_subscriptions:
                logger.debug(f"📊 Already subscribed to factors for {sub_key}")
                continue

            # Create Redis pub/sub consumer task for specific timeframe
            task_key = f"factors:{symbol}:{timeframe}"
            task = asyncio.create_task(
                self._consume_factor_stream(websocket, symbol, timeframe)
            )
            consumer_tasks[task_key] = task
            factor_subscriptions.add(sub_key)
            logger.info(f"📊 Subscribed to factors for {sub_key}")

            # Use coordinator for bootstrap if available (run in thread pool)
            if self._coordinator is not None:
                # Bootstrap all intraday timeframes since BarsBuilder builds all of them
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    self._coordinator.start_bootstrap,
                    symbol,
                    list(BOOTSTRAP_TIMEFRAMES),
                )

            # Pre-register bootstrap Event for the specific timeframe
            if self._factor_engine is not None:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    self._factor_engine.pre_register_bootstrap,
                    symbol,
                    timeframe,  # Pass timeframe for bar-based factors
                )

    async def _handle_factor_unsubscribe(
        self,
        websocket: WebSocket,
        data: dict,
        consumer_tasks: dict,
        factor_subscriptions: Set[str],
    ) -> None:
        """Handle factor unsubscription requests.

        Args:
            websocket: WebSocket connection
            data: Parsed JSON message {
                "action": "unsubscribe_factors",
                "symbols": ["AAPL"],
                "timeframe": "5m"  // optional, defaults to trade-based factors
            }
            consumer_tasks: Dict of task_key -> asyncio.Task
            factor_subscriptions: Set of currently subscribed symbols (with timeframe)
        """
        symbols = data.get("symbols", [])
        if isinstance(symbols, str):
            symbols = [s.strip().upper() for s in symbols.split(",")]
        else:
            symbols = [s.upper() for s in symbols if s]

        timeframe = data.get("timeframe", "trade")

        if not symbols:
            logger.warning("⚠️ No symbols in factor unsubscribe request")
            return

        logger.info(f"📊 Factor unsubscribe request: {symbols} (timeframe={timeframe})")

        for symbol in symbols:
            sub_key = f"{symbol}:{timeframe}"
            if sub_key not in factor_subscriptions:
                continue

            task_key = f"factors:{symbol}:{timeframe}"
            if task_key in consumer_tasks:
                consumer_tasks[task_key].cancel()
                del consumer_tasks[task_key]
            factor_subscriptions.discard(sub_key)
            logger.info(f"📊 Unsubscribed from factors for {sub_key}")

    async def _auto_subscribe_factors(
        self,
        websocket: WebSocket,
        symbol: str,
        timeframe: str,
        consumer_tasks: dict,
        factor_subscriptions: Set[str],
    ) -> None:
        """Auto-subscribe to factors when a bar subscription comes in.

        This ensures FactorEngine receives the subscription after
        unsubscribe/resubscribe cycles, fixing the bug where factors
        don't re-subscribe after unsubscribing.

        Args:
            websocket: WebSocket connection
            symbol: Ticker symbol
            timeframe: Timeframe from the bar subscription (e.g., "10s", "1m", "5m")
            consumer_tasks: Dict of task_key -> asyncio.Task
            factor_subscriptions: Set of currently subscribed symbols (with timeframe)
        """
        sub_key = f"{symbol}:{timeframe}"
        if sub_key in factor_subscriptions:
            logger.debug(f"📊 Already auto-subscribed to factors for {sub_key}")
            return

        # Create Redis pub/sub consumer task for this timeframe's factors
        task_key = f"factors:{symbol}:{timeframe}"
        task = asyncio.create_task(
            self._consume_factor_stream(websocket, symbol, timeframe)
        )
        consumer_tasks[task_key] = task
        factor_subscriptions.add(sub_key)
        logger.info(f"📊 Auto-subscribed to factors for {sub_key}")

        # Use coordinator for bootstrap if available (run in thread pool to avoid blocking)
        if self._coordinator is not None:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self._coordinator.start_bootstrap,
                symbol,
                list(BOOTSTRAP_TIMEFRAMES),
            )

        # Pre-register bootstrap Event for the specific timeframe
        if self._factor_engine is not None:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self._factor_engine.pre_register_bootstrap,
                symbol,
                timeframe,  # Pass timeframe for bar-based factors
            )

    async def _consume_factor_stream(
        self,
        websocket: WebSocket,
        symbol: str,
        timeframe: str = "trade",
    ) -> None:
        """Consume factor updates from Redis pub/sub and forward to WebSocket.

        Args:
            websocket: WebSocket connection
            symbol: Ticker symbol
            timeframe: Timeframe for factors (e.g., "1m", "5m", "trade")
        """
        # Wait for factor bootstrap to complete before consuming real-time updates
        # This prevents receiving real-time factors before historical bootstrap is ready
        if self._factor_engine is not None:
            logger.debug(
                f"_consume_factor_stream - {symbol}/{timeframe}: waiting for bootstrap"
            )
            loop = asyncio.get_running_loop()
            # Run wait_for_bootstrap in thread pool since it uses threading.Event
            ok = await loop.run_in_executor(
                None,
                lambda: self._factor_engine.wait_for_bootstrap(symbol, timeout=60.0),
            )
            if ok:
                logger.info(
                    f"_consume_factor_stream - {symbol}/{timeframe}: bootstrap complete, starting consumption"
                )
            else:
                logger.warning(
                    f"_consume_factor_stream - {symbol}/{timeframe}: bootstrap wait timed out, starting anyway"
                )

        channel = f"factors:{symbol}:{timeframe}"
        pubsub = self.r.pubsub()

        try:
            pubsub.subscribe(channel)
            logger.info(f"📊 Listening to Redis channel: {channel}")

            # Track the latest timestamp we've forwarded to detect lag
            last_forwarded_ts: dict[str, int] = {}

            while True:
                message = pubsub.get_message(timeout=0.1)
                if message and message["type"] == "message":
                    try:
                        factor_data = json.loads(message["data"])
                        ts_ms = factor_data.get("timestamp_ms", 0)

                        # Check for queued/old messages and skip if significantly behind
                        # This prevents lag from accumulating in fast replay mode
                        key = f"{symbol}:{timeframe}"
                        last_ts = last_forwarded_ts.get(key, 0)

                        if ts_ms < last_ts:
                            # Message is older than what we already sent - skip it
                            continue

                        last_forwarded_ts[key] = ts_ms

                        # Forward to WebSocket immediately with clock time for delay calculation
                        from jerry_trader import clock

                        await websocket.send_json(
                            {
                                "type": "factor_update",
                                "data": factor_data,
                                "clock_now_ns": clock.now_ns(),  # Current replay clock time
                            }
                        )
                    except json.JSONDecodeError as e:
                        logger.error(f"📊 Invalid JSON in factor message: {e}")
                    except Exception as e:
                        logger.error(f"📊 Error forwarding factor: {e}")
                        break

                # No sleep - process messages as fast as possible
                await asyncio.sleep(0)  # Yield to event loop but don't delay

        except asyncio.CancelledError:
            logger.info(f"📊 Factor consumer cancelled for {symbol}")
        except Exception as e:
            logger.error(f"📊 Factor consumer error for {symbol}: {e}")
        finally:
            try:
                pubsub.unsubscribe(channel)
                pubsub.close()
            except Exception:
                pass

    # ════════════════════════════════════════════════════════════════════════
    # Historical backfill: Polygon → ClickHouse
    # ════════════════════════════════════════════════════════════════════════

    def _trigger_custom_bar_backfill(self, ticker: str) -> None:
        """Trigger custom_bar_backfill for ALL TFs (except 10s) in background.

        Called on ticker subscription. Deduped per ticker — if backfill is
        already in progress or completed, this is a no-op.

        On service restart, in-memory _backfill_done is empty.  We check
        ClickHouse to see whether a previous session already backfilled
        this ticker — if so, treat it as a re-subscribe (skip backfill).

        Runs in a thread pool so the WebSocket handler is not blocked.
        """
        if ticker in self._backfill_done or ticker in self._backfill_in_progress:
            logger.debug(
                f"_trigger_custom_bar_backfill - {ticker}: "
                f"skipped (done={ticker in self._backfill_done}, "
                f"in_progress={ticker in self._backfill_in_progress})"
            )
            return

        self._backfill_in_progress.add(ticker)
        logger.info(
            f"_trigger_custom_bar_backfill - {ticker}: "
            f"starting background backfill for all TFs"
        )

        def _run():
            try:
                self.ch_client.custom_bar_backfill_all(ticker)
                self._backfill_done.add(ticker)
            except Exception as e:
                logger.error(f"_trigger_custom_bar_backfill - {ticker}: failed - {e}")
            finally:
                self._backfill_in_progress.discard(ticker)

        self._backfill_executor.submit(_run)

    def _needs_historical_backfill(
        self,
        ticker: str,
        ch_bars: list,
        builder_tf: str,
    ) -> bool:
        """Return True when a single-TF on-demand backfill is needed.

        Triggers on-demand backfill when ClickHouse has no bars for a given
        ticker+TF, regardless of subscription status. This covers:
          - Unsubscribed tickers (browsing charts without WS stream)
          - Subscribed tickers where custom_bar_backfill_all missed this TF
            (e.g., replay mode: minute_aggs exist but day_aggs don't)
          - Subscribed tickers where the background backfill hasn't finished yet

        Uses per-ticker-per-TF tracking to avoid futile repeated attempts
        when data genuinely doesn't exist.
        """
        # Already have bars → no backfill needed
        if ch_bars:
            return False

        # Background backfill in progress → let it finish, avoid race
        if ticker in self._backfill_in_progress:
            logger.debug(
                f"_needs_historical_backfill - {ticker}/{builder_tf}: "
                f"background backfill in progress, skipping on-demand"
            )
            return False

        # Already attempted this specific TF and it came back empty → don't retry
        attempted_tfs = self._backfill_attempted.get(ticker, set())
        if builder_tf in attempted_tfs:
            return False

        # ClickHouse empty, not in progress, not yet attempted → try backfill
        logger.debug(
            f"_needs_historical_backfill - {ticker}/{builder_tf}: "
            f"ClickHouse empty, triggering on-demand backfill"
        )
        return True

    def run(self, debug: bool = False):
        """Run the ChartBFF (blocking). Used by backend_starter."""
        import uvicorn

        logger.info("=" * 60)
        logger.info(f"Starting ChartBFF on {self.host}:{self.port}")
        logger.info("=" * 60)

        uvicorn.run(
            self.app,
            host=self.host,
            port=self.port,
            log_level="debug" if debug else "info",
        )

    def cleanup(self):
        """Clean up resources."""
        logger.info("ChartBFF resources cleaned up")


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
    Factory function for running ChartBFF standalone with uvicorn.

    Usage:
        uvicorn src.DataManager.tickdata_server:create_standalone_app --factory --port 8000
    """
    session_id = make_session_id(
        replay_date=os.getenv("REPLAY_DATE"),
        suffix_id=os.getenv("SUFFIX_ID"),
    )
    server = ChartBFF(session_id=session_id)

    # For standalone mode, start stream_forever on startup
    @server.app.on_event("startup")
    async def startup():
        asyncio.create_task(server.manager.stream_forever())

    return server.app
