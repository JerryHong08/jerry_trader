"""
JerryTrader Backend Starter

Starts selected backend services for JerryTrader based on working machine:
SnapshotProcessor, StaticDataWorker, JerryTrader BFF.
StateEngine.
NewsWorker, NewsProcessor.AgentBFF (News Processor Results).
Usage:
    # Start with machine config
    python -m jerry_trader.backend_starter --machine wsl2

    # Override defaults
    python -m jerry_trader.backend_starter --machine wsl2 --defaults.replay_date 20260115

    # Override nested config (llm model settings)
    python -m jerry_trader.backend_starter --machine mibuntu --llm.models.deepseek.thinking_mode true

Note: Ensure Redis and InfluxDB are running before starting.
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime
from datetime import time as dtime
from pathlib import Path
from threading import Thread
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
from dotenv import load_dotenv

load_dotenv()

from jerry_trader.utils.config_builder import (
    DEFAULT_CONFIG_PATH,
    build_runtime_config,
    load_yaml_config,
    parse_override_args,
)
from jerry_trader.utils.logger import setup_logger
from jerry_trader.utils.session import make_session_id

logger = setup_logger(__name__, log_to_file=True, level=logging.INFO)


class JerryTraderBackendStarter:
    """
    Manages and starts backend services for JerryTrader based on machine roles.

    Services (started based on machine config):
    - SnapshotProcessor: Receives data, processes, stores to InfluxDB and Redis
    - StateEngine: Computes ticker states and writes to state stream
    - StaticDataWorker: Fetches static data (fundamentals, float) for subscribed tickers
    - NewsWorker: Fetches and caches news data for subscribed tickers
    - NewsProcessor: Processes news with LLM
    - JerryTraderBFF: Backend For Frontend (FastAPI + WebSocket server)    - AgentBFF: News Processor Results BFF (separate machine support)
    """

    def __init__(self, config: dict):
        """
        Initialize backend starter with runtime config.

        Args:
            config: Runtime configuration dict containing roles, defaults, etc.
        """
        self.config = config
        self.roles = config.get("roles", {})

        # Extract common params from config
        self.replay_date = config.get("replay_date")
        self.replay_time = config.get("replay_time")  # HHMMSS (optional)
        self.suffix_id = config.get("suffix_id")
        self.load_history = config.get("load_history")
        self.limit = config.get("limit")  # market_open | market_close | None
        # Global preload list (used only when manager_type == "synced-replayer")
        self.preload_tickers = config.get("preload_tickers", [])

        # Unified session ID for all Redis keys and InfluxDB tags
        self.session_id = make_session_id(
            replay_date=self.replay_date,
            suffix_id=self.suffix_id,
        )
        logger.info(f"Session ID: {self.session_id}")

        self._running = False
        self._services = []
        self._threads = []

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Trading calendar for market hours check
        self.tz = ZoneInfo("America/New_York")
        self.calendar = xcals.get_calendar("XNYS")

        # Initialize clock *before* services — synced-replayer needs
        # the ReplayClock to exist so create_tick_replayer() can read
        # data_start_ts_ns and speed from it.
        self._init_clock()

        # Initialize only enabled services based on roles
        self._init_services()

    def _init_clock(self):
        """Initialize the global clock (replay or live mode).

        Must run before ``_init_services()`` because the synced-replayer
        path calls ``clock.create_tick_replayer()`` which requires an
        active ``ReplayClock``.
        """
        from jerry_trader import clock

        if self.replay_date:
            # Build replay start timestamp from replay_date (YYYYMMDD) + replay_time (HHMMSS).
            # Priority for start time:
            #   1. defaults.replay_time (HHMMSS)  — CLI or config.yaml
            #   2. Replayer role's start_from (HH:MM)
            #   3. 04:00:00 ET (premarket open)
            rd = str(self.replay_date)

            if len(rd) != 8 or not rd.isdigit():
                raise ValueError(f"replay_date must be YYYYMMDD, got: {rd!r}")

            year, month, day = int(rd[:4]), int(rd[4:6]), int(rd[6:8])

            rt = str(self.replay_time).zfill(6) if self.replay_time else None
            if rt and len(rt) == 6 and rt.isdigit():
                # replay_time = HHMMSS
                hour, minute, second = int(rt[:2]), int(rt[2:4]), int(rt[4:6])
            elif rt and len(rt) == 4 and rt.isdigit():
                # replay_time = HHMM
                hour, minute, second = int(rt[:2]), int(rt[2:4]), 0
            else:
                second = 0
                # Fallback: Replayer role's start_from (HH:MM)
                start_time_str = None
                if "Replayer" in self.roles:
                    start_time_str = self.roles["Replayer"].get("start_from")
                if start_time_str:
                    parts = start_time_str.split(":")
                    hour, minute = int(parts[0]), int(parts[1])
                else:
                    hour, minute = 4, 0  # premarket open

            replay_start_dt = datetime(
                year, month, day, hour, minute, second, tzinfo=self.tz
            )
            data_start_ts_ns = int(replay_start_dt.timestamp() * 1_000_000_000)

            replay_speed = 1.0
            if "Replayer" in self.roles:
                replay_speed = self.roles["Replayer"].get("speed", 1.0)

            clock.init_replay(data_start_ts_ns, speed=replay_speed)
            logger.info(
                f"🕐 ReplayClock initialized: {replay_start_dt.strftime('%Y-%m-%d %H:%M')} ET, "
                f"speed={replay_speed}x"
            )
        else:
            clock.set_live_mode()
            logger.info("🕐 Clock: live mode (time.time)")

    def _init_services(self):
        """Initialize backend services based on enabled roles."""
        logger.info("Initializing backend services based on machine roles...")
        logger.info(f"Enabled roles: {list(self.roles.keys())}")

        # Lazy imports - only import what we need
        if "JerryTraderBFF" in self.roles:
            from jerry_trader.BackendForFrontend.bff import JerryTraderBFF

            role_cfg = self.roles["JerryTraderBFF"]
            self.bff = JerryTraderBFF(
                host=role_cfg.get("host", "localhost"),
                port=role_cfg.get("port", 5001),
                session_id=self.session_id,
                redis_config=role_cfg.get("redis"),
                influxdb_config=role_cfg.get("influxdb"),
                clickhouse_config=role_cfg.get("clickhouse"),
            )
            self._services.append(("JerryTraderBFF", self.bff))
        else:
            self.bff = None

        if "SnapshotProcessor" in self.roles:
            from jerry_trader.core.snapshot.processor import SnapshotProcessor

            role_cfg = self.roles["SnapshotProcessor"]
            self.processor = SnapshotProcessor(
                session_id=self.session_id,
                load_history=self.load_history,
                redis_config=role_cfg.get("redis"),
                influxdb_config=role_cfg.get("influxdb"),
                clickhouse_config=role_cfg.get("clickhouse"),
            )
            self._services.append(("SnapshotProcessor", self.processor))
        else:
            self.processor = None

        if "StateEngine" in self.roles:
            from jerry_trader.core.signals.state_engine import StateEngine

            role_cfg = self.roles["StateEngine"]
            self.state_engine = StateEngine(
                session_id=self.session_id,
                redis_config=role_cfg.get("redis"),
                influxdb_config=role_cfg.get("influxdb"),
            )
            self._services.append(("StateEngine", self.state_engine))
        else:
            self.state_engine = None

        if "StaticDataWorker" in self.roles:
            from jerry_trader.DataManager.static_data_worker import StaticDataWorker

            role_cfg = self.roles["StaticDataWorker"]
            self.static_worker = StaticDataWorker(
                session_id=self.session_id,
                poll_interval=role_cfg.get("poll_interval", 1.0),
                batch_size=role_cfg.get("batch_size", 5),
                redis_config=role_cfg.get("redis"),
            )
            self._services.append(("StaticDataWorker", self.static_worker))
        else:
            self.static_worker = None

        if "NewsWorker" in self.roles:
            from jerry_trader.DataManager.news_worker import NewsWorker

            role_cfg = self.roles["NewsWorker"]
            self.news_worker = NewsWorker(
                session_id=self.session_id,
                poll_interval=role_cfg.get("poll_interval", 1.0),
                batch_size=role_cfg.get("batch_size", 5),
                news_limit=role_cfg.get("news_limit", 5),
                news_recency_hours=role_cfg.get("news_recency_hours", 24.0),
                sources=role_cfg.get("sources", ["momo", "benzinga"]),
                redis_config=role_cfg.get("redis"),
                postgres_config=role_cfg.get("postgres"),
            )
            self._services.append(("NewsWorker", self.news_worker))
        else:
            self.news_worker = None

        if "NewsProcessor" in self.roles:
            from jerry_trader.core.news.processor import NewsProcessor

            role_cfg = self.roles["NewsProcessor"]
            llm_cfg = self.config.get("llm", {})
            self.news_processor = NewsProcessor(
                # llm_model=active_model,
                llm_config=llm_cfg,
                session_id=self.session_id,
                redis_config=role_cfg.get("redis"),
                postgres_config=role_cfg.get("postgres"),
            )
            self._services.append(("NewsProcessor", self.news_processor))
        else:
            self.news_processor = None

        if "Collector" in self.roles:
            from jerry_trader.DataSupply.snapshotDataSupply.collector import (
                MarketsnapshotCollector,
            )

            role_cfg = self.roles["Collector"]
            self.collector = MarketsnapshotCollector(
                limit=role_cfg.get("limit", "market_open"),
                session_id=self.session_id,
                redis_config=role_cfg.get("redis"),
            )
            self._services.append(("Collector", self.collector))
        else:
            self.collector = None

        if "Replayer" in self.roles:
            from jerry_trader.DataSupply.snapshotDataSupply.replayer import (
                MarketSnapshotReplayer,
            )

            role_cfg = self.roles["Replayer"]

            # If clock_redis is configured and we're in replay mode, create a
            # RemoteClockFollower so the replayer is driven by the master
            # ReplayClock on the other machine instead of local asyncio.sleep.
            _remote_clock = None
            _clock_redis_cfg = role_cfg.get("clock_redis")
            if _clock_redis_cfg and self.replay_date:
                import redis as _redis_lib

                from jerry_trader.utils.remote_clock import RemoteClockFollower

                _clock_host = _clock_redis_cfg.get("host", "127.0.0.1")
                _clock_port = _clock_redis_cfg.get("port", 6379)
                _clock_db = _clock_redis_cfg.get("db", 0)

                if isinstance(_clock_host, str) and _clock_host.startswith("${"):
                    logger.warning(
                        "Replayer.clock_redis host is unresolved (%s). "
                        "Set the corresponding network env var before starting; "
                        "RemoteClockFollower disabled.",
                        _clock_host,
                    )
                else:
                    _clock_r = _redis_lib.Redis(
                        host=_clock_host,
                        port=_clock_port,
                        db=_clock_db,
                        decode_responses=True,
                    )
                    _remote_clock = RemoteClockFollower(_clock_r, self.session_id)
                    _remote_clock.start_listening()
                    logger.info(
                        f"RemoteClockFollower started — listening on "
                        f"clock:heartbeat:{self.session_id} "
                        f"(master Redis: {_clock_host}:{_clock_port})"
                    )

            self.replayer = MarketSnapshotReplayer(
                replay_date=self.replay_date,
                session_id=self.session_id,
                speed=role_cfg.get("speed", 1.0),
                file_format=role_cfg.get("format", "parquet"),
                start_from=role_cfg.get("start_from"),
                rollback_to=role_cfg.get("rollback_to"),
                clear=role_cfg.get("clear", False),
                redis_config=role_cfg.get("redis"),
                influxdb_config=role_cfg.get("influxdb"),
                remote_clock=_remote_clock,
            )
            self._services.append(("Replayer", self.replayer))
        else:
            self.replayer = None

        # ============================================================================
        # ChartBFF + FactorEngine — shared UnifiedTickManager when co-located
        # ============================================================================
        # Initialize ChartBFF first (if enabled) so its manager can be shared
        # with FactorEngine. When both run on the same machine, they share a single
        # connection to the data provider via fan-out queues.
        self._shared_ws_manager = None
        self._shared_ws_loop = None

        if "ChartBFF" in self.roles:
            from jerry_trader.BackendForFrontend.chartbff import ChartBFF
            from jerry_trader.DataSupply.tickDataSupply.unified_tick_manager import (
                UnifiedTickManager,
            )

            role_cfg = self.roles["ChartBFF"]

            # Determine manager type: ChartBFF config takes precedence,
            # fallback to FactorEngine config if co-located
            manager_type = role_cfg.get("manager_type")
            if not manager_type and "FactorEngine" in self.roles:
                manager_type = self.roles["FactorEngine"].get("manager_type")

            valid_manager_types = {"polygon", "theta", "replayer", "synced-replayer"}
            if manager_type and manager_type not in valid_manager_types:
                raise ValueError(
                    f"Invalid manager_type={manager_type!r} for ChartBFF. "
                    f"Valid choices: {sorted(valid_manager_types)}"
                )

            # Create shared UnifiedTickManager
            if manager_type == "synced-replayer":
                # In-process Rust TickDataReplayer — no WebSocket hop.
                from jerry_trader.clock import create_tick_replayer
                from jerry_trader.config import lake_data_dir
                from jerry_trader.DataSupply.tickDataSupply.synced_replayer_manager import (
                    SyncedReplayerManager,
                )

                if not self.replay_date:
                    raise ValueError(
                        "synced-replayer requires defaults.replay_date in config"
                    )

                # Build start_time in HH:MM:SS for the Rust replayer
                start_time_hms: str | None = None
                if self.replay_time:
                    t = str(self.replay_time).zfill(6)  # "080015" → "08:00:15"
                    start_time_hms = f"{t[:2]}:{t[2:4]}:{t[4:6]}"
                elif "Replayer" in self.roles and self.roles["Replayer"].get(
                    "start_from"
                ):
                    start_time_hms = self.roles["Replayer"]["start_from"]

                replayer_speed = 1.0
                if "Replayer" in self.roles:
                    replayer_speed = float(self.roles["Replayer"].get("speed", 1.0))

                tick_replayer = create_tick_replayer(
                    replay_date=self.replay_date,
                    lake_data_dir=lake_data_dir,
                    start_time=start_time_hms,
                )
                synced_mgr = SyncedReplayerManager(tick_replayer)
                self._shared_ws_manager = UnifiedTickManager(
                    provider="synced-replayer", manager=synced_mgr
                )
                self._tick_replayer = tick_replayer  # keep reference

                # Preload only in synced-replayer mode, sourced from defaults.
                self._preload_list = list(self.preload_tickers or [])
                logger.info(
                    "Created synced-replayer (in-process Rust) for date=%s",
                    self.replay_date,
                )
            else:
                self._shared_ws_manager = UnifiedTickManager(provider=manager_type)
                self._tick_replayer = None
                self._preload_list = []

            # Create shared event loop for the ws_manager
            self._shared_ws_loop = asyncio.new_event_loop()
            self._shared_ws_thread = Thread(
                target=self._run_shared_ws_loop, daemon=True, name="SharedWSLoop"
            )
            self._shared_ws_thread.start()

            # Pre-load ticker data (Parquet) while clock is paused so
            # the data is in memory before the frontend subscribes.
            if self._preload_list and self._tick_replayer is not None:
                self._preload_tickers(self._preload_list)

            self.tick_data_server = ChartBFF(
                host=role_cfg.get("host", "0.0.0.0"),
                port=role_cfg.get("port", 8000),
                session_id=self.session_id,
                ws_manager=self._shared_ws_manager,
                redis_config=role_cfg.get("redis"),
            )
            self._services.append(("ChartBFF", self.tick_data_server))
            logger.info(
                f"ChartBFF initialized with shared {self._shared_ws_manager.provider.upper()} manager"
            )
        else:
            self.tick_data_server = None

        if "FactorEngine" in self.roles:
            from jerry_trader.core.factors.engine import FactorManager

            role_cfg = self.roles["FactorEngine"]

            # If ChartBFF is also enabled, share the UnifiedTickManager
            if self.tick_data_server is not None:
                self.factor_engine = FactorManager(
                    session_id=self.session_id,
                    redis_config=role_cfg.get("redis"),
                    influxdb_config=role_cfg.get("influxdb"),
                    ws_manager=self.tick_data_server.manager,
                    ws_loop=self._shared_ws_loop,
                )
            else:
                # Standalone FactorEngine with its own manager
                self.factor_engine = FactorManager(
                    session_id=self.session_id,
                    manager_type=role_cfg.get("manager_type"),
                    redis_config=role_cfg.get("redis"),
                    influxdb_config=role_cfg.get("influxdb"),
                )
            self._services.append(("FactorEngine", self.factor_engine))
        else:
            self.factor_engine = None

        if "BarsBuilder" in self.roles:
            from jerry_trader.DataManager.bars_builder_service import BarsBuilderService

            role_cfg = self.roles["BarsBuilder"]

            # If ChartBFF is also enabled, share the UnifiedTickManager
            if self.tick_data_server is not None:
                self.bars_builder = BarsBuilderService(
                    session_id=self.session_id,
                    redis_config=role_cfg.get("redis"),
                    clickhouse_config=role_cfg.get("clickhouse"),
                    timeframes=role_cfg.get("timeframes"),
                    late_arrival_ms=role_cfg.get("late_arrival_ms", 100),
                    idle_close_ms=role_cfg.get("idle_close_ms", 2000),
                    bootstrap_late_arrival_ms=role_cfg.get(
                        "bootstrap_late_arrival_ms", 0
                    ),
                    bootstrap_idle_close_ms=role_cfg.get("bootstrap_idle_close_ms", 1),
                    ws_manager=self.tick_data_server.manager,
                    ws_loop=self._shared_ws_loop,
                )
            else:
                # Standalone BarsBuilder with its own manager
                self.bars_builder = BarsBuilderService(
                    session_id=self.session_id,
                    manager_type=role_cfg.get("manager_type"),
                    redis_config=role_cfg.get("redis"),
                    clickhouse_config=role_cfg.get("clickhouse"),
                    timeframes=role_cfg.get("timeframes"),
                    late_arrival_ms=role_cfg.get("late_arrival_ms", 100),
                    idle_close_ms=role_cfg.get("idle_close_ms", 2000),
                    bootstrap_late_arrival_ms=role_cfg.get(
                        "bootstrap_late_arrival_ms", 0
                    ),
                    bootstrap_idle_close_ms=role_cfg.get("bootstrap_idle_close_ms", 1),
                )
            self._services.append(("BarsBuilder", self.bars_builder))
        else:
            self.bars_builder = None

        # Wire BarsBuilder reference into ChartBFF so it can wait
        # for trades_backfill completion before serving bar REST responses.
        if self.tick_data_server is not None and self.bars_builder is not None:
            self.tick_data_server._bars_builder = self.bars_builder

        if "AgentBFF" in self.roles:
            from jerry_trader.BackendForFrontend.newsbff import AgentBFF

            role_cfg = self.roles["AgentBFF"]
            self.agent_bff = AgentBFF(
                host=role_cfg.get("host", "0.0.0.0"),
                port=role_cfg.get("port", 5003),
                session_id=self.session_id,
                redis_config=role_cfg.get("redis"),
            )
            self._services.append(("AgentBFF", self.agent_bff))
        else:
            self.agent_bff = None

        logger.info(f"Initialized {len(self._services)} services")

    def is_trading_day_today(self) -> bool:
        """Check if today is a valid trading day."""
        today = datetime.now(self.tz).date()
        return self.calendar.is_session(today)

    def in_limit_window(self) -> bool:
        """
        Check if the current time is within the configured limit window.

        Returns True if services should keep running:
        - limit=None         -> always True (never auto-stop)
        - limit="market_open" -> True during 04:00–09:30 ET
        - limit="market_close"-> True during 04:00–16:00 ET
        """
        if self.limit is None:
            return True
        now = datetime.now(self.tz).time()
        if self.limit == "market_open":
            return dtime(4, 0) <= now < dtime(9, 30)
        if self.limit == "market_close":
            return dtime(4, 0) <= now < dtime(16, 0)
        return True  # unknown limit value -> never stop

    def _limit_watchdog(self):
        """
        Background thread that monitors the limit window.
        When the window closes, triggers a graceful shutdown.
        """
        logger.info(f"Limit watchdog started (limit={self.limit})")
        while self._running:
            if not self.in_limit_window():
                logger.info(
                    f"⏹ Outside limit window (limit={self.limit}). "
                    f"Initiating graceful shutdown..."
                )
                self._running = False
                self._cleanup()
                os._exit(0)
            time.sleep(30)  # check every 30 seconds

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self._running = False
        self._cleanup()
        sys.exit(0)

    def _cleanup(self):
        """Clean up all services."""
        logger.info("Cleaning up services...")

        if self.processor:
            self.processor.close()

        if self.state_engine:
            self.state_engine.close()

        if self.static_worker:
            self.static_worker.stop()
            logger.info("StaticDataWorker stopped")

        if self.news_worker:
            self.news_worker.stop()
            logger.info("NewsWorker stopped")

        if self.replayer:
            self.replayer.stop()
            logger.info("Replayer stopped")

        if self.factor_engine:
            self.factor_engine.stop()
            logger.info("FactorEngine stopped")

        if self.bars_builder:
            self.bars_builder.stop()
            logger.info("BarsBuilder stopped")

        if self.tick_data_server:
            self.tick_data_server.cleanup()
            logger.info("ChartBFF stopped")

        # Stop shared WS loop if it exists
        if self._shared_ws_loop and self._shared_ws_loop.is_running():
            self._shared_ws_loop.call_soon_threadsafe(self._shared_ws_loop.stop)
            if hasattr(self, "_shared_ws_thread"):
                self._shared_ws_thread.join(timeout=2)
            logger.info("Shared WS loop stopped")

        if self.bff:
            self.bff.cleanup()

        if self.agent_bff:
            self.agent_bff.cleanup()

        logger.info("All services cleaned up")

    def _run_shared_ws_loop(self):
        """Run the shared WebSocket event loop for UnifiedTickManager."""
        asyncio.set_event_loop(self._shared_ws_loop)
        self._shared_ws_loop.create_task(self._shared_ws_manager.stream_forever())
        try:
            self._shared_ws_loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop=self._shared_ws_loop)
            for task in pending:
                task.cancel()
            if pending:
                self._shared_ws_loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            self._shared_ws_loop.run_until_complete(
                self._shared_ws_loop.shutdown_asyncgens()
            )
            self._shared_ws_loop.close()

    # ── Preload ──────────────────────────────────────────────────────

    _PRELOAD_CLIENT = "__preload__"

    def _preload_tickers(self, tickers: list[str]) -> None:
        """Batch-preload Parquet data for *tickers* with the clock paused.

        Uses ``batch_preload`` to scan each Parquet file only once for
        all tickers (instead of N separate scans).  The clock is paused
        during loading so virtual time does not advance.
        """
        import time as _time

        from jerry_trader import clock as clock_mod

        logger.info("Pre-loading %d ticker(s): %s", len(tickers), tickers)
        t0 = _time.monotonic()

        # Pause the clock so virtual time doesn't drift during I/O.
        clock_mod.pause()
        try:
            # Batch-load: 1 Parquet scan per data type for ALL tickers.
            self._tick_replayer.batch_preload(tickers, ["Q", "T"])
        finally:
            clock_mod.resume()

        elapsed = _time.monotonic() - t0
        logger.info("Pre-load complete: %d ticker(s) in %.1fs", len(tickers), elapsed)

    def _start_async_worker_in_thread(self, worker, name: str):
        """Start an async worker in a separate thread with its own event loop."""

        def run_worker():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                task = loop.create_task(worker.start())
                loop.run_until_complete(task)
            except asyncio.CancelledError:
                pass
            finally:
                loop.close()

        thread = Thread(target=run_worker, daemon=True, name=name)
        thread.start()
        self._threads.append(thread)
        return thread

    def run(self):
        """Run backend services based on enabled roles."""
        logger.info("=" * 70)
        logger.info("Starting JerryTrader Backend")
        logger.info(f"Enabled roles: {list(self.roles.keys())}")
        logger.info(f"Limit: {self.limit or 'None (never auto-stop)'}")
        logger.info("=" * 70)

        # Check if it's a trading day (only in live mode)
        if not self.replay_date:
            if not self.is_trading_day_today():
                logger.info("🚫 Not a trading day in live mode. Exiting.")
                return

        logger.info(f"Session: {self.session_id}")

        self._running = True

        # Start clock heartbeat publisher if in replay mode.
        # Remote machines running RemoteClockFollower subscribe to this channel
        # so their service timing stays in lock-step with the virtual clock here.
        if self.replay_date:
            _tick_role_cfg = self.roles.get("ChartBFF", {})
            _hb_redis_cfg = (
                _tick_role_cfg.get("heartbeat_redis") if _tick_role_cfg else None
            )
            if _hb_redis_cfg:
                import redis as _redis_lib

                from jerry_trader import clock as _clock_mod

                _hb_r = _redis_lib.Redis(
                    host=_hb_redis_cfg.get("host", "127.0.0.1"),
                    port=_hb_redis_cfg.get("port", 6379),
                    db=_hb_redis_cfg.get("db", 0),
                )
                _clock_mod.start_heartbeat_publisher(_hb_r, self.session_id)
                logger.info(
                    f"⏱ Clock heartbeat publisher started (100ms) → "
                    f"clock:heartbeat:{self.session_id}"
                    f"hb_redis_config: {_hb_redis_cfg}"
                )
            elif _tick_role_cfg:
                logger.warning(
                    "Replay mode with ChartBFF enabled but no explicit "
                    "ChartBFF.heartbeat_redis configured; clock heartbeat "
                    "publisher not started."
                )

        # Start limit watchdog if a limit is configured (and not in replay mode)
        if self.limit and not self.replay_date:
            if not self.in_limit_window():
                logger.info(
                    f"⏹ Already outside limit window (limit={self.limit}). Not starting."
                )
                return
            watchdog = Thread(
                target=self._limit_watchdog, daemon=True, name="LimitWatchdog"
            )
            watchdog.start()
            self._threads.append(watchdog)
            logger.info(f"Limit watchdog active: limit={self.limit}")

        # Start SnapshotProcessor (it creates its own thread internally)
        if self.processor:
            self.processor.start()
            logger.info("SnapshotProcessor started")

        # Start StateEngine (it creates its own thread internally)
        if self.state_engine:
            self.state_engine.start()
            logger.info("StateEngine started")

        # Start StaticDataWorker (async, runs in separate thread)
        if self.static_worker:
            self._start_async_worker_in_thread(self.static_worker, "StaticDataWorker")
            logger.info("StaticDataWorker started")

        # Start NewsWorker (async, runs in separate thread)
        if self.news_worker:
            self._start_async_worker_in_thread(self.news_worker, "NewsWorker")
            logger.info("NewsWorker started")

        # Start NewsProcessor if enabled
        if self.news_processor:
            # NewsProcessor might have its own start method
            if hasattr(self.news_processor, "start"):
                self._start_async_worker_in_thread(self.news_processor, "NewsProcessor")
            logger.info("NewsProcessor started")

        # Start Collector if enabled (runs in separate thread, blocking)
        if self.collector:

            def run_collector():
                self.collector.run_collector_engine()

            collector_thread = Thread(
                target=run_collector, daemon=True, name="Collector"
            )
            collector_thread.start()
            self._threads.append(collector_thread)
            logger.info("Collector started")

        # Start Replayer if enabled (async, runs in separate thread)
        if self.replayer:
            self._start_async_worker_in_thread(self.replayer, "Replayer")
            logger.info("Replayer started")

        # Start FactorEngine if enabled (it creates its own threads)
        if self.factor_engine:
            self.factor_engine.start()
            logger.info("FactorEngine started")

        # Start BarsBuilder if enabled (it creates its own threads)
        if self.bars_builder:
            self.bars_builder.start()
            logger.info("BarsBuilder started")

        # Start ChartBFF if enabled
        # Runs in a separate thread since it's a blocking uvicorn server
        if self.tick_data_server:
            tick_cfg = self.roles.get("ChartBFF", {})
            host = tick_cfg.get("host", "0.0.0.0")
            port = tick_cfg.get("port", 8000)
            logger.info(f"Starting ChartBFF on {host}:{port}")

            def run_tick_server():
                self.tick_data_server.run()

            tick_thread = Thread(target=run_tick_server, daemon=True, name="ChartBFF")
            tick_thread.start()
            self._threads.append(tick_thread)
            logger.info("ChartBFF started")

        # Start AgentBFF in separate thread if enabled
        if self.agent_bff:

            def run_agent_bff():
                self.agent_bff.run()

            agent_bff_thread = Thread(
                target=run_agent_bff, daemon=True, name="AgentBFF"
            )
            agent_bff_thread.start()
            self._threads.append(agent_bff_thread)
            logger.info("AgentBFF started")

        # Run BFF in the main thread (blocking) if enabled
        if self.bff:
            bff_cfg = self.roles.get("JerryTraderBFF", {})
            host = bff_cfg.get("host", "localhost")
            port = bff_cfg.get("port", 5001)
            logger.info(f"Starting JerryTrader BFF on {host}:{port}")
            self.bff.run()
        else:
            # If no BFF, just keep running
            # logger.info("No BFF - running in headless mode")
            while self._running:
                time.sleep(1)


def main():
    """Main entry point for JerryTrader backend."""
    parser = argparse.ArgumentParser(
        description="JerryTrader Backend Starter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Start with machine config (starts only roles enabled for that machine)
    python -m jerry_trader.backend_starter --machine wsl2

    # Override default settings (replay mode)
    python -m jerry_trader.backend_starter --machine wsl2 --replay_date 20260115 --suffix_id test

    # Override nested LLM config
    python -m jerry_trader.backend_starter --machine mibuntu --llm.active_model kimi
    python -m jerry_trader.backend_starter --machine mibuntu --llm.models.deepseek.thinking_mode true

    # Override role-specific settings
    python -m jerry_trader.backend_starter --machine wsl2 --roles.JerryTraderBFF.port 8080

    # Combine multiple overrides
    python -m jerry_trader.backend_starter --machine wsl2 \\
        --replay_date 20260115 \\
        --suffix_id test \\
        --roles.JerryTraderBFF.host 0.0.0.0

    # Dry run to see resolved config
    python -m jerry_trader.backend_starter --machine wsl2 --dry-run

Config override paths (dot notation):
    replay_date              - Replay date (YYYYMMDD)
    suffix_id                - Custom replay identifier
    load_history             - Load historical data from date (YYYYMMDD)
    log_level                - Log level (DEBUG, INFO, etc.)
    llm.active_model         - Active LLM model name
    llm.models.<model>.thinking_mode - Enable thinking mode for model
    roles.<role>.enabled     - Enable/disable a role
    roles.<role>.<param>     - Role-specific parameters
        """,
    )

    parser.add_argument(
        "--machine",
        required=True,
        help="Machine name from config (e.g., wsl2, mibuntu, oldman)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to config YAML file (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print resolved config and exit without starting services",
    )

    # Parse known args, collect unknown for config overrides
    args, unknown = parser.parse_known_args()

    # Parse unknown args as config overrides (dot notation)
    overrides = parse_override_args(unknown)

    # Load YAML config
    try:
        yaml_cfg = load_yaml_config(args.config)
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    # Build runtime config
    try:
        runtime_config = build_runtime_config(yaml_cfg, args.machine, overrides)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    # Log resolved config
    logger.info(f"Machine: {args.machine}")
    logger.info(f"Enabled roles: {list(runtime_config['roles'].keys())}")
    if overrides:
        logger.info(f"CLI overrides applied: {overrides}")

    if args.dry_run:
        import json

        print("\n=== Resolved Runtime Config ===")
        print(json.dumps(runtime_config, indent=2, default=str))
        sys.exit(0)

    # Create and run backend with resolved config
    starter = JerryTraderBackendStarter(config=runtime_config)

    try:
        starter.run()
    except Exception as e:
        logger.error(f"JerryTrader backend error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
