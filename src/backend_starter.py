"""
GridTrader Backend Starter

Starts selected backend services for GridTrader based on working machine:
SnapshotProcessor, StaticDataWorker, GridTrader BFF.
StateEngine.
NewsWorker, NewsProcessor.

Usage:
    # Start with machine config
    python -m src.backend_starter --machine wsl2

    # Override defaults
    python -m src.backend_starter --machine wsl2 --defaults.replay_date 20260115

    # Override nested config (llm model settings)
    python -m src.backend_starter --machine mibuntu --llm.models.deepseek.thinking_mode true

Note: Ensure Redis and InfluxDB are running before starting.
"""

import argparse
import asyncio
import copy
import logging
import os
import signal
import sys
import time
from datetime import datetime
from datetime import time as dtime
from pathlib import Path
from threading import Thread
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
from dotenv import load_dotenv

load_dotenv()

import yaml

from utils.logger import setup_logger
from utils.session import make_session_id

logger = setup_logger(__name__, log_to_file=True, level=logging.INFO)

# Project root for config file
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"


def load_yaml_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict:
    """Load YAML configuration file."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def deep_merge(base: dict, override: dict) -> dict:
    """Deep merge override into base dict. Override values take precedence."""
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def set_nested_value(d: dict, key_path: str, value: Any) -> None:
    """
    Set a nested value in a dict using dot notation.
    e.g., set_nested_value(d, "llm.models.deepseek.thinking_mode", True)
    """
    keys = key_path.split(".")
    current = d
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]

    # Type coercion for common cases
    final_key = keys[-1]
    if isinstance(value, str):
        # Convert string to appropriate type
        if value.lower() == "true":
            value = True
        elif value.lower() == "false":
            value = False
        elif value.lower() == "null" or value.lower() == "none":
            value = None
        else:
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass  # Keep as string

    current[final_key] = value


def parse_override_args(unknown_args: list) -> Dict[str, Any]:
    """
    Parse unknown args as config overrides.
    Supports: --key.subkey.subsubkey value or --key.subkey=value
    """
    overrides = {}
    i = 0
    while i < len(unknown_args):
        arg = unknown_args[i]
        if arg.startswith("--"):
            key = arg[2:]  # Remove --

            if "=" in key:
                # Handle --key=value format
                key, value = key.split("=", 1)
                overrides[key] = value
            elif i + 1 < len(unknown_args) and not unknown_args[i + 1].startswith("--"):
                # Handle --key value format
                overrides[key] = unknown_args[i + 1]
                i += 1
            else:
                # Flag without value, treat as True
                overrides[key] = True
        i += 1
    return overrides


def build_runtime_config(
    yaml_cfg: dict,
    machine: str,
    overrides: Dict[str, Any],
) -> dict:
    """
    Build runtime configuration by merging:
    1. Defaults from YAML
    2. Machine-specific config
    3. CLI overrides

    Overrides can use dot notation:
    - "replay_date" or "defaults.replay_date" -> sets replay_date
    - "llm.active_model" -> sets llm.active_model
    - "roles.GridTraderBFF.port" -> sets role-specific param

    Database references use "db-name@MACHINE" format:
    - "redis-b@OLDMAN" -> resolves host from OLDMAN_IP env var
    - "redis-b" -> defaults to LOCAL (127.0.0.1)
    """
    if machine not in yaml_cfg.get("machines", {}):
        available = list(yaml_cfg.get("machines", {}).keys())
        raise ValueError(f"Unknown machine '{machine}'. Available: {available}")

    machine_cfg = yaml_cfg["machines"][machine]

    # Start with defaults (these become top-level in runtime config)
    runtime = copy.deepcopy(yaml_cfg.get("defaults", {}))

    # Add machine-level limit (market_open | market_close | null)
    runtime["limit"] = machine_cfg.get("limit", None)

    # Add LLM config
    runtime["llm"] = copy.deepcopy(yaml_cfg.get("llm", {}))

    # Add enabled roles with their config, resolving database references
    runtime["roles"] = {}
    for role_name, role_cfg in machine_cfg.get("roles", {}).items():
        if role_cfg.get("enabled", True):
            resolved_cfg = copy.deepcopy(role_cfg)

            # Resolve database references (redis, postgres, influxdb)
            for db_type in ["redis", "postgres", "influxdb"]:
                db_ref = role_cfg.get(db_type)
                if db_ref and isinstance(db_ref, str):
                    resolved_cfg[db_type] = _resolve_db_reference(
                        yaml_cfg, db_type, db_ref, role_name
                    )

            runtime["roles"][role_name] = resolved_cfg

    # Apply CLI overrides using dot notation
    # Support both "replay_date" and "defaults.replay_date" for convenience
    for key_path, value in overrides.items():
        # Strip "defaults." prefix if present (for convenience)
        if key_path.startswith("defaults."):
            key_path = key_path[9:]  # Remove "defaults."
        set_nested_value(runtime, key_path, value)

    # Normalize date fields to strings (YAML parses YYYYMMDD as int)
    _normalize_date_fields(runtime)

    return runtime


def _resolve_db_reference(
    yaml_cfg: dict, db_type: str, db_ref: str, role_name: str
) -> Optional[dict]:
    """
    Resolve database reference string to actual config.

    Format: "db-name@MACHINE" or "db-name" (defaults to LOCAL)

    Examples:
        "redis-b@OLDMAN" -> {host: env(OLDMAN_IP), port: 6379, db: 0}
        "redis-b" -> {host: "127.0.0.1", port: 6379, db: 0}
        "postgres-a@WSL2" -> {host: ..., port: 5432, database: ..., url: "postgresql://..."}
    """
    # Parse "db-name@MACHINE" format
    if "@" in db_ref:
        db_name, machine_name = db_ref.rsplit("@", 1)
    else:
        db_name = db_ref
        machine_name = "LOCAL"

    # Get database template from databases section
    databases = yaml_cfg.get("databases", {})
    db_configs = databases.get(db_type, {})

    if db_name not in db_configs:
        logger.warning(
            f"{db_type.capitalize()} config '{db_name}' not found for role {role_name}. "
            f"Available: {list(db_configs.keys())}"
        )
        return None

    # Copy database template (handle None/empty entries in YAML)
    template = db_configs[db_name]
    resolved = copy.deepcopy(template) if template is not None else {}

    # Resolve host from network section
    network = yaml_cfg.get("network", {})
    if machine_name not in network:
        logger.warning(
            f"Machine '{machine_name}' not found in network section for {role_name}.{db_type}. "
            f"Available: {list(network.keys())}"
        )
        return None

    host_env_var = network[machine_name]

    if host_env_var is None:
        # LOCAL = 127.0.0.1
        resolved["host"] = "127.0.0.1"
    else:
        # Resolve from environment variable
        host_value = os.getenv(host_env_var)
        if host_value:
            resolved["host"] = host_value
        else:
            logger.warning(
                f"Environment variable '{host_env_var}' not set for {role_name}.{db_type}. "
                f"Using env var name as placeholder."
            )
            # Keep env var name as placeholder for debugging
            resolved["host"] = f"${{{host_env_var}}}"

    # For influxdb, resolve the URL env var based on machine
    if db_type == "influxdb":
        resolved["influx_url_env"] = f"{machine_name}_INFLUXDB_URL"

    # For postgres, resolve the URL env var based on machine
    if db_type == "postgres":
        database_url_env = f"{machine_name}_DATABASE_URL"
        resolved["database_url_env"] = database_url_env
        # Also resolve the actual URL for convenience
        resolved["url"] = os.getenv(database_url_env)

    return resolved


def _normalize_date_fields(config: dict) -> None:
    """
    Convert date fields from int to string format.
    YAML interprets values like 20260115 as integers, but we need them as strings.
    """
    date_fields = ["replay_date", "load_history", "start_from", "rollback_to"]

    for field in date_fields:
        if field in config and config[field] is not None:
            config[field] = str(config[field])

    # Also check in roles for role-specific date fields
    if "roles" in config:
        for role_name, role_cfg in config["roles"].items():
            if isinstance(role_cfg, dict):
                for field in date_fields:
                    if field in role_cfg and role_cfg[field] is not None:
                        role_cfg[field] = str(role_cfg[field])


class GridTraderBackendStarter:
    """
    Manages and starts backend services for GridTrader based on machine roles.

    Services (started based on machine config):
    - SnapshotProcessor: Receives data, processes, stores to InfluxDB and Redis
    - StateEngine: Computes ticker states and writes to state stream
    - StaticDataWorker: Fetches static data (fundamentals, float) for subscribed tickers
    - NewsWorker: Fetches and caches news data for subscribed tickers
    - NewsProcessor: Processes news with LLM
    - GridTraderBFF: Backend For Frontend (FastAPI + WebSocket server)
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
        self.suffix_id = config.get("suffix_id")
        self.load_history = config.get("load_history")
        self.limit = config.get("limit")  # market_open | market_close | None

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

        # Initialize only enabled services based on roles
        self._init_services()

    def _init_services(self):
        """Initialize backend services based on enabled roles."""
        logger.info("Initializing backend services based on machine roles...")
        logger.info(f"Enabled roles: {list(self.roles.keys())}")

        # Lazy imports - only import what we need
        if "GridTraderBFF" in self.roles:
            from BackendForFrontend.bff import GridTraderBFF

            role_cfg = self.roles["GridTraderBFF"]
            self.bff = GridTraderBFF(
                host=role_cfg.get("host", "localhost"),
                port=role_cfg.get("port", 5001),
                session_id=self.session_id,
                redis_config=role_cfg.get("redis"),
                influxdb_config=role_cfg.get("influxdb"),
            )
            self._services.append(("GridTraderBFF", self.bff))
        else:
            self.bff = None

        if "SnapshotProcessor" in self.roles:
            from ComputeEngine.snapshot_processor import SnapshotProcessor

            role_cfg = self.roles["SnapshotProcessor"]
            self.processor = SnapshotProcessor(
                session_id=self.session_id,
                load_history=self.load_history,
                redis_config=role_cfg.get("redis"),
                influxdb_config=role_cfg.get("influxdb"),
            )
            self._services.append(("SnapshotProcessor", self.processor))
        else:
            self.processor = None

        if "StateEngine" in self.roles:
            from ComputeEngine.state_engine import StateEngine

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
            from DataManager.static_data_worker import StaticDataWorker

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
            from DataManager.news_worker import NewsWorker

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
            from ComputeEngine.news_processor import NewsProcessor

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
            from DataSupply.snapshotDataSupply.collector import MarketsnapshotCollector

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
            from DataSupply.snapshotDataSupply.replayer import MarketSnapshotReplayer

            role_cfg = self.roles["Replayer"]
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
            )
            self._services.append(("Replayer", self.replayer))
        else:
            self.replayer = None

        # ============================================================================
        # TickDataServer + FactorEngine — shared UnifiedTickManager when co-located
        # ============================================================================
        # Initialize TickDataServer first (if enabled) so its manager can be shared
        # with FactorEngine. When both run on the same machine, they share a single
        # connection to the data provider via fan-out queues.
        self._shared_ws_manager = None
        self._shared_ws_loop = None

        if "TickDataServer" in self.roles:
            from DataManager.tickdata_server import TickDataServer
            from DataSupply.tickDataSupply.unified_tick_manager import (
                UnifiedTickManager,
            )

            role_cfg = self.roles["TickDataServer"]

            # Determine manager type: TickDataServer config takes precedence,
            # fallback to FactorEngine config if co-located
            manager_type = role_cfg.get("manager_type")
            if not manager_type and "FactorEngine" in self.roles:
                manager_type = self.roles["FactorEngine"].get("manager_type")

            # Create shared UnifiedTickManager
            self._shared_ws_manager = UnifiedTickManager(provider=manager_type)

            # Create shared event loop for the ws_manager
            self._shared_ws_loop = asyncio.new_event_loop()
            self._shared_ws_thread = Thread(
                target=self._run_shared_ws_loop, daemon=True, name="SharedWSLoop"
            )
            self._shared_ws_thread.start()

            self.tick_data_server = TickDataServer(
                host=role_cfg.get("host", "0.0.0.0"),
                port=role_cfg.get("port", 8000),
                session_id=self.session_id,
                ws_manager=self._shared_ws_manager,
                redis_config=role_cfg.get("redis"),
            )
            self._services.append(("TickDataServer", self.tick_data_server))
            logger.info(
                f"TickDataServer initialized with shared {self._shared_ws_manager.provider.upper()} manager"
            )
        else:
            self.tick_data_server = None

        if "FactorEngine" in self.roles:
            from ComputeEngine.factor_engine import FactorManager

            role_cfg = self.roles["FactorEngine"]

            # If TickDataServer is also enabled, share the UnifiedTickManager
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

        if self.tick_data_server:
            self.tick_data_server.cleanup()
            logger.info("TickDataServer stopped")

        # Stop shared WS loop if it exists
        if self._shared_ws_loop and self._shared_ws_loop.is_running():
            self._shared_ws_loop.call_soon_threadsafe(self._shared_ws_loop.stop)
            if hasattr(self, "_shared_ws_thread"):
                self._shared_ws_thread.join(timeout=2)
            logger.info("Shared WS loop stopped")

        if self.bff:
            self.bff.cleanup()

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
        logger.info("Starting GridTrader Backend")
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

        # Start TickDataServer if enabled
        # Runs in a separate thread since it's a blocking uvicorn server
        if self.tick_data_server:
            tick_cfg = self.roles.get("TickDataServer", {})
            host = tick_cfg.get("host", "0.0.0.0")
            port = tick_cfg.get("port", 8000)
            logger.info(f"Starting TickDataServer on {host}:{port}")

            def run_tick_server():
                self.tick_data_server.run()

            tick_thread = Thread(
                target=run_tick_server, daemon=True, name="TickDataServer"
            )
            tick_thread.start()
            self._threads.append(tick_thread)
            logger.info("TickDataServer started")

        # Run BFF in the main thread (blocking) if enabled
        if self.bff:
            bff_cfg = self.roles.get("GridTraderBFF", {})
            host = bff_cfg.get("host", "localhost")
            port = bff_cfg.get("port", 5001)
            logger.info(f"Starting GridTrader BFF on {host}:{port}")
            self.bff.run()
        else:
            # If no BFF, just keep running
            # logger.info("No BFF - running in headless mode")
            while self._running:
                time.sleep(1)


def main():
    """Main entry point for GridTrader backend."""
    parser = argparse.ArgumentParser(
        description="GridTrader Backend Starter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Start with machine config (starts only roles enabled for that machine)
    python -m src.backend_starter --machine wsl2

    # Override default settings (replay mode)
    python -m src.backend_starter --machine wsl2 --replay_date 20260115 --suffix_id test

    # Override nested LLM config
    python -m src.backend_starter --machine mibuntu --llm.active_model kimi
    python -m src.backend_starter --machine mibuntu --llm.models.deepseek.thinking_mode true

    # Override role-specific settings
    python -m src.backend_starter --machine wsl2 --roles.GridTraderBFF.port 8080

    # Combine multiple overrides
    python -m src.backend_starter --machine wsl2 \\
        --replay_date 20260115 \\
        --suffix_id test \\
        --roles.GridTraderBFF.host 0.0.0.0

    # Dry run to see resolved config
    python -m src.backend_starter --machine wsl2 --dry-run

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
    starter = GridTraderBackendStarter(config=runtime_config)

    try:
        starter.run()
    except Exception as e:
        logger.error(f"GridTrader backend error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
