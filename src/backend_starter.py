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
from pathlib import Path
from threading import Thread
from typing import Any, Dict, Optional

import yaml

from utils.logger import setup_logger

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
    
    Redis/Postgres references in roles are resolved to their actual configs.
    e.g., role.redis: "redis-1-a" -> resolves to yaml_cfg["redis-1-a"]
    """
    if machine not in yaml_cfg.get("machines", {}):
        available = list(yaml_cfg.get("machines", {}).keys())
        raise ValueError(f"Unknown machine '{machine}'. Available: {available}")

    machine_cfg = yaml_cfg["machines"][machine]
    
    # Start with defaults (these become top-level in runtime config)
    runtime = copy.deepcopy(yaml_cfg.get("defaults", {}))
    
    # Add LLM config
    runtime["llm"] = copy.deepcopy(yaml_cfg.get("llm", {}))
    
    # Add enabled roles with their config, resolving redis/postgres references
    runtime["roles"] = {}
    for role_name, role_cfg in machine_cfg.get("roles", {}).items():
        if role_cfg.get("enabled", True):
            resolved_cfg = copy.deepcopy(role_cfg)
            
            # Resolve redis reference if present
            redis_ref = role_cfg.get("redis")
            if redis_ref and isinstance(redis_ref, str):
                if redis_ref in yaml_cfg:
                    resolved_cfg["redis"] = copy.deepcopy(yaml_cfg[redis_ref])
                else:
                    logger.warning(f"Redis config '{redis_ref}' not found for role {role_name}")
            
            # Resolve postgres reference if present
            postgres_ref = role_cfg.get("postgres")
            if postgres_ref and isinstance(postgres_ref, str):
                if postgres_ref in yaml_cfg:
                    resolved_cfg["postgres"] = copy.deepcopy(yaml_cfg[postgres_ref])
                    # pg_cfg = yaml_cfg[postgres_ref]
                    # resolved_cfg["postgres"] = {
                    #     "database_url": os.getenv(
                    #         pg_cfg.get("database_url_env", ""),
                    #         ""
                    #     )
                    # }
                else:
                    logger.warning(f"Postgres config '{postgres_ref}' not found for role {role_name}")
            
            runtime["roles"][role_name] = resolved_cfg
    
    # Apply CLI overrides using dot notation
    # Support both "replay_date" and "defaults.replay_date" for convenience
    for key_path, value in overrides.items():
        # Strip "defaults." prefix if present (for convenience)
        if key_path.startswith("defaults."):
            key_path = key_path[9:]  # Remove "defaults."
        set_nested_value(runtime, key_path, value)
    
    return runtime


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
        
        self._running = False
        self._services = []
        self._threads = []
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
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
                replay_date=self.replay_date,
                suffix_id=self.suffix_id,
                redis_config=role_cfg.get("redis"),
            )
            self._services.append(("GridTraderBFF", self.bff))
        else:
            self.bff = None

        if "SnapshotProcessor" in self.roles:
            from ComputeEngine.snapshotProcessor import SnapshotProcessor
            role_cfg = self.roles["SnapshotProcessor"]
            self.processor = SnapshotProcessor(
                replay_date=self.replay_date,
                suffix_id=self.suffix_id,
                load_history=self.load_history,
                redis_config=role_cfg.get("redis"),
            )
            self._services.append(("SnapshotProcessor", self.processor))
        else:
            self.processor = None

        if "StateEngine" in self.roles:
            from ComputeEngine.stateEngine import StateEngine
            role_cfg = self.roles["StateEngine"]
            self.state_engine = StateEngine(
                replay_date=self.replay_date,
                suffix_id=self.suffix_id,
                redis_config=role_cfg.get("redis"),
            )
            self._services.append(("StateEngine", self.state_engine))
        else:
            self.state_engine = None

        if "StaticDataWorker" in self.roles:
            from DataSupply.staticdataSupply.static_data_worker import StaticDataWorker
            role_cfg = self.roles["StaticDataWorker"]
            self.static_worker = StaticDataWorker(
                replay_date=self.replay_date,
                poll_interval=role_cfg.get("poll_interval", 1.0),
                batch_size=role_cfg.get("batch_size", 5),
                redis_config=role_cfg.get("redis"),
            )
            self._services.append(("StaticDataWorker", self.static_worker))
        else:
            self.static_worker = None

        if "NewsWorker" in self.roles:
            from DataSupply.staticdataSupply.news_worker import NewsWorker
            role_cfg = self.roles["NewsWorker"]
            self.news_worker = NewsWorker(
                replay_date=self.replay_date,
                poll_interval=role_cfg.get("poll_interval", 1.0),
                batch_size=role_cfg.get("batch_size", 5),
                news_limit=role_cfg.get("news_limit", 5),
                news_recency_hours=role_cfg.get("news_recency_hours", 24.0),
                redis_config=role_cfg.get("redis"),
                postgres_config=role_cfg.get("postgres"),
            )
            self._services.append(("NewsWorker", self.news_worker))
        else:
            self.news_worker = None

        if "NewsProcessor" in self.roles:
            from ComputeEngine.NewsProcessor import NewsProcessor
            role_cfg = self.roles["NewsProcessor"]
            llm_cfg = self.config.get("llm", {})
            self.news_processor = NewsProcessor(
                # llm_model=active_model,
                llm_config= llm_cfg,
                redis_config=role_cfg.get("redis"),
                postgres_config=role_cfg.get("postgres"),
            )
            self._services.append(("NewsProcessor", self.news_processor))
        else:
            self.news_processor = None

        logger.info(f"Initialized {len(self._services)} services")

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

        if self.bff:
            self.bff.cleanup()

        logger.info("All services cleaned up")

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
        logger.info("=" * 70)

        if self.replay_date:
            logger.info(f"Mode: REPLAY (date={self.replay_date}, id={self.suffix_id})")
        else:
            logger.info("Mode: LIVE")

        self._running = True

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
            if hasattr(self.news_processor, 'start'):
                self._start_async_worker_in_thread(self.news_processor, "NewsProcessor")
            logger.info("NewsProcessor started")

        # Run BFF in the main thread (blocking) if enabled
        if self.bff:
            bff_cfg = self.roles.get("GridTraderBFF", {})
            host = bff_cfg.get("host", "localhost")
            port = bff_cfg.get("port", 5001)
            logger.info(f"Starting GridTrader BFF on {host}:{port}")
            self.bff.run()
        else:
            # If no BFF, just keep running
            logger.info("No BFF - running in headless mode")
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
