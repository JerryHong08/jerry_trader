"""
GridTrader Backend Starter

Starts all backend services for GridTrader: SnapshotProcessor, StateEngine,
StaticDataWorker, and GridTrader BFF.

Usage:
    python -m src.BackendForFrontend.gridtrader_starter --replay-date 20260115 --suffix-id test

Note: Ensure Redis and InfluxDB are running before starting.
"""

import argparse
import asyncio
import logging
import signal
import sys
import time
from datetime import datetime
from threading import Thread
from typing import Optional

from BackendForFrontend.bff import GridTraderBFF
from ComputeEngine.snapshotProcessor import SnapshotProcessor
from ComputeEngine.stateEngine import StateEngine
from DataSupply.staticdataSupply.static_data_worker import StaticDataWorker
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.INFO)


class GridTraderBackendStarter:
    """
    Manages and starts all backend services for GridTrader.

    Services:
    - SnapshotProcessor: Receives data, processes, stores to InfluxDB and Redis
    - StateEngine: Computes ticker states and writes to state stream
    - StaticDataWorker: Fetches static data (fundamentals, float, news) for subscribed tickers
    - GridTraderBFF: Backend For Frontend (FastAPI + WebSocket server)
    """

    def __init__(
        self,
        replay_date: Optional[str] = None,
        suffix_id: Optional[str] = None,
        load_history: Optional[str] = None,
        host: str = "localhost",
        port: int = 5001,
        no_bff: bool = False,
        use_callback: bool = False,  # Default to False - use Redis stream listener for proper async WebSocket broadcast
    ):
        self.replay_date = replay_date
        self.suffix_id = suffix_id
        self.load_history = load_history
        self.host = host
        self.port = port
        self.no_bff = no_bff
        self.use_callback = use_callback

        self._running = False
        self._services = []
        self._static_worker_task = None
        self._event_loop = None

        # Initialize services
        self._init_services()

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _init_services(self):
        """Initialize all backend services."""
        logger.info("Initializing GridTrader backend services...")

        if not self.no_bff:
            self.bff = GridTraderBFF(
                host=self.host,
                port=self.port,
                replay_date=self.replay_date,
                suffix_id=self.suffix_id,
                use_callback=self.use_callback,
            )
            self._services.append(("GridTraderBFF", self.bff))
        else:
            self.bff = None

        # Create callback for SnapshotProcessor
        def on_snapshot_processed(result: dict, is_historical: bool):
            """Callback invoked after each snapshot is processed and written to InfluxDB."""
            if self.bff is None:
                return

            # Use BFF's callback method
            self.bff.on_snapshot_processed(result, is_historical)

        # Initialize SnapshotProcessor
        self.processor = SnapshotProcessor(
            replay_date=self.replay_date,
            suffix_id=self.suffix_id,
            load_history=self.load_history,
            on_snapshot_processed=on_snapshot_processed if self.use_callback else None,
        )
        self._services.append(("SnapshotProcessor", self.processor))

        # Initialize StateEngine
        self.state_engine = StateEngine(
            replay_date=self.replay_date,
            suffix_id=self.suffix_id,
        )
        self._services.append(("StateEngine", self.state_engine))

        # Initialize StaticDataWorker
        self.static_worker = StaticDataWorker(
            replay_date=self.replay_date,
            poll_interval=1.0,
            batch_size=5,
            news_limit=5,
            news_recency_hours=24.0,
        )
        self._services.append(("StaticDataWorker", self.static_worker))

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

        if hasattr(self, "processor") and self.processor:
            self.processor.close()

        if hasattr(self, "state_engine") and self.state_engine:
            self.state_engine.close()

        if hasattr(self, "static_worker") and self.static_worker:
            self.static_worker.stop()
            logger.info("StaticDataWorker stopped")

        if self._static_worker_task:
            self._static_worker_task.cancel()

        if hasattr(self, "bff") and self.bff:
            self.bff.cleanup()

        logger.info("All services cleaned up")

    def _start_static_worker_in_thread(self):
        """Start static worker in a separate thread with its own event loop."""

        def run_static_worker():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._event_loop = loop
            try:
                self._static_worker_task = loop.create_task(self.static_worker.start())
                loop.run_until_complete(self._static_worker_task)
            except asyncio.CancelledError:
                pass
            finally:
                loop.close()

        thread = Thread(target=run_static_worker, daemon=True)
        thread.start()
        return thread

    def run(self, debug: bool = False):
        """Run all backend services."""
        logger.info("=" * 70)
        logger.info("Starting GridTrader Backend")
        logger.info("=" * 70)

        if self.replay_date:
            logger.info(f"Mode: REPLAY (date={self.replay_date}, id={self.suffix_id})")
        else:
            logger.info("Mode: LIVE")

        self._running = True

        # Start SnapshotProcessor (it creates its own thread internally)
        self.processor.start()
        logger.info("SnapshotProcessor started")

        # Start StateEngine (it creates its own thread internally)
        self.state_engine.start()
        logger.info("StateEngine started")

        # Start StaticDataWorker (async, runs in separate thread)
        self._start_static_worker_in_thread()
        logger.info("StaticDataWorker started")

        # Run BFF in the main thread (blocking)
        if self.bff:
            logger.info(f"Starting GridTrader BFF on {self.host}:{self.port}")
            self.bff.run(debug=debug)
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
    # Live mode
    python -m src.BackendForFrontend.gridtrader_starter

    # Replay mode with historical data
    python -m src.BackendForFrontend.gridtrader_starter --replay-date 20260115 --suffix-id test

    # Load historical data and replay
    python -m src.BackendForFrontend.gridtrader_starter --load-history 20260115 --replay-date 20260115 --suffix-id test

    # Custom host/port
    python -m src.BackendForFrontend.gridtrader_starter --host 0.0.0.0 --port 8080
        """,
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Host to bind BFF to (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5001,
        help="Port to bind BFF to (default: 5001)",
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
    parser.add_argument(
        "--load-history",
        help="Load historical data from date (YYYYMMDD)",
    )
    parser.add_argument(
        "--no-bff",
        action="store_true",
        help="Run without BFF (headless mode)",
    )
    parser.add_argument(
        "--use-callback",
        action="store_true",
        help="Use callback mode instead of Redis stream listener (not recommended for WebSocket updates)",
    )

    args = parser.parse_args()

    # Create and run backend
    starter = GridTraderBackendStarter(
        host=args.host,
        port=args.port,
        replay_date=args.replay_date,
        suffix_id=args.suffix_id,
        load_history=args.load_history,
        no_bff=args.no_bff,
        use_callback=args.use_callback,  # Default is False (use stream listener)
    )

    try:
        starter.run(debug=args.debug)
    except Exception as e:
        logger.error(f"GridTrader backend error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
