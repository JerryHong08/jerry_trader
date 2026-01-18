"""
Backend Starter for Market Mover Monitor
Starts all backend services: SnapshotProcessor, StateEngine, and BFF.

Usage:
    python src/live_monitor/market_mover_monitor/core/api/backendStarter.py --load-history 20260115 --replay-date 20260115 --suffix-id test
"""

import argparse
import logging
import signal
import sys
import time
from datetime import datetime
from threading import Thread
from typing import Optional

from BackendForFrontend.bff_mmm_version import BFF
from ComputeEngine.snapshotProcessor import SnapshotProcessor
from ComputeEngine.stateEngine import StateEngine
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.INFO)


class BackendStarter:
    """
    Manages and starts all backend services for Market Mover Monitor.

    Services:
    - SnapshotProcessor: Receives data, processes, stores to InfluxDB and Redis
    - StateEngine: Computes ticker states and writes to state stream
    - BFF: Backend For Frontend (Flask + WebSocket server)
    """

    def __init__(
        self,
        replay_date: Optional[str] = None,
        suffix_id: Optional[str] = None,
        load_history: Optional[str] = None,
        host: str = "localhost",
        port: int = 5000,
        no_bff: bool = False,
        no_use_callback: bool = True,
    ):
        self.replay_date = replay_date
        self.suffix_id = suffix_id
        self.load_history = load_history
        self.host = host
        self.port = port
        self.no_bff = no_bff
        self.use_callback = not no_use_callback

        self._running = False
        self._services = []

        # Initialize services
        self._init_services()

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _init_services(self):
        """Initialize all backend services."""
        logger.info("Initializing backend services...")

        # Initialize BFF first (unless disabled) - needed for callback
        if not self.no_bff:
            self.bff = BFF(
                host=self.host,
                port=self.port,
                replay_date=self.replay_date,
                suffix_id=self.suffix_id,
                use_callback=self.use_callback,  # Use callback for chart updates instead of stream listener
            )
            self._services.append(("BFF", self.bff))
        else:
            self.bff = None

        # Create callback for SnapshotProcessor
        # This ensures chart data is refreshed and emitted synchronously after InfluxDB write
        def on_snapshot_processed(result: dict, is_historical: bool):
            """Callback invoked after each snapshot is processed and written to InfluxDB."""
            if self.bff is None:
                return

            # Mark chart data as dirty to trigger refresh
            self.bff.chart_manager.mark_dirty()

            # Get chart data - InfluxDB write is already complete at this point
            chart_data = self.bff.chart_manager.get_mmm_version_chart_data(
                force_refresh=True
            )

            logger.debug(
                f"on_snapshot_processed - Snapshot processed: "
                f"{result.get('new_subscriptions', [])} new subs, "
                f"{result.get('total_subscribed', 0)} total, "
                f"datasets={len(chart_data.get('datasets', []))}"
            )

            # Emit to all connected WebSocket clients
            self.bff.socketio.emit("chart_update", chart_data)

        # Initialize SnapshotProcessor with callback
        self.snapshot_processor = SnapshotProcessor(
            replay_date=self.replay_date,
            suffix_id=self.suffix_id,
            load_history=self.load_history,
            on_snapshot_processed=on_snapshot_processed if self.bff else None,
        )
        self._services.append(("SnapshotProcessor", self.snapshot_processor))

        # Initialize StateEngine
        self.state_engine = StateEngine(
            replay_date=self.replay_date,
            suffix_id=self.suffix_id,
        )
        self._services.append(("StateEngine", self.state_engine))
        logger.info(f"Initialized {len(self._services)} backend services")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.stop()
        sys.exit(0)

    def start(self):
        """Start all backend services."""
        logger.info("=" * 60)
        logger.info("Starting Market Mover Backend Services")
        logger.info("=" * 60)

        if self.replay_date:
            logger.info(f"Mode: REPLAY (date={self.replay_date}, id={self.suffix_id})")
        else:
            logger.info("Mode: LIVE")

        if self.load_history:
            logger.info(f"Loading historical data for: {self.load_history}")

        self._running = True

        # Start SnapshotProcessor
        logger.info("Starting SnapshotProcessor...")
        self.snapshot_processor.start()

        # Start StateEngine
        logger.info("Starting StateEngine...")
        self.state_engine.start()

        # Start BFF (runs Flask server - this blocks or runs in thread)
        if self.bff:
            logger.info(f"Starting BFF on {self.host}:{self.port}...")
            # Run BFF in a separate thread so we can still handle signals
            self._bff_thread = Thread(target=self._run_bff, daemon=True)
            self._bff_thread.start()
            logger.info(f"Web interface available at http://{self.host}:{self.port}")

        logger.info("=" * 60)
        logger.info("All backend services started successfully")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 60)

        # Keep main thread alive
        self._run_forever()

    def _run_bff(self):
        """Run BFF server in a thread."""
        try:
            self.bff.run(debug=False)
        except Exception as e:
            logger.error(f"BFF error: {e}")

    def _run_forever(self):
        """Keep the main thread running with periodic health checks."""
        check_interval = 60  # seconds

        while self._running:
            try:
                time.sleep(check_interval)
                self._health_check()
            except KeyboardInterrupt:
                break

    def _health_check(self):
        """Perform periodic health check on services."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.debug(f"[{timestamp}] Health check - all services running")

    def stop(self):
        """Stop all backend services gracefully."""
        logger.info("Stopping backend services...")
        self._running = False

        # Close services in reverse order
        for name, service in reversed(self._services):
            try:
                if hasattr(service, "close"):
                    logger.info(f"Closing {name}...")
                    service.close()
            except Exception as e:
                logger.error(f"Error closing {name}: {e}")

        logger.info("All backend services stopped")


def main():
    """Main entry point for backend starter."""
    parser = argparse.ArgumentParser(
        description="Market Mover Backend Starter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Live mode (starts all services including BFF)
    python backendStarter.py

    # Replay mode with historical data loading
    python backendStarter.py --load-history 20260115 --replay-date 20260115 --suffix-id test

    # Custom BFF host/port
    python backendStarter.py --host 0.0.0.0 --port 8080

    # Backend only (no BFF)
    python backendStarter.py --no-bff --replay-date 20260115
        """,
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
        help="Load historical data for date (YYYYMMDD)",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Host for BFF server (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="Port for BFF server (default: 5000)",
    )
    parser.add_argument(
        "--no-bff",
        action="store_true",
        help="Disable BFF server (run backend only)",
    )
    parser.add_argument(
        "--no-use-callback",
        action="store_true",
        help="whether to use callback for chart updates instead of stream listener",
    )

    args = parser.parse_args()

    # Create and start backend
    backend = BackendStarter(
        replay_date=args.replay_date,
        suffix_id=args.suffix_id,
        load_history=args.load_history,
        host=args.host,
        port=args.port,
        no_bff=args.no_bff,
        no_use_callback=args.no_use_callback,
    )

    try:
        backend.start()
    except Exception as e:
        logger.error(f"Backend starter error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        backend.stop()


if __name__ == "__main__":
    main()
