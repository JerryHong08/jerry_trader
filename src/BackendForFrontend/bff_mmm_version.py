"""
BFF (Backend For Frontend) for Market Mover Monitor
Flask + WebSocket server for real-time visualization.

This server connects to the backend services via Redis streams
and serves the frontend with real-time updates.

Usage:
    python src/live_monitor/market_mover_monitor/core/api/bff.py --replay-date 20260115 --suffix-id test

Note: Run backendStarter.py first to start the backend services.
"""

import argparse
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from threading import Thread
from typing import Dict, Optional

import redis
from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit

from ChartdataManager.overviewchartdataManager import ChartDataManager
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class BFF:
    """
    Backend For Frontend - serves the web interface and handles WebSocket connections.

    Connects to backend services via Redis streams:
    - market_snapshot_processed:{date} - For chart updates
    - movers_state:{date} - For state change notifications
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5000,
        replay_date: Optional[str] = None,
        suffix_id: Optional[str] = None,
        use_callback: bool = False,
    ):
        self.host = host
        self.port = port
        self.replay_date = replay_date
        self.suffix_id = suffix_id

        # If True, chart updates come via callback from SnapshotProcessor
        # If False, chart updates come from snapshot stream listener
        self.use_callback = use_callback

        # Setup Flask app
        current_dir = Path(__file__).parent  # core/api/
        front_dir = current_dir.parent.parent / "frontend"
        template_dir = front_dir / "templates"
        static_dir = front_dir / "static"

        self.app = Flask(
            __name__,
            template_folder=str(template_dir),
            static_folder=str(static_dir),
            static_url_path="/static",
        )
        self.app.config["SECRET_KEY"] = "market_mover_secret"

        self.socketio = SocketIO(
            self.app,
            cors_allowed_origins="*",
            async_mode="threading",
        )

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
        self.chart_manager = ChartDataManager(
            replay_date=replay_date,
            suffix_id=suffix_id,
        )

        # Connected clients tracking
        self.connected_clients = set()

        # Setup routes and socket events
        self._setup_routes()
        self._setup_socket_events()

        logger.info(
            f"BFF initialized: host={host}, port={port}, "
            f"replay_date={replay_date}, suffix_id={suffix_id}"
        )

    def _setup_routes(self):
        """Setup Flask routes."""

        @self.app.route("/")
        def index():
            return render_template("index.html")

        @self.app.route("/debug")
        def debug():
            return render_template("debug.html")

        @self.app.route("/simple")
        def simple():
            return render_template("simple.html")

        @self.app.route("/api/stock/<ticker>")
        def get_stock_detail(ticker):
            """API endpoint to get detailed stock information."""
            state = self.chart_manager.get_ticker_latest_state(ticker)
            history = self.chart_manager.get_ticker_history(ticker, limit=100)

            if state or history:
                return json.dumps(
                    {
                        "ticker": ticker,
                        "state": state,
                        "history": [
                            {
                                "timestamp": h["timestamp"].isoformat(),
                                "percent_change": h["percent_change"],
                                "current_price": h["current_price"],
                                "rank": h["rank"],
                            }
                            for h in history
                        ],
                    },
                    default=str,
                )
            else:
                return json.dumps({"error": "Stock not found"}), 404

        @self.app.route("/api/subscribed")
        def get_subscribed_tickers():
            """API endpoint to get all subscribed tickers."""
            tickers = self.chart_manager.get_subscribed_tickers()
            return json.dumps({"tickers": tickers, "count": len(tickers)})

        @self.app.route("/api/test-data")
        def test_data():
            """Test endpoint to check current data."""
            chart_data = self.chart_manager.get_mmm_version_chart_data()
            subscribed = self.chart_manager.get_subscribed_tickers()
            return {
                "datasets_count": len(chart_data.get("datasets", [])),
                "highlights_count": len(chart_data.get("highlights", [])),
                "subscribed_count": len(subscribed),
                "sample_dataset": (
                    chart_data.get("datasets", [{}])[0]
                    if chart_data.get("datasets")
                    else None
                ),
            }

        @self.app.route("/health")
        def health():
            """Health check endpoint."""
            try:
                self.r.ping()
                redis_status = "connected"
            except Exception:
                redis_status = "disconnected"

            return {
                "status": "ok",
                "redis": redis_status,
                "connected_clients": len(self.connected_clients),
                "replay_date": self.replay_date,
                "suffix_id": self.suffix_id,
            }

    def _setup_socket_events(self):
        """Setup WebSocket event handlers."""

        @self.socketio.on("connect")
        def handle_connect():
            logger.info(f"Client connected: {request.sid}")
            self.connected_clients.add(request.sid)

            # Send current chart data to new client
            chart_data = self.chart_manager.get_mmm_version_chart_data()
            emit("chart_update", chart_data)

        @self.socketio.on("disconnect")
        def handle_disconnect():
            logger.info(f"Client disconnected: {request.sid}")
            self.connected_clients.discard(request.sid)

        @self.socketio.on("request_stock_detail")
        def handle_stock_detail_request(data):
            """Handle request for detailed stock information."""
            ticker = data.get("ticker")
            if not ticker:
                emit("error", {"message": "No ticker specified"})
                return

            state = self.chart_manager.get_ticker_latest_state(ticker)
            history = self.chart_manager.get_ticker_history(ticker, limit=100)

            if state or history:
                emit(
                    "stock_detail",
                    {
                        "ticker": ticker,
                        "state": state,
                        "history_count": len(history),
                    },
                )
            else:
                emit("error", {"message": f"Stock {ticker} not found"})

        @self.socketio.on("refresh_chart")
        def handle_refresh_chart():
            """Handle request to force refresh chart data."""
            chart_data = self.chart_manager.get_mmm_version_chart_data(
                force_refresh=True
            )
            emit("chart_update", chart_data)

    def _start_snapshot_stream_listener(self):
        """Listen to processed snapshot stream for chart updates."""
        logger.info("Starting snapshot stream listener...")

        # Create consumer group for BFF
        try:
            self.r.xgroup_create(
                self.SNAPSHOT_STREAM_NAME,
                "bff_snapshot_consumers",
                id="0",
                mkstream=True,
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        consumer_name = f"bff_snapshot_{datetime.now().timestamp()}"

        while True:
            try:
                messages = self.r.xreadgroup(
                    "bff_snapshot_consumers",
                    consumer_name,
                    {self.SNAPSHOT_STREAM_NAME: ">"},
                    count=1,
                    block=2000,
                )

                if messages:
                    for stream_name, message_list in messages:
                        for message_id, message_data in message_list:
                            # Small delay to ensure InfluxDB write propagation
                            # This is a safety margin for eventual consistency
                            # time.sleep(0.1) # when live mode it's not necessary, but in replay mode it helps

                            # Refresh chart data
                            self.chart_manager.mark_dirty()
                            chart_data = self.chart_manager.get_mmm_version_chart_data(
                                force_refresh=True
                            )

                            # Log update info
                            timestamp = message_data.get("timestamp", "unknown")
                            logger.debug(
                                f"Snapshot update received: timestamp={timestamp}, "
                                f"datasets={len(chart_data.get('datasets', []))}"
                            )

                            # Emit to all connected clients
                            self.socketio.emit("chart_update", chart_data)

                            # Acknowledge the message
                            self.r.xack(
                                self.SNAPSHOT_STREAM_NAME,
                                "bff_snapshot_consumers",
                                message_id,
                            )

            except Exception as e:
                logger.error(f"Snapshot stream listener error: {e}")
                time.sleep(5)

    def _start_state_stream_listener(self):
        """Listen to state stream and broadcast state changes to WebSocket clients."""
        logger.info("Starting state stream listener...")

        # Create consumer group for BFF
        try:
            self.r.xgroup_create(
                self.STATE_STREAM_NAME, "bff_state_consumers", id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        consumer_name = f"bff_state_{datetime.now().timestamp()}"

        while True:
            try:
                messages = self.r.xreadgroup(
                    "bff_state_consumers",
                    consumer_name,
                    {self.STATE_STREAM_NAME: ">"},
                    count=10,
                    block=1000,
                )

                if messages:
                    for stream_name, message_list in messages:
                        for message_id, message_data in message_list:
                            # Log state change
                            logger.info(
                                f"State change: {message_data.get('symbol')} "
                                f"{message_data.get('from')} -> {message_data.get('to')}"
                            )

                            # Emit state change to clients
                            self.socketio.emit("state_change", message_data)

                            # Acknowledge the message
                            self.r.xack(
                                self.STATE_STREAM_NAME,
                                "bff_state_consumers",
                                message_id,
                            )

            except Exception as e:
                logger.error(f"State stream listener error: {e}")
                time.sleep(5)

    def run(self, debug: bool = False):
        """Run the BFF server."""
        logger.info("=" * 60)
        logger.info(f"Starting BFF on {self.host}:{self.port}")
        logger.info("=" * 60)

        if self.replay_date:
            logger.info(f"Mode: REPLAY (date={self.replay_date}, id={self.suffix_id})")
        else:
            logger.info("Mode: LIVE")

        # Start stream listeners in background threads
        # Only start snapshot listener if not using callback mode
        if not self.use_callback:
            snapshot_listener = Thread(
                target=self._start_snapshot_stream_listener, daemon=True
            )
            snapshot_listener.start()
            logger.info("Snapshot stream listener started")
        else:
            logger.info("Using callback mode - snapshot stream listener disabled")

        state_listener = Thread(target=self._start_state_stream_listener, daemon=True)
        state_listener.start()

        logger.info("Stream listeners started")
        logger.info(f"Web interface available at http://{self.host}:{self.port}")
        logger.info("=" * 60)

        # Run Flask-SocketIO server
        self.socketio.run(
            self.app,
            host=self.host,
            port=self.port,
            debug=debug,
            allow_unsafe_werkzeug=True,
        )

    def cleanup(self):
        """Clean up resources on shutdown."""
        if self.chart_manager:
            self.chart_manager.close()
        logger.info("BFF resources cleaned up")


def main():
    """Main entry point for BFF."""
    parser = argparse.ArgumentParser(
        description="Market Mover BFF (Backend For Frontend)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Live mode
    python bff.py

    # Replay mode
    python bff.py --replay-date 20260115 --suffix-id test

    # Custom host/port
    python bff.py --host 0.0.0.0 --port 8080
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
        default=5000,
        help="Port to bind to (default: 5000)",
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
    bff = BFF(
        host=args.host,
        port=args.port,
        replay_date=args.replay_date,
        suffix_id=args.suffix_id,
    )

    try:
        bff.run(debug=args.debug)
    except Exception as e:
        logger.error(f"BFF error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        bff.cleanup()


if __name__ == "__main__":
    main()
