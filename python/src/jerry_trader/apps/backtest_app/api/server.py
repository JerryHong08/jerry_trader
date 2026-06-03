"""Backtest Visualization API Server - FastAPI application entry point."""

import asyncio
import os
from contextlib import asynccontextmanager

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from jerry_trader.apps.backtest_app.api.routes import router
from jerry_trader.apps.backtest_app.api.websocket import websocket_router
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger(__name__)

load_dotenv(override=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    logger.info("Starting Backtest Visualization API")

    # Initialize ClickHouse table if needed
    from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

    ch = get_clickhouse_client()
    if ch:
        try:
            # Create table if not exists
            ch.command(
                """
                CREATE TABLE IF NOT EXISTS backtest_signals
                (
                    experiment_id String,
                    date Date,
                    ticker String,
                    event_name String,
                    entry_time DateTime64(3),
                    entry_price Float64,
                    exit_time DateTime64(3),
                    exit_price Float64,
                    return_pct Float64,
                    exit_reason String,
                    factors String,
                    max_price Float64,
                    min_price Float64,
                    time_to_max_ms Int64,
                    time_to_min_ms Int64,
                    created_at DateTime64(3) DEFAULT now64(3)
                )
                ENGINE = MergeTree()
                PARTITION BY toYYYYMM(date)
                ORDER BY (experiment_id, date, ticker, entry_time)
            """
            )
            logger.info("ClickHouse backtest_signals table ready")
        except Exception as e:
            logger.warning(f"Could not create backtest_signals table: {e}")

    yield

    # Cleanup
    logger.info("Shutting down Backtest Visualization API")


app = FastAPI(
    title="Backtest Visualization API",
    description="Real-time backtest execution and visualization",
    version="1.0.0",
    lifespan=lifespan,
)


# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Include routers
app.include_router(router, prefix="/api/backtest")
app.include_router(websocket_router)


@app.get("/")
def root():
    """Server status."""
    return {
        "status": "ok",
        "service": "Backtest Visualization API",
        "version": "1.0.0",
    }


@app.get("/health")
def health():
    """Health check."""
    from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

    ch = get_clickhouse_client()
    return {
        "status": "ok",
        "clickhouse": ch is not None,
    }


def start_server(host: str = "0.0.0.0", port: int = 5005):
    """Start the server."""
    print(f"\n🌐 Starting Backtest Visualization API on http://{host}:{port}")
    print(f"📚 API Documentation: http://{host}:{port}/docs")
    print(f"🔌 WebSocket: ws://{host}:{port}/ws/backtest\n")

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
    )


if __name__ == "__main__":
    start_server()
