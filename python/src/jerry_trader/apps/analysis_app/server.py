"""AnalysisBFF — REST API server for offline analysis.

Follows the same class-based pattern as ChartBFF, JerryTraderBFF, and AgentBFF.
"""

from __future__ import annotations

import logging

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from jerry_trader.apps.analysis_app.api.routes import router

logger = logging.getLogger("analysis_bff")


class AnalysisBFF:
    """REST API for offline analysis (classifier trace, factor lab, strategy eval)."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 5006,
        session_id: str = "",
        redis_config: dict | None = None,
        clickhouse_config: dict | None = None,
    ):
        self.host = host
        self.port = port
        self.session_id = session_id
        self._redis_config = redis_config
        self._ch_config = clickhouse_config

        # FastAPI app
        self.app = FastAPI(title="Jerry Trader Analysis API")
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        self.app.include_router(router, prefix="/api/analysis")

        # Store config on app state so routes can access it
        self.app.state.ch_config = clickhouse_config
        self.app.state.session_id = session_id

    def run(self, debug: bool = False) -> None:
        logger.info("=" * 60)
        logger.info(f"Starting AnalysisBFF on {self.host}:{self.port}")
        logger.info(f"Session ID: {self.session_id}")
        if self._ch_config:
            logger.info(f"ClickHouse: {self._ch_config.get('host', '?')}:{self._ch_config.get('port', '?')}")
        logger.info("=" * 60)

        uvicorn.run(
            self.app,
            host=self.host,
            port=self.port,
            log_level="debug" if debug else "info",
        )

    def cleanup(self) -> None:
        logger.info("AnalysisBFF: cleaned up")
