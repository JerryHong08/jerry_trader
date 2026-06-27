"""FastAPI application for the Analysis BFF."""

from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from jerry_trader.apps.analysis_app.api.routes import router


def create_app() -> FastAPI:
    app = FastAPI(title="Jerry Trader Analysis API")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router, prefix="/api/analysis")

    return app


def start_server(host: str = "0.0.0.0", port: int = 5006) -> None:
    import uvicorn

    uvicorn.run(
        "jerry_trader.apps.analysis_app.api.server:create_app",
        host=host,
        port=port,
        factory=True,
        reload=False,
    )


app = create_app()
