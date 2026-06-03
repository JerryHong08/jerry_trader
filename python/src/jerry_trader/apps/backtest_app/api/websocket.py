"""WebSocket handler for backtest progress."""

import logging
from typing import Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)

websocket_router = APIRouter()
websocket_clients: Set[WebSocket] = set()


@websocket_router.websocket("/ws/backtest")
async def backtest_websocket(websocket: WebSocket):
    """WebSocket endpoint for real-time backtest progress.

    Client receives:
    - progress: step and percent updates
    - signal: new signal found (ticker, entry_time, entry_price)
    - error: error message
    - complete: experiment completed
    """
    await websocket.accept()
    websocket_clients.add(websocket)

    logger.info(f"WebSocket client connected (total: {len(websocket_clients)})")

    try:
        # Send welcome
        await websocket.send_json(
            {
                "type": "connection",
                "message": "Connected to Backtest WebSocket",
            }
        )

        # Keep connection alive
        while True:
            data = await websocket.receive_text()
            # Handle client messages (e.g., heartbeat)
            if data == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        websocket_clients.discard(websocket)
        logger.info(f"WebSocket client disconnected (total: {len(websocket_clients)})")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        websocket_clients.discard(websocket)


def broadcast_to_clients(message: dict):
    """Broadcast message to all connected WebSocket clients."""
    import asyncio

    for ws in websocket_clients:
        try:
            asyncio.create_task(ws.send_json(message))
        except Exception:
            websocket_clients.discard(ws)
