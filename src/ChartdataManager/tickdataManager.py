import asyncio
import json
import os
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import redis
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from tickDataSupply.unified_tick_manager import UnifiedTickManager
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)

load_dotenv()
app = FastAPI()

# ============================================================================
# Unified Manager - Single point of configuration
# ============================================================================
manager = UnifiedTickManager()  # Reads DATA_MANAGER from env or defaults to polygon

logger.info(f"🚀 Using {manager.provider.upper()} data manager")

r = redis.Redis(host="localhost", port=6379, db=0)

stream_message_ids: Dict[str, str] = {}  # ticker -> message_id

if replay_date := os.getenv("REPLAY_DATE"):
    logger.info(f"Replay mode activated for date: {replay_date}")
    STREAM_NAME = f"factor_tasks_replay:{replay_date}"
else:
    today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
    STREAM_NAME = f"factor_tasks:{today}"


@app.on_event("startup")
async def startup_event():
    # Start streaming task for all managers (replayer also needs stream_forever)
    asyncio.create_task(manager.stream_forever())


# debug
@app.get("/debug/status")
async def debug_status():
    """check for all conneccted clients status"""
    all_client_symbols = {}
    for client, stream_keys in manager.connections.items():
        client_id = (
            f"client_{id(client)}" if hasattr(client, "__hash__") else str(client)
        )
        all_client_symbols[client_id] = list(stream_keys)

    return {
        "connected": manager.connected,
        "subscribed_streams": list(manager.subscribed_streams),
        "active_connections": len(manager.connections),
        "client_subscriptions": all_client_symbols,
        "queue_lengths": {
            stream: queue.qsize() for stream, queue in manager.queues.items()
        },
    }


@app.get("/debug/latest/{symbol}")
async def get_latest_quote(symbol: str, event: str = "Q"):
    """get symbol latest event data (default Q=quote). Use ?event=T for trades."""
    symbol = symbol.upper()
    stream_key = f"{event}.{symbol}"
    queue = manager.queues.get(stream_key)

    if not queue:
        return {"error": f"Stream {stream_key} not subscribed"}

    if queue.empty():
        return {"message": f"No data available for {stream_key}"}

    # get the latest event data
    try:
        latest_data = None
        while not queue.empty():
            latest_data = await asyncio.wait_for(queue.get(), timeout=0.1)

        return latest_data or {"message": "No data"}
    except asyncio.TimeoutError:
        return {"message": "No recent data"}


@app.get("/debug/subscribe/{symbol}")
@app.post("/debug/subscribe/{symbol}")
async def debug_subscribe(symbol: str, events: str = "Q"):
    """subscribe a symbol. Pass events comma-separated via ?events=Q,T"""
    symbol = symbol.upper()
    evs = [e.strip().upper() for e in events.split(",") if e.strip()]
    stream_message_ids[symbol] = r.xadd(
        STREAM_NAME, {"action": "add", "ticker": symbol}
    )
    print(f"Subscribing to {symbol} events={evs}")
    # await manager.subscribe("debug_client", [symbol], events=evs)
    return {"message": f"Subscribed to {symbol} events={evs}"}


@app.get("/debug/unsubscribe/{symbol}")
@app.post("/debug/unsubscribe/{symbol}")
async def debug_unsubscribe(symbol: str, events: str = "Q"):
    """unsubscribe a symbol. Pass events comma-separated via ?events=Q,T"""
    symbol = symbol.upper()
    evs = [e.strip().upper() for e in events.split(",") if e.strip()]
    print(f"Unsubscribing from {symbol} events={evs}")
    stream_message_ids[symbol] = r.xadd(
        STREAM_NAME, {"action": "remove", "ticker": symbol}
    )
    # await manager.unsubscribe("debug_client", symbol, events=evs)
    return {"message": f"Unsubscribed {symbol} events={evs}"}


@app.get("/debug", response_class=HTMLResponse)
async def debug_page():
    """Debug Page"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Polygon WebSocket Debug</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .container { max-width: 800px; }
            .section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; }
            button { margin: 5px; padding: 8px 16px; cursor: pointer; }
            #output { background: #f5f5f5; padding: 10px; height: 200px; overflow-y: auto; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🔍 Polygon WebSocket Debug</h1>

            <div class="section">
                <h3>📊 Status</h3>
                <button onclick="checkStatus()">Check Status</button>
                <div id="status"></div>
            </div>

            <div class="section">
                <h3>📡 Subscribe</h3>
                <input type="text" id="symbolInput" placeholder="Enter symbol (e.g., AAPL)" />
                <button onclick="subscribe()">Subscribe</button>
            </div>

            <div class="section">
                <h3>📡 Unsubscribe</h3>
                <input type="text" id="unsymbolInput" placeholder="Enter symbol (e.g., AAPL)" />
                <button onclick="unsubscribe()">Unsubscribe</button>
            </div>

            <div class="section">
                <h3>📈 Latest Quote</h3>
                <input type="text" id="quoteSymbol" placeholder="Symbol" />
                <button onclick="getLatestQuote()">Get Quote</button>
            </div>

            <div class="section">
                <h3>📋 Output</h3>
                <div id="output"></div>
                <button onclick="clearOutput()">Clear</button>
            </div>
        </div>

        <script>
            function log(message) {
                const output = document.getElementById('output');
                output.innerHTML += '<div>' + new Date().toLocaleTimeString() + ': ' + message + '</div>';
                output.scrollTop = output.scrollHeight;
            }

            async function checkStatus() {
                try {
                    const response = await fetch('/debug/status');
                    const data = await response.json();
                    document.getElementById('status').innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
                    log('✅ Status checked');
                } catch (error) {
                    log('❌ Error checking status: ' + error);
                }
            }

            async function subscribe() {
                const symbol = document.getElementById('symbolInput').value.toUpperCase();
                if (!symbol) {
                    log('❌ Please enter a symbol');
                    return;
                }

                try {
                    const response = await fetch(`/debug/subscribe/${symbol}`, { method: 'POST' });
                    const data = await response.json();
                    log(`📡 ${data.message}`);
                    document.getElementById('symbolInput').value = '';
                } catch (error) {
                    log('❌ Error subscribing: ' + error);
                }
            }

            async function unsubscribe() {
                const symbol = document.getElementById('unsymbolInput').value.toUpperCase();
                if (!symbol) {
                    log('❌ Please enter a symbol');
                    return;
                }

                try {
                    const response = await fetch(`/debug/unsubscribe/${symbol}`, { method: 'POST' });
                    const data = await response.json();
                    log(`📡 ${data.message}`);
                    document.getElementById('unsymbolInput').value = '';
                } catch (error) {
                    log('❌ Error unsubscribing: ' + error);
                }
            }

            async function getLatestQuote() {
                const symbol = document.getElementById('quoteSymbol').value.toUpperCase();
                if (!symbol) {
                    log('❌ Please enter a symbol');
                    return;
                }

                try {
                    const response = await fetch(`/debug/latest/${symbol}`);
                    const data = await response.json();
                    if (data.error || data.message) {
                        log(`ℹ️ ${data.error || data.message}`);
                    } else {
                        log(`📈 ${symbol}: Bid $${data.bid} x ${data.bid_size} | Ask $${data.ask} x ${data.ask_size}`);
                    }
                } catch (error) {
                    log('❌ Error getting quote: ' + error);
                }
            }

            function clearOutput() {
                document.getElementById('output').innerHTML = '';
            }

            // auto check status
            // checkStatus();
            // setInterval(checkStatus, 5000);
        </script>
    </body>
    </html>
    """


# websocket
@app.websocket("/ws/tickdata")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    consumer_tasks = {}
    current_streams = set()

    try:
        while True:
            msg = await websocket.receive_text()
            try:
                data = json.loads(msg)

                # ============================================================================
                # UNSUBSCRIBE HANDLING (Unified)
                # ============================================================================
                if data.get("action") == "unsubscribe":
                    subscriptions = data.get("subscriptions", [])

                    # Backward compatibility: single symbol unsubscribe
                    if not subscriptions and "symbol" in data:
                        subscriptions = [
                            {
                                "symbol": data.get("symbol"),
                                "events": data.get("events", ["Q"]),
                                "sec_type": data.get("sec_type", "STOCK"),
                                "contract": data.get("contract", {}),
                            }
                        ]

                    logger.info(f"📤 Unsubscribe request: {subscriptions}")

                    # Unsubscribe using unified interface
                    await manager.unsubscribe(websocket, subscriptions=subscriptions)

                    # Cancel consumer tasks for unsubscribed streams
                    unsubscribed_keys = manager.generate_stream_keys(
                        subscriptions=subscriptions
                    )
                    for stream_key in unsubscribed_keys:
                        if stream_key in current_streams:
                            consumer_tasks[stream_key].cancel()
                            del consumer_tasks[stream_key]
                            current_streams.discard(stream_key)

                    continue

                # ============================================================================
                # SUBSCRIBE HANDLING (Unified)
                # ============================================================================
                subscriptions = data.get("subscriptions", [])

                # Backward compatibility: legacy polygon format
                if not subscriptions:
                    symbols = data.get("symbols", [])
                    events = data.get("events", ["Q"])
                    if isinstance(symbols, str):
                        symbols = [s.strip() for s in symbols.split(",")]

                    subscriptions = [
                        {"symbol": sym, "events": events, "sec_type": "STOCK"}
                        for sym in symbols
                        if sym
                    ]

                if not subscriptions:
                    logger.warning("⚠️ No subscriptions found in message")
                    continue

                logger.info(f"📥 Subscribe request: {subscriptions}")

                # Subscribe using unified interface
                await manager.subscribe(websocket, subscriptions=subscriptions)

                # Generate stream keys and create consumer tasks
                new_streams = manager.generate_stream_keys(subscriptions=subscriptions)
                to_add = new_streams - current_streams

                for sk in to_add:
                    task = asyncio.create_task(consume_stream(websocket, sk))
                    consumer_tasks[sk] = task
                    logger.debug(f"📡 Created consumer task for {sk}")

                current_streams.update(to_add)

            except json.JSONDecodeError as e:
                logger.error(f"⚠️ JSON decode error: {e}")

    except WebSocketDisconnect:
        logger.info("🔌 WebSocket disconnected")
        await manager.disconnect(websocket)
        for task in consumer_tasks.values():
            task.cancel()


async def consume_stream(websocket: WebSocket, stream_key: str):
    """Consume a specific stream_key queue and push to frontend"""
    q = manager.queues.get(stream_key)
    if q is None:
        logger.warning(f"⚠️ No queue found for stream_key: {stream_key}")
        return

    logger.debug(f"🎧 Consumer started for {stream_key}")

    try:
        while True:
            data = await q.get()

            # Normalize data format for frontend using unified manager
            normalized_data = manager.normalize_data(data)

            await websocket.send_json(normalized_data)
    except asyncio.CancelledError:
        logger.debug(f"🛑 Consumer cancelled for {stream_key}")
    except Exception as e:
        logger.error(f"❌ Error in consumer for {stream_key}: {e}")
