# src/BFF/bff.py
"""
This module serves as Backend For Frontend (BFF) for the application.

the main tasks are(README.md Stage2):

- using figma make frontend code package.
  this frontend layout of trading system is gird layout, and each grid/module is seperated for easy to develop and customize.
  - Top Gainers/Rank List
  - Overview Chart
  - Order Management
  - Stock Detail
  - Portfolio
  - Chart

- migrate the backend data source to the frontend replacing the mock data.
  - start with building a backend for frontend(bff), then migrate data step by step.
  - first start with the Top Gainers data and Overview Chart data.
  - then Stock Detail. the data is driven by top gainers and backend data management, so it should use cache, and also the data request can be driven by the frontend button.
  - then the Portfolio and Order Management. this module is isolated, so it's easier to integrate.
  - the last one is the Chart module, based on tradingview lightweight chart, the focus is balance between the real-time update and historical data retrival.

"""

# redis pub/sub streams and corresponding socket.io events
# stream:rank_updates
# stream:ticker_state_updates
# stream:candle_updates:{symbol}:{timeframe}

# @socketio.on("connect")
# @socketio.on("disconnect")

# @socketio.on("subscribe_rank")
"""frontend subscribes to rank updates when the Top Gainers module is mounted"""
# @socketio.on("unsubscribe_rank")
"""frontend unsubscribes from rank updates when the Top Gainers module is unmounted"""

# if not any_client_subscribed("rank"):
#     skip_emit()
# else:
# socketio.emit("rank_snapshot")
"""
emit when rank_updates stream sends a new snapshot and only when there is at least one client subscribed
data example:
{

}
"""

# socketio.emit("rank_update")

# socketio.emit("ticker_state_update") # rank list state column
"""automatically emitted when there is a change in the ticker state (e.g., trading halts)"""

# @socketio.on("request_news")
# @socketio.on("request_fundamental")

# socketio.emit("news_result")
# socketio.emit("fundamental_result")

# @socketio.on("subscribe_chart")
# @socketio.on("unsubscribe_chart")

# socketio.emit("chart_snapshot")

"""
data example:
{
  "symbol": "AAPL",
  "timeframe": "1m",
  "candles": [...]
}
"""
# socketio.emit("chart_candle_update")

"""
data example:
    {
    "symbol": "AAPL",
    "timeframe": "1m",
    "candle": {
        "time": 1700000000,
        "open": 190.1,
        "high": 190.5,
        "low": 189.9,
        "close": 190.3,
        "volume": 120300
    }
    }
"""
