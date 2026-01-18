# Factor Engine

## Overview

The Factor Engine is a real-time market data processor that computes trading factors from streaming quote and trade data. It supports multiple data sources (Polygon, ThetaData, Replayer) with a unified interface.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Factor Engine                         │
│                                                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │         FactorSubscriptionAdapter                 │  │
│  │  • Translate subscriptions to provider format    │  │
│  │  • Generate stream keys                          │  │
│  │  • Normalize data formats                        │  │
│  └──────────────────────────────────────────────────┘  │
│                         │                                │
│         ┌───────────────┼───────────────┐               │
│         ▼               ▼               ▼               │
│  ┌──────────┐   ┌─────────────┐   ┌──────────┐        │
│  │ Polygon  │   │  ThetaData  │   │ Replayer │        │
│  │ Manager  │   │   Manager   │   │ Manager  │        │
│  └──────────┘   └─────────────┘   └──────────┘        │
│                                                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │          Ticker Context (per symbol)              │  │
│  │  • Quote window (circular buffer)                │  │
│  │  • Trade window (circular buffer)                │  │
│  │  • Aggressor detection                           │  │
│  │  • Belief state computation                      │  │
│  └──────────────────────────────────────────────────┘  │
│                                                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │         Factor Computation (ThreadPool)           │  │
│  │  • Trade rate                                     │  │
│  │  • Trade acceleration                            │  │
│  │  • Aggressiveness                                │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

## Features

### Multi-Source Support
- **Polygon.io**: Real-time market data
- **ThetaData**: Historical and real-time options/equities data
- **Replayer**: Replay historical data from local server

### Data Processing
- **Quote Processing**: Tracks bid/ask spreads in rolling window
- **Trade Processing**: Monitors trade flow and identifies aggressor side
- **Belief Dynamics**: Computes trade rate, acceleration, and aggressiveness

### Extensibility
- Clean adapter pattern for adding new data sources
- Unified data format for processing
- Modular factor computation

## Usage

### Basic Usage

```bash
# Use default data source (from DATA_MANAGER env var or polygon)
python -m src.FactorEngine.factor_engine

# Specify data source explicitly
python -m src.FactorEngine.factor_engine --manager-type polygon
python -m src.FactorEngine.factor_engine --manager-type theta
python -m src.FactorEngine.factor_engine --manager-type replayer

# Replay mode with specific date
python -m src.FactorEngine.factor_engine --replay-date 20240115 --manager-type replayer
```

### Environment Variables

Create a `.env` file or set these environment variables:

```bash
# Data source type (polygon, theta, replayer)
DATA_MANAGER=polygon

# Polygon configuration
POLYGON_API_KEY=your_api_key_here

# Replayer configuration (for historical replay)
REPLAY_URL=ws://127.0.0.1:8765

# Redis configuration
REDIS_HOST=localhost
REDIS_PORT=6379
```

### Redis Integration

The Factor Engine listens to Redis Streams for ticker subscription requests:

```python
import redis

redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Add a ticker to track
redis_client.xadd(
    'factor_tasks:20241219',  # Stream name (date-based)
    {
        'action': 'add',
        'ticker': 'AAPL'
    }
)

# Remove a ticker
redis_client.xadd(
    'factor_tasks:20241219',
    {
        'action': 'remove',
        'ticker': 'AAPL'
    }
)
```

## Data Format

### Unified Tick Data Format

All data sources are normalized to this format:

**Quote Event:**
```python
{
    "event_type": "Q",
    "symbol": "AAPL",
    "bid": 180.50,
    "ask": 180.52,
    "bid_size": 100,
    "ask_size": 200,
    "timestamp": 1702998400000  # milliseconds
}
```

**Trade Event:**
```python
{
    "event_type": "T",
    "symbol": "AAPL",
    "price": 180.51,
    "size": 50,
    "timestamp": 1702998400000  # milliseconds
}
```

## Computed Factors

### Belief Dynamics

The engine computes the following factors based on recent trade activity:

- **trade_rate**: Number of trades per unit time (inverse of mean interval)
- **accel**: Trade acceleration (change in trade frequency)
- **aggressiveness**: Net buying/selling pressure (-1 to 1)

Example output:
```python
{
    "trade_rate": 15.3,      # trades per second
    "accel": 0.0523,         # acceleration
    "aggressiveness": 0.65   # 65% buy-side aggression
}
```

## Adding New Data Sources

To add a new data source:

1. **Create a new manager** in `src/DataSupply/` (e.g., `new_source_manager.py`)
2. **Update the adapter** in `factor_engine.py`:

```python
@staticmethod
def to_new_source(symbol: str, events: list) -> Any:
    """Convert to new source format"""
    # Implement conversion logic
    pass

@staticmethod
def generate_stream_keys(...):
    # Add case for new source
    elif manager_type == "new_source":
        stream_keys.append(f"{ev}.{symbol}")
```

3. **Add normalization** in `normalize_tick_data()`:

```python
if manager_type == "new_source":
    # Convert to unified format
    return {...}
```

4. **Update manager initialization**:

```python
elif self.manager_type == "new_source":
    self.ws_manager = NewSourceManager(...)
```

## Performance Considerations

- **Circular Buffers**: Quote and trade windows use circular buffers (max 1000 items)
- **Thread Pool**: Factor computation runs in ThreadPool to avoid blocking I/O
- **Asyncio Loop**: WebSocket data streaming runs in separate event loop
- **Lock-free Reading**: Most operations are read-heavy, locks only during updates

## Troubleshooting

### No data received
- Check DATA_MANAGER env var is set correctly
- Verify API keys are configured
- Check websocket connection in logs

### Missing factors
- Ensure at least 20 trades have occurred (required for belief dynamics)
- Check that both quotes and trades are being received

### Redis connection issues
- Verify Redis is running: `redis-cli ping`
- Check Redis host/port configuration

## See Also

- [Tickdata Server](../ChartdataManager/tickdataManager.py) - Similar multi-source architecture for frontend
- [DataSupply Managers](../DataSupply/) - Individual data source implementations
