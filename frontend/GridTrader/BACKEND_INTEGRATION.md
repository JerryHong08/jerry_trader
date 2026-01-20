# GridTrader Backend Integration

This document describes how to connect the GridTrader frontend to the Python backend via the BFF (Backend For Frontend).

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         GridTrader Frontend                              │
│  ┌──────────────┐  ┌───────────────────┐  ┌─────────────────────────┐   │
│  │   RankList   │  │ OverviewChartModule│  │    Other Modules        │   │
│  └──────┬───────┘  └─────────┬─────────┘  └───────────┬─────────────┘   │
│         │                    │                        │                  │
│         └────────────────────┼────────────────────────┘                  │
│                              ▼                                           │
│                    ┌─────────────────────┐                              │
│                    │   useWebSocket.ts   │                              │
│                    │ (Native WebSocket)  │                              │
│                    └──────────┬──────────┘                              │
└───────────────────────────────┼─────────────────────────────────────────┘
                                │ WebSocket (ws://localhost:5001/ws/{client_id})
                                ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                       GridTrader BFF (Python FastAPI)                      │
│  ┌───────────────────────────────────────────────────────────────────┐   │
│  │                    bff.py                               │   │
│  │  - FastAPI + Native WebSocket (real-time updates)                  │   │
│  │  - REST API endpoints                                               │   │
│  │  - Redis Stream listeners                                           │   │
│  │  - uvicorn server                                                   │   │
│  └────────────────────────────────┬──────────────────────────────────┘   │
│                                   │                                       │
│  ┌────────────────────────────────▼──────────────────────────────────┐   │
│  │              overviewchartdataManager.py                           │   │
│  │  - InfluxDB queries for chart data                                 │   │
│  │  - Segment calculation for state-colored lines                     │   │
│  │  - Rank list data formatting                                        │   │
│  └───────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                     Backend Services                                       │
│  ┌──────────────────┐  ┌──────────────┐  ┌─────────────────────────────┐ │
│  │ SnapshotProcessor│  │  StateEngine │  │   Redis & InfluxDB          │ │
│  └──────────────────┘  └──────────────┘  └─────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Start Backend Services

```bash
# Make sure Redis and InfluxDB are running first

# Start all backend services (live mode)
python -m src.BackendForFrontend.backend_starter

# Or with replay mode (historical data)
python -m src.BackendForFrontend.backend_starter \
  --replay-date 20260115 \
  --suffix-id test \
  --load-history 20260115
```

### 2. Start Frontend

```bash
cd frontend/GridTrader
pnpm install  # First time only
pnpm dev
```

The frontend will connect to `ws://localhost:5001/ws/{client_id}` by default.

### 3. Configure Custom BFF URL (Optional)

Create a `.env` file in `frontend/GridTrader/`:

```env
VITE_BFF_URL=http://your-server:5001
```

## Data Flow

### RankList Updates

1. **Backend**: `SnapshotProcessor` receives market data and writes to Redis stream
2. **BFF**: Listens to `market_snapshot_processed:{date}` stream
3. **BFF**: Reads latest data from Redis and formats for frontend
4. **Frontend**: Receives `rank_list_update` message via WebSocket
5. **Frontend**: `RankList` component updates with new data

### OverviewChart Updates

1. **Backend**: State changes are computed by `StateEngine`
2. **BFF**: Queries InfluxDB for historical data with state segments
3. **BFF**: Calculates segment info (colored line segments per ticker)
4. **Frontend**: Receives `overview_chart_update` message via WebSocket
5. **Frontend**: `OverviewChartModule` renders multi-colored lines

## WebSocket Messages

The frontend uses native WebSocket to communicate with the FastAPI backend.

### Client → Server (JSON messages)

| Message Type | Payload | Description |
|--------------|---------|-------------|
| `subscribe_market_snapshot` | `{}` | Subscribe to rank list and chart updates |
| `unsubscribe_market_snapshot` | `{}` | Unsubscribe from updates |
| `request_stock_detail` | `{ ticker: string }` | Request detailed data for a ticker |
| `request_news` | `{ ticker: string }` | Request news for a ticker |
| `request_fundamental` | `{ ticker: string }` | Request fundamental data |
| `refresh_chart` | `{}` | Force refresh chart data |
| `refresh_rank_list` | `{}` | Force refresh rank list data |

### Server → Client (JSON messages)

| Message Type | Description |
|--------------|-------------|
| `rank_list_update` | Updated rank list data |
| `overview_chart_update` | Updated chart data with segments |
| `state_change` | Real-time state transition notification |
| `stock_detail` | Detailed stock data response |
| `news_result` | News data response |
| `fundamental_result` | Fundamental data response |
| `error` | Error message |

## Data Formats

### RankItem (Frontend Type)

```typescript
interface RankItem {
  symbol: string;
  price: number;
  change: number;
  changePercent: number;
  volume: number;
  marketCap: number;
  state: TickerState;
  float: number;
  relativeVolumeDaily: number;
  relativeVolume5min: number;
  latestNewsTime?: number;
}

type TickerState = 'Best' | 'Good' | 'OnWatch' | 'NotGood' | 'Bad';
```

### OverviewChartData (Backend → Frontend)

```typescript
interface OverviewChartData {
  data: Array<{
    date: string;         // Time label (e.g., "9:30")
    timestamp: number;    // Epoch milliseconds
    // For each ticker:
    [symbol_value]: number;    // e.g., AAPL_value
    [symbol_state]: TickerState; // e.g., AAPL_state
    [symbol_segN]: number | null; // e.g., AAPL_seg0, AAPL_seg1
  }>;
  segmentInfo: {
    [symbol: string]: Array<{
      key: string;      // e.g., "AAPL_seg0"
      color: string;    // e.g., "#10b981"
      startIdx: number;
      endIdx: number;
    }>;
  };
  rankData: RankItem[];
  timestamp: string | null;
}
```

## State Color Mapping

| Frontend State | Color | Backend States |
|---------------|-------|----------------|
| `Best` | Green (#10b981) | `rising_fast`, `rising` |
| `Good` | Purple (#a855f7) | `new_entrant` |
| `OnWatch` | Blue (#3b82f6) | `stable` |
| `NotGood` | Yellow (#eab308) | `falling` |
| `Bad` | Gray (#6b7280) | `falling_fast` |

## Mock/Live Toggle

Both `RankList` and `OverviewChartModule` include a toggle button to switch between:
- **Live**: Real data from backend via WebSocket
- **Mock**: Generated mock data for testing UI

The connection status is shown with a WiFi icon:
- 🟢 Green: Connected
- 🟡 Yellow: Connecting
- 🔴 Red: Error
- ⚪ Gray: Disconnected

## Troubleshooting

### Frontend can't connect to backend

1. Check BFF is running: `curl http://localhost:5001/health`
2. Check CORS settings in BFF
3. Verify `VITE_BFF_URL` environment variable

### No data appearing

1. Check Redis is running: `redis-cli ping`
2. Check InfluxDB is running
3. Verify data exists in streams: `redis-cli XLEN market_snapshot_processed:YYYYMMDD`

### Chart not updating

1. Check console for WebSocket connection status
2. Verify backend logs show data processing
3. Try clicking the refresh button (↻)

## Files Created/Modified

### New Files
- `src/BackendForFrontend/bff.py` - GridTrader BFF server (FastAPI + WebSocket)
- `src/BackendForFrontend/backend_starter.py` - Backend starter script
- `src/ChartdataManager/overviewchartdataManager.py` - Chart data manager
- `frontend/GridTrader/src/hooks/useWebSocket.ts` - Native WebSocket hook
- `frontend/GridTrader/src/vite-env.d.ts` - Vite type declarations

### Modified Files
- `frontend/GridTrader/src/hooks/useBackendTimestamps.ts` - Added exports for integration
- `frontend/GridTrader/src/components/RankList.tsx` - WebSocket integration
- `frontend/GridTrader/src/components/OverviewChartModule.tsx` - WebSocket integration

## REST API Endpoints

In addition to WebSocket, the FastAPI BFF provides REST endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Service info |
| `/health` | GET | Health check with Redis status |
| `/api/rank-list` | GET | Get current rank list data |
| `/api/overview-chart` | GET | Get overview chart data |
| `/api/stock/{ticker}` | GET | Get stock detail |
| `/api/subscribed` | GET | Get all subscribed tickers |
| `/api/test-data` | GET | Test endpoint to check data status |
