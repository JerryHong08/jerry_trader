# Replayer Controller Design

**Created:** 2026-04-07
**Related:** Task 9.9, 9.9.1

## Overview

ReplayerController wraps Replayer to provide playback control with proper state cleanup on seek.

## Components

```
┌─────────────────────────────────────────────────────────────────┐
│                     ReplayerController                          │
│                                                                 │
│  - Play/Pause/Stop                                              │
│  - Seek (forward/backward with cleanup)                         │
│  - Get time range / current position                            │
│  - WebSocket broadcast                                          │
│                                                                 │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Replayer                                  │
│                                                                 │
│  Data source: ClickHouse market_snapshot_collector              │
│  Output: Redis INPUT Stream (market_snapshot_stream)            │
│                                                                 │
│  Methods:                                                       │
│    - get_time_range() -> (start_ms, end_ms)                     │
│    - seek_to(target_ms)                                          │
│    - start() / stop()                                            │
│                                                                 │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                  State Cleanup (on seek backward)               │
│                                                                 │
│  1. Redis INPUT Stream   → DELETE                               │
│  2. Redis OUTPUT Stream  → DELETE                               │
│  3. Redis subscribed_set → DELETE                               │
│  4. Redis state_cursor    → DELETE                              │
│  5. Processor.reset()                                           │
│  6. BFF.chart_manager.clear()                                   │
│  7. WS broadcast: {"type": "replay_reset"}                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## API

### ReplayerController

```python
class ReplayerController:
    """Playback controller with state management."""

    def __init__(
        self,
        session_id: str,
        redis_client: redis.Redis,
        ch_client: clickhouse_connect.driver.Client,
        processor: SnapshotProcessor,
        bff_chart_manager: JerryTraderChartDataManager,
        ws_broadcaster: Callable[[dict], Awaitable[None]],
    ): ...

    # Query
    def get_time_range(self) -> Tuple[int, int]:
        """Return (start_ms, end_ms) of available data."""

    def get_current_position(self) -> Optional[int]:
        """Return current playback position in epoch ms."""

    def get_status(self) -> Literal["idle", "playing", "paused"]:
        """Return playback status."""

    # Control
    async def seek_to(self, target_ms: int) -> Dict:
        """Seek to target time.

        If target < current_position: clears all downstream state.
        Returns: {"success": bool, "position_ms": int, "tickers": int}
        """

    async def play(self, speed: float = 1.0) -> None:
        """Start playback from current position."""

    async def pause(self) -> None:
        """Pause playback."""

    async def stop(self) -> None:
        """Stop playback and reset position."""

    # Internal
    async def _cleanup_state(self) -> None:
        """Clear all downstream state on seek backward."""
```

### WebSocket Messages

**Server → Client:**

```json
// On seek
{"type": "replay_reset", "target_ms": 1772790000000, "reason": "seek_backward"}

// On position update (every second)
{"type": "replay_position", "current_ms": 1772790050000, "start_ms": 1772787600000, "end_ms": 1772807400000}

// On status change
{"type": "replay_status", "status": "playing", "speed": 1.0}
```

**Client → Server:**

```json
// Request seek
{"type": "replay_seek", "target_ms": 1772790000000}

// Request play
{"type": "replay_play", "speed": 1.0}

// Request pause
{"type": "replay_pause"}
```

## State Cleanup Details

### Seek Backward Flow

```python
async def seek_to(self, target_ms: int) -> Dict:
    current = self._current_position_ms

    # 1. Stop playback
    self.replayer.stop()

    # 2. If going backward, cleanup all state
    if current is not None and target_ms < current:
        await self._cleanup_state()

    # 3. Reset processor
    self.processor.reset()

    # 4. Clear BFF cache
    self.bff_chart_manager.clear()

    # 5. Notify frontend
    await self.ws_broadcaster({
        "type": "replay_reset",
        "target_ms": target_ms,
        "reason": "seek_backward" if current and target_ms < current else "seek_forward"
    })

    # 6. Seek replayer and get initial data
    result = self.replayer.seek_to_time(target_ms)

    # 7. Update position
    self._current_position_ms = target_ms

    return result

async def _cleanup_state(self) -> None:
    """Clear all downstream state."""
    session = self.session_id

    # Redis streams
    self.redis.delete(f"market_snapshot_stream:{session}")
    self.redis.delete(f"market_snapshot_processed:{session}")

    # Redis sets/cursors
    self.redis.delete(f"movers_subscribed:{session}")
    self.redis.delete(f"state_cursor:{session}")

    # Create fresh streams
    self.redis.xgroup_create(
        f"market_snapshot_stream:{session}",
        "market_consumers",
        id="0",
        mkstream=True
    )
```

## Frontend Integration

### TimeSlider Component

```tsx
interface TimeSliderProps {
  startMs: number;
  endMs: number;
  currentMs: number;
  onSeek: (ms: number) => void;
  isPlaying: boolean;
  onPlayPause: () => void;
}

// Render:
// [04:00] ━━━━━━━━━━●━━━━━━━━━━ [09:30]
//           ▶️/⏸️  08:15:30
```

### Store Reset

```typescript
// On receiving replay_reset
useMarketDataStore.getState().reset();
```

## Speed Considerations

- Default speed: 1.0x (real-time)
- Supported range: 0.5x to 5.0x
- Higher speeds risk overwhelming frontend
- Speed limited by min_interval_ms (50ms default)

## Implementation Location

ReplayerController lives in:
- `services/market_snapshot/replayer_controller.py`

Started by:
- `backend_starter.py` when `Replayer` role is enabled

WebSocket handling:
- Integrated into `market_bff/server.py` (add replay message handlers)
