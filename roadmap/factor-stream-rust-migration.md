# Factor Stream Rust Migration

**Status:** todo
**Trigger:** Signal Engine implementation starts

## Current Architecture (Python)

```
FactorEngine (Rust compute)
    ↓ publish
Redis pub/sub
    ↓ subscribe
Python asyncio consumer → WebSocket → Frontend
```

### Current Bottlenecks

| Layer | Current State | Issue |
|-------|---------------|-------|
| Redis publish | Python wrapper | Serialization overhead |
| Redis subscribe | Python asyncio | Per-message serialization |
| WebSocket send | Python + JSON | asyncio overhead, JSON encoding |

### Current Optimization

`asyncio.sleep(0)` instead of `sleep(0.01)`:
- `sleep(0.01)`: Forces 10ms wait per loop iteration → accumulates delay
- `sleep(0)`: Yields control, returns immediately if no other tasks

This reduces latency for single consumer (frontend), but doesn't scale to multiple consumers.

## Proposed Rust Architecture

```
FactorEngine (Rust)
    ↓ 内存通道 / 无锁队列
FactorBroadcaster (Rust)
    ↓ tokio::sync::broadcast (zero-copy)
    ├── WebSocket handler (Rust) → Frontend
    ├── Signal Engine (Rust) → 直接消费
    └── Redis fallback → 跨机器场景
```

### Rust Advantages

1. **`tokio::sync::broadcast`** - Zero-copy multi-subscriber
   - Single publish → all subscribers receive same data
   - No per-subscriber serialization

2. **Direct memory channel** with FactorEngine
   - No Redis middleware for local consumers
   - Lower latency, no network hop

3. **Better backpressure handling**
   - Configurable buffer sizes
   - Slow consumer policies (drop oldest, block, etc.)

4. **No Python overhead**
   - No GC pauses
   - No asyncio task switching
   - Native thread pool for I/O

## Implementation Plan

### Phase 1: FactorBroadcaster Service

```rust
// rust/src/factor_broadcaster.rs
pub struct FactorBroadcaster {
    receiver: Receiver<FactorSnapshot>,
    subscribers: Broadcast<FactorSnapshot>,
}

impl FactorBroadcaster {
    pub fn new(factor_engine: &FactorEngine) -> Self {
        // Connect to FactorEngine's output channel
    }

    pub fn subscribe(&self) -> Receiver<FactorSnapshot> {
        self.subscribers.subscribe()
    }

    pub async fn run(&self) {
        while let Ok(snapshot) = self.receiver.recv() {
            self.subscribers.send(snapshot);
        }
    }
}
```

### Phase 2: WebSocket Handler

```rust
// rust/src/ws_handler.rs
pub async fn handle_factor_ws(
    broadcaster: &FactorBroadcaster,
    ws: WebSocket,
) {
    let mut receiver = broadcaster.subscribe();
    while let Ok(snapshot) = receiver.recv() {
        let msg = FactorMessage::from(snapshot);
        ws.send(Message::Text(msg.to_json())).await;
    }
}
```

### Phase 3: Signal Engine Integration

```rust
// rust/src/signal_engine.rs
pub struct SignalEngine {
    factor_receiver: Receiver<FactorSnapshot>,
}

impl SignalEngine {
    pub fn new(broadcaster: &FactorBroadcaster) -> Self {
        Self {
            factor_receiver: broadcaster.subscribe(),
        }
    }

    pub async fn process_factors(&mut self) {
        while let Ok(snapshot) = self.factor_receiver.recv() {
            // Direct consumption, no serialization
            self.evaluate_signals(&snapshot);
        }
    }
}
```

### Phase 4: Redis Fallback (Optional)

For cross-machine scenarios where Signal Engine runs on different machine:

```rust
// Redis publisher as another subscriber
pub struct RedisPublisher {
    receiver: Receiver<FactorSnapshot>,
    redis: RedisClient,
}

impl RedisPublisher {
    pub async fn run(&mut self) {
        while let Ok(snapshot) = self.receiver.recv() {
            self.redis.publish("factor_stream", snapshot.to_json());
        }
    }
}
```

## Why Not Now?

Current Python architecture is **sufficient for single consumer**:
- Latency optimized via `sleep(0)` (~1s inherent delay from throttle)
- Signal Engine not yet implemented
- Refactoring cost vs benefit ratio low

## When to Migrate

**Trigger conditions:**
1. Signal Engine implementation begins
2. Multiple factor consumers needed (frontend + Signal Engine + monitoring)
3. Cross-machine factor distribution required

**Expected benefit:**
- ~10x latency reduction for local consumers
- Zero-copy multi-subscriber efficiency
- Better integration with Rust Signal Engine
