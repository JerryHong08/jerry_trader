"""Domain Layer Usage Examples

Shows how to use the new domain models in services.
"""

from jerry_trader.domain import Bar, BarPeriod, Order, Signal, SignalType, Tick

# =============================================================================
# Example 1: Converting Rust BarBuilder output to typed Bar objects
# =============================================================================


def process_bars_from_rust(bar_builder):
    """Example: Converting Rust output to domain Bar objects"""
    # Rust returns list[dict]
    raw_bars = bar_builder.drain_completed()

    # Convert to domain objects for type safety
    bars = [Bar.from_rust_dict(b) for b in raw_bars]

    for bar in bars:
        # Now IDE provides autocomplete and type checking!
        print(f"{bar.symbol} {bar.timeframe}: {bar.open} -> {bar.close}")

        # Domain methods available
        if bar.is_bullish:
            print(f"  Bullish candle, range: {bar.range}")

        # Time calculations via BarPeriod
        if bar.period.is_complete(now_ms=1234567890):
            print("  Bar is complete")

    return bars


# =============================================================================
# Example 2: Meeting bar merge (REST + WebSocket)
# =============================================================================


def merge_meeting_bars(rest_bar: Bar, ws_bar: Bar) -> Bar:
    """Example: Merging REST and WebSocket bars for the same period"""
    # Both bars should be for same timeframe and period
    assert rest_bar.timeframe == ws_bar.timeframe
    assert rest_bar.bar_start == ws_bar.bar_start

    # Use domain merge method
    merged = rest_bar.merge(ws_bar)

    print(f"Merged bar: {merged}")
    print(f"  REST trades: {rest_bar.trade_count}")
    print(f"  WS trades: {ws_bar.trade_count}")
    print(f"  Total trades: {merged.trade_count}")

    return merged


# =============================================================================
# Example 3: Creating ClickHouse-ready dict from Bar
# =============================================================================


def write_bar_to_clickhouse(bar: Bar, ch_client):
    """Example: Converting Bar to ClickHouse format"""
    # Bar.to_clickhouse_dict() returns format expected by ohlcv_writer
    row = bar.to_clickhouse_dict()

    # Insert into ClickHouse
    # ch_client.insert("ohlcv_bars", [row])
    print(f"Writing to CH: {row}")


# =============================================================================
# Example 4: Order lifecycle with OrderState
# =============================================================================


def simulate_order_lifecycle():
    """Example: Tracking an order through its lifecycle"""
    import time

    from jerry_trader.domain import Fill, OrderState, OrderStatus

    # Create immutable order
    order = Order(
        order_id="order-001",
        symbol="AAPL",
        side="buy",
        quantity=100,
        order_type="limit",
        limit_price=150.0,
    )

    # Create mutable state tracker
    state = OrderState(
        order_id=order.order_id,
        remaining_qty=order.quantity,
        created_at_ns=time.time_ns(),
    )

    # Simulate fills
    state.mark_submitted(time.time_ns())
    print(f"Status: {state.status}")  # SUBMITTED

    # Partial fill
    fill1 = Fill(
        fill_id="fill-001",
        order_id=order.order_id,
        timestamp_ns=time.time_ns(),
        quantity=30,
        price=150.0,
        commission=0.5,
    )
    state.apply_fill(fill1, order.quantity)
    print(f"Status: {state.status}")  # PARTIAL_FILL
    print(f"Filled: {state.filled_qty}/{order.quantity}")

    # Complete fill
    fill2 = Fill(
        fill_id="fill-002",
        order_id=order.order_id,
        timestamp_ns=time.time_ns(),
        quantity=70,
        price=150.05,
        commission=0.5,
    )
    state.apply_fill(fill2, order.quantity)
    print(f"Status: {state.status}")  # FILLED
    print(f"Avg price: {state.avg_fill_price}")


# =============================================================================
# Example 5: Signal generation
# =============================================================================


def generate_breakout_signal(bar: Bar, prev_bar: Bar) -> Signal:
    """Example: Creating a trading signal"""
    if bar.close > prev_bar.high:
        return Signal(
            symbol=bar.symbol,
            timestamp_ns=bar.bar_start * 1_000_000,  # ms to ns
            signal_type=SignalType.ENTRY_LONG,
            confidence=0.85,
            reason=f"Breakout above previous high {prev_bar.high}",
            metadata={
                "breakout_level": prev_bar.high,
                "volume_ratio": (
                    bar.volume / prev_bar.volume if prev_bar.volume > 0 else 0
                ),
                "candle_range": bar.range,
            },
        )
    return None


# =============================================================================
# Run examples
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Example 4: Order Lifecycle")
    print("=" * 60)
    simulate_order_lifecycle()

    print("\n" + "=" * 60)
    print("Example 2: Meeting Bar Merge")
    print("=" * 60)

    rest_bar = Bar(
        symbol="AAPL",
        timeframe="1m",
        open=150.0,
        high=150.5,
        low=149.9,
        close=150.2,
        volume=1000.0,
        trade_count=50,
        vwap=150.15,
        bar_start=1234567890000,
        bar_end=1234567950000,
        session="regular",
    )

    ws_bar = Bar(
        symbol="AAPL",
        timeframe="1m",
        open=150.2,  # Continues from REST close
        high=150.8,
        low=150.1,
        close=150.6,
        volume=500.0,
        trade_count=25,
        vwap=150.45,
        bar_start=1234567890000,  # Same period
        bar_end=1234567950000,
        session="regular",
    )

    merge_meeting_bars(rest_bar, ws_bar)
