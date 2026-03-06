# Test script for ThetaDataManager
import asyncio
import logging

from jerry_trader.tickDataSupply.thetadata_manager import ThetaDataManager
from jerry_trader.utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=False, level=logging.DEBUG)


async def consume_stream(manager, stream_key, duration=10):
    """Consume messages from a specific stream for testing."""
    q = manager.queues.get(stream_key)
    if not q:
        logger.error(f"❌ No queue found for {stream_key}")
        return

    logger.info(f"📡 Consuming from {stream_key} for {duration} seconds...")
    end_time = asyncio.get_event_loop().time() + duration
    count = 0

    while asyncio.get_event_loop().time() < end_time:
        try:
            data = await asyncio.wait_for(q.get(), timeout=1.0)
            count += 1
            logger.info(f"📨 [{stream_key}] Message #{count}:")
            logger.info(f"   Event: {data.get('event_type')}")
            logger.info(f"   Symbol: {data.get('symbol')}")
            if data.get("event_type") == "QUOTE":
                logger.info(f"   Bid: {data.get('bid')} x {data.get('bid_size')}")
                logger.info(f"   Ask: {data.get('ask')} x {data.get('ask_size')}")
            elif data.get("event_type") == "TRADE":
                logger.info(f"   Price: {data.get('price')}, Size: {data.get('size')}")
        except asyncio.TimeoutError:
            continue

    logger.info(f"✅ Received {count} messages from {stream_key}")


async def test_stock_subscriptions():
    """Test subscribing to stock quotes and trades."""
    logger.info("=" * 60)
    logger.info("TEST 1: Stock Subscriptions (AAPL)")
    logger.info("=" * 60)

    manager = ThetaDataManager()

    # Start the stream_forever task
    stream_task = asyncio.create_task(manager.stream_forever())

    # Wait for connection
    await asyncio.sleep(2)

    # Subscribe to AAPL quotes and trades
    client_id = "test_client_1"
    subscriptions = [
        {
            "sec_type": "STOCK",
            "req_types": ["QUOTE", "TRADE"],
            "contract": {"root": "AAPL"},
        }
    ]

    await manager.subscribe(client_id, subscriptions)

    # Consume messages
    await asyncio.gather(
        consume_stream(manager, "STOCK.QUOTE.AAPL", duration=10),
        consume_stream(manager, "STOCK.TRADE.AAPL", duration=10),
    )

    # Unsubscribe
    await manager.unsubscribe(client_id, "STOCK", ["QUOTE", "TRADE"], {"root": "AAPL"})

    stream_task.cancel()
    logger.info("✅ Test 1 completed\n")


async def test_option_subscriptions():
    """Test subscribing to option quotes and trades."""
    logger.info("=" * 60)
    logger.info("TEST 2: Option Subscriptions (QQQ Put)")
    logger.info("=" * 60)

    manager = ThetaDataManager()

    stream_task = asyncio.create_task(manager.stream_forever())
    await asyncio.sleep(2)

    # Subscribe to QQQ option
    client_id = "test_client_2"
    subscriptions = [
        {
            "sec_type": "OPTION",
            "req_types": ["TRADE"],
            "contract": {
                "root": "QQQ",
                "expiration": "20250428",
                "strike": "462000",
                "right": "P",
            },
        }
    ]

    await manager.subscribe(client_id, subscriptions)

    # Consume messages
    await consume_stream(manager, "OPTION.TRADE.QQQ_20250428_462000_P", duration=15)

    # Unsubscribe
    await manager.unsubscribe(
        client_id,
        "OPTION",
        ["TRADE"],
        {"root": "QQQ", "expiration": "20250428", "strike": "462000", "right": "P"},
    )

    stream_task.cancel()
    logger.info("✅ Test 2 completed\n")


async def test_multiple_clients():
    """Test multiple clients subscribing to same and different streams."""
    logger.info("=" * 60)
    logger.info("TEST 3: Multiple Clients")
    logger.info("=" * 60)

    manager = ThetaDataManager()

    stream_task = asyncio.create_task(manager.stream_forever())
    await asyncio.sleep(2)

    # Client 1 subscribes to AAPL
    client1 = "test_client_3a"
    await manager.subscribe(
        client1,
        [{"sec_type": "STOCK", "req_types": ["QUOTE"], "contract": {"root": "AAPL"}}],
    )

    # Client 2 also subscribes to AAPL (should reuse stream)
    client2 = "test_client_3b"
    await manager.subscribe(
        client2,
        [{"sec_type": "STOCK", "req_types": ["QUOTE"], "contract": {"root": "AAPL"}}],
    )

    # Client 2 subscribes to NVDA
    await manager.subscribe(
        client2,
        [{"sec_type": "STOCK", "req_types": ["QUOTE"], "contract": {"root": "NVDA"}}],
    )

    logger.info(f"📊 Subscribed streams: {manager.subscribed_streams}")
    logger.info(f"📊 Client 1 streams: {manager.connections[client1]}")
    logger.info(f"📊 Client 2 streams: {manager.connections[client2]}")

    # Consume for a bit
    await asyncio.sleep(5)

    # Client 1 disconnects (AAPL should still be subscribed for client 2)
    await manager.disconnect(client1)
    logger.info(f"📊 After client 1 disconnect: {manager.subscribed_streams}")

    # Client 2 disconnects (all streams should be unsubscribed)
    await manager.disconnect(client2)
    logger.info(f"📊 After client 2 disconnect: {manager.subscribed_streams}")

    stream_task.cancel()
    logger.info("✅ Test 3 completed\n")


async def main():
    """Run all tests."""
    logger.info("🚀 Starting ThetaDataManager Tests")
    logger.info("Make sure ThetaData Terminal is running on ws://127.0.0.1:25520")
    logger.info("")

    try:
        # Run tests sequentially
        await test_stock_subscriptions()
        await asyncio.sleep(2)

        await test_option_subscriptions()
        await asyncio.sleep(2)

        await test_multiple_clients()

        logger.info("=" * 60)
        logger.info("✅ All tests completed successfully!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
