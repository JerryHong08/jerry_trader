#!/usr/bin/env python3
"""
Unified Subscription API Test Script
测试统一订阅接口的各个 manager
"""

import asyncio
import json
import sys

import websockets


async def test_subscribe_unsubscribe(manager_type="polygon"):
    """测试订阅和取消订阅功能"""
    uri = "ws://localhost:8000/ws/tickdata"

    print(f"\n{'='*60}")
    print(f"Testing {manager_type.upper()} Manager")
    print(f"{'='*60}\n")

    async with websockets.connect(uri) as websocket:
        # 1. Subscribe to multiple symbols
        subscribe_msg = {
            "subscriptions": [
                {"symbol": "AAPL", "events": ["QUOTE", "TRADE"]},
                {"symbol": "TSLA", "events": ["QUOTE"]},
            ]
        }

        if manager_type == "theta":
            subscribe_msg["subscriptions"][0]["sec_type"] = "STOCK"
            subscribe_msg["subscriptions"][1]["sec_type"] = "STOCK"

        print(f"📤 Subscribing...")
        print(f"   Message: {json.dumps(subscribe_msg, indent=2)}")
        await websocket.send(json.dumps(subscribe_msg))

        # 2. Receive some messages
        print(f"\n📥 Receiving messages (10 seconds)...")
        message_count = 0

        try:
            for _ in range(50):  # Receive up to 50 messages
                message = await asyncio.wait_for(websocket.recv(), timeout=0.5)
                data = json.loads(message)
                message_count += 1

                # Print first few messages
                if message_count <= 3:
                    print(f"   [{message_count}] {json.dumps(data, indent=2)}")
                elif message_count == 4:
                    print(f"   ...")
        except asyncio.TimeoutError:
            pass

        print(f"\n✅ Received {message_count} messages")

        # 3. Unsubscribe from TRADE events on AAPL
        unsubscribe_msg = {
            "action": "unsubscribe",
            "subscriptions": [{"symbol": "AAPL", "events": ["TRADE"]}],
        }

        print(f"\n📤 Unsubscribing from AAPL TRADE...")
        await websocket.send(json.dumps(unsubscribe_msg))

        # 4. Continue receiving (should only get AAPL quotes now)
        print(f"\n📥 Receiving after unsubscribe (5 seconds)...")
        trade_count = 0
        quote_count = 0

        try:
            for _ in range(20):
                message = await asyncio.wait_for(websocket.recv(), timeout=0.5)
                data = json.loads(message)

                event_type = data.get("event_type", data.get("ev"))
                if event_type in ("T", "TRADE"):
                    trade_count += 1
                elif event_type in ("Q", "QUOTE"):
                    quote_count += 1
        except asyncio.TimeoutError:
            pass

        print(f"\n📊 After unsubscribe:")
        print(f"   Quote messages: {quote_count}")
        print(f"   Trade messages: {trade_count}")

        if trade_count > 0:
            print(f"   ⚠️  WARNING: Still receiving TRADE messages after unsubscribe!")
        else:
            print(f"   ✅ Unsubscribe successful!")

        # 5. Unsubscribe all
        unsubscribe_all = {
            "action": "unsubscribe",
            "subscriptions": [
                {"symbol": "AAPL", "events": ["QUOTE"]},
                {"symbol": "TSLA", "events": ["QUOTE"]},
            ],
        }

        print(f"\n📤 Unsubscribing all...")
        await websocket.send(json.dumps(unsubscribe_all))

        await asyncio.sleep(1)
        print(f"\n✅ Test completed!")


async def test_theta_options():
    """测试 ThetaData 期权订阅"""
    uri = "ws://localhost:8000/ws/tickdata"

    print(f"\n{'='*60}")
    print(f"Testing THETADATA Options")
    print(f"{'='*60}\n")

    async with websockets.connect(uri) as websocket:
        subscribe_msg = {
            "subscriptions": [
                {
                    "symbol": "QQQ",
                    "events": ["QUOTE"],
                    "sec_type": "OPTION",
                    "contract": {
                        "root": "QQQ",
                        "expiration": "20250428",
                        "strike": "462000",
                        "right": "P",
                    },
                }
            ]
        }

        print(f"📤 Subscribing to QQQ option...")
        print(f"   Message: {json.dumps(subscribe_msg, indent=2)}")
        await websocket.send(json.dumps(subscribe_msg))

        print(f"\n📥 Receiving option data (10 seconds)...")
        message_count = 0

        try:
            for _ in range(20):
                message = await asyncio.wait_for(websocket.recv(), timeout=0.5)
                data = json.loads(message)
                message_count += 1

                if message_count <= 3:
                    print(f"   [{message_count}] {json.dumps(data, indent=2)}")
        except asyncio.TimeoutError:
            pass

        print(f"\n✅ Received {message_count} option messages")


def print_usage():
    print(
        """
Usage: python test_unified_api.py [manager_type]

manager_type:
  polygon     - Test Polygon real-time data (default)
  theta       - Test ThetaData local terminal
  theta-opt   - Test ThetaData options
  simulator   - Test historical data replay

Examples:
  python test_unified_api.py polygon
  python test_unified_api.py theta
  python test_unified_api.py theta-opt
  python test_unified_api.py simulator

Prerequisites:
  1. Server running: poetry run uvicorn src.OrderBook.server:app --reload
  2. Set DATA_MANAGER in .env file to match manager_type
  3. For ThetaData: ThetaData Terminal running on localhost:25520
  4. For Simulator: Valid REPLAY_DATE in .env
"""
    )


async def main():
    if len(sys.argv) < 2:
        manager_type = "polygon"
    else:
        manager_type = sys.argv[1].lower()

    if manager_type not in ["polygon", "theta", "theta-opt", "simulator"]:
        print(f"❌ Unknown manager type: {manager_type}")
        print_usage()
        return

    print(
        f"""
🧪 Unified Subscription API Test
================================

Testing: {manager_type.upper()}
Server: ws://localhost:8000/ws/tickdata

⚠️  Make sure:
   1. Server is running with DATA_MANAGER={manager_type.split('-')[0]}
   2. Appropriate data source is available
"""
    )

    try:
        if manager_type == "theta-opt":
            await test_theta_options()
        else:
            await test_subscribe_unsubscribe(manager_type.split("-")[0])
    except ConnectionRefusedError:
        print("\n❌ Connection refused. Is the server running?")
        print("   Start server: poetry run uvicorn src.OrderBook.server:app --reload")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    if "--help" in sys.argv or "-h" in sys.argv:
        print_usage()
    else:
        asyncio.run(main())
