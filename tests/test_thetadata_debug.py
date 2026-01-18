#!/usr/bin/env python3
"""
ThetaData Manager 测试脚本
用于测试和调试 ThetaData 订阅功能
"""

import asyncio
import json
import sys
from datetime import datetime

# 添加项目路径
sys.path.insert(0, "/home/jerryhong/code-projects/jerryib_trade")

from src.tickDataSupply.thetadata_manager import ThetaDataManager


async def test_basic_subscription():
    """测试基本订阅功能"""
    print("\n" + "=" * 60)
    print("ThetaData Manager 基础订阅测试")
    print("=" * 60 + "\n")

    manager = ThetaDataManager()

    # 模拟 websocket 客户端
    fake_client = "test_client_123"

    try:
        # 1. 连接
        print("📡 连接到 ThetaData Terminal...")
        await manager.connect()
        print(f"✅ 连接成功: {manager.connected}\n")

        # 2. 订阅 AAPL 的 QUOTE 和 TRADE
        print("📥 订阅 AAPL (QUOTE + TRADE)...")
        subscriptions = [
            {
                "sec_type": "STOCK",
                "req_types": ["QUOTE", "TRADE"],
                "contract": {"root": "AAPL"},
            }
        ]

        await manager.subscribe(fake_client, subscriptions)
        print(f"✅ 订阅完成")
        print(f"   Subscribed streams: {manager.subscribed_streams}")
        print(f"   Queues created: {list(manager.queues.keys())}\n")

        # 3. 启动消息接收
        print("📨 开始接收数据 (10秒)...\n")
        stream_task = asyncio.create_task(manager.stream_forever())

        # 4. 监听队列并打印消息
        message_count = {"QUOTE": 0, "TRADE": 0, "OTHER": 0}
        start_time = asyncio.get_event_loop().time()

        async def consume_queue(stream_key, event_type):
            nonlocal message_count
            queue = manager.queues.get(stream_key)
            if not queue:
                return

            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=0.1)
                    message_count[event_type] += 1

                    # 只打印前3条消息
                    if message_count[event_type] <= 3:
                        timestamp = datetime.fromtimestamp(
                            data.get("timestamp", 0) / 1000
                        )
                        print(
                            f"[{event_type}] {data.get('symbol')} @ {timestamp.strftime('%H:%M:%S.%f')[:-3]}"
                        )
                        if event_type == "QUOTE":
                            print(
                                f"   Bid: ${data.get('bid'):.2f} x {data.get('bid_size')}"
                            )
                            print(
                                f"   Ask: ${data.get('ask'):.2f} x {data.get('ask_size')}"
                            )
                        elif event_type == "TRADE":
                            print(
                                f"   Price: ${data.get('price'):.2f} x {data.get('size')}"
                            )
                        print()
                except asyncio.TimeoutError:
                    # 检查是否超时
                    if asyncio.get_event_loop().time() - start_time > 10:
                        break
                except Exception as e:
                    print(f"❌ Error consuming {stream_key}: {e}")
                    break

        # 启动消费者任务
        consumers = [
            asyncio.create_task(consume_queue("STOCK.QUOTE.AAPL", "QUOTE")),
            asyncio.create_task(consume_queue("STOCK.TRADE.AAPL", "TRADE")),
        ]

        # 等待10秒
        await asyncio.sleep(10)

        # 取消任务
        stream_task.cancel()
        for consumer in consumers:
            consumer.cancel()

        print("\n" + "=" * 60)
        print("📊 测试结果")
        print("=" * 60)
        print(f"QUOTE 消息数: {message_count['QUOTE']}")
        print(f"TRADE 消息数: {message_count['TRADE']}")
        print(f"其他消息数: {message_count['OTHER']}")

        if message_count["QUOTE"] > 0 or message_count["TRADE"] > 0:
            print("\n✅ 测试成功！收到了数据")
        else:
            print("\n⚠️  没有收到数据，请检查：")
            print("   1. ThetaData Terminal 是否运行")
            print("   2. AAPL 是否在交易时间")
            print("   3. 订阅是否成功")

        # 清理
        await manager.disconnect(fake_client)

    except ConnectionRefusedError:
        print("\n❌ 连接失败！")
        print("   请确保 ThetaData Terminal 正在运行")
        print("   默认地址: ws://127.0.0.1:25520/v1/events")
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()


async def test_message_filtering():
    """测试消息过滤功能"""
    print("\n" + "=" * 60)
    print("ThetaData 消息过滤测试")
    print("=" * 60 + "\n")

    manager = ThetaDataManager()

    print("📡 连接到 ThetaData...")
    await manager.connect()

    # 订阅
    fake_client = "filter_test"
    await manager.subscribe(
        fake_client,
        [{"sec_type": "STOCK", "req_types": ["QUOTE"], "contract": {"root": "AAPL"}}],
    )

    print("✅ 订阅成功")
    print("⏳ 监控5秒，观察是否有 OHLC 或其他消息被过滤...\n")

    # 启动stream并观察日志
    stream_task = asyncio.create_task(manager.stream_forever())
    await asyncio.sleep(5)
    stream_task.cancel()

    print("\n✅ 测试完成，查看上面的日志输出：")
    print("   - 应该看到 '⏭️ Skipping unsupported event type: OHLC'")
    print("   - QUOTE 消息应该被正常处理")

    await manager.disconnect(fake_client)


def print_usage():
    print(
        """
ThetaData Manager 测试工具

用法:
  python test_thetadata_debug.py [test_name]

可用测试:
  basic     - 基础订阅测试（默认）
  filter    - 消息过滤测试
  all       - 运行所有测试

示例:
  python test_thetadata_debug.py
  python test_thetadata_debug.py basic
  python test_thetadata_debug.py filter
  python test_thetadata_debug.py all

前提条件:
  - ThetaData Terminal 运行在 ws://127.0.0.1:25520/v1/events
  - 测试时间在美股交易时间内（有实时数据）
"""
    )


async def main():
    if len(sys.argv) > 1:
        test_name = sys.argv[1].lower()
    else:
        test_name = "basic"

    if test_name == "help" or test_name == "-h" or test_name == "--help":
        print_usage()
        return

    print(
        f"""
🧪 ThetaData Manager 测试
========================

测试: {test_name}
时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
    )

    try:
        if test_name == "basic":
            await test_basic_subscription()
        elif test_name == "filter":
            await test_message_filtering()
        elif test_name == "all":
            await test_basic_subscription()
            await asyncio.sleep(2)
            await test_message_filtering()
        else:
            print(f"❌ 未知测试: {test_name}")
            print_usage()
    except KeyboardInterrupt:
        print("\n\n⚠️  测试被用户中断")
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
