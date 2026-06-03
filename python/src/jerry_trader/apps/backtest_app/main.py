"""
Backtest Visualization App - Main entry point

Usage:
    poetry run uvicorn jerry_trader.apps.backtest_app.main:app --reload --port 5005
"""

import os
import sys

from jerry_trader.apps.backtest_app.api.server import app, start_server


def main():
    """Main entry point for direct script execution."""
    print("=" * 60)
    print("Backtest Visualization API")
    print("=" * 60)
    print()

    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "5005"))

    print(f"Starting server on {host}:{port}")
    print(f"API Documentation: http://localhost:{port}/docs")
    print(f"WebSocket endpoint: ws://localhost:{port}/ws/backtest")
    print()
    print("Press Ctrl+C to stop")
    print("=" * 60)
    print()

    try:
        start_server(host=host, port=port)
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


__all__ = ["app", "main"]


if __name__ == "__main__":
    main()
