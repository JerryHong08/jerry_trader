"""
IBKR Bot - Main entry point
"""

import os
import sys

from jerry_trader.apps.order_app.api.server import app, start_server


def main():
    """Main entry point for direct script execution"""
    print("=" * 60)
    print("order_management Trading Platform")
    print("=" * 60)
    print()

    # Get configuration from environment
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8888"))

    print(f"Starting server on {host}:{port}")
    print(f"API Documentation: http://localhost:{port}/docs")
    print(f"WebSocket endpoint: ws://localhost:{port}/ws")
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


# Export app for uvicorn command line usage
# Usage: poetry run uvicorn src.order_management.main:app --reload
__all__ = ["app", "main"]


if __name__ == "__main__":
    main()
