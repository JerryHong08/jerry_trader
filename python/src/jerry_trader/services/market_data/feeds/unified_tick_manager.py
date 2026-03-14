"""
Unified Tick Data Manager

A unified interface for multiple market data sources (Polygon, ThetaData, Replayer).
This manager eliminates the need for if-else branches in consuming code by providing
a consistent API regardless of the underlying data provider.

Architecture:
    - Strategy Pattern: Different managers are strategies
    - Adapter Pattern: Unified interface adapts to each manager's specific format
    - Single Responsibility: All data source logic centralized here

Usage:
    manager = UnifiedTickManager(provider="polygon")
    await manager.subscribe(client, ["AAPL", "GOOGL"], events=["Q", "T"])
    await manager.stream_forever()
"""

import os
from typing import Any, Dict, List, Optional, Set, Union

from dotenv import load_dotenv

from jerry_trader.services.market_data.feeds.polygon_manager import (
    PolygonWebSocketManager,
)
from jerry_trader.services.market_data.feeds.replayer_manager import (
    ReplayerWebSocketManager,
)
from jerry_trader.services.market_data.feeds.thetadata_manager import ThetaDataManager

load_dotenv()


class UnifiedTickManager:
    """
    Unified interface for multiple tick data providers.

    Eliminates provider-specific if-else branches by providing a consistent API
    that adapts to the underlying provider's requirements.

    Supported Providers:
        - polygon: Polygon.io real-time data
        - theta: ThetaData market data
        - replayer: Historical data replay (WebSocket to external binary)
        - synced-replayer: In-process Rust TickDataReplayer (no WebSocket)

    Key Features:
        - Unified subscription format
        - Automatic data normalization
        - Consistent stream key generation
        - Single point of configuration
    """

    def __init__(self, provider: Optional[str] = None, manager: Any = None):
        """
        Initialize unified manager with specified provider.

        Args:
            provider: Data provider type ('polygon', 'theta', 'replayer',
                     'synced-replayer').
                     If None, reads from DATA_MANAGER env var (default: polygon)
            manager: Optional pre-built manager instance. When provided,
                     skips internal factory logic and wraps *manager*
                     directly. Used by ``synced-replayer`` where the
                     caller constructs a ``SyncedReplayerManager`` with
                     the Rust ``TickDataReplayer``.
        """
        self.provider = provider or os.getenv("DATA_MANAGER", "polygon")

        if manager is not None:
            # Caller supplied a pre-built manager (e.g. SyncedReplayerManager)
            self._manager = manager
            return

        # Initialize the appropriate underlying manager
        if self.provider == "polygon":
            self._manager = PolygonWebSocketManager(
                api_key=os.getenv("POLYGON_API_KEY")
            )
        elif self.provider == "theta":
            self._manager = ThetaDataManager()
        elif self.provider == "replayer":
            replay_url = os.getenv("REPLAY_URL", "ws://127.0.0.1:8765")
            self._manager = ReplayerWebSocketManager(replay_url=replay_url)
        elif self.provider == "synced-replayer":
            raise ValueError(
                "synced-replayer requires a pre-built SyncedReplayerManager — "
                "pass it via the 'manager' parameter"
            )
        else:
            raise ValueError(
                f"Unknown provider: {self.provider}. "
                f"Supported: polygon, theta, replayer, synced-replayer"
            )

    @property
    def connected(self) -> bool:
        """Check if underlying manager is connected."""
        return self._manager.connected

    @property
    def queues(self) -> Dict:
        """Access to underlying manager's queues (backward-compatible flat view)."""
        return self._manager.queues

    @property
    def _client_queues(self) -> Dict:
        """Access to underlying manager's per-client queue structure."""
        return self._manager._client_queues

    def get_client_queue(self, client, stream_key: str):
        """Get a specific client's queue for a stream_key."""
        return self._manager.get_client_queue(client, stream_key)

    @property
    def connections(self) -> Dict:
        """Access to underlying manager's connections."""
        return self._manager.connections

    @property
    def subscribed_streams(self) -> Set:
        """Access to underlying manager's subscribed streams."""
        return self._manager.subscribed_streams

    async def stream_forever(self):
        """Start the underlying manager's streaming loop."""
        await self._manager.stream_forever()

    async def disconnect(self, websocket_client: Any):
        """Disconnect a client from the manager."""
        await self._manager.disconnect(websocket_client)

    # ========================================================================
    # Unified Subscription Interface
    # ========================================================================

    async def subscribe(
        self,
        websocket_client: Any,
        symbols: Optional[List[str]] = None,
        events: Optional[List[str]] = None,
        subscriptions: Optional[List[Dict]] = None,
    ):
        """
        Subscribe to market data with unified interface.

        Two subscription formats supported:

        1. Simple format (for Polygon/Replayer style):
           await manager.subscribe(client, ["AAPL", "GOOGL"], events=["Q", "T"])

        2. Advanced format (supports all providers including ThetaData options):
           await manager.subscribe(client, subscriptions=[
               {"symbol": "AAPL", "events": ["Q", "T"], "sec_type": "STOCK"},
               {"symbol": "SPY", "events": ["Q"], "sec_type": "STOCK"}
           ])

        Args:
            websocket_client: Client identifier (WebSocket or string)
            symbols: List of symbols (simple format)
            events: List of event types ["Q", "T"] or ["QUOTE", "TRADE"]
            subscriptions: List of subscription dicts (advanced format)
        """
        if subscriptions:
            # Advanced format - convert to provider-specific format
            if self.provider == "polygon":
                symbols_list, events_list = self._to_polygon(subscriptions)
                await self._manager.subscribe(
                    websocket_client, symbols_list, events=events_list
                )
            elif self.provider == "theta":
                theta_subs = self._to_theta(subscriptions)
                await self._manager.subscribe(websocket_client, theta_subs)
            elif self.provider in ("replayer", "synced-replayer"):
                symbols_list, events_list = self._to_replayer(subscriptions)
                await self._manager.subscribe(
                    websocket_client, symbols_list, events=events_list
                )
        else:
            # Simple format - convert to subscription format first
            if not symbols or not events:
                raise ValueError(
                    "Must provide either 'subscriptions' or both 'symbols' and 'events'"
                )

            subscriptions = [
                {"symbol": sym, "events": events, "sec_type": "STOCK"}
                for sym in symbols
            ]

            # Recursive call with advanced format
            await self.subscribe(websocket_client, subscriptions=subscriptions)

    async def unsubscribe(
        self,
        websocket_client: Any,
        symbol: Optional[str] = None,
        events: Optional[List[str]] = None,
        subscriptions: Optional[List[Dict]] = None,
        **kwargs,
    ):
        """
        Unsubscribe from market data with unified interface.

        Two unsubscription formats supported:

        1. Simple format:
           await manager.unsubscribe(client, "AAPL", events=["Q", "T"])

        2. Advanced format:
           await manager.unsubscribe(client, subscriptions=[
               {"symbol": "AAPL", "events": ["Q", "T"]}
           ])

        Args:
            websocket_client: Client identifier
            symbol: Symbol to unsubscribe (simple format)
            events: Event types to unsubscribe
            subscriptions: List of subscription dicts (advanced format)
            **kwargs: Additional provider-specific parameters
        """
        if subscriptions:
            # Advanced format - handle each subscription
            for sub in subscriptions:
                await self._unsubscribe_single(
                    websocket_client,
                    sub.get("symbol"),
                    sub.get("events", ["Q", "T"]),
                    sub.get("sec_type", "STOCK"),
                    sub.get("contract", {}),
                )
        else:
            # Simple format
            if not symbol:
                raise ValueError("Must provide either 'subscriptions' or 'symbol'")

            sec_type = kwargs.get("sec_type", "STOCK")
            contract = kwargs.get("contract", {})
            await self._unsubscribe_single(
                websocket_client, symbol, events or ["Q", "T"], sec_type, contract
            )

    async def _unsubscribe_single(
        self,
        websocket_client: Any,
        symbol: str,
        events: List[str],
        sec_type: str = "STOCK",
        contract: Dict = None,
    ):
        """Internal method to unsubscribe a single symbol."""
        if self.provider == "polygon":
            await self._manager.unsubscribe(websocket_client, symbol, events=events)

        elif self.provider == "theta":
            # Convert event names for ThetaData
            req_types = [
                "QUOTE" if e.upper() in ("Q", "QUOTE") else "TRADE" for e in events
            ]
            contract = contract or {"root": symbol}
            if "root" not in contract:
                contract["root"] = symbol

            await self._manager.unsubscribe(
                websocket_client, sec_type, req_types, contract
            )

        elif self.provider in ("replayer", "synced-replayer"):
            await self._manager.unsubscribe(websocket_client, symbol, events=events)

    # ========================================================================
    # Stream Key Generation
    # ========================================================================

    def generate_stream_keys(
        self,
        symbols: Optional[List[str]] = None,
        events: Optional[List[str]] = None,
        subscriptions: Optional[List[Dict]] = None,
    ) -> Set[str]:
        """
        Generate stream keys for the given subscriptions.

        Stream keys are used to route data from queues to consumers.

        Args:
            symbols: List of symbols (simple format)
            events: List of event types
            subscriptions: List of subscription dicts (advanced format)

        Returns:
            Set of stream keys for this provider

        Examples:
            Polygon/Replayer: {"Q.AAPL", "T.AAPL"}
            ThetaData: {"STOCK.QUOTE.AAPL", "STOCK.TRADE.AAPL"}
        """
        if subscriptions:
            return self._generate_stream_keys_from_subs(subscriptions)
        elif symbols and events:
            subscriptions = [
                {"symbol": sym, "events": events, "sec_type": "STOCK"}
                for sym in symbols
            ]
            return self._generate_stream_keys_from_subs(subscriptions)
        else:
            raise ValueError(
                "Must provide either 'subscriptions' or both 'symbols' and 'events'"
            )

    def _generate_stream_keys_from_subs(self, subscriptions: List[Dict]) -> Set[str]:
        """Generate stream keys from subscription list."""
        stream_keys = set()

        for sub in subscriptions:
            symbol = sub.get("symbol", "").upper()
            events = sub.get("events", ["Q"])
            sec_type = sub.get("sec_type", "STOCK").upper()
            contract = sub.get("contract", {})

            for ev in events:
                ev = ev.upper()
                # Normalize event name
                if ev == "QUOTE":
                    ev = "Q"
                elif ev == "TRADE":
                    ev = "T"

                if self.provider in ("polygon", "replayer", "synced-replayer"):
                    stream_keys.add(f"{ev}.{symbol}")

                elif self.provider == "theta":
                    # Generate ThetaData stream key
                    if sec_type in ("STOCK", "INDEX"):
                        identifier = symbol
                    elif sec_type == "OPTION":
                        root = contract.get("root", symbol)
                        exp = contract.get("expiration", "")
                        strike = contract.get("strike", "")
                        right = contract.get("right", "")
                        identifier = f"{root}_{exp}_{strike}_{right}"
                    else:
                        identifier = symbol

                    # Map to ThetaData event names
                    theta_ev = "QUOTE" if ev == "Q" else "TRADE"
                    stream_keys.add(f"{sec_type}.{theta_ev}.{identifier}")

        return stream_keys

    # ========================================================================
    # Data Normalization
    # ========================================================================

    def normalize_data(self, data: Dict) -> Dict:
        """
        Normalize data from any provider to unified format.

        Unified format:
        - Quote: { event_type: "Q", symbol, bid, ask, bid_size, ask_size, timestamp }
        - Trade: { event_type: "T", symbol, price, size, timestamp }

        All timestamps are normalized to milliseconds (13 digits).

        Args:
            data: Raw data from provider

        Returns:
            Normalized data dict
        """
        if self.provider in ("polygon", "replayer", "synced-replayer"):
            normalized = data.copy()
            if "timestamp" in normalized:
                normalized["timestamp"] = self._to_milliseconds(normalized["timestamp"])
            return normalized

        elif self.provider == "theta":
            return self._normalize_theta_data(data)

        # Fallback
        return data

    def _normalize_theta_data(self, data: Dict) -> Dict:
        """Normalize ThetaData format to unified format."""
        event_type = data.get("event_type", "").upper()
        symbol = data.get("symbol", "")
        timestamp = self._to_milliseconds(data.get("timestamp"))

        if event_type == "QUOTE":
            return {
                "event_type": "Q",
                "symbol": symbol,
                "bid": data.get("bid"),
                "ask": data.get("ask"),
                "bid_size": data.get("bid_size"),
                "ask_size": data.get("ask_size"),
                "timestamp": timestamp,
            }
        elif event_type == "TRADE":
            return {
                "event_type": "T",
                "symbol": symbol,
                "price": data.get("price"),
                "size": data.get("size"),
                "timestamp": timestamp,
            }

        return data

    @staticmethod
    def _to_milliseconds(ts: Optional[Union[int, float]]) -> Optional[int]:
        """Convert any timestamp format to milliseconds."""
        if ts is None:
            return None

        # Nanoseconds (19 digits) - from Rust replayer
        if ts > 1e15:
            return int(ts // 1e6)
        # Already milliseconds (13 digits)
        elif ts > 1e12:
            return int(ts)
        # Seconds (10 digits)
        else:
            return int(ts * 1000)

    # ========================================================================
    # Adapter Methods (Private)
    # ========================================================================

    @staticmethod
    def _to_polygon(subscriptions: List[Dict]) -> tuple:
        """Convert to Polygon format: (symbols, events)"""
        symbols = []
        events = set()

        for sub in subscriptions:
            symbol = sub.get("symbol", "").upper()
            sub_events = sub.get("events", ["Q"])

            if symbol:
                symbols.append(symbol)

            for ev in sub_events:
                ev = ev.upper()
                if ev in ("QUOTE", "Q"):
                    events.add("Q")
                elif ev in ("TRADE", "T"):
                    events.add("T")

        return symbols, list(events)

    @staticmethod
    def _to_theta(subscriptions: List[Dict]) -> List[Dict]:
        """Convert to ThetaData format: list of subscription dicts"""
        theta_subs = []

        for sub in subscriptions:
            symbol = sub.get("symbol", "").upper()
            events = sub.get("events", ["Q"])
            sec_type = sub.get("sec_type", "STOCK").upper()
            contract = sub.get("contract", {})

            # Build contract dict
            if not contract:
                contract = {"root": symbol}
            elif "root" not in contract:
                contract["root"] = symbol

            # Map unified event names to ThetaData codes
            req_types = []
            for ev in events:
                ev = ev.upper()
                if ev in ("QUOTE", "Q"):
                    req_types.append("QUOTE")
                elif ev in ("TRADE", "T"):
                    req_types.append("TRADE")

            theta_subs.append(
                {"sec_type": sec_type, "req_types": req_types, "contract": contract}
            )

        return theta_subs

    @staticmethod
    def _to_replayer(subscriptions: List[Dict]) -> tuple:
        """Convert to Replayer format (same as Polygon): (symbols, events)"""
        return UnifiedTickManager._to_polygon(subscriptions)


# ============================================================================
# Convenience Functions
# ============================================================================


def create_manager(provider: Optional[str] = None) -> UnifiedTickManager:
    """
    Factory function to create a unified tick manager.

    Args:
        provider: Data provider type or None for env var

    Returns:
        Configured UnifiedTickManager instance

    Example:
        manager = create_manager("polygon")
        await manager.subscribe(client, ["AAPL"], events=["Q", "T"])
    """
    return UnifiedTickManager(provider=provider)
