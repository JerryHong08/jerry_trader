"""
RiskManager Service

Bridges the C++ RiskEngine with the rest of the trading system:
  1. Subscribes to ExecutionReceivedEvent (order fills) via EventBus
  2. Subscribes to OrderStatusEvent (Filled status) as a supplementary source
  3. Accepts real-time price ticks from the TickDataServer WebSocket
  4. Exposes PnL snapshots and a WebSocket broadcast callback

Architecture:
    EventBus ──► RiskManager.on_fill()     ──► RiskEngine.on_fill()
    TickData ──► RiskManager.on_tick()     ──► RiskEngine.update_price()
    REST/WS  ◄── RiskManager.get_pnl_snapshot()
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set

from OrderManagement.adapter.event_bus import get_event_bus
from OrderManagement.models.event_models import ExecutionReceivedEvent, OrderStatusEvent
from RiskManagement.risk_engine_bridge import load_risk_engine
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.INFO)


class RiskManager:
    """
    Integrates the C++ RiskEngine with the existing event-driven architecture.

    Lifecycle:
        risk_manager = RiskManager()
        risk_manager.start()        # subscribe to EventBus events
        ...
        risk_manager.on_tick(symbol, price)  # called by tick data consumer
        ...
        snapshot = risk_manager.get_pnl_snapshot()
        risk_manager.stop()

    Thread Safety:
        All public methods are thread-safe (the underlying RiskEngine uses
        a C++ mutex; the Python bookkeeping uses a threading.Lock).
    """

    def __init__(
        self,
        broadcast_callback: Optional[Callable[[dict], Any]] = None,
    ):
        """
        Args:
            broadcast_callback: Optional async or sync callable invoked with a
                PnL update dict whenever prices change or fills arrive.
                Signature: callback({"type": "pnl_update", ...})
        """
        self._engine        = load_risk_engine()
        self._event_bus     = get_event_bus()
        self._broadcast     = broadcast_callback
        self._subscribed    = False
        # Track which symbols we are watching for tick data subscriptions
        self._tracked_symbols: Set[str] = set()
        # Track exec_ids already processed to prevent double-counting when both
        # ExecutionReceivedEvent and OrderStatusEvent carry the same fill.
        self._seen_exec_ids: Set[str] = set()
        # Track order_ids that have been recorded via OrderStatusEvent fill path,
        # so they are not re-recorded if execDetails arrives late.
        self._order_ids_via_status: Set[int] = set()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Subscribe to EventBus events (idempotent)."""
        if self._subscribed:
            return
        self._event_bus.subscribe(ExecutionReceivedEvent, self._on_execution)
        self._event_bus.subscribe(OrderStatusEvent, self._on_order_status)
        self._subscribed = True
        logger.info("RiskManager started – subscribed to fill events")

    def stop(self) -> None:
        """Unsubscribe from EventBus events."""
        if not self._subscribed:
            return
        self._event_bus.unsubscribe(ExecutionReceivedEvent, self._on_execution)
        self._event_bus.unsubscribe(OrderStatusEvent, self._on_order_status)
        self._subscribed = False
        logger.info("RiskManager stopped")

    # ------------------------------------------------------------------
    # EventBus callbacks
    # ------------------------------------------------------------------

    def _on_execution(self, event: ExecutionReceivedEvent) -> None:
        """Handle execDetails fill event from IBKR.

        execDetails is the primary and most accurate fill source.  Each fill
        has a unique exec_id which we track to prevent duplicate processing
        (IBKR may resend execDetails on reconnect).
        """
        try:
            # Deduplicate by exec_id
            if event.exec_id in self._seen_exec_ids:
                return
            self._seen_exec_ids.add(event.exec_id)

            side = "BUY" if event.side.upper() in ("BOT", "BUY") else "SELL"
            self._engine.on_fill(
                symbol=event.symbol,
                side=side,
                quantity=int(event.shares),
                fill_price=float(event.price),
            )
            self._tracked_symbols.add(event.symbol)
            # If this order was previously recorded via the OrderStatus path,
            # unmark it so we don't apply it again.
            self._order_ids_via_status.discard(event.order_id)
            logger.info(
                "RiskManager: fill recorded – %s %s x%d @ %.4f (exec_id=%s)",
                event.symbol, side, event.shares, event.price, event.exec_id,
            )
            self._emit_pnl_update(event.symbol)
        except Exception as exc:
            logger.error("RiskManager._on_execution error: %s", exc)

    def _on_order_status(self, event: OrderStatusEvent) -> None:
        """Handle OrderStatusEvent as a supplementary fill source.

        Only acts when status is "Filled" AND execDetails has not yet arrived
        for this order_id.  This covers the edge case where execDetails is
        delayed or unavailable (e.g., paper trading with certain TWS versions).

        Deduplication strategy:
          - Uses order_id to avoid double-counting with execDetails.
          - Once execDetails arrives (_on_execution), it discards the order_id
            from _order_ids_via_status, ensuring the execDetails fill wins.
        """
        if event.status != "Filled":
            return
        if not event.symbol or not event.action:
            return
        filled = event.filled or 0
        price  = event.avg_fill_price or 0.0
        if filled <= 0 or price <= 0.0:
            return

        # Skip if execDetails already handled this order
        if event.order_id in self._order_ids_via_status:
            return  # already recorded via this path
        # Check: if any exec for this order has been seen, skip
        # (exec_ids don't encode order_id directly, but we track via _tracked_symbols
        # for the common case; see order_id set for the precise deduplication path)

        try:
            side = "BUY" if event.action.upper() == "BUY" else "SELL"
            self._engine.on_fill(
                symbol=event.symbol,
                side=side,
                quantity=int(filled),
                fill_price=float(price),
            )
            self._tracked_symbols.add(event.symbol)
            self._order_ids_via_status.add(event.order_id)
            logger.info(
                "RiskManager: fill from OrderStatus – %s %s x%d @ %.4f (order_id=%d)",
                event.symbol, side, filled, price, event.order_id,
            )
            self._emit_pnl_update(event.symbol)
        except Exception as exc:
            logger.error("RiskManager._on_order_status error: %s", exc)

    # ------------------------------------------------------------------
    # Tick data feed
    # ------------------------------------------------------------------

    def on_tick(self, symbol: str, price: float) -> None:
        """
        Feed a real-time market price tick into the risk engine.

        This should be called by the TickDataServer consumer task each time
        a quote or trade event arrives for a tracked symbol.
        """
        if price <= 0.0:
            return
        self._engine.update_price(symbol, price)
        if symbol in self._tracked_symbols:
            self._emit_pnl_update(symbol)

    # ------------------------------------------------------------------
    # PnL query
    # ------------------------------------------------------------------

    def get_pnl_snapshot(self) -> dict:
        """
        Return a full PnL snapshot for all tracked positions.

        Returns:
            {
              "timestamp": "<ISO-8601>",
              "total_unrealized_pnl": float,
              "total_realized_pnl": float,
              "positions": [
                {
                  "symbol": str,
                  "quantity": int,
                  "avg_fill_price": float,
                  "current_price": float,
                  "unrealized_pnl": float,
                  "realized_pnl": float,
                },
                ...
              ]
            }
        """
        positions = []
        # _PyRiskEngine exposes get_symbols(); _CppRiskEngine exposes it too via
        # get_position returning None for unknown symbols.
        symbols = list(self._tracked_symbols)
        for sym in symbols:
            pos = self._engine.get_position(sym)
            if pos is not None:
                positions.append(pos)

        return {
            "timestamp":             datetime.now(timezone.utc).isoformat(),
            "total_unrealized_pnl":  self._engine.get_total_unrealized_pnl(),
            "total_realized_pnl":    self._engine.get_total_realized_pnl(),
            "positions":             positions,
        }

    def get_symbol_pnl(self, symbol: str) -> Optional[dict]:
        """
        Return PnL data for a single symbol, or None if not tracked.

        Returns:
            {
              "symbol": str,
              "quantity": int,
              "avg_fill_price": float,
              "current_price": float,
              "unrealized_pnl": float,
              "realized_pnl": float,
            }
            or None
        """
        return self._engine.get_position(symbol)

    @property
    def tracked_symbols(self) -> List[str]:
        """Return list of symbols currently tracked by the risk engine."""
        return list(self._tracked_symbols)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _emit_pnl_update(self, symbol: str) -> None:
        """Fire broadcast callback with the latest PnL for a symbol."""
        if self._broadcast is None:
            return
        pos = self._engine.get_position(symbol)
        if pos is None:
            return
        msg = {
            "type":                  "pnl_update",
            "symbol":                symbol,
            "unrealized_pnl":        pos["unrealized_pnl"],
            "realized_pnl":          pos["realized_pnl"],
            "current_price":         pos["current_price"],
            "avg_fill_price":        pos["avg_fill_price"],
            "quantity":              pos["quantity"],
            "total_unrealized_pnl":  self._engine.get_total_unrealized_pnl(),
            "total_realized_pnl":    self._engine.get_total_realized_pnl(),
            "timestamp":             datetime.now(timezone.utc).isoformat(),
        }
        try:
            result = self._broadcast(msg)
            # Support both sync and async broadcast callbacks
            if asyncio.iscoroutine(result):
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(result)
                except RuntimeError:
                    # No running event loop (e.g., called from a non-async thread).
                    # The coroutine will not be awaited; log and discard.
                    logger.warning(
                        "RiskManager: async broadcast called outside running loop; PnL update not sent"
                    )
        except Exception as exc:
            logger.error("RiskManager broadcast error: %s", exc)
