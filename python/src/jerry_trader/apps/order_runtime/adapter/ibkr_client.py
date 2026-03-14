import time
from typing import Dict, List

import pandas as pd
from ibapi.client import EClient
from ibapi.order_cancel import OrderCancel

from jerry_trader.shared.utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)


class IBClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        self.market_data = {}

    def place_order(self, contract, order):
        """
        Place an order with IBKR
        Returns: order_id
        """
        order_id = self.wrapper.nextValidOrderId
        self.placeOrder(orderId=order_id, contract=contract, order=order)
        self.reqIds(-1)
        return order_id

    def cancel_order(self, order_id):
        """Cancel an order by order_id"""

        # Create OrderCancel object with default values
        order_cancel = OrderCancel()
        order_cancel.manualOrderCancelTime = ""

        self.cancelOrder(orderId=order_id, orderCancel=order_cancel)
        return True

    def cancel_all_orders(self):
        """Cancel all open orders"""
        self.reqGlobalCancel()
        return True

    def request_completed_orders(self, api_only: bool = True) -> bool:
        """Request completed orders from IBKR (best-effort).

        IB will respond via wrapper callbacks:
        - completedOrder(...)
        - completedOrdersEnd()
        """
        try:
            # EClient method: reqCompletedOrders(apiOnly)
            self.reqCompletedOrders(api_only)
            return True
        except Exception as e:
            logger.error(f"✗ Failed to request completed orders: {e}")
            return False

    def get_open_orders(self):
        """
        Get all open orders (ACTIVE ORDERS ONLY)

        ⚠️  LIMITATION: IB Gateway only returns ACTIVE orders (PreSubmitted, Submitted, PendingCancel).
        Filled, cancelled, or rejected orders are NOT included.

        For complete order history, you need to:
        1. Persist orders to your own database as they occur
        2. Use IB's Flex Queries API for historical data
        3. Use reqExecutions() for recent fills (last 24 hours)

        Returns: Dict[order_id, order_data]
        """
        self.reqOpenOrders()
        time.sleep(1)
        # Filter orders with status that indicates they are still open
        open_statuses = ["PreSubmitted", "Submitted", "PendingSubmit", "PendingCancel"]
        return {
            oid: data
            for oid, data in self.wrapper.orders.items()
            if data.get("status") in open_statuses
        }

    def get_all_orders(self):
        """
        Get all orders tracked in current session

        ⚠️  NOTE: This returns orders from the current session only.
        IB Gateway does not provide historical order data via API.

        Returns: Dict[order_id, order_data]
        """
        return self.wrapper.orders.copy()

    def get_recent_executions(self, request_id=9999):
        """
        Get execution history from the last 24 hours

        This retrieves FILLS (executions), not order details.
        Use this to see what orders were executed recently.

        ⚠️  LIMITATION: Only covers last 24 hours

        Args:
            request_id: Unique request identifier

        Returns: Dict[execution_id, execution_data]

        Example execution data:
        {
            'execId': 'xxx',
            'time': '20231211  13:45:23',
            'symbol': 'AAPL',
            'shares': 100,
            'price': 183.50,
            'side': 'BOT',  # BOT=buy, SLD=sell
            'orderId': 3,
        }
        """
        from ibapi.execution import ExecutionFilter

        # Empty filter = get all executions
        exec_filter = ExecutionFilter()

        self.reqExecutions(request_id, exec_filter)
        time.sleep(2)

        # Executions are stored in wrapper
        return getattr(self.wrapper, "executions", {})

    def get_positions(self):
        """
        Get current positions
        Returns: Dict[symbol, position_data]
        """
        self.reqAccountUpdates(True, self.account)
        time.sleep(2)
        self.reqAccountUpdates(False, self.account)
        return self.wrapper.positions.copy()

    def get_account_summary(self):
        """
        Get account summary information
        Returns: Dict with account values
        """
        self.reqAccountUpdates(True, self.account)
        time.sleep(2)
        self.reqAccountUpdates(False, self.account)
        return self.wrapper.account_values.copy()

    # ========== Legacy Methods (kept for backward compatibility) ==========

    def get_pnl(self, request_id):
        self.reqPnL(request_id, self.account, "")
        time.sleep(2)
        return self.account_pnl

    def get_streaming_pnl(self, request_id, interval=60, pnl_type="unrealized_pnl"):
        interval = max(interval, 5) - 2
        while True:
            pnl = self.get_pnl(request_id=request_id)
            yield {"date": pd.Timestamp.now(), "pnl": pnl[request_id].get(pnl_type)}
            time.sleep(interval)

    def get_streaming_returns(self, request_id, interval, pnl_type):
        returns = pd.Series
        for snapshot in self.get_streaming_pnl(
            request_id=request_id, interval=interval, pnl_type=pnl_type
        ):
            returns.loc[snapshot["date"]] = snapshot["pnl"]
            if len(returns) > 1:
                self.portfolio_returns = returns.pct_change().dropna()
