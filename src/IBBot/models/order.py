from ibapi.order import Order

BUY = "BUY"
SELL = "SELL"


def market(action, quantity, OutsideRth=False, tif="DAY"):
    """Create a market order"""
    order = Order()
    order.action = action
    order.orderType = "MKT"
    order.totalQuantity = quantity
    order.outsideRth = OutsideRth
    order.tif = tif
    return order


def limit(action, quantity, limit_price, OutsideRth=False, tif="DAY"):
    """Create a limit order"""
    order = Order()
    order.action = action
    order.orderType = "LMT"
    order.totalQuantity = quantity
    order.lmtPrice = limit_price
    order.outsideRth = OutsideRth
    order.tif = tif
    return order


def stop(action, quantity, stop_price, OutsideRth=False, tif="DAY"):
    """Create a stop order"""
    order = Order()
    order.action = action
    order.orderType = "STP"
    order.auxPrice = stop_price
    order.totalQuantity = quantity
    order.outsideRth = OutsideRth
    order.tif = tif
    return order


def stop_limit(action, quantity, stop_price, limit_price, OutsideRth=False, tif="DAY"):
    """Create a stop-limit order"""
    order = Order()
    order.action = action
    order.orderType = "STP LMT"
    order.auxPrice = stop_price
    order.lmtPrice = limit_price
    order.totalQuantity = quantity
    order.outsideRth = OutsideRth
    order.tif = tif
    return order
