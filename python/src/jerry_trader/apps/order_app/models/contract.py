from ibapi.contract import Contract

from jerry_trader.apps.order_app.models.order_models import OrderRequest


def stock(symbol, exchange, currency):
    contract = Contract()
    contract.symbol = symbol
    contract.exchange = exchange
    contract.currency = currency
    contract.secType = "STK"
    return contract


def from_request(
    req: OrderRequest, exchange: str = "SMART", currency: str = "USD"
) -> Contract:
    """Convert an OrderRequest into an IBKR Contract."""
    if req.sec_type == "STK":
        return stock(req.symbol, exchange, currency)
    raise ValueError(f"Unsupported security type: {req.sec_type}")
