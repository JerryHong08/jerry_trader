"""Return and metrics computation for backtest.

Given trigger points from SignalEvaluator and raw trade/quote data,
computes:
  - Slippage-adjusted entry price (ask * (1 + buffer))
  - Multi-horizon returns (30s, 1m, 2m, 5m, ..., 60m)
  - MFE (Maximum Favorable Excursion)
  - MAE (Maximum Adverse Excursion)
  - Time to peak

Uses actual trade prices for return computation — no synthetic pricing.
"""

from __future__ import annotations

from bisect import bisect_left

from jerry_trader.domain.backtest.types import SignalResult
from jerry_trader.services.backtest.batch_engine import FactorTimeseries
from jerry_trader.services.backtest.config import TickerData, horizon_label
from jerry_trader.services.backtest.evaluator import TriggerPoint
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.metrics", log_to_file=True)


# ─────────────────────────────────────────────────────────────────────────────
# Slippage Model
# ─────────────────────────────────────────────────────────────────────────────


def find_ask_at_time(
    quotes: list[tuple[int, float, float, int, int]],
    target_ms: int,
) -> float | None:
    """Find the ask price at or just before target_ms.

    Args:
        quotes: Sorted list of (ts_ms, bid, ask, bid_size, ask_size).
        target_ms: Target timestamp in ms.

    Returns:
        Ask price, or None if no quote available.
    """
    if not quotes:
        return None

    # Binary search for the last quote at or before target_ms
    timestamps = [q[0] for q in quotes]
    idx = bisect_left(timestamps, target_ms)

    if idx < len(timestamps) and timestamps[idx] == target_ms:
        ask = quotes[idx][2]
        return ask if ask > 0 else None

    if idx > 0:
        ask = quotes[idx - 1][2]
        return ask if ask > 0 else None

    return None


def compute_entry_price(
    trigger_price: float,
    ask_price: float | None,
    slippage_buffer: float = 0.001,
    default_slippage: float = 0.002,
) -> tuple[float, float]:
    """Compute slippage-adjusted entry price.

    Args:
        trigger_price: Price at signal trigger time.
        ask_price: Ask price at or near trigger time (from quotes).
        slippage_buffer: Buffer on ask price (e.g. 0.001 = 0.1%).
        default_slippage: Fallback slippage when no ask available.

    Returns:
        (entry_price, slippage_pct) tuple.
    """
    if ask_price and ask_price > 0:
        entry_price = ask_price * (1 + slippage_buffer)
    else:
        entry_price = trigger_price * (1 + default_slippage)

    slippage_pct = (entry_price - trigger_price) / trigger_price
    return entry_price, slippage_pct


# ─────────────────────────────────────────────────────────────────────────────
# Price Lookup from Trades
# ─────────────────────────────────────────────────────────────────────────────


def _build_trade_price_index(
    trades: list[tuple[int, float, int]],
) -> tuple[list[int], list[float]]:
    """Extract sorted timestamps and prices from trades.

    Returns:
        (timestamps, prices) — parallel arrays for bisect lookup.
    """
    timestamps = [int(t) for t, _, _ in trades]
    prices = [float(p) for _, p, _ in trades]
    return timestamps, prices


def get_price_at(
    trade_ts: list[int],
    trade_prices: list[float],
    target_ms: int,
) -> float | None:
    """Get trade price at or just before target_ms.

    Returns:
        Price, or None if no trade before target_ms.
    """
    if not trade_ts:
        return None

    idx = bisect_left(trade_ts, target_ms)

    if idx < len(trade_ts) and trade_ts[idx] == target_ms:
        return trade_prices[idx]

    if idx > 0:
        return trade_prices[idx - 1]

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Return / MFE / MAE Computation
# ─────────────────────────────────────────────────────────────────────────────


def compute_returns_and_metrics(
    trigger: TriggerPoint,
    ticker_data: TickerData,
    factor_ts: FactorTimeseries,
    horizons_ms: list[int],
    slippage_buffer: float = 0.001,
    default_slippage: float = 0.002,
) -> SignalResult:
    """Compute full return metrics for a single trigger event.

    Steps:
      1. Find ask price at trigger → compute entry price with slippage
      2. For each horizon, look up trade price and compute return from entry
      3. Scan all trades after trigger for MFE/MAE
      4. Record time to peak

    Args:
        trigger: The trigger event from SignalEvaluator.
        ticker_data: Raw data for this ticker.
        factor_ts: Factor timeseries (unused here, for future extensions).
        horizons_ms: List of return horizons to compute.
        slippage_buffer: Buffer for slippage model.
        default_slippage: Fallback slippage when no ask available.

    Returns:
        SignalResult with all computed metrics.
    """
    trigger_ms = trigger.trigger_time_ms
    trigger_price = (
        trigger.trigger_price or ticker_data.candidate.price_at_entry
        if ticker_data.candidate
        else 0.0
    )

    if trigger_price <= 0:
        # Fallback: use first available trade price
        if ticker_data.trades:
            trigger_price = ticker_data.trades[0][1]
        else:
            logger.warning(f"No price for {trigger.symbol} at trigger, skipping")
            trigger_price = 0.0

    # 1. Slippage
    ask_price = find_ask_at_time(ticker_data.quotes, trigger_ms)
    entry_price, slippage_pct = compute_entry_price(
        trigger_price, ask_price, slippage_buffer, default_slippage
    )

    # 2. Build trade price index for lookups
    trade_ts, trade_prices = _build_trade_price_index(ticker_data.trades)

    # Find the trade index at trigger time (start scanning from here)
    trigger_idx = bisect_left(trade_ts, trigger_ms)

    # 3. Compute returns at each horizon
    returns: dict[str, float] = {}
    for h_ms in horizons_ms:
        target_ms = trigger_ms + h_ms
        future_price = get_price_at(trade_ts, trade_prices, target_ms)
        if future_price is not None and entry_price > 0:
            ret = (future_price - entry_price) / entry_price
            returns[horizon_label(h_ms)] = ret

    # 4. MFE / MAE / Time-to-peak — scan all trades after trigger
    mfe = 0.0
    mae = 0.0
    time_to_peak_ms: int | None = None

    # Define a scan window (e.g. up to 60 minutes after trigger)
    scan_end_ms = (
        trigger_ms + max(horizons_ms) if horizons_ms else trigger_ms + 3_600_000
    )

    max_return = -float("inf")
    for i in range(trigger_idx, len(trade_ts)):
        ts = trade_ts[i]
        if ts > scan_end_ms:
            break
        price = trade_prices[i]
        if entry_price <= 0:
            continue
        ret = (price - entry_price) / entry_price
        if ret > max_return:
            max_return = ret
            time_to_peak_ms = ts
        mfe = max(mfe, ret)
        mae = min(mae, ret)

    if mfe == 0.0 and mae == 0.0 and trigger_idx >= len(trade_ts):
        # No trades after trigger
        mfe = None  # type: ignore
        mae = None  # type: ignore
        time_to_peak_ms = None

    return SignalResult(
        rule_id=trigger.rule_id,
        symbol=trigger.symbol,
        trigger_time_ns=trigger_ms * 1_000_000,
        trigger_price=trigger_price,
        entry_price=entry_price,
        slippage_pct=slippage_pct,
        factors=trigger.factors,
        ask_price=ask_price,
        returns=returns,
        mfe=mfe,
        mae=mae,
        time_to_peak_ms=time_to_peak_ms,
    )


def compute_batch_metrics(
    triggers: list[TriggerPoint],
    ticker_data: TickerData,
    factor_ts: FactorTimeseries,
    horizons_ms: list[int],
    slippage_buffer: float = 0.001,
    default_slippage: float = 0.002,
) -> list[SignalResult]:
    """Compute metrics for all triggers of a single ticker.

    Args:
        triggers: All trigger points for this ticker.
        ticker_data: Raw data for this ticker.
        factor_ts: Factor timeseries.
        horizons_ms: Return horizons.
        slippage_buffer: Slippage buffer.
        default_slippage: Fallback slippage.

    Returns:
        List of SignalResult, one per trigger.
    """
    results: list[SignalResult] = []
    for trigger in triggers:
        try:
            result = compute_returns_and_metrics(
                trigger,
                ticker_data,
                factor_ts,
                horizons_ms,
                slippage_buffer,
                default_slippage,
            )
            results.append(result)
        except Exception as e:
            logger.warning(
                f"Metrics failed for {trigger.symbol} "
                f"rule={trigger.rule_id} ts={trigger.trigger_time_ms}: {e}"
            )
    return results
