"""Verify live and backtest factor computation paths produce identical results.

Key insight: both paths must use the SAME bars and SAME trades to be
comparable. This test builds bars once externally, then runs both
paths on the shared data.

Comparison:
  - Bar factors: run both paths on the same bars, compare per-bar_end
  - Tick factors: run both paths on the same trades, compare per 1s interval
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python" / "src"))

from jerry_trader._rust import BarBuilder as RustBarBuilder
from jerry_trader.domain.market import Bar
from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.services.factor.factor_registry import get_factor_registry
from jerry_trader.services.factor.indicators import BarIndicator, TickIndicator
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("factor_identity_test", log_to_file=True)


def load_trades(
    ch, ticker: str, date: str, limit: int = 0
) -> list[tuple[int, float, int]]:
    limit_clause = f" LIMIT {limit}" if limit > 0 else ""
    query = f"""
        SELECT sip_timestamp, price, size
        FROM trades WHERE ticker = '{ticker}' AND date = '{date}'
        ORDER BY sip_timestamp{limit_clause}
    """
    result = ch.query(query)
    trades = []
    for row in result.result_rows:
        ts_ms = int(int(row[0]) / 1_000_000)
        trades.append((ts_ms, float(row[1]), int(float(row[2])) if row[2] else 1))
    return trades


def build_bars(trades: list[tuple[int, float, int]], symbol: str) -> list[Bar]:
    builder = RustBarBuilder(["1m"])
    builder.configure_watermark(late_arrival_ms=0, idle_close_ms=1)
    builder.ingest_trades_batch(symbol, [(t, p, float(s)) for t, p, s in trades])
    builder.flush()
    bars = []
    for raw in builder.drain_completed():
        try:
            bars.append(Bar.from_rust_dict(raw))
        except (ValueError, KeyError):
            pass
    return bars


def compute_bar_factors(
    bars: list[Bar], indicators: list[BarIndicator]
) -> dict[int, dict[str, float]]:
    """Feed bars to indicators chronologically. Same as live AND backtest."""
    ts: dict[int, dict[str, float]] = {}
    for bar in bars:
        factors: dict[str, float] = {}
        for ind in indicators:
            value = ind.update(bar)
            if value is not None:
                factors[ind.name] = value
        if factors:
            ts[bar.bar_end] = factors
    return ts


def compute_tick_factors_live(
    trades: list[tuple[int, float, int]], indicators: list[TickIndicator]
) -> dict[int, dict[str, float]]:
    """Python tick indicator path: on_tick() + compute() at uniform 1s grid.

    Uses the same uniform 1s compute grid as bootstrap_trade_rate so both
    paths produce identical results when the algorithms match.
    """
    ts: dict[int, dict[str, float]] = {}
    sorted_trades = sorted(trades, key=lambda x: x[0])
    if not sorted_trades:
        return ts

    t0 = sorted_trades[0][0]
    t_end = sorted_trades[-1][0]
    # Align to next 1s boundary, matching bootstrap_trade_rate's grid:
    #   first_ts - (first_ts % 1000) + 1000
    first_compute = t0 - (t0 % 1000) + 1000
    compute_grid = list(range(first_compute, t_end + 1000, 1000))

    trade_idx = 0
    for compute_ts in compute_grid:
        while (
            trade_idx < len(sorted_trades) and sorted_trades[trade_idx][0] <= compute_ts
        ):
            ts_ms, price, size = sorted_trades[trade_idx]
            for ind in indicators:
                ind.on_tick(ts_ms, float(price), int(size))
            trade_idx += 1

        factors: dict[str, float] = {}
        for ind in indicators:
            value = ind.compute(compute_ts)
            if value is not None:
                factors[ind.name] = value
        if factors:
            ts[compute_ts] = factors
    return ts


def compute_tick_factors_backtest(
    trades: list[tuple[int, float, int]], indicators: list[TickIndicator]
) -> dict[int, dict[str, float]]:
    """Rust batch tick path: bootstrap_trade_rate (current backtest path)."""
    from jerry_trader._rust import bootstrap_trade_rate

    timestamps = [int(t) for t, _, _ in trades]
    if not timestamps:
        return {}

    ts: dict[int, dict[str, float]] = {}
    for ind in indicators:
        if hasattr(ind, "window_ms"):
            result = bootstrap_trade_rate(
                timestamps,
                window_ms=getattr(ind, "window_ms", 20_000),
                min_trades=getattr(ind, "min_trades", 5),
                compute_interval_ms=1000,
            )
            for t_ms, rate in result.snapshots:
                ts.setdefault(t_ms, {})[ind.name] = rate
    return ts


def compare(
    live: dict[int, dict[str, float]],
    backtest: dict[int, dict[str, float]],
    tol: float = 1e-9,
) -> tuple[int, list[tuple]]:
    """Compare two factor timeseries.
    Returns (matched_count, list of mismatch tuples).
    """
    common_ts = sorted(set(live.keys()) & set(backtest.keys()))
    matched = 0
    mismatches = []

    for ts in common_ts:
        lf = live[ts]
        bf = backtest[ts]
        all_names = set(lf.keys()) | set(bf.keys())
        for name in sorted(all_names):
            lv = lf.get(name)
            bv = bf.get(name)
            if lv is not None and bv is not None:
                if abs(lv - bv) < tol:
                    matched += 1
                else:
                    mismatches.append((ts, name, lv, bv, abs(lv - bv)))
            elif lv is not None:
                mismatches.append((ts, name, lv, None, None))
            elif bv is not None:
                mismatches.append((ts, name, None, bv, None))

    return matched, mismatches


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2026-03-13")
    parser.add_argument("--tickers", default="EONR,CXAI")
    parser.add_argument("--max-trades", type=int, default=100_000)
    args = parser.parse_args()

    tickers = [t.strip() for t in args.tickers.split(",")]
    ch = get_clickhouse_client()
    registry = get_factor_registry()

    all_mismatches = []
    all_matched = 0

    for ticker in tickers:
        print(f"\n{'='*60}")
        print(f"{ticker} on {args.date}")
        print(f"{'='*60}")

        trades = load_trades(ch, ticker, args.date, args.max_trades)
        print(f"  Trades: {len(trades)}")
        if not trades:
            continue

        # Build bars ONCE, shared by both paths
        bars = build_bars(trades, ticker)
        print(f"  Bars: {len(bars)}")
        if not bars:
            continue

        # ── Bar factors: IDENTICAL path ──
        # Live path == Backtest path (both use ind.update(bar))
        # Test: run with two separate indicator sets to verify determinism
        bar_inds1 = [
            i
            for i in registry.create_indicators_for_type("bar")
            if isinstance(i, BarIndicator)
        ]
        bar_inds2 = [
            i
            for i in registry.create_indicators_for_type("bar")
            if isinstance(i, BarIndicator)
        ]
        bar_vals1 = compute_bar_factors(bars, bar_inds1)
        bar_vals2 = compute_bar_factors(bars, bar_inds2)

        bar_matched, bar_mismatches = compare(bar_vals1, bar_vals2)
        all_matched += bar_matched
        all_mismatches.extend(bar_mismatches)

        print(
            f"  Bar factors (same path, separate instances): "
            f"matched={bar_matched} mismatches={len(bar_mismatches)}"
        )

        for ts, name, v1, v2, diff in bar_mismatches[:3]:
            print(f"    MISMATCH: {ts} {name}: inst1={v1} inst2={v2} diff={diff}")

        # ── Tick factors: DIVERGENT paths ──
        # Live path: Python on_tick() + compute() every 1s
        # Backtest path: Rust bootstrap_trade_rate()
        tick_inds = [
            i
            for i in registry.create_indicators_for_type("trade")
            if isinstance(i, TickIndicator)
        ]
        live_tick = compute_tick_factors_live(trades, tick_inds)

        tick_inds2 = [
            i
            for i in registry.create_indicators_for_type("trade")
            if isinstance(i, TickIndicator)
        ]
        bt_tick = compute_tick_factors_backtest(trades, tick_inds2)

        tick_matched, tick_mismatches = compare(live_tick, bt_tick)
        all_matched += tick_matched
        all_mismatches.extend(tick_mismatches)

        print(
            f"  Tick factors (live vs backtest path): "
            f"matched={tick_matched} mismatches={len(tick_mismatches)} "
            f"(live tss={len(live_tick)} bt tss={len(bt_tick)} "
            f"overlap={len(set(live_tick)&set(bt_tick))})"
        )

        for ts, name, lv, bv, diff in tick_mismatches[:3]:
            print(f"    MISMATCH: {ts} {name}: live={lv} bt={bv} diff={diff}")

    # ── Final ──
    print(f"\n{'='*60}")
    if all_mismatches:
        print(
            f"VERDICT: FAIL — {len(all_mismatches)} mismatches ({all_matched} matched)"
        )
        for ts, name, v1, v2, diff in all_mismatches[:10]:
            print(f"  {ts} {name}: {v1} vs {v2} (Δ={diff})")
    else:
        print(f"VERDICT: PASS — all {all_matched} factor pairs match")


if __name__ == "__main__":
    main()
