"""Output module for backtest results.

Two output modes:
  1. Console — formatted summary table with per-rule stats
  2. ClickHouse — persist results to backtest_results table
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from jerry_trader.domain.backtest.types import BacktestResult, SignalResult
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.output", log_to_file=True)


# ─────────────────────────────────────────────────────────────────────────────
# Console Output
# ─────────────────────────────────────────────────────────────────────────────


def print_summary(result: BacktestResult) -> None:
    """Print formatted backtest summary to console.

    Shows per-rule breakdown, overall stats, and individual signal details.
    """
    print("\n" + "=" * 80)
    print(f"  BACKTEST RESULT — {result.date}")
    print("=" * 80)

    if not result.signals:
        print("\n  No signals triggered.")
        print("=" * 80 + "\n")
        return

    # Group signals by rule
    by_rule: dict[str, list[SignalResult]] = {}
    for sig in result.signals:
        by_rule.setdefault(sig.rule_id, []).append(sig)

    # Per-rule summary
    for rule_id, signals in by_rule.items():
        _print_rule_summary(rule_id, signals)

    # Overall stats
    print(f"\n{'─' * 80}")
    print(f"  TOTAL SIGNALS: {result.total_signals}")
    print(f"  RULES TESTED:  {', '.join(result.rules_tested)}")

    if result.win_rate:
        win_str = "  ".join(f"{h}: {r:.0%}" for h, r in result.win_rate.items())
        print(f"  WIN RATES:     {win_str}")

    if result.avg_return:
        ret_str = "  ".join(f"{h}: {r:+.2%}" for h, r in result.avg_return.items())
        print(f"  AVG RETURNS:   {ret_str}")

    print(f"  PROFIT FACTOR: {result.profit_factor:.2f}")
    print(f"  AVG SLIPPAGE:  {result.avg_slippage:.3%}")
    print(f"  AVG MFE:       {result.avg_mfe:+.2%}")
    print(f"  AVG MAE:       {result.avg_mae:+.2%}")

    if result.avg_time_to_peak_ms > 0:
        peak_sec = result.avg_time_to_peak_ms / 1000
        print(f"  AVG TIME PEAK: {peak_sec:.0f}s ({peak_sec / 60:.1f}m)")

    # Per-rule grouped stats
    if by_rule:
        print(f"\n  Per-Rule Statistics")
        print(f"  {'─' * 76}")
        _print_grouped_stats("Rule", by_rule)

    # Per-ticker grouped stats
    by_ticker: dict[str, list[SignalResult]] = {}
    for sig in result.signals:
        by_ticker.setdefault(sig.symbol, []).append(sig)
    if by_ticker and len(by_ticker) > 1:
        print(f"\n  Per-Ticker Statistics")
        print(f"  {'─' * 76}")
        _print_grouped_stats("Ticker", by_ticker)

    print("=" * 80 + "\n")


def _print_grouped_stats(
    label: str,
    groups: dict[str, list[SignalResult]],
) -> None:
    """Print grouped statistics (per-rule or per-ticker).

    Shows count, win rate, avg return, profit factor, avg MFE/MAE per group.
    """
    print(
        f"  {label:<16} {'#':>4} {'Win%':>6} "
        f"{'AvgRet':>8} {'PF':>6} {'MFE':>7} {'MAE':>7} {'Slip':>6}"
    )
    print(f"  {'─' * 76}")

    # Sort groups by signal count descending
    sorted_groups = sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)

    for group_id, signals in sorted_groups:
        n = len(signals)

        # Collect horizons
        all_horizons: set[str] = set()
        for sig in signals:
            all_horizons.update(sig.returns.keys())

        # Use first horizon for win rate and avg return
        primary_horizon = min(all_horizons) if all_horizons else None
        if primary_horizon:
            values = [
                sig.returns[primary_horizon]
                for sig in signals
                if primary_horizon in sig.returns
            ]
            wins = [v for v in values if v > 0]
            win_pct = len(wins) / len(values) if values else 0.0
            avg_ret = sum(values) / len(values) if values else 0.0
        else:
            win_pct = sum(1 for s in signals if s.is_winner) / n if n else 0.0
            avg_ret = 0.0

        # Profit factor
        rets: list[float] = []
        for sig in signals:
            for h in sorted(all_horizons):
                if h in sig.returns:
                    rets.append(sig.returns[h])
                    break
        gross_wins = sum(r for r in rets if r > 0)
        gross_losses = abs(sum(r for r in rets if r < 0))
        pf = gross_wins / gross_losses if gross_losses > 0 else 0.0

        # MFE / MAE / Slippage
        mfes = [s.mfe for s in signals if s.mfe is not None]
        maes = [s.mae for s in signals if s.mae is not None]
        slips = [s.slippage_pct for s in signals]
        avg_mfe = sum(mfes) / len(mfes) if mfes else 0.0
        avg_mae = sum(maes) / len(maes) if maes else 0.0
        avg_slip = sum(slips) / len(slips) if slips else 0.0

        print(
            f"  {group_id:<16} {n:>4} {win_pct:>5.0%} "
            f"{avg_ret:>+7.2%} {pf:>6.2f} {avg_mfe:>+6.2%} {avg_mae:>+6.2%} "
            f"{avg_slip:>5.2%}"
        )


def _print_rule_summary(rule_id: str, signals: list[SignalResult]) -> None:
    """Print per-rule signal details."""
    print(f"\n  Rule: {rule_id} ({len(signals)} signals)")
    print(f"  {'─' * 76}")
    print(
        f"  {'Ticker':<8} {'Time':>10} {'Price':>8} {'Entry':>8} "
        f"{'Slip%':>6} {'Returns':>30} {'MFE':>7} {'MAE':>7}"
    )
    print(f"  {'─' * 76}")

    for sig in signals:
        time_str = _format_time(sig.trigger_time_ns // 1_000_000)
        ret_str = _format_returns(sig.returns)
        mfe_str = f"{sig.mfe:+.2%}" if sig.mfe is not None else "    -"
        mae_str = f"{sig.mae:+.2%}" if sig.mae is not None else "    -"

        print(
            f"  {sig.symbol:<8} {time_str:>10} {sig.trigger_price:>8.2f} "
            f"{sig.entry_price:>8.2f} {sig.slippage_pct:>5.2%} "
            f"{ret_str:>30} {mfe_str:>7} {mae_str:>7}"
        )


def _format_time(ts_ms: int) -> str:
    """Format ms timestamp to HH:MM:SS (Eastern)."""
    try:
        dt = datetime.utcfromtimestamp(ts_ms / 1000)
        return dt.strftime("%H:%M:%S")
    except (OSError, ValueError):
        return str(ts_ms)


def _format_returns(returns: dict[str, float]) -> str:
    """Format returns dict as compact string."""
    if not returns:
        return "-"
    parts = [f"{h}:{r:+.1%}" for h, r in returns.items()]
    return " ".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# ClickHouse Persistence
# ─────────────────────────────────────────────────────────────────────────────


_BACKTEST_RESULTS_SQL = """
CREATE TABLE IF NOT EXISTS backtest_results (
    run_id String,
    date Date,
    rule_id String,
    ticker String,
    trigger_time_ns Int64,
    entry_price Float64,
    trigger_price Float64,
    slippage_pct Float64,
    factors String,
    return_30s Nullable(Float64),
    return_1m Nullable(Float64),
    return_2m Nullable(Float64),
    return_5m Nullable(Float64),
    return_10m Nullable(Float64),
    return_15m Nullable(Float64),
    return_30m Nullable(Float64),
    return_60m Nullable(Float64),
    mfe Nullable(Float64),
    mae Nullable(Float64),
    time_to_peak_ms Nullable(Int64),
    inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, rule_id, ticker, trigger_time_ns)
"""


def ensure_backtest_table(ch_client: Any) -> None:
    """Create backtest_results table if it doesn't exist."""
    try:
        ch_client.command(_BACKTEST_RESULTS_SQL)
        logger.info("ClickHouse: backtest_results table ensured")
    except Exception as e:
        logger.error(f"ClickHouse: failed to create backtest_results table: {e}")


def persist_results(
    result: BacktestResult,
    ch_client: Any,
    run_id: str,
    database: str = "jerry_trader",
) -> int:
    """Persist backtest results to ClickHouse.

    Args:
        result: BacktestResult with signals.
        ch_client: ClickHouse client.
        run_id: Unique run identifier.
        database: Database name.

    Returns:
        Number of rows inserted.
    """
    if not result.signals:
        return 0

    ensure_backtest_table(ch_client)

    rows: list[list] = []
    from datetime import date as date_type

    result_date = result.date
    if isinstance(result_date, str):
        result_date = datetime.strptime(result_date, "%Y-%m-%d").date()
    elif isinstance(result_date, datetime):
        result_date = result_date.date()

    for sig in result.signals:
        row = [
            run_id,
            result_date,
            sig.rule_id,
            sig.symbol,
            sig.trigger_time_ns,
            sig.entry_price,
            sig.trigger_price,
            sig.slippage_pct,
            json.dumps(sig.factors),
            sig.returns.get("30s"),
            sig.returns.get("1m"),
            sig.returns.get("2m"),
            sig.returns.get("5m"),
            sig.returns.get("10m"),
            sig.returns.get("15m"),
            sig.returns.get("30m"),
            sig.returns.get("60m"),
            sig.mfe,
            sig.mae,
            sig.time_to_peak_ms,
        ]
        rows.append(row)

    columns = [
        "run_id",
        "date",
        "rule_id",
        "ticker",
        "trigger_time_ns",
        "entry_price",
        "trigger_price",
        "slippage_pct",
        "factors",
        "return_30s",
        "return_1m",
        "return_2m",
        "return_5m",
        "return_10m",
        "return_15m",
        "return_30m",
        "return_60m",
        "mfe",
        "mae",
        "time_to_peak_ms",
    ]

    try:
        ch_client.insert(
            f"{database}.backtest_results",
            rows,
            column_names=columns,
        )
        logger.info(f"ClickHouse: persisted {len(rows)} results for run {run_id}")
        return len(rows)
    except Exception as e:
        logger.error(f"ClickHouse: failed to persist results: {e}")
        return 0
