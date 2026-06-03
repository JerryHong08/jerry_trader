"""Comprehensive root cause diagnostic for all-negative-EV strategies.

Tests:
  1. Single-stage entry (no WATCH precondition) vs two-stage WATCH→ENTRY
  2. WATCH timeout impact (30/60/120min)
  3. Exit strategy variants
  4. Factor distributions at entry for winners vs losers

Usage:
    poetry run python scripts/diagnose_root_cause.py --date 2026-03-13
    poetry run python scripts/diagnose_root_cause.py --dates 2026-03-04,2026-03-13
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Add project root
sys.path.insert(0, str(Path(__file__).parent.parent / "python" / "src"))

from jerry_trader.domain.event import Event, EventStage, TriggerType
from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.services.backtest.batch_engine import FactorEngineBatchAdapter
from jerry_trader.services.backtest.config import BacktestConfig
from jerry_trader.services.backtest.data_loader import DataLoader
from jerry_trader.services.backtest.event_evaluator import (
    WATCH_TIMEOUT_MS,
    EventEvaluator,
    TickerState,
    add_signal_density_to_factor_ts,
)
from jerry_trader.services.backtest.exit_strategy import (
    ExitResult,
    ExitStrategyConfig,
    apply_exit_strategy,
    get_default_exit_strategy,
)
from jerry_trader.services.backtest.pipeline import (
    BacktestPipeline,
    _expand_events_with_preconditions,
)
from jerry_trader.services.backtest.pre_filter import PreFilter, PreFilterConfig
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("diagnose_root_cause", log_to_file=True)


# ── Exit strategy variants ──────────────────────────────────────────────────

EXIT_VARIANTS = {
    "tp15_sl20": ExitStrategyConfig(
        name="tp15_sl20",
        description="止盈+15%，止损-20%",
        partial_exits=[(15.0, 1.0)],
        stop_loss_pct=20.0,
    ),
    "tp10_sl15": ExitStrategyConfig(
        name="tp10_sl15",
        description="止盈+10%，止损-15%",
        partial_exits=[(10.0, 1.0)],
        stop_loss_pct=15.0,
    ),
    "tp15_sl10": ExitStrategyConfig(
        name="tp15_sl10",
        description="止盈+15%，止损-10%",
        partial_exits=[(15.0, 1.0)],
        stop_loss_pct=10.0,
    ),
    "partial3_sl15": ExitStrategyConfig(
        name="partial3_sl15",
        description="+3%卖30%, +10%卖70%, SL=-15%",
        partial_exits=[(3.0, 0.30), (10.0, 0.70)],
        stop_loss_pct=15.0,
    ),
    "partial5_sl15": ExitStrategyConfig(
        name="partial5_sl15",
        description="+5%卖50%, +10%卖50%, SL=-15%",
        partial_exits=[(5.0, 0.50), (10.0, 0.50)],
        stop_loss_pct=15.0,
    ),
    "trail10_sl15": ExitStrategyConfig(
        name="trail10_sl15",
        description="Trail+10%后回撤5%止损, SL=-15%",
        partial_exits=[],
        stop_loss_pct=15.0,
        trailing_stop_trigger_pct=10.0,
        trailing_stop_pct=5.0,
    ),
    "no_exit_hold": ExitStrategyConfig(
        name="no_exit_hold",
        description="持有到收盘（无止损）",
        partial_exits=[],
        stop_loss_pct=100.0,
    ),
}


# ── Helpers ──────────────────────────────────────────────────────────────────


def _summarize(signals: list[dict]) -> dict:
    """Summarize a list of signal dicts with return_pct."""
    if not signals:
        return {"count": 0, "win_rate": 0, "avg_return": 0, "ev": 0}
    n = len(signals)
    wins = [s for s in signals if s["return_pct"] > 0]
    wr = len(wins) / n * 100 if n else 0
    avg_r = sum(s["return_pct"] for s in signals) / n if n else 0
    return {
        "count": n,
        "win_rate": round(wr, 1),
        "avg_return": round(avg_r, 2),
        "ev": round(avg_r, 2),
    }


def _load_data(date: str, tickers: list[str] | None = None):
    """Load ticker data for a date."""
    ch = get_clickhouse_client()
    pre_filter = PreFilter(ch_client=ch)
    candidates = pre_filter.find(date, PreFilterConfig())
    if tickers:
        candidates = [c for c in candidates if c.symbol in tickers]
    if not candidates:
        return {}, []
    loader = DataLoader(ch_client=ch)
    bt_config = BacktestConfig(date=date)
    ticker_data_map = loader.load(candidates, bt_config)
    return ticker_data_map, candidates


def _get_session_bounds(date: str) -> tuple[int, int]:
    """Get session start/end epoch ms."""
    from datetime import datetime

    import pytz

    ny_tz = pytz.timezone("America/New_York")
    dt = ny_tz.localize(datetime.strptime(date, "%Y-%m-%d"))
    start = int(dt.replace(hour=4, minute=0, second=0).timestamp() * 1000)
    end = int(dt.replace(hour=9, minute=30, second=0).timestamp() * 1000)
    return start, end


def _evaluate_signals_with_exit(
    entry_signals: list[dict],
    ticker_data_map: dict,
    exit_strategy: ExitStrategyConfig,
) -> list[dict]:
    """Apply exit strategy to entry signals and return enriched signals."""
    results = []
    for sig in entry_signals:
        symbol = sig["symbol"]
        td = ticker_data_map.get(symbol)
        if not td:
            continue

        sorted_trades = sorted(
            [(int(t[0]), float(t[1])) for t in td.trades], key=lambda x: x[0]
        )
        entry_time = sig["trigger_time_ms"]
        entry_price = sig.get("trigger_price", 0.0)

        # Find exact entry price
        for ts, price in sorted_trades:
            if ts <= entry_time:
                entry_price = price
            else:
                break

        entry_trades = [(ts, p) for ts, p in sorted_trades if ts >= entry_time]
        exit_result = apply_exit_strategy(
            trades=entry_trades,
            entry_time_ms=entry_time,
            entry_price=entry_price,
            strategy=exit_strategy,
        )

        sig_copy = dict(sig)
        sig_copy["return_pct"] = exit_result.total_return_pct
        sig_copy["max_price"] = exit_result.max_price
        sig_copy["min_price"] = exit_result.min_price
        sig_copy["mfe"] = (exit_result.max_price - entry_price) / entry_price * 100
        sig_copy["mae"] = (exit_result.min_price - entry_price) / entry_price * 100
        sig_copy["exit_reasons"] = exit_result.exit_reasons
        sig_copy["entry_price"] = entry_price
        results.append(sig_copy)

    return results


# ── Test 1: Single-stage vs Two-stage entry ──────────────────────────────────


def test_single_stage_vs_two_stage(
    ticker_data_map: dict,
    candidates: list,
    date: str,
    evaluator: EventEvaluator,
) -> dict:
    """Compare single-stage direct entry vs two-stage WATCH→ENTRY.

    Single-stage: evaluate ENTRY events WITHOUT requiring WATCH precondition.
    Two-stage: only evaluate ENTRY after a WATCH event triggers (current logic).
    """
    session_start_ms, session_end_ms = _get_session_bounds(date)
    adapter = FactorEngineBatchAdapter()
    exit_strategy = get_default_exit_strategy()

    # Build first_entry_map
    first_entry_map: dict[str, int] = {}
    for c in candidates:
        first_entry_map[c.symbol] = c.first_entry_ms

    watch_events = [e for e in evaluator.events if e.stage == EventStage.WATCH]
    entry_events = [e for e in evaluator.events if e.stage == EventStage.ENTRY]

    # ── Two-stage (current) ──
    two_stage_signals = []
    for symbol, td in ticker_data_map.items():
        factor_ts = adapter.compute(td)
        factor_ts = add_signal_density_to_factor_ts(factor_ts)
        trigger_prices = {int(t[0]): float(t[1]) for t in td.trades}
        first_entry_ms = first_entry_map.get(symbol)
        ticker_state = TickerState(symbol=symbol)

        # WATCH first
        for event in watch_events:
            _, ticker_state = evaluator.evaluate_ticker(
                event=event,
                symbol=symbol,
                factor_ts=factor_ts,
                session_start_ms=session_start_ms,
                session_end_ms=session_end_ms,
                trigger_prices=trigger_prices,
                first_entry_ms=first_entry_ms,
                ticker_state=ticker_state,
            )

        # ENTRY after WATCH
        if ticker_state.watch_triggered:
            for event in entry_events:
                sigs, ticker_state = evaluator.evaluate_ticker(
                    event=event,
                    symbol=symbol,
                    factor_ts=factor_ts,
                    session_start_ms=session_start_ms,
                    session_end_ms=session_end_ms,
                    trigger_prices=trigger_prices,
                    first_entry_ms=first_entry_ms,
                    ticker_state=ticker_state,
                )
                for s in sigs:
                    s["symbol"] = symbol
                two_stage_signals.extend(sigs)
                if ticker_state.entry_triggered:
                    break

    two_stage_signals = _evaluate_signals_with_exit(
        two_stage_signals, ticker_data_map, exit_strategy
    )
    two_stage_summary = _summarize(two_stage_signals)

    # ── Single-stage (skip WATCH, direct ENTRY) ──
    single_stage_signals = []
    for symbol, td in ticker_data_map.items():
        factor_ts = adapter.compute(td)
        factor_ts = add_signal_density_to_factor_ts(factor_ts)
        trigger_prices = {int(t[0]): float(t[1]) for t in td.trades}
        first_entry_ms = first_entry_map.get(symbol)
        ticker_state = TickerState(symbol=symbol)

        # Direct ENTRY: set watch_triggered=True to bypass precondition
        ticker_state.watch_triggered = True
        ticker_state.watch_time_ms = first_entry_ms or session_start_ms

        for event in entry_events:
            sigs, ticker_state = evaluator.evaluate_ticker(
                event=event,
                symbol=symbol,
                factor_ts=factor_ts,
                session_start_ms=session_start_ms,
                session_end_ms=session_end_ms,
                trigger_prices=trigger_prices,
                first_entry_ms=first_entry_ms,
                ticker_state=ticker_state,
            )
            for s in sigs:
                s["symbol"] = symbol
            single_stage_signals.extend(sigs)
            if ticker_state.entry_triggered:
                break

    single_stage_signals = _evaluate_signals_with_exit(
        single_stage_signals, ticker_data_map, exit_strategy
    )
    single_stage_summary = _summarize(single_stage_signals)

    # ── Also test: WATCH only (count how many pass WATCH but don't enter) ──
    watch_count = 0
    entry_after_watch_count = 0
    for symbol, td in ticker_data_map.items():
        factor_ts = adapter.compute(td)
        factor_ts = add_signal_density_to_factor_ts(factor_ts)
        trigger_prices = {int(t[0]): float(t[1]) for t in td.trades}
        first_entry_ms = first_entry_map.get(symbol)
        ticker_state = TickerState(symbol=symbol)

        for event in watch_events:
            _, ticker_state = evaluator.evaluate_ticker(
                event=event,
                symbol=symbol,
                factor_ts=factor_ts,
                session_start_ms=session_start_ms,
                session_end_ms=session_end_ms,
                trigger_prices=trigger_prices,
                first_entry_ms=first_entry_ms,
                ticker_state=ticker_state,
            )
        if ticker_state.watch_triggered:
            watch_count += 1
            for event in entry_events:
                sigs, ticker_state = evaluator.evaluate_ticker(
                    event=event,
                    symbol=symbol,
                    factor_ts=factor_ts,
                    session_start_ms=session_start_ms,
                    session_end_ms=session_end_ms,
                    trigger_prices=trigger_prices,
                    first_entry_ms=first_entry_ms,
                    ticker_state=ticker_state,
                )
                if sigs:
                    entry_after_watch_count += 1
                    break

    return {
        "two_stage": two_stage_summary,
        "two_stage_detail": two_stage_signals,
        "single_stage": single_stage_summary,
        "single_stage_detail": single_stage_signals,
        "funnel": {
            "candidates": len(candidates),
            "watch_triggered": watch_count,
            "entry_after_watch": entry_after_watch_count,
            "watch_pass_rate": f"{watch_count}/{len(candidates)} ({watch_count/len(candidates)*100:.0f}%)",
            "entry_pass_rate": (
                f"{entry_after_watch_count}/{watch_count} ({entry_after_watch_count/watch_count*100:.0f}%)"
                if watch_count
                else "N/A"
            ),
        },
    }


# ── Test 2: WATCH timeout impact ──────────────────────────────────────────────


def test_watch_timeout(
    ticker_data_map: dict,
    candidates: list,
    date: str,
    evaluator: EventEvaluator,
) -> dict:
    """Test different WATCH timeout values."""
    timeouts = [15, 30, 60, 120]  # minutes
    results = {}
    session_start_ms, session_end_ms = _get_session_bounds(date)
    adapter = FactorEngineBatchAdapter()
    exit_strategy = get_default_exit_strategy()

    first_entry_map: dict[str, int] = {}
    for c in candidates:
        first_entry_map[c.symbol] = c.first_entry_ms

    watch_events = [e for e in evaluator.events if e.stage == EventStage.WATCH]
    entry_events = [e for e in evaluator.events if e.stage == EventStage.ENTRY]

    for timeout_min in timeouts:
        timeout_ms = timeout_min * 60 * 1000

        # Patch WATCH_TIMEOUT_MS
        import jerry_trader.services.backtest.event_evaluator as ev_mod

        ev_mod.WATCH_TIMEOUT_MS = timeout_ms

        all_signals = []
        for symbol, td in ticker_data_map.items():
            factor_ts = adapter.compute(td)
            factor_ts = add_signal_density_to_factor_ts(factor_ts)
            trigger_prices = {int(t[0]): float(t[1]) for t in td.trades}
            first_entry_ms = first_entry_map.get(symbol)
            ticker_state = TickerState(symbol=symbol)

            for event in watch_events:
                _, ticker_state = evaluator.evaluate_ticker(
                    event=event,
                    symbol=symbol,
                    factor_ts=factor_ts,
                    session_start_ms=session_start_ms,
                    session_end_ms=session_end_ms,
                    trigger_prices=trigger_prices,
                    first_entry_ms=first_entry_ms,
                    ticker_state=ticker_state,
                )

            if ticker_state.watch_triggered:
                for event in entry_events:
                    sigs, ticker_state = evaluator.evaluate_ticker(
                        event=event,
                        symbol=symbol,
                        factor_ts=factor_ts,
                        session_start_ms=session_start_ms,
                        session_end_ms=session_end_ms,
                        trigger_prices=trigger_prices,
                        first_entry_ms=first_entry_ms,
                        ticker_state=ticker_state,
                    )
                    for s in sigs:
                        s["symbol"] = symbol
                    all_signals.extend(sigs)
                    if ticker_state.entry_triggered:
                        break

        all_signals = _evaluate_signals_with_exit(
            all_signals, ticker_data_map, exit_strategy
        )
        results[f"timeout_{timeout_min}min"] = _summarize(all_signals)
        results[f"timeout_{timeout_min}min_detail"] = all_signals

    # Restore default
    ev_mod.WATCH_TIMEOUT_MS = 30 * 60 * 1000

    return results


# ── Test 3: Exit strategy comparison ─────────────────────────────────────────


def test_exit_strategies(
    ticker_data_map: dict,
    candidates: list,
    date: str,
    evaluator: EventEvaluator,
) -> dict:
    """Compare different exit strategies on the SAME entry signals."""
    session_start_ms, session_end_ms = _get_session_bounds(date)
    adapter = FactorEngineBatchAdapter()

    first_entry_map: dict[str, int] = {}
    for c in candidates:
        first_entry_map[c.symbol] = c.first_entry_ms

    watch_events = [e for e in evaluator.events if e.stage == EventStage.WATCH]
    entry_events = [e for e in evaluator.events if e.stage == EventStage.ENTRY]

    # First, collect all entry signals (using current logic)
    raw_signals = []
    for symbol, td in ticker_data_map.items():
        factor_ts = adapter.compute(td)
        factor_ts = add_signal_density_to_factor_ts(factor_ts)
        trigger_prices = {int(t[0]): float(t[1]) for t in td.trades}
        first_entry_ms = first_entry_map.get(symbol)
        ticker_state = TickerState(symbol=symbol)

        for event in watch_events:
            _, ticker_state = evaluator.evaluate_ticker(
                event=event,
                symbol=symbol,
                factor_ts=factor_ts,
                session_start_ms=session_start_ms,
                session_end_ms=session_end_ms,
                trigger_prices=trigger_prices,
                first_entry_ms=first_entry_ms,
                ticker_state=ticker_state,
            )

        if ticker_state.watch_triggered:
            for event in entry_events:
                sigs, ticker_state = evaluator.evaluate_ticker(
                    event=event,
                    symbol=symbol,
                    factor_ts=factor_ts,
                    session_start_ms=session_start_ms,
                    session_end_ms=session_end_ms,
                    trigger_prices=trigger_prices,
                    first_entry_ms=first_entry_ms,
                    ticker_state=ticker_state,
                )
                for s in sigs:
                    s["symbol"] = symbol
                raw_signals.extend(sigs)
                if ticker_state.entry_triggered:
                    break

    # Now test each exit strategy on these same signals
    results = {}
    for name, strategy in EXIT_VARIANTS.items():
        signals_with_exit = _evaluate_signals_with_exit(
            raw_signals, ticker_data_map, strategy
        )
        results[name] = _summarize(signals_with_exit)
        results[f"{name}_detail"] = signals_with_exit

    return results


# ── Test 4: Factor distribution at entry (winners vs losers) ─────────────────


def analyze_factor_distributions(signals: list[dict]) -> dict:
    """Analyze factor distributions for winners vs losers at entry point."""
    if not signals:
        return {}

    winners = [s for s in signals if s["return_pct"] > 0]
    losers = [s for s in signals if s["return_pct"] <= 0]

    # Collect all factor names
    factor_names = set()
    for s in signals:
        factor_names.update(s.get("factors", {}).keys() if "factors" in s else s.keys())
    # Remove non-factor keys
    non_factors = {
        "symbol",
        "trigger_time_ms",
        "trigger_time_ns",
        "trigger_price",
        "event_name",
        "event_stage",
        "return_pct",
        "max_price",
        "min_price",
        "mfe",
        "mae",
        "exit_reasons",
        "entry_price",
        "session_phase",
    }
    factor_names -= non_factors

    analysis = {}
    for fname in sorted(factor_names):
        w_vals = []
        l_vals = []
        for s in winners:
            factors = s.get("factors", s)
            if fname in factors and factors[fname] is not None:
                w_vals.append(float(factors[fname]))
        for s in losers:
            factors = s.get("factors", s)
            if fname in factors and factors[fname] is not None:
                l_vals.append(float(factors[fname]))

        if not w_vals or not l_vals:
            continue

        import statistics

        analysis[fname] = {
            "winner_mean": round(statistics.mean(w_vals), 3),
            "loser_mean": round(statistics.mean(l_vals), 3),
            "winner_median": round(statistics.median(w_vals), 3),
            "loser_median": round(statistics.median(l_vals), 3),
            "diff_pct": round(
                (statistics.mean(w_vals) - statistics.mean(l_vals))
                / (abs(statistics.mean(l_vals)) + 0.0001)
                * 100,
                1,
            ),
        }

    return analysis


# ── Test 5: Price trajectory analysis (MFE/MAE profile) ──────────────────────


def analyze_trajectories(signals: list[dict]) -> dict:
    """Analyze price trajectories for all signals."""
    if not signals:
        return {}

    mfe_values = [s.get("mfe", 0) for s in signals if "mfe" in s]
    mae_values = [s.get("mae", 0) for s in signals if "mae" in s]

    # Bucket MFE (max favorable excursion)
    mfe_buckets = {
        "mfe_0_3": 0,
        "mfe_3_5": 0,
        "mfe_5_10": 0,
        "mfe_10_15": 0,
        "mfe_15plus": 0,
    }
    for mfe in mfe_values:
        if mfe < 3:
            mfe_buckets["mfe_0_3"] += 1
        elif mfe < 5:
            mfe_buckets["mfe_3_5"] += 1
        elif mfe < 10:
            mfe_buckets["mfe_5_10"] += 1
        elif mfe < 15:
            mfe_buckets["mfe_10_15"] += 1
        else:
            mfe_buckets["mfe_15plus"] += 1

    # For losers, how many had positive MFE?
    losers_with_mfe = sum(
        1 for s in signals if s["return_pct"] <= 0 and s.get("mfe", 0) >= 3
    )
    losers_count = sum(1 for s in signals if s["return_pct"] <= 0)

    # MFE for winners vs losers
    winners = [s for s in signals if s["return_pct"] > 0]
    losers = [s for s in signals if s["return_pct"] <= 0]

    import statistics

    return {
        "mfe_buckets": mfe_buckets,
        "mfe_mean": round(statistics.mean(mfe_values), 1) if mfe_values else 0,
        "mae_mean": round(statistics.mean(mae_values), 1) if mae_values else 0,
        "losers_with_positive_mfe": (
            f"{losers_with_mfe}/{losers_count} ({losers_with_mfe/losers_count*100:.0f}%)"
            if losers_count
            else "N/A"
        ),
        "winner_mfe_mean": (
            round(statistics.mean([w.get("mfe", 0) for w in winners]), 1)
            if winners
            else 0
        ),
        "loser_mfe_mean": (
            round(statistics.mean([l.get("mfe", 0) for l in losers]), 1)
            if losers
            else 0
        ),
        "winner_mae_mean": (
            round(statistics.mean([w.get("mae", 0) for w in winners]), 1)
            if winners
            else 0
        ),
        "loser_mae_mean": (
            round(statistics.mean([l.get("mae", 0) for l in losers]), 1)
            if losers
            else 0
        ),
    }


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Root cause diagnostic")
    parser.add_argument("--date", type=str, default=None, help="Single date")
    parser.add_argument(
        "--dates", type=str, default="2026-03-13", help="Comma-separated dates"
    )
    parser.add_argument(
        "--tickers", type=str, default=None, help="Comma-separated tickers"
    )
    args = parser.parse_args()

    dates = (
        args.dates.split(",")
        if args.dates
        else ([args.date] if args.date else ["2026-03-13"])
    )
    tickers = args.tickers.split(",") if args.tickers else None

    from jerry_trader.shared.utils.paths import PROJECT_ROOT

    evaluator = EventEvaluator(config_path=str(PROJECT_ROOT / "config" / "events.yaml"))

    for date in dates:
        print(f"\n{'='*80}")
        print(f"  DATE: {date}")
        print(f"{'='*80}")

        ticker_data_map, candidates = _load_data(date, tickers)
        if not ticker_data_map:
            print(f"  No data for {date}")
            continue
        print(f"  Loaded {len(ticker_data_map)} tickers, {len(candidates)} candidates")

        # ── Test 1: Single-stage vs Two-stage ──────────────────────────────
        print(f"\n── Test 1: Single-stage vs Two-stage Entry ──")
        t1_start = time.time()
        t1 = test_single_stage_vs_two_stage(
            ticker_data_map, candidates, date, evaluator
        )
        print(f"  Funnel: {t1['funnel']}")
        print(f"  Two-stage:   {t1['two_stage']}")
        print(f"  Single-stage: {t1['single_stage']}")
        if t1["two_stage"]["count"] > 0 and t1["single_stage"]["count"] > 0:
            delta_ev = t1["single_stage"]["ev"] - t1["two_stage"]["ev"]
            delta_n = t1["single_stage"]["count"] - t1["two_stage"]["count"]
            print(f"  Δ signals: +{delta_n}, Δ EV: {delta_ev:+.2f}%")
        print(f"  (elapsed: {time.time() - t1_start:.1f}s)")

        # ── Test 2: WATCH timeout ──────────────────────────────────────────
        print(f"\n── Test 2: WATCH Timeout Impact ──")
        t2_start = time.time()
        t2 = test_watch_timeout(ticker_data_map, candidates, date, evaluator)
        for key in sorted(t2):
            if not key.endswith("_detail"):
                print(f"  {key}: {t2[key]}")
        print(f"  (elapsed: {time.time() - t2_start:.1f}s)")

        # ── Test 3: Exit strategies ────────────────────────────────────────
        print(f"\n── Test 3: Exit Strategy Comparison ──")
        t3_start = time.time()
        t3 = test_exit_strategies(ticker_data_map, candidates, date, evaluator)
        for key in sorted(t3):
            if not key.endswith("_detail"):
                print(f"  {key}: {t3[key]}")
        print(f"  (elapsed: {time.time() - t3_start:.1f}s)")

        # ── Test 4: Factor distributions ───────────────────────────────────
        print(f"\n── Test 4: Factor Distributions (Winners vs Losers) ──")
        # Use two-stage signals from Test 1
        t4 = analyze_factor_distributions(t1["two_stage_detail"])
        if t4:
            print(f"  {'Factor':<30} {'Winner μ':>10} {'Loser μ':>10} {'Δ%':>8}")
            print(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*8}")
            for fname, stats in sorted(
                t4.items(), key=lambda x: abs(x[1]["diff_pct"]), reverse=True
            ):
                print(
                    f"  {fname:<30} {stats['winner_mean']:>10.3f} {stats['loser_mean']:>10.3f} {stats['diff_pct']:>7.1f}%"
                )
        else:
            print("  No factor data available")

        # ── Test 5: Trajectory analysis ────────────────────────────────────
        print(f"\n── Test 5: Price Trajectory Analysis ──")
        t5 = analyze_trajectories(t1["two_stage_detail"])
        for key, val in t5.items():
            print(f"  {key}: {val}")

    print(f"\n{'='*80}")
    print("  Diagnostic complete.")


if __name__ == "__main__":
    main()
