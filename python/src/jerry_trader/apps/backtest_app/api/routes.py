"""API routes for backtest visualization."""

import asyncio
import json
import logging
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException

from jerry_trader.apps.backtest_app.api.websocket import broadcast_to_clients
from jerry_trader.apps.backtest_app.models import (
    BacktestProgress,
    BacktestRequest,
    FactorsTimelineBatchRequest,
    generate_experiment_id,
)
from jerry_trader.apps.backtest_app.runner import run_backtest
from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.services.backtest.event_evaluator import EventEvaluator
from jerry_trader.shared.utils.paths import PROJECT_ROOT

logger = logging.getLogger(__name__)

router = APIRouter()

# Store running experiments
running_experiments: dict[str, dict] = {}

# Module-level candidate cache for label endpoint (in-memory + disk)
_label_candidate_cache: dict[str, list] = {}
_label_float_map_cache: dict[str, float] = {}
_LABEL_CACHE_DIR = PROJECT_ROOT / "data" / ".label_cache"

# Per-date cache of (trades_df, quotes_df) keyed by ticker.
# Populated by the prefetch endpoint so subsequent single-ticker requests avoid CH.
_label_tq_cache: dict[tuple[str, str], tuple["pd.DataFrame", "pd.DataFrame"]] = {}


def _load_candidates_from_disk(date: str) -> list | None:
    """Try to load cached candidates from disk."""
    import json
    from pathlib import Path

    cache_file = _LABEL_CACHE_DIR / f"{date}.json"
    if not cache_file.exists():
        return None
    try:
        data = json.loads(cache_file.read_text())

        # Reconstruct as simple objects with the attributes we need
        class CandidateProxy:
            def __init__(self, d):
                self.__dict__.update(d)

        return [CandidateProxy(d) for d in data]
    except Exception:
        return None


def _save_candidates_to_disk(date: str, candidates: list) -> None:
    """Serialize candidates to disk cache."""
    import json

    _LABEL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = _LABEL_CACHE_DIR / f"{date}.json"
    data = [
        {
            "symbol": c.symbol,
            "first_entry_ms": c.first_entry_ms,
            "gain_at_entry": c.gain_at_entry,
            "prev_close": c.prev_close,
            "volume_at_entry": c.volume_at_entry,
            "relative_volume": c.relative_volume,
            "max_gain": c.max_gain,
        }
        for c in candidates
    ]
    cache_file.write_text(json.dumps(data, indent=2))


def _get_available_events() -> list[str]:
    """Get list of available event names from events.yaml."""
    config_path = PROJECT_ROOT / "config" / "events.yaml"
    evaluator = EventEvaluator(config_path=str(config_path))
    return [e.name for e in evaluator.events]


@router.get("/")
def backtest_root():
    """Backtest API status."""
    return {
        "status": "ok",
        "service": "Backtest Visualization API",
        "version": "1.0.0",
        "running_experiments": len(running_experiments),
    }


@router.get("/events")
def get_events():
    """Get available event names."""
    return {"events": _get_available_events()}


@router.post("/start")
async def start_backtest(request: BacktestRequest):
    """Start a backtest run.

    Returns experiment_id for tracking.
    """
    experiment_id = generate_experiment_id()

    # Validate events
    available_events = _get_available_events()
    for event in request.events:
        if event not in available_events:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown event: {event}. Available: {available_events}",
            )

    # Store experiment metadata
    running_experiments[experiment_id] = {
        "experiment_id": experiment_id,
        "date": request.date,
        "events": request.events,
        "tickers": request.tickers,
        "hold_duration_minutes": request.hold_duration_minutes,
        "status": "pending",
        "created_at": datetime.now(),
        "total_signals": 0,
    }

    # Progress callback for WebSocket broadcast
    def progress_callback(progress: BacktestProgress):
        _broadcast_progress(progress)

    # Run backtest in background
    asyncio.create_task(_run_and_store(experiment_id, request, progress_callback))

    return {
        "experiment_id": experiment_id,
        "status": "pending",
        "message": f"Backtest started for {request.date}",
    }


async def _run_and_store(
    experiment_id: str,
    request: BacktestRequest,
    progress_callback: callable,
):
    """Run backtest and store results."""
    try:
        running_experiments[experiment_id]["status"] = "running"

        result = await run_backtest(
            date=request.date,
            events=request.events,
            tickers=request.tickers,
            hold_duration_minutes=request.hold_duration_minutes,
            progress_callback=progress_callback,
        )

        running_experiments[experiment_id]["status"] = "completed"
        running_experiments[experiment_id]["total_signals"] = result["total_signals"]
        running_experiments[experiment_id]["avg_return"] = result["avg_return"]
        running_experiments[experiment_id]["win_rate"] = result["win_rate"]
        running_experiments[experiment_id]["completed_at"] = datetime.now()

    except Exception as e:
        running_experiments[experiment_id]["status"] = "failed"
        running_experiments[experiment_id]["error"] = str(e)
        progress_callback(BacktestProgress(type="error", message=str(e)))


def _broadcast_progress(progress: BacktestProgress):
    """Broadcast progress to all WebSocket clients."""
    message = {
        "type": progress.type,
        "date": progress.date,
        "step": progress.step,
        "percent": progress.percent,
        "ticker": progress.ticker,
        "entry_time": progress.entry_time,
        "entry_price": progress.entry_price,
        "message": progress.message,
        "experiment_id": progress.experiment_id,
        "total_signals": progress.total_signals,
    }
    broadcast_to_clients(message)


@router.get("/results/{experiment_id}")
def get_results(experiment_id: str):
    """Get experiment summary."""
    if experiment_id not in running_experiments:
        raise HTTPException(status_code=404, detail="Experiment not found")

    return running_experiments[experiment_id]


@router.get("/chart/{experiment_id}/{ticker}")
def get_chart_data(experiment_id: str, ticker: str):
    """Get chart data for single ticker.

    Returns signals + bar data for visualization.
    """
    ch = get_clickhouse_client()
    if not ch:
        raise HTTPException(status_code=500, detail="ClickHouse unavailable")

    # Get signals from backtest_signals
    try:
        signals_result = ch.query(
            """
            SELECT entry_time, entry_price, exit_time, exit_price,
                   return_pct, exit_reason, factors, max_price, min_price,
                   event_name, date
            FROM jerry_trader.backtest_signals
            WHERE experiment_id = {exp_id:String}
              AND ticker = {ticker:String}
            ORDER BY entry_time ASC
            """,
            parameters={"exp_id": experiment_id, "ticker": ticker},
        )

        signals = []
        factor_names = set()
        signal_date = None
        for row in signals_result.result_rows:
            (
                entry_time,
                entry_price,
                exit_time,
                exit_price,
                return_pct,
                exit_reason,
                factors_json,
                max_price,
                min_price,
                event_name,
                date_val,
            ) = row

            if signal_date is None:
                signal_date = date_val.isoformat() if date_val else None

            factors = json.loads(factors_json) if factors_json else {}
            factor_names.update(factors.keys())

            # Entry signal
            signals.append(
                {
                    "time": int(entry_time.timestamp()),
                    "type": "entry",
                    "price": entry_price,
                    "event_name": event_name,
                    "factors": factors,
                }
            )

            # Exit signal
            signals.append(
                {
                    "time": int(exit_time.timestamp()),
                    "type": "exit",
                    "price": exit_price,
                    "return_pct": return_pct,
                    "max_price": max_price,
                    "min_price": min_price,
                }
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

    # Get bars from backtest_bars (for backtest visualization)
    # Use date from signal to filter, plus time range around signals
    if signals:
        first_signal_time = signals[0]["time"]
        last_signal_time = signals[-1]["time"]
    else:
        # No signals - return empty bars
        first_signal_time = 0
        last_signal_time = 0
        signal_date = None

    try:
        # Query backtest_bars (separate from realtime ohlcv_bars)
        if signal_date:
            bars_result = ch.query(
                """
                SELECT bar_start, open, high, low, close, volume
                FROM jerry_trader.backtest_bars
                WHERE ticker = {ticker:String}
                  AND date = {date:String}
                  AND timeframe = '1m'
                  AND bar_start >= {start_ms:Int64}
                  AND bar_start <= {end_ms:Int64}
                ORDER BY bar_start ASC
                """,
                parameters={
                    "ticker": ticker,
                    "date": signal_date,
                    "start_ms": first_signal_time * 1000 - 3600000,  # 1 hour before
                    "end_ms": last_signal_time * 1000 + 3600000,  # 1 hour after
                },
            )
        else:
            # No date available - return empty bars
            bars_result = ch.query(
                """
                SELECT bar_start, open, high, low, close, volume
                FROM jerry_trader.backtest_bars
                WHERE ticker = {ticker:String}
                  AND timeframe = '1m'
                  AND bar_start >= {start_ms:Int64}
                  AND bar_start <= {end_ms:Int64}
                ORDER BY bar_start ASC
                LIMIT 1000
                """,
                parameters={
                    "ticker": ticker,
                    "start_ms": first_signal_time * 1000 - 3600000,
                    "end_ms": last_signal_time * 1000 + 3600000,
                },
            )

        bars = []
        for row in bars_result.result_rows:
            bar_start, o, h, l, c, vol = row
            bars.append(
                {
                    "time": bar_start // 1000,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": vol,
                }
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bars query failed: {e}")

    # Build factors time-series from entry signals
    factors_ts = []
    for sig in signals:
        if sig["type"] == "entry" and sig.get("factors"):
            factor_point: dict = {"time": sig["time"]}
            # Only include numeric factor values (skip strings like symbol, session_phase)
            for k, v in sig["factors"].items():
                if isinstance(v, (int, float)):
                    factor_point[k] = v
            factors_ts.append(factor_point)

    return {
        "meta": {
            "ticker": ticker,
            "experiment_id": experiment_id,
            "date": signal_date,
            "total_signals": len(signals) // 2,
            "bar_count": len(bars),
            "factor_names": sorted(
                k
                for k in factor_names
                if any(
                    isinstance(s.get("factors", {}).get(k), (int, float))
                    for s in signals
                    if s["type"] == "entry"
                )
            ),
        },
        "bars": bars,
        "signals": signals,
        "factors": factors_ts,
    }


@router.get("/experiments")
def list_experiments():
    """List all experiments from memory + ClickHouse persisted."""
    # Get experiments from memory (currently running or recently completed)
    in_memory_experiments = {
        exp_id: {
            "experiment_id": exp_id,
            "date": exp["date"],
            "status": exp["status"],
            "total_signals": exp.get("total_signals", 0),
            "created_at": (
                exp["created_at"].isoformat() if "created_at" in exp else None
            ),
        }
        for exp_id, exp in running_experiments.items()
    }

    # Get persisted experiments from ClickHouse (for experiments not in memory)
    ch = get_clickhouse_client()
    persisted_experiments = {}

    if ch:
        try:
            result = ch.query(
                """
                SELECT
                    experiment_id,
                    min(date) as date,
                    count(*) as total_signals,
                    avg(return_pct) as avg_return,
                    avg(case when return_pct > 0 then 1 else 0 end) as win_rate
                FROM jerry_trader.backtest_signals
                GROUP BY experiment_id
                ORDER BY experiment_id DESC
                """
            )

            for row in result.result_rows:
                exp_id, date, total_signals, avg_return, win_rate = row
                # Only add if not already in memory
                if exp_id not in in_memory_experiments:
                    persisted_experiments[exp_id] = {
                        "experiment_id": exp_id,
                        "date": date.isoformat() if date else None,
                        "status": "completed",  # All persisted are completed
                        "total_signals": total_signals,
                        "avg_return": round(avg_return, 2) if avg_return else 0,
                        "win_rate": round(win_rate, 2) if win_rate else 0,
                        "created_at": None,
                    }

        except Exception as e:
            # Log but don't fail - just return in-memory experiments
            import logging

            logging.getLogger(__name__).warning(f"ClickHouse query failed: {e}")

    # Combine: in-memory experiments take precedence
    all_experiments = {**persisted_experiments, **in_memory_experiments}

    # Sort by experiment_id descending (most recent first)
    sorted_experiments = sorted(
        all_experiments.values(),
        key=lambda x: x["experiment_id"],
        reverse=True,
    )

    return {"experiments": sorted_experiments}


@router.get("/experiments/{experiment_id}/tickers")
def get_experiment_tickers(experiment_id: str):
    """Get tickers with signals in this experiment."""
    ch = get_clickhouse_client()
    if not ch:
        raise HTTPException(status_code=500, detail="ClickHouse unavailable")

    try:
        result = ch.query(
            """
            SELECT
                ticker,
                count(*) as signal_count,
                avg(return_pct) as avg_return,
                avg(case when return_pct > 0 then 1 else 0 end) as win_rate
            FROM jerry_trader.backtest_signals
            WHERE experiment_id = {exp_id:String}
            GROUP BY ticker
            ORDER BY signal_count DESC
            """,
            parameters={"exp_id": experiment_id},
        )

        tickers = []
        for row in result.result_rows:
            ticker, signal_count, avg_return, win_rate = row
            tickers.append(
                {
                    "ticker": ticker,
                    "signal_count": signal_count,
                    "avg_return": round(avg_return, 2) if avg_return else 0,
                    "win_rate": round(win_rate, 2) if win_rate else 0,
                }
            )

        return {"experiment_id": experiment_id, "tickers": tickers}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")


@router.get("/diagnostics/{experiment_id}")
def get_diagnostics(experiment_id: str):
    """Get comprehensive signal diagnostics for an experiment.

    Returns:
        - Price path analysis (avg time to max/min, MFE/MAE ratio)
        - Exit reason breakdown
        - Factor correlations with returns
        - Loss attribution patterns
    """
    ch = get_clickhouse_client()
    if not ch:
        raise HTTPException(status_code=500, detail="ClickHouse unavailable")

    # Query all signals for the experiment
    try:
        result = ch.query(
            """
            SELECT
                entry_time, entry_price, exit_time, exit_price,
                return_pct, exit_reason, factors, max_price, min_price,
                time_to_max_ms, time_to_min_ms, event_name, ticker
            FROM jerry_trader.backtest_signals
            WHERE experiment_id = {exp_id:String}
            ORDER BY entry_time ASC
            """,
            parameters={"exp_id": experiment_id},
        )

        signals_data = []
        all_factors: dict[str, list[float]] = {}
        returns_list: list[float] = []

        for row in result.result_rows:
            (
                entry_time,
                entry_price,
                exit_time,
                exit_price,
                return_pct,
                exit_reason,
                factors_json,
                max_price,
                min_price,
                time_to_max_ms,
                time_to_min_ms,
                event_name,
                ticker,
            ) = row

            factors = json.loads(factors_json) if factors_json else {}
            returns_list.append(return_pct)

            # Collect factor values for correlation
            for factor_name, factor_value in factors.items():
                if factor_name not in all_factors:
                    all_factors[factor_name] = []
                # Handle various factor value types (float, int, string)
                try:
                    if isinstance(factor_value, (float, int)):
                        all_factors[factor_name].append(float(factor_value))
                    elif isinstance(factor_value, str):
                        # Try to parse string as float, skip if not numeric
                        all_factors[factor_name].append(float(factor_value))
                    else:
                        all_factors[factor_name].append(0.0)
                except (ValueError, TypeError):
                    # Skip non-numeric factor values (e.g., "PAYP", session_phase)
                    pass

            signals_data.append(
                {
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "return_pct": return_pct,
                    "exit_reason": exit_reason,
                    "max_price": max_price,
                    "min_price": min_price,
                    "time_to_max_ms": time_to_max_ms,
                    "time_to_min_ms": time_to_min_ms,
                    "event_name": event_name,
                    "ticker": ticker,
                    "factors": factors,
                }
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

    if not signals_data:
        return {
            "experiment_id": experiment_id,
            "total_signals": 0,
            "diagnostics": None,
        }

    # Calculate diagnostics
    total_signals = len(signals_data)
    avg_return = sum(s["return_pct"] for s in signals_data) / total_signals
    win_count = sum(1 for s in signals_data if s["return_pct"] > 0)
    win_rate = win_count / total_signals

    # Price path analysis
    avg_time_to_max_sec = (
        sum(s["time_to_max_ms"] for s in signals_data) / total_signals / 1000
    )
    avg_time_to_min_sec = (
        sum(s["time_to_min_ms"] for s in signals_data) / total_signals / 1000
    )

    # MFE (Maximum Favorable Excursion) / MAE (Maximum Adverse Excursion)
    # MFE = max upside potential, MAE = max downside risk
    mfe_values = []
    mae_values = []
    for s in signals_data:
        entry_price = s["entry_price"]
        max_price = s["max_price"]
        min_price = s["min_price"]
        if entry_price > 0:
            mfe = ((max_price - entry_price) / entry_price) * 100  # max upside %
            mae = ((min_price - entry_price) / entry_price) * 100  # max downside %
            mfe_values.append(mfe)
            mae_values.append(abs(mae))

    avg_mfe = sum(mfe_values) / len(mfe_values) if mfe_values else 0
    avg_mae = sum(mae_values) / len(mae_values) if mae_values else 0
    mfe_mae_ratio = avg_mfe / avg_mae if avg_mae > 0 else 0

    # Exit reason breakdown
    exit_reason_counts: dict[str, int] = {}
    for s in signals_data:
        reason = s["exit_reason"]
        exit_reason_counts[reason] = exit_reason_counts.get(reason, 0) + 1

    # Event-based win rate
    event_win_rates: dict[str, dict] = {}
    for s in signals_data:
        event = s["event_name"]
        if event not in event_win_rates:
            event_win_rates[event] = {"wins": 0, "total": 0, "returns": []}
        event_win_rates[event]["total"] += 1
        event_win_rates[event]["returns"].append(s["return_pct"])
        if s["return_pct"] > 0:
            event_win_rates[event]["wins"] += 1

    for event in event_win_rates:
        total = event_win_rates[event]["total"]
        wins = event_win_rates[event]["wins"]
        avg_ret = sum(event_win_rates[event]["returns"]) / total
        event_win_rates[event]["win_rate"] = wins / total if total > 0 else 0
        event_win_rates[event]["avg_return"] = round(avg_ret, 2)
        del event_win_rates[event]["returns"]

    # Factor correlations (Pearson)
    factor_correlations: dict[str, float] = {}
    for factor_name, factor_values in all_factors.items():
        if len(factor_values) >= 2 and len(factor_values) == len(returns_list):
            # Simple Pearson correlation
            n = len(factor_values)
            mean_f = sum(factor_values) / n
            mean_r = sum(returns_list) / n

            cov = (
                sum(
                    (f - mean_f) * (r - mean_r)
                    for f, r in zip(factor_values, returns_list)
                )
                / n
            )
            std_f = (sum((f - mean_f) ** 2 for f in factor_values) / n) ** 0.5
            std_r = (sum((r - mean_r) ** 2 for r in returns_list) / n) ** 0.5

            if std_f > 0 and std_r > 0:
                correlation = cov / (std_f * std_r)
                factor_correlations[factor_name] = round(correlation, 4)

    # Loss attribution - identify patterns in losing trades
    loss_trades = [s for s in signals_data if s["return_pct"] < 0]
    loss_patterns: list[dict] = []

    if loss_trades:
        # High gap percentage pattern
        high_gap_losses = [
            s for s in loss_trades if s["factors"].get("gap_percent", 0) > 8
        ]
        if high_gap_losses:
            loss_patterns.append(
                {
                    "pattern": "high_gap_low_win",
                    "description": "Gap > 8% with negative return",
                    "count": len(high_gap_losses),
                    "avg_return": round(
                        sum(s["return_pct"] for s in high_gap_losses)
                        / len(high_gap_losses),
                        2,
                    ),
                }
            )

        # High relative volume pattern
        high_vol_losses = [
            s for s in loss_trades if s["factors"].get("relative_volume", 0) > 5
        ]
        if high_vol_losses:
            loss_patterns.append(
                {
                    "pattern": "high_rel_vol_low_win",
                    "description": "Rel_vol > 5 with negative return",
                    "count": len(high_vol_losses),
                    "avg_return": round(
                        sum(s["return_pct"] for s in high_vol_losses)
                        / len(high_vol_losses),
                        2,
                    ),
                }
            )

        # Early max (reversal pattern)
        early_max_losses = [
            s
            for s in loss_trades
            if s["time_to_max_ms"] < 60 * 1000  # max within 1 min
        ]
        if early_max_losses:
            loss_patterns.append(
                {
                    "pattern": "early_max_reversal",
                    "description": "Max price reached within 60s then reversed",
                    "count": len(early_max_losses),
                    "avg_return": round(
                        sum(s["return_pct"] for s in early_max_losses)
                        / len(early_max_losses),
                        2,
                    ),
                }
            )

    return {
        "experiment_id": experiment_id,
        "total_signals": total_signals,
        "avg_return": round(avg_return, 2),
        "win_rate": round(win_rate, 2),
        "diagnostics": {
            "avg_time_to_max_sec": round(avg_time_to_max_sec, 1),
            "avg_time_to_min_sec": round(avg_time_to_min_sec, 1),
            "avg_mfe_pct": round(avg_mfe, 2),
            "avg_mae_pct": round(avg_mae, 2),
            "mfe_mae_ratio": round(mfe_mae_ratio, 2),
            "exit_reason_breakdown": exit_reason_counts,
            "event_win_rates": event_win_rates,
            "factor_correlations": factor_correlations,
        },
        "loss_attribution": loss_patterns,
    }


@router.get("/export/{experiment_id}")
def export_experiment(experiment_id: str, format: str = "json"):
    """Export experiment results as JSON or CSV.

    Returns all signals with factors for external analysis.
    """
    from fastapi.responses import Response

    ch = get_clickhouse_client()
    if not ch:
        raise HTTPException(status_code=500, detail="ClickHouse unavailable")

    try:
        result = ch.query(
            """
            SELECT
                ticker, entry_time, entry_price, exit_time, exit_price,
                return_pct, exit_reason, factors, max_price, min_price,
                time_to_max_ms, time_to_min_ms, event_name, date
            FROM jerry_trader.backtest_signals
            WHERE experiment_id = {exp_id:String}
            ORDER BY entry_time ASC
            """,
            parameters={"exp_id": experiment_id},
        )

        signals = []
        for row in result.result_rows:
            (
                ticker,
                entry_time,
                entry_price,
                exit_time,
                exit_price,
                return_pct,
                exit_reason,
                factors_json,
                max_price,
                min_price,
                time_to_max_ms,
                time_to_min_ms,
                event_name,
                date_val,
            ) = row

            signals.append(
                {
                    "ticker": ticker,
                    "entry_time": entry_time.isoformat() if entry_time else None,
                    "entry_price": entry_price,
                    "exit_time": exit_time.isoformat() if exit_time else None,
                    "exit_price": exit_price,
                    "return_pct": return_pct,
                    "exit_reason": exit_reason,
                    "factors": json.loads(factors_json) if factors_json else {},
                    "max_price": max_price,
                    "min_price": min_price,
                    "time_to_max_ms": time_to_max_ms,
                    "time_to_min_ms": time_to_min_ms,
                    "event_name": event_name,
                    "date": date_val.isoformat() if date_val else None,
                }
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

    if format == "csv":
        import csv
        import io

        output = io.StringIO()
        writer = csv.writer(output)

        # Header
        writer.writerow(
            [
                "ticker",
                "date",
                "entry_time",
                "entry_price",
                "exit_time",
                "exit_price",
                "return_pct",
                "exit_reason",
                "max_price",
                "min_price",
                "time_to_max_ms",
                "time_to_min_ms",
                "event_name",
            ]
        )

        # Data rows
        for sig in signals:
            writer.writerow(
                [
                    sig["ticker"],
                    sig["date"],
                    sig["entry_time"],
                    sig["entry_price"],
                    sig["exit_time"],
                    sig["exit_price"],
                    sig["return_pct"],
                    sig["exit_reason"],
                    sig["max_price"],
                    sig["min_price"],
                    sig["time_to_max_ms"],
                    sig["time_to_min_ms"],
                    sig["event_name"],
                ]
            )

        return Response(
            content=output.getvalue(),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={experiment_id}.csv"
            },
        )

    else:  # JSON
        return Response(
            content=json.dumps(
                {
                    "experiment_id": experiment_id,
                    "signals": signals,
                    "total_signals": len(signals),
                },
                indent=2,
            ),
            media_type="application/json",
            headers={
                "Content-Disposition": f"attachment; filename={experiment_id}.json"
            },
        )


@router.get("/factors-timeline/{experiment_id}/{ticker}")
def get_factors_timeline(
    experiment_id: str,
    ticker: str,
    factor: str,
    window_minutes: int = 60,
    warmup_minutes: int = 30,  # Extra time before entry for factor warmup
    resolution: str = "1m",  # Output resolution: "1s", "1m", "5m"
):
    """Get factor timeline for visualization.

    Computes factors in real-time for the time window around entry signals.
    Uses the same FactorEngineBatchAdapter as backtest for consistency.

    Args:
        experiment_id: Experiment ID
        ticker: Ticker symbol
        factor: Factor name to compute (e.g., "relative_volume", "trade_rate")
        window_minutes: Minutes before/after entry to display (default: 60)
        warmup_minutes: Extra minutes before window for factor warmup (default: 30)
        resolution: Output time resolution - "1s" (per second), "1m" (per minute/bar), "5m" (per 5 min)

    Returns:
        List of {time, value} points for the factor, sampled at specified resolution
    """
    ch = get_clickhouse_client()
    if not ch:
        raise HTTPException(status_code=500, detail="ClickHouse unavailable")

    # 1. Get entry signal time and date
    try:
        result = ch.query(
            """
            SELECT entry_time, date
            FROM jerry_trader.backtest_signals
            WHERE experiment_id = {exp_id:String}
              AND ticker = {ticker:String}
              AND entry_time IS NOT NULL
            ORDER BY entry_time ASC
            LIMIT 1
            """,
            parameters={"exp_id": experiment_id, "ticker": ticker},
        )

        if not result.result_rows:
            raise HTTPException(status_code=404, detail="No entry signal found")

        entry_time, date_val = result.result_rows[0]
        date_str = date_val.isoformat() if date_val else None

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

    # 2. Compute time range (with warmup for bar-based factors)
    from datetime import timedelta

    # Display window: entry ± window_minutes
    display_window_ms = window_minutes * 60 * 1000
    entry_ms = int(entry_time.timestamp() * 1000)
    display_start_ms = entry_ms - display_window_ms
    display_end_ms = entry_ms + display_window_ms

    # Data loading window: add warmup before display_start for factor initialization
    warmup_ms = warmup_minutes * 60 * 1000
    data_start_ms = display_start_ms - warmup_ms

    # 3. Load trades for the time range
    try:
        trades_result = ch.query(
            """
            SELECT sip_timestamp, price, size
            FROM jerry_trader.trades
            WHERE ticker = {ticker:String}
              AND date = {date:String}
              AND sip_timestamp >= {start_ns:Int64}
              AND sip_timestamp <= {end_ns:Int64}
            ORDER BY sip_timestamp ASC
            """,
            parameters={
                "ticker": ticker,
                "date": date_str,
                "start_ns": data_start_ms * 1_000_000,
                "end_ns": display_end_ms * 1_000_000,
            },
        )

        trades = [
            (int(row[0] // 1_000_000), float(row[1]), int(row[2]))
            for row in trades_result.result_rows
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Trades query failed: {e}")

    # 4. Load quotes for quote-based factors
    quotes = []
    try:
        quotes_result = ch.query(
            """
            SELECT sip_timestamp, bid_price, ask_price, bid_size, ask_size
            FROM jerry_trader.quotes
            WHERE ticker = {ticker:String}
              AND date = {date:String}
              AND sip_timestamp >= {start_ns:Int64}
              AND sip_timestamp <= {end_ns:Int64}
            ORDER BY sip_timestamp ASC
            """,
            parameters={
                "ticker": ticker,
                "date": date_str,
                "start_ns": data_start_ms * 1_000_000,
                "end_ns": display_end_ms * 1_000_000,
            },
        )

        quotes = [
            (
                int(row[0] // 1_000_000),
                float(row[1]),
                float(row[2]),
                int(row[3]),
                int(row[4]),
            )
            for row in quotes_result.result_rows
        ]

    except Exception as e:
        # Quotes may not exist for some tickers/dates - continue without them
        logger.warning(f"Quotes query failed for {ticker}: {e}")

    # 5. Get candidate info for entry_gap_pct
    # NOTE: snapshot table may not exist for some deployments
    # entry_gap_pct will still be available in signals data via chart API
    candidate = None
    # Removed snapshot query - table doesn't exist in this deployment

    # 6. Compute factor timeline using FactorEngineBatchAdapter
    from jerry_trader.services.backtest.batch_engine import FactorEngineBatchAdapter
    from jerry_trader.services.backtest.config import TickerData

    # Create TickerData with all data sources
    ticker_data = TickerData(
        symbol=ticker,
        trades=trades,
        quotes=quotes,
        bars_1m=[],  # Bars built from trades by FactorEngineBatchAdapter
        candidate=candidate,
    )

    # Compute factors
    engine = FactorEngineBatchAdapter()
    factor_ts = engine.compute(ticker_data)

    # 7. Extract requested factor (filter to display window only)
    timeline = []
    for ts_ms, factors in sorted(factor_ts.items()):
        # Only include timestamps in the display window
        if ts_ms < display_start_ms or ts_ms > display_end_ms:
            continue
        if factor in factors:
            timeline.append(
                {
                    "time": ts_ms // 1000,  # Convert to seconds for frontend
                    "value": factors[factor],
                }
            )

    # 8. Sample timeline to requested resolution
    def sample_timeline(timeline: list[dict], resolution: str) -> list[dict]:
        """Sample timeline to specified resolution.

        Args:
            timeline: Raw timeline (may have multiple points per interval)
            resolution: "1s", "1m", or "5m"

        Returns:
            Sampled timeline with one point per interval (last value for each interval)
        """
        if not timeline or resolution == "1s":
            return timeline

        # Calculate interval in seconds
        interval_sec = {"1m": 60, "5m": 300}.get(resolution, 60)

        # Group by interval and take last value
        sampled: dict[int, float] = {}
        for point in timeline:
            ts = point["time"]
            # Floor to interval
            interval_ts = (ts // interval_sec) * interval_sec
            sampled[interval_ts] = point["value"]  # Keep last value in interval

        # Convert to sorted list
        return [{"time": t, "value": v} for t, v in sorted(sampled.items())]

    sampled_timeline = sample_timeline(timeline, resolution)

    return {
        "experiment_id": experiment_id,
        "ticker": ticker,
        "factor": factor,
        "entry_time": int(entry_time.timestamp()),
        "window_minutes": window_minutes,
        "warmup_minutes": warmup_minutes,
        "resolution": resolution,
        "trades_count": len(trades),
        "quotes_count": len(quotes),
        "raw_points": len(timeline),
        "points": len(sampled_timeline),
        "timeline": sampled_timeline,
    }


# Factor timeline cache (in-memory, per-process)
_factor_timeline_cache: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_SECONDS = 300  # 5 minutes


@router.post("/factors-timeline-batch/{experiment_id}/{ticker}")
def get_factors_timeline_batch(
    experiment_id: str,
    ticker: str,
    request: FactorsTimelineBatchRequest,
):
    """Batch compute multiple factor timelines in a single request.

    This is more efficient than individual requests because:
    1. Trades/quotes are loaded once
    2. FactorEngine computes all factors in one pass
    3. Results are extracted for each requested factor

    Args:
        experiment_id: Experiment ID
        ticker: Ticker symbol
        factors: List of factor names to compute
        window_minutes: Minutes before/after entry to display
        warmup_minutes: Extra minutes before window for factor warmup
        resolution: Output time resolution

    Returns:
        Dict mapping factor_name -> timeline
    """
    ch = get_clickhouse_client()
    if not ch:
        raise HTTPException(status_code=500, detail="ClickHouse unavailable")

    # 1. Get entry signal time and date
    try:
        result = ch.query(
            """
            SELECT entry_time, date
            FROM jerry_trader.backtest_signals
            WHERE experiment_id = {exp_id:String}
              AND ticker = {ticker:String}
              AND entry_time IS NOT NULL
            ORDER BY entry_time ASC
            LIMIT 1
            """,
            parameters={"exp_id": experiment_id, "ticker": ticker},
        )

        if not result.result_rows:
            raise HTTPException(status_code=404, detail="No entry signal found")

        entry_time, date_val = result.result_rows[0]
        date_str = date_val.isoformat() if date_val else None

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

    # 2. Compute time range
    from datetime import timedelta

    factors = request.factors
    window_minutes = request.window_minutes
    warmup_minutes = request.warmup_minutes
    resolution = request.resolution

    display_window_ms = window_minutes * 60 * 1000
    entry_ms = int(entry_time.timestamp() * 1000)
    display_start_ms = entry_ms - display_window_ms
    display_end_ms = entry_ms + display_window_ms
    warmup_ms = warmup_minutes * 60 * 1000
    data_start_ms = display_start_ms - warmup_ms

    # 3. Check cache
    cache_key = f"{experiment_id}:{ticker}:{date_str}:{data_start_ms}:{display_end_ms}"
    current_time = time.time()

    if cache_key in _factor_timeline_cache:
        cache_time, cached_factors = _factor_timeline_cache[cache_key]
        if current_time - cache_time < _CACHE_TTL_SECONDS:
            # Cache hit - extract requested factors
            timelines: dict[str, list[dict]] = {}
            for factor_id in factors:
                if factor_id in cached_factors:
                    timelines[factor_id] = cached_factors[factor_id]
                else:
                    timelines[factor_id] = {"timeline": [], "points": 0}

            return {
                "experiment_id": experiment_id,
                "ticker": ticker,
                "factors": factors,
                "cached": True,
                "timelines": timelines,
            }

    # 4. Load trades
    try:
        trades_result = ch.query(
            """
            SELECT sip_timestamp, price, size
            FROM jerry_trader.trades
            WHERE ticker = {ticker:String}
              AND date = {date:String}
              AND sip_timestamp >= {start_ns:Int64}
              AND sip_timestamp <= {end_ns:Int64}
            ORDER BY sip_timestamp ASC
            """,
            parameters={
                "ticker": ticker,
                "date": date_str,
                "start_ns": data_start_ms * 1_000_000,
                "end_ns": display_end_ms * 1_000_000,
            },
        )

        trades = [
            (int(row[0] // 1_000_000), float(row[1]), int(row[2]))
            for row in trades_result.result_rows
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Trades query failed: {e}")

    # 5. Load quotes
    quotes = []
    try:
        quotes_result = ch.query(
            """
            SELECT sip_timestamp, bid_price, ask_price, bid_size, ask_size
            FROM jerry_trader.quotes
            WHERE ticker = {ticker:String}
              AND date = {date:String}
              AND sip_timestamp >= {start_ns:Int64}
              AND sip_timestamp <= {end_ns:Int64}
            ORDER BY sip_timestamp ASC
            """,
            parameters={
                "ticker": ticker,
                "date": date_str,
                "start_ns": data_start_ms * 1_000_000,
                "end_ns": display_end_ms * 1_000_000,
            },
        )

        quotes = [
            (
                int(row[0] // 1_000_000),
                float(row[1]),
                float(row[2]),
                int(row[3]),
                int(row[4]),
            )
            for row in quotes_result.result_rows
        ]

    except Exception as e:
        logger.warning(f"Quotes query failed for {ticker}: {e}")

    # 6. Compute all factors in one pass
    from jerry_trader.services.backtest.batch_engine import FactorEngineBatchAdapter
    from jerry_trader.services.backtest.config import TickerData

    ticker_data = TickerData(
        symbol=ticker,
        trades=trades,
        quotes=quotes,
        bars_1m=[],
        candidate=None,
    )

    engine = FactorEngineBatchAdapter()
    factor_ts = engine.compute(ticker_data)

    # 7. Extract all factor timelines and cache
    all_timelines: dict[str, list[dict]] = {}

    # Sample function
    def sample_timeline(timeline: list[dict], resolution: str) -> list[dict]:
        if not timeline or resolution == "1s":
            return timeline

        interval_sec = {"1m": 60, "5m": 300}.get(resolution, 60)
        sampled: dict[int, float] = {}
        for point in timeline:
            ts = point["time"]
            interval_ts = (ts // interval_sec) * interval_sec
            sampled[interval_ts] = point["value"]

        return [{"time": t, "value": v} for t, v in sorted(sampled.items())]

    # Extract each factor
    for factor_id in set(factors):
        timeline = []
        for ts_ms, factors_dict in sorted(factor_ts.items()):
            if ts_ms < display_start_ms or ts_ms > display_end_ms:
                continue
            if factor_id in factors_dict:
                timeline.append(
                    {
                        "time": ts_ms // 1000,
                        "value": factors_dict[factor_id],
                    }
                )

        sampled = sample_timeline(timeline, resolution)
        all_timelines[factor_id] = {"timeline": sampled, "points": len(sampled)}

    # Cache all computed factors
    _factor_timeline_cache[cache_key] = (current_time, all_timelines)

    # 8. Return requested factors
    result_timelines = {}
    for factor_id in factors:
        if factor_id in all_timelines:
            result_timelines[factor_id] = all_timelines[factor_id]
        else:
            result_timelines[factor_id] = {"timeline": [], "points": 0}

    return {
        "experiment_id": experiment_id,
        "ticker": ticker,
        "factors": factors,
        "cached": False,
        "timelines": result_timelines,
        "trades_count": len(trades),
        "quotes_count": len(quotes),
    }


@router.post("/label/prefetch")
def prefetch_label_data(data: dict):
    """Batch-load trades + quotes for multiple tickers into the in-memory cache.

    Body: { date, tickers: ["A", "B", ...] }

    Uses 2 CH queries total (1 batch trades, 1 batch quotes) regardless of
    how many tickers are requested.  Subsequent single-ticker label requests
    will find the data in _label_tq_cache and skip CH entirely.
    """
    import pandas as pd

    from jerry_trader.services.backtest.data_loading import (
        _load_quotes_batch_ch,
        _load_trades_batch_ch,
    )

    date = data.get("date")
    tickers = data.get("tickers", [])

    if not date or not tickers:
        raise HTTPException(status_code=400, detail="date and tickers required")

    tickers = [t.upper() for t in tickers]

    # Filter to tickers not already cached
    missing = [t for t in tickers if (t, date) not in _label_tq_cache]
    if not missing:
        return {"status": "ok", "cached": len(tickers), "loaded": 0, "date": date}

    # Batch-load trades and quotes in 2 CH queries
    try:
        trades_map = _load_trades_batch_ch(missing, date)
    except Exception:
        trades_map = {
            t: pd.DataFrame(columns=["ts_ms", "price", "size"]) for t in missing
        }

    try:
        quotes_map = _load_quotes_batch_ch(missing, date)
    except Exception:
        quotes_map = {
            t: pd.DataFrame(columns=["ts_ms", "bid", "ask", "bid_size", "ask_size"])
            for t in missing
        }

    loaded = 0
    for t in missing:
        trades_df = trades_map.get(t, pd.DataFrame(columns=["ts_ms", "price", "size"]))
        quotes_df = quotes_map.get(
            t,
            pd.DataFrame(columns=["ts_ms", "bid", "ask", "bid_size", "ask_size"]),
        )
        _label_tq_cache[(t, date)] = (trades_df, quotes_df)
        loaded += 1

    return {
        "status": "ok",
        "cached": len(tickers) - len(missing),
        "loaded": loaded,
        "date": date,
    }


@router.get("/label/ticker/{ticker}")
def get_label_chart_data(
    ticker: str,
    date: str,
    refresh: bool = False,
    timeframe: str = "10s",
    spread_bps: float = 100.0,
    vol_exp_ratio: float = 3.0,
    surge_sigma: float = 3.0,
    sustained_min_trades: int = 3,
):
    """Get premarket trades, OHLCV bars, factors, entry, ignition for labeling.

    Candidate list is disk-cached in data/.label_cache/ to survive restarts.
    Pass ?refresh=true to force re-running PreFilter.
    Pass ?timeframe=10s|30s|1m for bar interval (default 10s).
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    import numpy as np
    import pandas as pd
    import polars as pl

    from jerry_trader.platform.config.config import float_shares_dir
    from jerry_trader.services.backtest.config import PreFilterConfig
    from jerry_trader.services.backtest.data_loading import (
        _load_quotes_ch,
        _load_trades_ch,
    )
    from jerry_trader.services.backtest.layer_lab.ignition import _detect_ignitions
    from jerry_trader.services.backtest.layer_lab.lab import ExitLab
    from jerry_trader.services.backtest.pre_filter import PreFilter

    ticker = ticker.upper()

    # ── 1. Load or cache candidates for this date ──
    if date not in _label_candidate_cache:
        if not refresh:
            disk_candidates = _load_candidates_from_disk(date)
            if disk_candidates is not None:
                _label_candidate_cache[date] = disk_candidates

    if date not in _label_candidate_cache:
        ch = get_clickhouse_client()
        if not ch:
            raise HTTPException(status_code=500, detail="ClickHouse unavailable")
        try:
            config = PreFilterConfig(top_n=999, min_gain_pct=0.0, new_entry_only=False)
            candidates = PreFilter(ch_client=ch).find(date, config)
            _label_candidate_cache[date] = candidates
            _save_candidates_to_disk(date, candidates)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"PreFilter failed: {e}")

    candidates = _label_candidate_cache[date]
    candidate = next((c for c in candidates if c.symbol.upper() == ticker), None)
    if candidate is None:
        raise HTTPException(
            status_code=404, detail=f"{ticker} not in candidates for {date}"
        )

    # ── 2. Load trades and quotes (check cache first) ──
    cache_key_tq = (ticker, date)
    if cache_key_tq in _label_tq_cache:
        trades_df, quotes_df = _label_tq_cache[cache_key_tq]
    else:
        try:
            trades_df = _load_trades_ch(ticker, date)
        except Exception:
            trades_df = pd.DataFrame(columns=["ts_ms", "price", "size"])

        try:
            quotes_df = _load_quotes_ch(ticker, date)
        except Exception:
            quotes_df = pd.DataFrame(
                columns=["ts_ms", "bid", "ask", "bid_size", "ask_size"]
            )

        _label_tq_cache[cache_key_tq] = (trades_df, quotes_df)

    if trades_df.empty:
        raise HTTPException(status_code=404, detail=f"No trades for {ticker} on {date}")

    # ── 3. Find entry price ──
    entry_price = ExitLab.find_entry_price(trades_df, candidate.first_entry_ms)
    if entry_price is None:
        raise HTTPException(status_code=404, detail=f"No entry price for {ticker}")

    # ── 4. Premarket window ──
    et = ZoneInfo("America/New_York")
    dt_et = datetime.strptime(date, "%Y-%m-%d").replace(hour=9, minute=30, tzinfo=et)
    session_end_ms = int(dt_et.timestamp() * 1000)
    premarket_start = datetime.strptime(date, "%Y-%m-%d").replace(
        hour=4, minute=0, tzinfo=et
    )
    premarket_start_ms = int(premarket_start.timestamp() * 1000)

    # ── 5. Detect ignition ──
    ig_kwargs = dict(
        spread_bps=spread_bps,
        vol_exp_ratio=vol_exp_ratio,
        surge_sigma=surge_sigma,
        sustained_min_trades=sustained_min_trades,
    )
    if not quotes_df.empty:
        ignition = _detect_ignitions(
            trades_df,
            candidate.first_entry_ms,
            session_end_ms,
            quotes=quotes_df,
            **ig_kwargs,
        )
    else:
        ignition = _detect_ignitions(
            trades_df,
            candidate.first_entry_ms,
            session_end_ms,
            **ig_kwargs,
        )

    # ── 5b. Breakout peaks: find max % return in short windows after entry ──
    fwd_trades = trades_df[
        (trades_df["ts_ms"] > candidate.first_entry_ms)
        & (trades_df["ts_ms"] <= session_end_ms)
    ]
    breakout_peaks: dict[str, dict | None] = {}
    if not fwd_trades.empty:
        fwd_prices = fwd_trades["price"].values
        fwd_times = fwd_trades["ts_ms"].values

        def _breakout_marker(window_ms: int) -> dict | None:
            mask = fwd_times <= candidate.first_entry_ms + window_ms
            if not mask.any():
                return None
            idx = int(np.argmax(fwd_prices[mask]))
            peak_price = float(fwd_prices[mask][idx])
            peak_ms = int(fwd_times[mask][idx])
            pct = (peak_price - entry_price) / entry_price * 100
            if pct <= 0:
                return None
            return {
                "ms": peak_ms,
                "time": peak_ms // 1000,
                "price": round(peak_price, 4),
                "pct": round(pct, 2),
            }

        breakout_peaks = {
            "peak_1min": _breakout_marker(60_000),
            "peak_3min": _breakout_marker(180_000),
            "peak_5min": _breakout_marker(300_000),
        }

    # ── 6. Peak metrics ──
    try:
        peak_result = ExitLab.simulate_one(
            trades_df, candidate.first_entry_ms, entry_price, session_end_ms
        )
        if peak_result:
            peak_metrics = {
                "mfe_pct": peak_result.mfe_pct,
                "mae_pct": peak_result.mae_pct,
                "final_pnl_pct": peak_result.pnl_pct,
                "exit_reason": peak_result.exit_reason,
                "duration_min": peak_result.duration_min,
            }
        else:
            peak_metrics = None
    except Exception:
        peak_metrics = None

    # ── 7. Filter trades/quotes to premarket for chart payload ──
    pre_trades = trades_df[
        (trades_df["ts_ms"] >= premarket_start_ms)
        & (trades_df["ts_ms"] <= session_end_ms)
    ]
    pre_quotes = (
        quotes_df[
            (quotes_df["ts_ms"] >= premarket_start_ms)
            & (quotes_df["ts_ms"] <= session_end_ms)
        ]
        if not quotes_df.empty
        else quotes_df
    )

    # ── 8. Downsample for chart payload (max ~5000 points) ──
    max_points = 5000
    trade_step = max(1, len(pre_trades) // max_points)
    quote_step = max(1, len(pre_quotes) // max_points)

    # ── 9. Compute tick factors from premarket trades ──
    tick_factors = _compute_labeling_factors(pre_trades)

    # ── 10. Build bars + compute bar factors ──
    tf_map = {"10s": 10000, "30s": 30000, "1m": 60000}
    bar_interval_ms = tf_map.get(timeframe, 10000)
    bars_df = _build_bars(pre_trades, interval_ms=bar_interval_ms)
    bar_factors = _compute_bar_factors(bars_df)

    # Resample tick factors to bar time grid so sparse trade-timestamp
    # factors display as continuous lines aligned with OHLCV bars.
    bar_times = bars_df["time"].tolist() if not bars_df.empty else []
    if bar_times:
        for key in list(tick_factors.keys()):
            tick_factors[key] = _resample_to_bar_grid(tick_factors[key], bar_times)

    # Serialize bars
    bar_rows = bars_df.to_dict(orient="records") if not bars_df.empty else []

    # ── 11. Serialize trades/quotes for chart payload ──
    trade_rows = []
    for i, (_, row) in enumerate(pre_trades.iterrows()):
        if i % trade_step != 0:
            continue
        trade_rows.append(
            {
                "time": int(row["ts_ms"]) // 1000,
                "price": round(float(row["price"]), 4),
                "size": int(row["size"]),
            }
        )

    quote_rows = []
    for i, (_, row) in enumerate(pre_quotes.iterrows()):
        if i % quote_step != 0:
            continue
        quote_rows.append(
            {
                "time": int(row["ts_ms"]) // 1000,
                "bid": round(float(row["bid"]), 4),
                "ask": round(float(row["ask"]), 4),
                "bidSize": int(row["bid_size"]) if pd.notna(row["bid_size"]) else 0,
                "askSize": int(row["ask_size"]) if pd.notna(row["ask_size"]) else 0,
            }
        )

    # Serialize ignition
    ignition_ser: dict[str, dict | None] = {}
    for method, ig in ignition.items():
        if ig and ig.get("ms"):
            ignition_ser[method] = {
                "ms": ig["ms"],
                "time": ig["ms"] // 1000,
                "price": round(ig["price"], 4),
            }
        else:
            ignition_ser[method] = None

    entry_time = datetime.fromtimestamp(
        candidate.first_entry_ms / 1000, tz=et
    ).strftime("%H:%M:%S")

    return {
        "ticker": ticker,
        "date": date,
        "candidate": {
            "entry_ms": candidate.first_entry_ms,
            "entry_time": entry_time,
            "entry_price": round(entry_price, 4),
            "gain_at_entry": round(candidate.gain_at_entry, 2),
            "prev_close": round(candidate.prev_close, 4),
            "volume_at_entry": int(candidate.volume_at_entry),
            "relative_volume": round(candidate.relative_volume, 2),
            "max_gain": round(candidate.max_gain, 2),
        },
        "trades": trade_rows,
        "quotes": quote_rows,
        "session_end_ms": session_end_ms,
        "ignition": ignition_ser,
        "breakout_peaks": breakout_peaks,
        "peak_metrics": peak_metrics,
        "trade_count": len(trade_rows),
        "quote_count": len(quote_rows),
        "tick_factors": tick_factors,
        "bars": bar_rows,
        "bar_factors": bar_factors,
        "timeframe": timeframe,
    }


def _compute_labeling_factors(
    trades_df: "pd.DataFrame", window_ms: int = 30000, max_points: int = 2000
) -> dict:
    """Compute factor series from premarket trade data.

    Returns { trade_rate, large_trade_ratio, aggressor_ratio } using
    rolling-window computation – no bar aggregation needed.
    """
    import numpy as np

    if trades_df.empty:
        return {"trade_rate": [], "large_trade_ratio": [], "aggressor_ratio": []}

    ts = trades_df["ts_ms"].values
    prices = trades_df["price"].values
    sizes = trades_df["size"].values
    n = len(ts)

    trade_rate: list[dict] = []
    large_trade_ratio: list[dict] = []
    aggressor_ratio: list[dict] = []
    left = 0
    size_sum = 0
    buy_count = 0
    # Track buy/sell per trade for aggressor (window needs individual flags)
    is_buy_flags: list[bool] = []
    last_price: float | None = None
    last_side: bool = True  # default buy for first trade

    for right_idx in range(n):
        t = ts[right_idx]
        price = float(prices[right_idx])
        size_sum += float(sizes[right_idx])

        # Tick test for aggressor
        if last_price is not None:
            if price > last_price:
                last_side = True
            elif price < last_price:
                last_side = False
            # price == last_price: keep last_side
        is_buy_flags.append(last_side)
        buy_count += 1 if last_side else 0
        last_price = price

        # Shrink left side of window
        while left < right_idx and ts[left] < t - window_ms:
            size_sum -= float(sizes[left])
            if is_buy_flags[left]:
                buy_count -= 1
            left += 1

        count = right_idx - left + 1
        time_sec = int(t) // 1000

        if count >= 5 and t - ts[left] > 0:
            rate = count / ((t - ts[left]) / 1000.0)
            # Large trade ratio
            mean_size = size_sum / count
            threshold = mean_size * 2.0
            large_count = int(np.sum(sizes[left : right_idx + 1] > threshold))
            # Aggressor ratio
            aggr = buy_count / count

            trade_rate.append({"time": time_sec, "value": round(rate, 3)})
            large_trade_ratio.append(
                {"time": time_sec, "value": round(large_count / count, 3)}
            )
            aggressor_ratio.append({"time": time_sec, "value": round(aggr, 3)})
        else:
            trade_rate.append({"time": time_sec, "value": 0.0})
            large_trade_ratio.append({"time": time_sec, "value": 0.0})
            aggressor_ratio.append({"time": time_sec, "value": 0.0})

    # Downsample
    step = max(1, n // max_points)
    return {
        "trade_rate": trade_rate[::step],
        "large_trade_ratio": large_trade_ratio[::step],
        "aggressor_ratio": aggressor_ratio[::step],
    }


def _resample_to_bar_grid(points: list[dict], bar_times: list[int]) -> list[dict]:
    """Forward-fill sparse tick-factor points onto the bar time grid.

    ``points`` is [{"time": seconds, "value": float}, ...] sorted by time.
    ``bar_times`` is the sorted integer-second time grid from bars_df["time"].
    Returns one point per bar time, forward-filled from the last known value.
    """
    if not points or not bar_times:
        return [{"time": t, "value": 0.0} for t in bar_times]

    # Build lookup: time → value
    pt_map: dict[int, float] = {}
    for p in points:
        pt_map[p["time"]] = p["value"]

    point_times = sorted(pt_map.keys())
    result: list[dict] = []
    pi = 0
    last_val = pt_map.get(point_times[0], 0.0)

    for bt in bar_times:
        # Advance pointer to the last point ≤ bar time
        while pi < len(point_times) and point_times[pi] <= bt:
            last_val = pt_map[point_times[pi]]
            pi += 1
        result.append({"time": bt, "value": round(last_val, 3)})

    return result


def _build_bars(trades_df: "pd.DataFrame", interval_ms: int = 10000) -> "pd.DataFrame":
    """Build OHLCV bars from trade ticks at the given interval (ms).

    Missing bars (no trades in an interval, common for illiquid tickers) are
    forward-filled so the chart has no gaps and factor computations are continuous.
    """
    import numpy as np
    import pandas as pd

    if trades_df.empty:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

    ts = trades_df["ts_ms"].values
    bar_ts = (ts // interval_ms) * interval_ms

    temp = pd.DataFrame(
        {
            "bar_ts": bar_ts,
            "price": trades_df["price"].values,
            "size": trades_df["size"].values,
        }
    )
    grouped = temp.groupby("bar_ts")
    bars = grouped.agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("size", "sum"),
    ).reset_index()
    bars.rename(columns={"bar_ts": "time"}, inplace=True)
    bars["time"] = (bars["time"] // 1000).astype(int)

    # Forward-fill gaps: illiquid tickers (e.g. VICG) have empty intervals
    if len(bars) > 1:
        min_time = int(bars["time"].min())
        max_time = int(bars["time"].max())
        step_sec = interval_ms // 1000
        full_times = np.arange(min_time, max_time + step_sec, step_sec, dtype=int)
        full_df = pd.DataFrame({"time": full_times})
        bars = full_df.merge(bars, on="time", how="left")
        bars["open"] = bars["open"].ffill()
        bars["high"] = bars["high"].ffill()
        bars["low"] = bars["low"].ffill()
        bars["close"] = bars["close"].ffill()
        bars["volume"] = bars["volume"].fillna(0).astype(int)

    return bars


def _compute_bar_factors(bars_df: "pd.DataFrame") -> dict:
    """Compute bar-based factors from OHLCV bars.

    Returns { ema_20, vwap, vwap_deviation, relative_volume } as [{time, value}].
    """
    if bars_df.empty or len(bars_df) < 2:
        return {}

    closes = bars_df["close"].values
    highs = bars_df["high"].values
    lows = bars_df["low"].values
    volumes = bars_df["volume"].values
    times = bars_df["time"].values
    n = len(bars_df)

    # EMA(20)
    alpha = 2.0 / 21.0
    ema20 = [float(closes[0])]
    for i in range(1, n):
        ema20.append(alpha * float(closes[i]) + (1.0 - alpha) * ema20[-1])

    # VWAP (cumulative, session-reset at start)
    vwap = []
    cum_pv = 0.0
    cum_vol = 0.0
    for i in range(n):
        typical = (float(highs[i]) + float(lows[i]) + float(closes[i])) / 3.0
        cum_pv += typical * float(volumes[i])
        cum_vol += float(volumes[i])
        vwap.append(cum_pv / cum_vol if cum_vol > 0 else float(closes[i]))

    # VWAP deviation %
    vwap_dev = [
        round((float(closes[i]) - vwap[i]) / vwap[i] * 100.0, 3) for i in range(n)
    ]

    # Relative volume (vs 20-bar trailing average)
    rel_vol = []
    for i in range(n):
        start = max(0, i - 19)
        window_vols = [float(volumes[j]) for j in range(start, i + 1)]
        avg = sum(window_vols) / len(window_vols)
        rel_vol.append(round(float(volumes[i]) / avg, 3) if avg > 0 else 1.0)

    def _pts(values):
        return [{"time": int(times[i]), "value": values[i]} for i in range(n)]

    return {
        "ema_20": _pts(ema20),
        "vwap": _pts(vwap),
        "vwap_deviation": _pts(vwap_dev),
        "relative_volume": _pts(rel_vol),
    }


@router.get("/label/factors")
def list_label_factors():
    """Return available factors for the labeling chart module.

    Each factor has: id, name, color, priceScale (left/right), mode (overlay/panel).
    overlay = drawn on the price panel, panel = separate sub-chart.
    """
    return {
        "factors": [
            {
                "id": "ema_20",
                "name": "EMA(20)",
                "color": "#3b82f6",
                "priceScale": "right",
                "mode": "overlay",
                "source": "bar",
            },
            {
                "id": "vwap",
                "name": "VWAP",
                "color": "#f59e0b",
                "priceScale": "right",
                "mode": "overlay",
                "source": "bar",
            },
            {
                "id": "vwap_deviation",
                "name": "VWAP Dev%",
                "color": "#ec4899",
                "priceScale": "right",
                "mode": "panel",
                "source": "bar",
            },
            {
                "id": "relative_volume",
                "name": "RelVol",
                "color": "#f97316",
                "priceScale": "right",
                "mode": "panel",
                "source": "bar",
            },
            {
                "id": "trade_rate",
                "name": "TradeRate",
                "color": "#f97316",
                "priceScale": "right",
                "mode": "panel",
                "source": "tick",
            },
            {
                "id": "large_trade_ratio",
                "name": "LargeTrade",
                "color": "#a855f7",
                "priceScale": "right",
                "mode": "panel",
                "source": "tick",
            },
            {
                "id": "aggressor_ratio",
                "name": "Aggressor",
                "color": "#06b6d4",
                "priceScale": "right",
                "mode": "panel",
                "source": "tick",
            },
        ]
    }


LABELING_CSV_DIR = PROJECT_ROOT / "data"
LABELING_CSV_HEADER = (
    "date,ticker,entry_price,gain_at_entry,entry_to_peak_pct,"
    "is_momo,ignited,ignition_method,ignition_delay_sec,"
    "ig_trade_count_5min,ig_dollar_vol_5min,ig_price_range_pct,"
    "ig_btd,ig_max_gap_sec,ig_to_peak_pct,exit_reason,"
    "final_pnl_pct,label,label_notes"
)


def _validate_labeling_csv(path: str) -> bool:
    """Check that a CSV file has the expected labeling header."""
    from pathlib import Path

    p = Path(path)
    if not p.exists() or p.suffix.lower() != ".csv":
        return False
    try:
        first_line = p.read_text().split("\n", 1)[0].strip()
        cols = first_line.split(",")
        return len(cols) >= 18 and cols[0] == "date" and cols[1] == "ticker"
    except Exception:
        return False


@router.get("/label/files")
def list_label_files():
    """List labeling CSV files in the data directory."""
    from pathlib import Path

    files = []
    if LABELING_CSV_DIR.exists():
        for f in sorted(LABELING_CSV_DIR.iterdir()):
            if f.suffix.lower() == ".csv":
                valid = _validate_labeling_csv(str(f))
                files.append(
                    {
                        "name": f.name,
                        "path": str(f),
                        "size": f.stat().st_size,
                        "valid": valid,
                    }
                )
    return {"files": files, "data_dir": str(LABELING_CSV_DIR)}


@router.get("/label/file")
def load_label_file(filename: str):
    """Load a labeling CSV file and return parsed rows."""
    from pathlib import Path

    file_path = (LABELING_CSV_DIR / filename).resolve()
    if not str(file_path).startswith(str(LABELING_CSV_DIR.resolve())):
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {filename}")

    text = file_path.read_text()
    return {"filename": filename, "content": text}


@router.post("/label/save")
def save_label(data: dict):
    """Save a label update to a CSV file.

    Body: { filename, key (date:ticker), label ('1'|'0'|'-1'|''), notes, break_label ('1'|'0'|'') }

    Uses proper CSV parsing (csv module) and finds label/label_notes/break_label columns
    by header name, not hardcoded index. Works with any CSV layout.
    """
    import csv
    import io
    from pathlib import Path

    filename = data.get("filename")
    key = data.get("key")
    label = data.get("label", "")
    notes = data.get("notes", "")
    break_label = data.get("break_label", "")

    if not filename or not key:
        raise HTTPException(status_code=400, detail="filename and key required")

    file_path = (LABELING_CSV_DIR / filename).resolve()
    if not str(file_path).startswith(str(LABELING_CSV_DIR.resolve())):
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {filename}")

    # Read CSV properly with quoting support
    content = file_path.read_text()
    reader = csv.DictReader(io.StringIO(content))
    headers = reader.fieldnames
    if not headers or "label" not in headers:
        raise HTTPException(status_code=400, detail="CSV missing label column")
    if len(headers) < 2:
        raise HTTPException(status_code=400, detail="CSV file is empty")

    updated = False
    out_rows = [headers]

    for row in reader:
        if not row.get("date") or not row.get("ticker"):
            out_rows.append(row)
            continue
        row_key = f"{row['date']}:{row['ticker']}"
        if row_key == key:
            row["label"] = label
            row["label_notes"] = notes
            if break_label:
                row["break_label"] = break_label
            updated = True
        out_rows.append(row)

    if not updated:
        raise HTTPException(status_code=404, detail=f"Row {key} not found in CSV")

    # Write back with proper quoting
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers, lineterminator="\n")
    writer.writeheader()
    writer.writerows(out_rows[1:])
    file_path.write_text(output.getvalue())

    return {"status": "ok", "key": key, "label": label}
