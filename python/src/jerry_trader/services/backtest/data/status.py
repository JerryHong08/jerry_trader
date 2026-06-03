"""Data status command.

Checks data availability across all stages and distinguishes:
  - Trading days missing data → needs download/import
  - Non-trading days → expected empty

Usage:
    poetry run python -m jerry_trader.services.backtest.data.cli status
    poetry run python -m jerry_trader.services.backtest.data.cli status --update
    poetry run python -m jerry_trader.services.backtest.data.cli status --date 2026-03-13
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml

from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.services.backtest.data.trading_calendar import (
    get_missing_dates,
    get_trading_days,
    is_trading_day,
)
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.utils.paths import PROJECT_ROOT

logger = setup_logger("backtest.data.status")

STATUS_FILE = PROJECT_ROOT / "data" / "status.yaml"


def check_date_status(
    date: str,
    ch_client: Any | None = None,
    source_dir: str | None = None,
) -> dict[str, bool | int | str]:
    """Check data status for a single date.

    Checks all stages:
      - Source CSV.gz files
      - ClickHouse tables (trades, quotes, market_snapshot)
      - Mining results

    Args:
        date: Date YYYY-MM-DD.
        ch_client: ClickHouse client (optional).
        source_dir: Override source directory.

    Returns:
        Dict with status for each stage.
    """
    import os

    from jerry_trader.platform.config.config import lake_data_dir

    year, month, _ = date.split("-")
    base_dir = Path(source_dir) if source_dir else Path(lake_data_dir)

    status: dict[str, bool | int | str] = {
        "date": date,
        "trading_day": is_trading_day(date),
    }

    # 1. Check source files (raw CSV.gz, not lake parquet)
    raw_base = Path("/mnt/blackdisk/quant_data/polygon_data/raw")
    trades_csv = raw_base / f"us_stocks_sip/trades_v1/{year}/{month}/{date}.csv.gz"
    quotes_csv = raw_base / f"us_stocks_sip/quotes_v1/{year}/{month}/{date}.csv.gz"
    status["trades_csv"] = trades_csv.exists()
    status["quotes_csv"] = quotes_csv.exists()

    # 2. Check ClickHouse tables
    if ch_client:
        try:
            r = ch_client.query(
                "SELECT count() FROM jerry_trader.trades FINAL WHERE date = {date:String}",
                parameters={"date": date},
            )
            status["trades_ch"] = r.result_rows[0][0] > 0
            status["trades_ch_count"] = r.result_rows[0][0]
        except Exception:
            status["trades_ch"] = False
            status["trades_ch_count"] = 0

        try:
            r = ch_client.query(
                "SELECT count() FROM jerry_trader.quotes FINAL WHERE date = {date:String}",
                parameters={"date": date},
            )
            status["quotes_ch"] = r.result_rows[0][0] > 0
            status["quotes_ch_count"] = r.result_rows[0][0]
        except Exception:
            status["quotes_ch"] = False
            status["quotes_ch_count"] = 0

        try:
            r = ch_client.query(
                "SELECT count() FROM jerry_trader.market_snapshot FINAL WHERE date = {date:String}",
                parameters={"date": date},
            )
            status["snapshot_ch"] = r.result_rows[0][0] > 0
        except Exception:
            status["snapshot_ch"] = False

        try:
            r = ch_client.query(
                "SELECT count() FROM jerry_trader.backtest_results FINAL WHERE date = {date:String}",
                parameters={"date": date},
            )
            status["mining_done"] = r.result_rows[0][0] > 0
            status["signals"] = r.result_rows[0][0]
        except Exception:
            status["mining_done"] = False
            status["signals"] = 0

    return status


def check_date_range_status(
    start_date: str,
    end_date: str,
    ch_client: Any | None = None,
) -> list[dict]:
    """Check status for all dates in a range.

    Args:
        start_date: Start YYYY-MM-DD.
        end_date: End YYYY-MM-DD.
        ch_client: ClickHouse client.

    Returns:
        List of status dicts.
    """
    trading_days = get_trading_days(start_date, end_date)
    results = []

    for date in trading_days:
        status = check_date_status(date, ch_client)
        results.append(status)

    return results


def print_status_summary(results: list[dict]) -> None:
    """Print formatted status summary."""
    print("\n" + "=" * 80)
    print("  DATA STATUS SUMMARY")
    print("=" * 80)

    print(
        f"\n{'Date':<12} {'Trading':>8} {'CSV':>6} {'CH.Tr':>7} {'CH.Qu':>7} "
        f"{'Snap':>6} {'Mining':>7} {'Signals':>8} {'Notes':<20}"
    )
    print("-" * 80)

    for r in results:
        date = r.get("date", "N/A")
        trading = "✓" if r.get("trading_day") else "—"

        # CSV status
        csv_status = ""
        if r.get("trades_csv") and r.get("quotes_csv"):
            csv_status = "✓"
        elif r.get("trading_day"):
            csv_status = "❌"
        else:
            csv_status = "—"

        # CH trades
        ch_tr = (
            "✓" if r.get("trades_ch") else ("—" if not r.get("trading_day") else "❌")
        )
        ch_qu = (
            "✓" if r.get("quotes_ch") else ("—" if not r.get("trading_day") else "❌")
        )

        # Snapshot
        snap = (
            "✓" if r.get("snapshot_ch") else ("—" if not r.get("trading_day") else "❌")
        )

        # Mining
        mining = "✓" if r.get("mining_done") else "—"
        signals = r.get("signals", 0)

        # Notes
        notes = ""
        if not r.get("trading_day"):
            notes = "non-trading"
        elif not r.get("trades_csv"):
            notes = "need download"
        elif not r.get("trades_ch"):
            notes = "need import"
        elif not r.get("mining_done"):
            notes = "ready for mining"
        else:
            notes = "complete"

        print(
            f"{date:<12} {trading:>8} {csv_status:>6} {ch_tr:>7} {ch_qu:>7} "
            f"{snap:>6} {mining:>7} {signals:>8} {notes:<20}"
        )

    # Summary stats
    trading = [r for r in results if r.get("trading_day")]
    complete = [
        r
        for r in trading
        if r.get("trades_ch")
        and r.get("quotes_ch")
        and r.get("snapshot_ch")
        and r.get("mining_done")
    ]
    need_download = [
        r for r in trading if not r.get("trades_csv") or not r.get("quotes_csv")
    ]
    need_import = [
        r
        for r in trading
        if r.get("trades_csv")
        and not r.get("trades_ch")
        or (r.get("quotes_csv") and not r.get("quotes_ch"))
    ]
    need_mining = [
        r
        for r in trading
        if r.get("trades_ch") and r.get("snapshot_ch") and not r.get("mining_done")
    ]

    print("-" * 80)
    print(f"\nSummary:")
    print(f"  Trading days: {len(trading)}")
    print(f"  Complete: {len(complete)}")
    print(
        f"  Need download: {len(need_download)} → {[r['date'] for r in need_download]}"
    )
    print(f"  Need import: {len(need_import)} → {[r['date'] for r in need_import]}")
    print(f"  Need mining: {len(need_mining)} → {[r['date'] for r in need_mining]}")

    # Total signals
    total_signals = sum(r.get("signals", 0) for r in results)
    print(f"  Total signals: {total_signals:,}")


def update_status_file(results: list[dict]) -> None:
    """Update status.yaml file."""
    import os

    data = {
        "last_updated": datetime.now().isoformat(),
        "dates": {},
        "summary": {
            "total_trading_days": len([r for r in results if r.get("trading_day")]),
            "complete_dates": len(
                [
                    r
                    for r in results
                    if r.get("trading_day")
                    and r.get("trades_ch")
                    and r.get("quotes_ch")
                    and r.get("mining_done")
                ]
            ),
            "total_signals": sum(r.get("signals", 0) for r in results),
        },
    }

    for r in results:
        date = r.get("date")
        if not date:
            continue

        data["dates"][date] = {
            "trading_day": r.get("trading_day", False),
            "trades_csv": r.get("trades_csv", False),
            "quotes_csv": r.get("quotes_csv", False),
            "trades_ch": r.get("trades_ch", False),
            "quotes_ch": r.get("quotes_ch", False),
            "snapshot_ch": r.get("snapshot_ch", False),
            "mining_done": r.get("mining_done", False),
            "signals": r.get("signals", 0),
            "notes": _get_note(r),
        }

    # Ensure directory exists
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(STATUS_FILE, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Updated status file: {STATUS_FILE}")


def _get_note(r: dict) -> str:
    """Generate note for a date."""
    if not r.get("trading_day"):
        return "non-trading day"
    if not r.get("trades_csv"):
        return "need download"
    if not r.get("trades_ch"):
        return "need import"
    if not r.get("mining_done"):
        return "ready for mining"
    return "complete"


# Type hint fix
from typing import Any

if __name__ == "__main__":
    # Demo
    ch = get_clickhouse_client()
    results = check_date_range_status("2026-03-01", "2026-03-20", ch)
    print_status_summary(results)
