"""Trading calendar utilities using pandas_market_calendars.

Uses NYSE calendar as the authoritative source for US market trading days.
No manual YAML needed — the library handles all holidays and early closes.

Usage:
    from jerry_trader.services.backtest.data.trading_calendar import (
        is_trading_day,
        get_trading_days,
        is_early_close,
        get_early_close_days,
    )

    is_trading_day("2026-03-09")  # True
    is_trading_day("2026-03-07")  # False (Saturday)
    is_trading_day("2026-04-03")  # False (Good Friday)
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas_market_calendars as mcal

# Singleton NYSE calendar — created once, reused
_nyse = mcal.get_calendar("NYSE")


def is_trading_day(date_str: str) -> bool:
    """Check if a date is a US stock market trading day.

    Uses NYSE calendar as the authoritative source.
    Handles weekends, holidays, and special closures automatically.

    Args:
        date_str: Date in YYYY-MM-DD format.

    Returns:
        True if trading day, False otherwise.
    """
    try:
        schedule = _nyse.schedule(start_date=date_str, end_date=date_str)
        return len(schedule) > 0
    except Exception:
        return False


def is_early_close(date_str: str) -> bool:
    """Check if market has early close (1:00 PM ET instead of 4:00 PM ET).

    Typical early close days:
      - Day after Thanksgiving
      - Christmas Eve
      - Day before Independence Day (sometimes)

    Note: Pre-market hours (4:00-9:30 AM ET) are NOT affected by early close.
    Only the regular session ends earlier.

    Args:
        date_str: Date in YYYY-MM-DD format.

    Returns:
        True if early close day.
    """
    try:
        schedule = _nyse.schedule(start_date=date_str, end_date=date_str)
        if len(schedule) == 0:
            return False
        early = _nyse.early_closes(schedule)
        return len(early) > 0
    except Exception:
        return False


def get_market_close_et(date_str: str) -> str:
    """Get market close time in ET for a given date.

    Returns:
        "13:00" for early close days, "16:00" for normal days,
        "" for non-trading days.
    """
    try:
        schedule = _nyse.schedule(start_date=date_str, end_date=date_str)
        if len(schedule) == 0:
            return ""
        early = _nyse.early_closes(schedule)
        if len(early) > 0:
            return "13:00"  # 1:00 PM ET
        return "16:00"  # 4:00 PM ET
    except Exception:
        return ""


def get_trading_days(start_date: str, end_date: str) -> list[str]:
    """Get all trading days in a date range.

    Args:
        start_date: Start date YYYY-MM-DD (inclusive).
        end_date: End date YYYY-MM-DD (inclusive).

    Returns:
        List of trading day strings, sorted chronologically.
    """
    try:
        schedule = _nyse.schedule(start_date=start_date, end_date=end_date)
        return [idx.strftime("%Y-%m-%d") for idx in schedule.index]
    except Exception:
        return []


def get_early_close_days(start_date: str, end_date: str) -> list[str]:
    """Get all early close days in a date range.

    Args:
        start_date: Start date YYYY-MM-DD.
        end_date: End date YYYY-MM-DD.

    Returns:
        List of early close day strings.
    """
    try:
        schedule = _nyse.schedule(start_date=start_date, end_date=end_date)
        early = _nyse.early_closes(schedule)
        return [idx.strftime("%Y-%m-%d") for idx in early.index]
    except Exception:
        return []


def get_missing_dates(
    available_dates: list[str],
    start_date: str,
    end_date: str,
) -> dict[str, str]:
    """Find missing dates and classify reason.

    Args:
        available_dates: Dates that have data.
        start_date: Start of range.
        end_date: End of range.

    Returns:
        Dict mapping missing date → reason ("non_trading", "missing").
    """
    trading_days = set(get_trading_days(start_date, end_date))
    available = set(available_dates)

    missing: dict[str, str] = {}

    # Check all calendar days in range
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start

    while current <= end:
        date_str = current.strftime("%Y-%m-%d")

        if date_str not in available:
            if date_str in trading_days:
                missing[date_str] = "missing"
            else:
                missing[date_str] = "non_trading"

        current += timedelta(days=1)

    return missing


def print_calendar_summary(start_date: str, end_date: str) -> None:
    """Print calendar summary for a date range."""
    trading_days = get_trading_days(start_date, end_date)
    early_close = get_early_close_days(start_date, end_date)

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end - start).days + 1

    print(f"Date range: {start_date} to {end_date}")
    print(f"  Total calendar days: {total_days}")
    print(f"  Trading days: {len(trading_days)}")
    print(f"  Early close days: {len(early_close)}")
    if early_close:
        for d in early_close:
            print(f"    {d}")


if __name__ == "__main__":
    print("=== NYSE Calendar Demo ===\n")

    print_calendar_summary("2026-01-01", "2026-12-31")

    print("\n=== March 2026 Trading Days ===")
    for d in get_trading_days("2026-03-01", "2026-03-15"):
        ec = " (early close)" if is_early_close(d) else ""
        print(f"  {d}: trading={is_trading_day(d)}{ec}")

    print("\n=== Non-trading day tests ===")
    for d in ["2026-03-07", "2026-03-08", "2026-03-09", "2026-04-03", "2026-07-04"]:
        print(f"  {d}: trading={is_trading_day(d)}")

    print("\n=== 2026 Early Close Days ===")
    for d in get_early_close_days("2026-01-01", "2026-12-31"):
        print(f"  {d}: closes at {get_market_close_et(d)} ET")
