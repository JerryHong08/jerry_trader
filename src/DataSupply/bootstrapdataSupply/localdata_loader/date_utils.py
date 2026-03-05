"""
Date and calendar utilities for trading date calculations.
"""

import datetime
from typing import List, Tuple

import exchange_calendars as xcals


def resolve_date_range(
    start_date: str, timedelta: int = 0, calendar_name: str = "XNYS"
) -> Tuple[str, str]:
    """
    Resolve exact trading start and end dates based on calendar.

    Args:
        start_date: Reference date in format 'YYYY-MM-DD'
        timedelta: Number of trading days to offset (positive=forward, negative=backward)
        calendar_name: Exchange calendar name (default: XNYS for NYSE)

    Returns:
        Tuple of (start_date, end_date) as strings in 'YYYY-MM-DD' format
    """
    cal = xcals.get_calendar(calendar_name)
    snys_schedule = cal.schedule

    df_schedule = snys_schedule.reset_index()
    start = datetime.datetime.fromisoformat(start_date)

    try:
        date_column = df_schedule.columns[0]

        # find the closest date before or equal to given start_date
        mask = df_schedule[date_column].dt.date <= start.date()
        matching_indices = df_schedule.index[mask].tolist()

        if not matching_indices or len(matching_indices) == len(df_schedule):
            raise ValueError("start_date is out of range")
        else:
            start_idx = matching_indices[-1]

        start_date = str(df_schedule.iloc[start_idx][date_column].date())
        target_idx = start_idx + timedelta

        # make sure end_date not out of the range
        if target_idx < 0 or target_idx >= len(df_schedule):
            raise IndexError(
                f"Target index {target_idx} is out of range [0, {len(df_schedule)-1}]"
            )

        end_row = df_schedule.iloc[target_idx]

        if end_row[date_column].date() < start.date():
            end_date = start_date
            start_date = str(end_row[date_column].date())
        else:
            end_date = str(end_row[date_column].date())

        return start_date, end_date

    except Exception as e:
        print(f"Error: {e}")
        return start_date, start_date


def generate_backtest_dates(
    start_date: str,
    reverse: bool = False,
    reverse_limit: str = None,
    period: str = "week",
    reverse_limit_count: int = 52,
) -> List[str]:
    """
    Generate a list of backtest dates based on period and direction.

    Args:
        start_date: Starting date in format 'YYYY-MM-DD'
        reverse: If True, generate dates going backward from start_date
        reverse_limit: Stop date when going backward (YYYY-MM-DD)
        period: 'day', 'week', or 'month'
        reverse_limit_count: Max number of periods when reverse=True and no reverse_limit

    Returns:
        List of date strings in 'YYYY-MM-DD' format
    """
    backtest_dates = []

    if not reverse:
        current_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        today = datetime.datetime.now()

        while current_date <= today:
            backtest_dates.append(current_date.strftime("%Y-%m-%d"))

            if period == "week":
                current_date += datetime.timedelta(weeks=1)
            elif period == "month":
                if current_date.month == 12:
                    current_date = current_date.replace(
                        year=current_date.year + 1, month=1
                    )
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
            elif period == "day":
                current_date += datetime.timedelta(days=1)
    else:
        current_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")

        if reverse_limit:
            limit_date = datetime.datetime.strptime(reverse_limit, "%Y-%m-%d")
        else:
            limit_date = None

        count = 0
        while True:
            if reverse_limit and current_date < limit_date:
                break
            if not reverse_limit and count >= reverse_limit_count:
                break

            backtest_dates.append(current_date.strftime("%Y-%m-%d"))

            if period == "week":
                current_date -= datetime.timedelta(weeks=1)
            elif period == "month":
                if current_date.month == 1:
                    current_date = current_date.replace(
                        year=current_date.year - 1, month=12
                    )
                else:
                    current_date = current_date.replace(month=current_date.month - 1)
            elif period == "day":
                current_date -= datetime.timedelta(days=1)

            count += 1

    return backtest_dates


# Alias for backward compatibility
generate_backtest_date = generate_backtest_dates


if __name__ == "__main__":
    # Test resolve_date_range
    start, end = resolve_date_range(start_date="2025-10-17", timedelta=-5)
    print(f"Date range: {start} to {end}")

    # Test generate_backtest_dates
    dates = generate_backtest_dates(
        start_date="2025-01-01", reverse=False, period="week"
    )
    print(f"\nGenerated {len(dates)} backtest dates")
    print(f"First 5: {dates[:5]}")
    print(f"Last 5: {dates[-5:]}")
