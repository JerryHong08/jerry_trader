"""Build 1m bars from trades for backtest visualization.

Reuses Rust BarBuilder for consistency with realtime/replay pipeline.

Usage:
    build_bars_from_trades("2026-03-13", ch_client)
"""

from __future__ import annotations

from datetime import date as dt_date
from datetime import datetime as dt_datetime
from pathlib import Path
from typing import Any

from jerry_trader._rust import BarBuilder
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.time.timezone import convert_bar_et_to_utc, utc_ms_to_et_ms

logger = setup_logger("backtest.bar_builder", log_to_file=True)


def build_bars_from_trades(
    date: str,
    ch_client: Any = None,
    database: str = "jerry_trader",
    timeframe: str = "1m",
    tickers: list[str] | None = None,
) -> int:
    """Build bars from trades for a given date using Rust BarBuilder.

    Reads trades from the Polygon parquet file directly (lake_data_dir),
    which is always available after the convert step. Falls back to
    ClickHouse if parquet is missing.

    Args:
        date: Date in YYYY-MM-DD format
        ch_client: ClickHouse client (optional, for checking existing bars)
        database: Database name
        timeframe: Bar timeframe (default 1m)
        tickers: Optional list of tickers to filter (default: all)

    Returns:
        Number of bars created
    """
    from jerry_trader.platform.config.config import lake_data_dir

    year, month, _ = date.split("-")
    trades_parquet = (
        Path(lake_data_dir)
        / "us_stocks_sip"
        / "trades_v1"
        / year
        / month
        / f"{date}.parquet"
    )

    if not trades_parquet.exists():
        logger.warning(f"Trades parquet not found: {trades_parquet}")
        return 0

    # Check if bars already exist for this date (ClickHouse)
    if ch_client:
        try:
            result = ch_client.query(
                f"""
                SELECT count() FROM {database}.backtest_bars
                WHERE date = {{date:String}}
                """,
                parameters={"date": date},
            )
            existing_count = result.result_rows[0][0] if result.result_rows else 0

            if existing_count > 0:
                logger.info(f"Bars already exist for {date}: {existing_count} bars")
                return existing_count

        except Exception as e:
            logger.warning(f"Could not check existing bars: {e}")

    # Load trades from parquet directly (always available after convert step)
    logger.info(
        f"Building {timeframe} bars from trades for {date} " f"({trades_parquet})"
    )

    try:
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.execute("SET memory_limit='4GB'")

        # Build ticker filter
        if tickers:
            ticker_list = ", ".join(f"'{t}'" for t in tickers)
            ticker_filter = f"WHERE ticker IN ({ticker_list})"
        else:
            ticker_filter = ""

        # Read trades grouped by ticker, sorted by timestamp
        result = conn.execute(
            f"""
            SELECT ticker, sip_timestamp, price, size
            FROM read_parquet('{trades_parquet}')
            {ticker_filter}
            ORDER BY ticker, sip_timestamp
        """
        ).fetchall()

        conn.close()

        if not result:
            logger.info(f"No trades found for {date}")
            return 0

        # Group trades by ticker
        trades_by_ticker: dict[str, list[tuple[int, float, float]]] = {}
        for row in result:
            ticker, sip_ns, price, size = row
            # Convert nanoseconds to milliseconds, then UTC → ET
            ts_utc_ms = int(sip_ns // 1_000_000)
            ts_et_ms = utc_ms_to_et_ms(ts_utc_ms)
            trades_by_ticker.setdefault(ticker, []).append(
                (ts_et_ms, float(price), float(size))
            )

        logger.info(f"Loaded trades for {len(trades_by_ticker)} tickers")

        # Build bars using Rust BarBuilder
        bar_builder = BarBuilder([timeframe])
        bar_builder.configure_watermark(late_arrival_ms=0, idle_close_ms=1)

        all_bars: list[dict] = []

        for ticker, trades_et in trades_by_ticker.items():
            # Batch ingest trades
            bars_et = bar_builder.ingest_trades_batch(ticker, trades_et)

            # Close time-expired bars
            if trades_et:
                last_trade_et_ms = trades_et[-1][0]
                expired = bar_builder.advance(last_trade_et_ms)
                if expired:
                    bars_et.extend(expired)

            # Flush remaining partial bars
            remaining = bar_builder.flush()
            if remaining:
                bars_et.extend(remaining)

            # Convert ET → UTC for ClickHouse storage
            bars_utc = [convert_bar_et_to_utc(bar) for bar in bars_et]
            all_bars.extend(bars_utc)

            # Reset builder for next ticker
            bar_builder.remove_ticker(ticker)

        # Write bars to ClickHouse (if available)
        if all_bars and ch_client:
            date_obj = dt_datetime.strptime(date, "%Y-%m-%d").date()

            rows = []
            for bar in all_bars:
                rows.append(
                    [
                        bar["ticker"],
                        timeframe,
                        date_obj,
                        bar["bar_start"],
                        bar["bar_end"],
                        bar["open"],
                        bar["high"],
                        bar["low"],
                        bar["close"],
                        bar["volume"],
                        bar["trade_count"],
                        bar.get("vwap", 0.0),
                        bar.get("session", "regular"),
                    ]
                )

            ch_client.insert(
                f"{database}.backtest_bars",
                rows,
                column_names=[
                    "ticker",
                    "timeframe",
                    "date",
                    "bar_start",
                    "bar_end",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "trade_count",
                    "vwap",
                    "session",
                ],
            )

        logger.info(f"Built {len(all_bars)} bars for {date}")
        return len(all_bars)

    except Exception as e:
        logger.error(f"Failed to build bars from trades: {e}")
        raise


def build_bars_date_range(
    start_date: str,
    end_date: str,
    ch_client: Any,
    database: str = "jerry_trader",
    tickers: list[str] | None = None,
) -> dict[str, int]:
    """Build bars for a date range.

    Args:
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD
        ch_client: ClickHouse client
        database: Database name
        tickers: Optional list of tickers to filter (default: all)

    Returns:
        Dict mapping date to bar count
    """
    from jerry_trader.services.backtest.data.trading_calendar import get_trading_days

    dates = get_trading_days(start_date, end_date)
    results = {}

    for date in dates:
        try:
            count = build_bars_from_trades(date, ch_client, database, tickers=tickers)
            results[date] = count
        except Exception as e:
            logger.error(f"Failed to build bars for {date}: {e}")
            results[date] = 0

    total = sum(results.values())
    logger.info(f"Built {total} bars for {len(dates)} dates")

    return results
