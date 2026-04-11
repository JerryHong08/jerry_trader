"""Snapshot builder — batch generate market_snapshot from trades + quotes Parquet.

TWO-STAGE STREAMING PIPELINE (memory-safe for 150M+ trades):

  Stage 1 — sink_parquet (streaming, bounded memory):
    scan trades → join prev_close → bucket into windows → aggregate OHLCV
    scan quotes → bucket into windows → aggregate bid/ask (last value)
    → sink to intermediate parquets

  Stage 2 — chunked join + rank (bounded memory per chunk):
    Join trades_agg + quotes_agg
    Split by time ranges (30-min chunks)
    → rank + sort each chunk separately
    → merge chunks via scan → sink_parquet
    → output collector_format matching Polygon Collector API for Replay

This avoids loading the full 150M-row trades into memory at any point.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from jerry_trader.platform.config.config import get_splits_data, lake_data_dir
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.time.timezone import ms_to_hhmmss

ET = ZoneInfo("America/New_York")

# Default time filter: US pre-market session
DEFAULT_FILTER_START_ET = "04:00"
DEFAULT_FILTER_END_ET = "09:30"

logger = setup_logger("backtest.data.snapshot_builder", log_to_file=True)

# ClickHouse market_snapshot columns (matching sql/clickhouse_market_snapshot.sql)
_SNAPSHOT_COLUMNS = [
    "symbol",
    "date",
    "mode",
    "session_id",
    "event_time_ms",
    "event_time",
    "price",
    "changePercent",
    "volume",
    "prev_close",
    "prev_volume",
    "vwap",
    "bid",
    "ask",
    "bid_size",
    "ask_size",
    "rank",
    "competition_rank",
    "change",
    "relativeVolumeDaily",
    "relativeVolume5min",
]

# Collector format columns (matching Polygon API output)
_COLLECTOR_COLUMNS = [
    "ticker",
    "timestamp",
    "price",
    "volume",
    "prev_close",
    "prev_volume",
    "vwap",
    "bid",
    "ask",
    "bid_size",
    "ask_size",
    "changePercent",
    "change",
    "rank",
    "date",
    "mode",
    "session_id",
]


def _trades_path(date: str) -> Path:
    year, month = date.split("-")[0], date.split("-")[1]
    return Path(
        f"{lake_data_dir}/us_stocks_sip/trades_v1/{year}/{month}/{date}.parquet"
    )


def _quotes_path(date: str) -> Path:
    year, month = date.split("-")[0], date.split("-")[1]
    return Path(
        f"{lake_data_dir}/us_stocks_sip/quotes_v1/{year}/{month}/{date}.parquet"
    )


def _day_aggs_path(date: str) -> Path:
    year, month = date.split("-")[0], date.split("-")[1]
    return Path(
        f"{lake_data_dir}/us_stocks_sip/day_aggs_v1/{year}/{month}/{date}.parquet"
    )


def _et_to_epoch_ms(date: str, time_et: str) -> int:
    """Convert ET time string (HH:MM) to epoch milliseconds for the given date."""
    dt = datetime.strptime(date, "%Y-%m-%d")
    h, m = map(int, time_et.split(":"))
    aware = datetime(dt.year, dt.month, dt.day, h, m, tzinfo=ET)
    return int(aware.timestamp() * 1000)


def _adjust_prev_close_for_splits(
    prev_close_df: pl.DataFrame,
    date: str,
) -> pl.DataFrame:
    """Adjust prev_close for stocks that have splits on the given date.

    When a stock splits on `date`, the prev_close needs to be adjusted
    to correctly calculate the changePercent.

    Args:
        prev_close_df: DataFrame with ticker and prev_close columns
        date: The trading date (YYYY-MM-DD) to check for splits

    Returns:
        DataFrame with adjusted prev_close values
    """
    try:
        splits = get_splits_data()
    except Exception as e:
        logger.warning(f"Could not load splits data: {e}")
        return prev_close_df

    if splits is None or len(splits) == 0:
        return prev_close_df

    # Filter splits for the given date (execution_date == date)
    # execution_date is the date when the split takes effect
    date_splits = splits.filter(pl.col("execution_date") == date)

    if len(date_splits) == 0:
        logger.debug(f"No splits on {date}")
        return prev_close_df

    logger.info(f"Found {len(date_splits)} splits on {date}")

    # Calculate adjustment factor: split_from / split_to
    # - Reverse split (25:1): split_from=25, split_to=1 → factor=25, price UP
    # - Forward split (1:5): split_from=1, split_to=5 → factor=0.2, price DOWN
    # Example: DUKR reverse split 25:1, prev_close $1 → $25 after adjustment
    split_adjustments = date_splits.select(
        [
            pl.col("ticker"),
            (pl.col("split_from") / pl.col("split_to")).alias("adjustment_factor"),
        ]
    )

    # Join with prev_close and adjust
    adjusted_df = (
        prev_close_df.join(split_adjustments, on="ticker", how="left")
        .with_columns(
            [
                pl.when(pl.col("adjustment_factor").is_not_null())
                .then(pl.col("prev_close") * pl.col("adjustment_factor"))
                .otherwise(pl.col("prev_close"))
                .alias("prev_close")
            ]
        )
        .drop("adjustment_factor")
    )

    # Log adjusted tickers
    adjusted_tickers = (
        adjusted_df.join(split_adjustments, on="ticker", how="inner")
        .select("ticker")
        .to_series()
        .to_list()
    )
    if adjusted_tickers:
        logger.info(
            f"Adjusted prev_close for splits: {adjusted_tickers[:10]}{'...' if len(adjusted_tickers) > 10 else ''}"
        )

    return adjusted_df


def _stage1_aggregate_trades(
    trades_path: Path,
    prev_close_df: pl.DataFrame,
    window_ms: int,
    start_ns: int | None,
    end_ns: int | None,
    agg_output: str,
) -> None:
    """Stage 1a: Aggregate trades → OHLCV per window."""
    trades_lazy = pl.scan_parquet(str(trades_path))

    # Detect timestamp column
    ts_col = "participant_timestamp"
    if ts_col not in trades_lazy.collect_schema().names():
        ts_col = "sip_timestamp"

    # Apply time filter
    if start_ns is not None and end_ns is not None:
        trades_lazy = trades_lazy.filter(
            (pl.col(ts_col) >= start_ns) & (pl.col(ts_col) < end_ns)
        )

        pipeline = (
            trades_lazy.join(prev_close_df.lazy(), on="ticker", how="left")
            .select(
                [
                    pl.col("ticker"),
                    pl.col("price"),
                    pl.col("size"),
                    pl.col("prev_close"),
                    pl.col("prev_volume"),
                    (pl.col(ts_col) // 1_000_000 // window_ms * window_ms).alias("_ws"),
                ]
            )
            .group_by(["ticker", "_ws"])
            .agg(
                [
                    pl.col("price").last().alias("price"),
                    pl.col("size").sum().alias("volume"),
                    (pl.col("price") * pl.col("size")).sum().alias("_turnover"),
                    pl.col("prev_close").first().alias("prev_close"),
                    pl.col("prev_volume").first().alias("prev_volume"),
                ]
            )
            .with_columns(
                [
                    (
                        (pl.col("price") - pl.col("prev_close"))
                        / pl.col("prev_close")
                        * 100
                    ).alias("changePercent"),
                    (pl.col("price") - pl.col("prev_close")).alias("change"),
                ]
            )
        )

    Path(agg_output).parent.mkdir(parents=True, exist_ok=True)
    pipeline.sink_parquet(agg_output, compression="zstd", compression_level=3)
    logger.info(f"Stage 1a (trades): {agg_output}")


def _stage1_aggregate_quotes(
    quotes_path: Path,
    window_ms: int,
    start_ns: int | None,
    end_ns: int | None,
    agg_output: str,
) -> None:
    """Stage 1b: Aggregate quotes → last bid/ask per window."""
    quotes_lazy = pl.scan_parquet(str(quotes_path))

    # Detect timestamp column
    ts_col = "participant_timestamp"
    if ts_col not in quotes_lazy.collect_schema().names():
        ts_col = "sip_timestamp"

    # Apply time filter
    if start_ns is not None and end_ns is not None:
        quotes_lazy = quotes_lazy.filter(
            (pl.col(ts_col) >= start_ns) & (pl.col(ts_col) < end_ns)
        )

    pipeline = (
        quotes_lazy.select(
            [
                pl.col("ticker"),
                pl.col("bid_price"),
                pl.col("ask_price"),
                pl.col("bid_size"),
                pl.col("ask_size"),
                (pl.col(ts_col) // 1_000_000 // window_ms * window_ms).alias("_ws"),
            ]
        )
        .group_by(["ticker", "_ws"])
        .agg(
            [
                pl.col("bid_price").last().alias("bid"),
                pl.col("ask_price").last().alias("ask"),
                pl.col("bid_size").last().alias("bid_size"),
                pl.col("ask_size").last().alias("ask_size"),
            ]
        )
    )

    pipeline.sink_parquet(agg_output, compression="zstd", compression_level=3)
    logger.info(f"Stage 1b (quotes): {agg_output}")


def _stage2_join_and_rank(
    trades_agg_path: str,
    quotes_agg_path: str | None,
    output_dir: str,
    date: str,
    mode: str = "live",
    session_id: str = "",
    include_quotes: bool = True,
) -> str:
    """Stage 2: Join trades + quotes, compute ranks, output collector format.

    Returns:
        collector_path - path to output file
    """
    trades_agg = pl.scan_parquet(trades_agg_path)

    # Join with quotes if available
    if include_quotes and quotes_agg_path and Path(quotes_agg_path).exists():
        quotes_agg = pl.scan_parquet(quotes_agg_path)
        joined = trades_agg.join(quotes_agg, on=["ticker", "_ws"], how="left")
        joined = joined.with_columns(
            [
                pl.col("bid").fill_null(0.0),
                pl.col("ask").fill_null(0.0),
                pl.col("bid_size").fill_null(0.0),
                pl.col("ask_size").fill_null(0.0),
            ]
        )
    else:
        joined = trades_agg.with_columns(
            [
                pl.lit(0.0).alias("bid"),
                pl.lit(0.0).alias("ask"),
                pl.lit(0.0).alias("bid_size"),
                pl.lit(0.0).alias("ask_size"),
            ]
        )

    # Get time range for chunking
    bounds = joined.select(
        [pl.col("_ws").min().alias("lo"), pl.col("_ws").max().alias("hi")]
    ).collect()
    ws_min: int = int(bounds["lo"][0])
    ws_max: int = int(bounds["hi"][0])
    logger.info(f"Stage 2: time range {ms_to_hhmmss(ws_min)} → {ms_to_hhmmss(ws_max)}")

    # Process in 30-minute chunks
    chunk_ms = 30 * 60 * 1000
    collector_chunks: list[str] = []
    # Carry-over from previous chunk: ticker -> (cum_volume, cum_turnover)
    prev_cum: dict[str, tuple[float, float]] = {}

    for chunk_start in range(ws_min, ws_max + chunk_ms, chunk_ms):
        chunk_end = chunk_start + chunk_ms

        chunk_df = (
            joined.filter((pl.col("_ws") >= chunk_start) & (pl.col("_ws") < chunk_end))
            .with_columns(
                [
                    pl.col("changePercent")
                    .rank(method="ordinal", descending=True)
                    .over([pl.col("_ws")])
                    .cast(pl.Int32)
                    .alias("rank"),
                ]
            )
            .sort(["_ws", "rank"])
            .collect()
        )

        if len(chunk_df) == 0:
            continue

        # Convert per-window incremental volume/turnover to cumulative per ticker.
        # Live mode sends cumulative volume (min.av from Polygon API), so replay
        # must match to keep VolumeTracker's relativeVolume5min calculation correct.
        chunk_df = (
            chunk_df.sort(["ticker", "_ws"])
            .with_columns(
                pl.col("volume")
                .cast(pl.Float64)
                .cum_sum()
                .over("ticker")
                .alias("volume")
            )
            .with_columns(
                pl.col("_turnover")
                .cast(pl.Float64)
                .cum_sum()
                .over("ticker")
                .alias("_cum_turnover")
            )
        )

        # Add carry-over from previous chunk so cumulative values span the
        # full session instead of resetting every 30 minutes.
        if prev_cum:
            cum_volume_offset = pl.Series(
                "volume",
                [prev_cum.get(t, (0.0, 0.0))[0] for t in chunk_df["ticker"].to_list()],
                dtype=pl.Float64,
            )
            cum_turnover_offset = pl.Series(
                "_cum_turnover",
                [prev_cum.get(t, (0.0, 0.0))[1] for t in chunk_df["ticker"].to_list()],
                dtype=pl.Float64,
            )
            chunk_df = chunk_df.with_columns(
                (pl.col("volume") + cum_volume_offset).alias("volume"),
                (pl.col("_cum_turnover") + cum_turnover_offset).alias("_cum_turnover"),
            )

        # Cumulative VWAP = cumulative turnover / cumulative volume
        chunk_df = chunk_df.with_columns(
            pl.when(pl.col("volume") > 0)
            .then(pl.col("_cum_turnover") / pl.col("volume"))
            .otherwise(pl.lit(0.0))
            .alias("vwap")
        )

        # Save carry-over for next chunk BEFORE dropping helper columns.
        # Use pre-round volume for accurate carry-over.
        last_per_ticker = (
            chunk_df.sort(["ticker", "_ws"])
            .group_by("ticker")
            .agg(
                [
                    pl.col("volume").last().alias("_cv"),
                    pl.col("_cum_turnover").last().alias("_ct"),
                ]
            )
        )
        for row in last_per_ticker.iter_rows(named=True):
            prev_cum[row["ticker"]] = (float(row["_cv"]), float(row["_ct"]))

        # Round volume to integer (shares) to avoid float precision noise
        chunk_df = chunk_df.with_columns(pl.col("volume").round(0)).drop(
            "_turnover", "_cum_turnover"
        )

        # Output: collector_format (full, for Replay)
        collector_chunk = chunk_df.select(
            [
                pl.col("ticker"),
                pl.col("_ws").alias("timestamp"),
                pl.col("price"),
                pl.col("volume"),
                pl.col("prev_close"),
                pl.col("prev_volume").cast(pl.Float64).fill_null(0.0),
                pl.col("vwap"),
                pl.col("bid"),
                pl.col("ask"),
                pl.col("bid_size"),
                pl.col("ask_size"),
                pl.col("changePercent"),
                pl.col("change"),
                pl.col("rank"),
            ]
        )
        collector_path = f"{output_dir}/.collector_chunk_{chunk_start}.parquet"
        collector_chunk.write_parquet(collector_path)
        collector_chunks.append(collector_path)

        logger.info(f"  Chunk {ms_to_hhmmss(chunk_start)}: {len(chunk_df):,} rows")

    # Merge chunks
    collector_output = f"{output_dir}/collector_format.parquet"

    if collector_chunks:
        pl.scan_parquet(collector_chunks).with_columns(
            [
                pl.lit(date).alias("date"),
                pl.lit(mode).alias("mode"),
                pl.lit(session_id).alias("session_id"),
            ]
        ).sink_parquet(collector_output, compression="zstd", compression_level=3)
        for f in collector_chunks:
            Path(f).unlink(missing_ok=True)

    logger.info(f"Stage 2 done: {collector_output}")
    return collector_output


def build_to_parquet(
    date: str,
    output_dir: str,
    window_ms: int = 5_000,
    filter_start_et: str | None = DEFAULT_FILTER_START_ET,
    filter_end_et: str | None = DEFAULT_FILTER_END_ET,
    mode: str = "live",
    session_id: str = "",
    include_quotes: bool = True,
) -> str:
    """Build snapshots to Parquet files (internal helper).

    Args:
        date: Trading date (YYYY-MM-DD).
        output_dir: Output directory for parquet files.
        window_ms: Aggregation window in milliseconds.
        filter_start_et: Start time filter in ET.
        filter_end_et: End time filter in ET.
        mode: Run mode ("live" or "replay").
        session_id: Session identifier.
        include_quotes: Whether to process quotes.

    Returns:
        collector_path - path to output file
    """
    # Time filter
    if filter_start_et and filter_end_et:
        start_ns = _et_to_epoch_ms(date, filter_start_et) * 1_000_000
        end_ns = _et_to_epoch_ms(date, filter_end_et) * 1_000_000
    else:
        start_ns = None
        end_ns = None

    # Load prev_close
    dt = datetime.strptime(date, "%Y-%m-%d")
    prev_date = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_close_path = _day_aggs_path(prev_date)

    prev_close_df = pl.DataFrame({"ticker": [], "prev_close": [], "prev_volume": []})
    if prev_close_path.exists():
        prev_close_df = (
            pl.scan_parquet(str(prev_close_path))
            .select(["ticker", "close", "volume"])
            .rename({"close": "prev_close", "volume": "prev_volume"})
            .collect()
        )
        logger.info(
            f"Loaded {len(prev_close_df):,} prev_close + prev_volume from {prev_date}"
        )

        # Adjust prev_close for stocks with splits on the trading date
        prev_close_df = _adjust_prev_close_for_splits(prev_close_df, date)
    else:
        logger.warning(
            f"No day_aggs for {prev_date} — prev_close and prev_volume will be null"
        )

    # Stage 1a: Aggregate trades
    trades_path = _trades_path(date)
    if not trades_path.exists():
        raise FileNotFoundError(f"Trades not found: {trades_path}")

    trades_agg_path = f"{output_dir}/.trades_agg.parquet"
    _stage1_aggregate_trades(
        trades_path, prev_close_df, window_ms, start_ns, end_ns, trades_agg_path
    )

    # Stage 1b: Aggregate quotes (optional)
    quotes_agg_path = None
    if include_quotes:
        quotes_path = _quotes_path(date)
        if quotes_path.exists():
            quotes_agg_path = f"{output_dir}/.quotes_agg.parquet"
            _stage1_aggregate_quotes(
                quotes_path, window_ms, start_ns, end_ns, quotes_agg_path
            )
        else:
            logger.warning(f"Quotes not found: {quotes_path} — bid/ask will be 0")

    # Stage 2: Join and output
    try:
        collector_path = _stage2_join_and_rank(
            trades_agg_path,
            quotes_agg_path,
            output_dir,
            date,
            mode,
            session_id,
            include_quotes,
        )
    finally:
        Path(trades_agg_path).unlink(missing_ok=True)
        if quotes_agg_path:
            Path(quotes_agg_path).unlink(missing_ok=True)

    return collector_path


# =============================================================================
# Main entry point (ClickHouse only)
# =============================================================================


def build(
    date: str,
    ch_client: Any,
    database: str = "jerry_trader",
    window_ms: int = 5_000,
    mode: str = "live",
    session_id: str = "",
    batch_size: int = 50_000,
    filter_start_et: str | None = DEFAULT_FILTER_START_ET,
    filter_end_et: str | None = DEFAULT_FILTER_END_ET,
    include_quotes: bool = True,
    output_parquet: str | None = None,
) -> int:
    """Build snapshots and insert into ClickHouse.

    Primary workflow: builds in temp directory, inserts to CH, cleans up.

    Args:
        date: Trading date (YYYY-MM-DD).
        ch_client: ClickHouse client.
        database: ClickHouse database name.
        window_ms: Aggregation window in milliseconds (default 5000 = 5s).
        mode: Run mode ("live" or "replay").
        session_id: Session identifier.
        batch_size: Batch size for CH inserts.
        filter_start_et: Start time filter in ET (HH:MM). Default "04:00".
        filter_end_et: End time filter in ET (HH:MM). Default "09:30".
        include_quotes: Whether to process quotes for bid/ask data.
        output_parquet: If provided, also save Parquet files to this directory.

    Returns:
        collector_inserted - row count inserted into ClickHouse
    """
    import tempfile
    import time

    t0 = time.time()

    # Build in temp directory
    with tempfile.TemporaryDirectory() as tmpdir:
        collector_path = build_to_parquet(
            date,
            output_dir=tmpdir,
            window_ms=window_ms,
            filter_start_et=filter_start_et,
            filter_end_et=filter_end_et,
            mode=mode,
            session_id=session_id,
            include_quotes=include_quotes,
        )

        collector_df = pl.read_parquet(collector_path)

        # Fill nulls for numeric columns (ClickHouse can't handle None)
        collector_df = collector_df.with_columns(
            [
                pl.col("price").fill_null(0.0),
                pl.col("volume").fill_null(0.0),
                pl.col("prev_close").fill_null(0.0),
                pl.col("prev_volume").fill_null(0.0),
                pl.col("vwap").fill_null(0.0),
                pl.col("bid").fill_null(0.0),
                pl.col("ask").fill_null(0.0),
                pl.col("bid_size").fill_null(0.0),
                pl.col("ask_size").fill_null(0.0),
                pl.col("changePercent").fill_null(0.0),
                pl.col("change").fill_null(0.0),
                pl.col("rank").fill_null(0),
            ]
        )

        # Optionally save to parquet
        if output_parquet:
            import shutil

            Path(output_parquet).mkdir(parents=True, exist_ok=True)
            shutil.copy(collector_path, f"{output_parquet}/collector_format.parquet")
            logger.info(f"Parquet files saved to {output_parquet}")

    elapsed = time.time() - t0
    logger.info(f"Built {len(collector_df):,} collector rows in {elapsed:.1f}s")

    # Insert collector_format
    collector_inserted = 0
    if len(collector_df) > 0:
        rows = collector_df.select(_COLLECTOR_COLUMNS).rows()
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            try:
                ch_client.insert(
                    f"{database}.market_snapshot_collector",
                    batch,
                    column_names=_COLLECTOR_COLUMNS,
                )
                collector_inserted += len(batch)
            except Exception as e:
                logger.error(f"ClickHouse insert failed at batch {i}: {e}")
                break
        logger.info(
            f"Inserted {collector_inserted:,} rows into market_snapshot_collector"
        )

    return collector_inserted


# Backwards compatibility alias
build_and_insert = build


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(
        description="Build market snapshots from trades + quotes"
    )
    parser.add_argument("--date", required=True, help="Trading date (YYYY-MM-DD)")
    parser.add_argument(
        "--window-ms", type=int, default=5000, help="Aggregation window (ms)"
    )
    parser.add_argument("--start-et", default="04:00", help="Start time filter (ET)")
    parser.add_argument("--end-et", default="09:30", help="End time filter (ET)")
    parser.add_argument(
        "--no-quotes", action="store_true", help="Skip quotes processing"
    )
    parser.add_argument("--mode", default="live", help="Mode (live/replay)")
    parser.add_argument("--session-id", default="", help="Session ID")
    parser.add_argument(
        "--output-parquet", help="Also save Parquet files to this directory"
    )
    parser.add_argument("--ch-host", default="localhost", help="ClickHouse host")
    parser.add_argument("--ch-port", type=int, default=8123, help="ClickHouse port")
    parser.add_argument(
        "--ch-database", default="jerry_trader", help="ClickHouse database"
    )
    args = parser.parse_args()

    # Connect to ClickHouse
    import clickhouse_connect

    ch_password = os.getenv("CLICKHOUSE_PASSWORD", "")
    ch_client = clickhouse_connect.get_client(
        host=args.ch_host,
        port=args.ch_port,
        database=args.ch_database,
        password=ch_password,
    )

    # Build and insert
    collector_inserted = build(
        date=args.date,
        ch_client=ch_client,
        window_ms=args.window_ms,
        filter_start_et=args.start_et,
        filter_end_et=args.end_et,
        mode=args.mode,
        session_id=args.session_id,
        include_quotes=not args.no_quotes,
        output_parquet=args.output_parquet,
    )

    print(f"\nInserted to ClickHouse:")
    print(f"  market_snapshot_collector: {collector_inserted:,} rows")
