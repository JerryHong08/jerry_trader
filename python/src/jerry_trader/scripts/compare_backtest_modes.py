#!/usr/bin/env python3
"""Compare Batch vs Streaming backtest modes.

This script runs both modes on the same date and compares:
- Number of signals
- Trigger timing differences
- Win rate and returns
"""

import argparse
import logging
from datetime import datetime

from jerry_trader.services.backtest.event.pipeline import BacktestPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def compare_modes(date: str, tickers: list[str] | None = None):
    """Run both modes and compare results."""
    print(f"\n{'=' * 70}")
    print(f"COMPARING BACKTEST MODES: {date}")
    print(f"{'=' * 70}\n")

    # Run Batch mode
    print("Running BATCH mode...")
    batch_pipeline = BacktestPipeline(
        experiment_id=f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        mode="batch",
    )
    batch_result = batch_pipeline.run(date=date, tickers=tickers)

    print(f"\nBatch Results:")
    print(f"  Total Signals: {batch_result.total_signals}")
    print(f"  Win Rate: {batch_result.win_rate:.1f}%")
    print(f"  Avg Return: {batch_result.avg_return:.2f}%")
    print(f"  Elapsed: {batch_result.elapsed_seconds:.2f}s")

    # Run Streaming mode
    print("\nRunning STREAMING mode...")
    streaming_pipeline = BacktestPipeline(
        experiment_id=f"streaming_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        mode="streaming",
    )
    streaming_result = streaming_pipeline.run(date=date, tickers=tickers)

    print(f"\nStreaming Results:")
    print(f"  Total Signals: {streaming_result.total_signals}")
    print(f"  Win Rate: {streaming_result.win_rate:.1f}%")
    print(f"  Avg Return: {streaming_result.avg_return:.2f}%")
    print(f"  Elapsed: {streaming_result.elapsed_seconds:.2f}s")

    # Compare
    print(f"\n{'=' * 70}")
    print("COMPARISON")
    print(f"{'=' * 70}\n")

    print(f"{'Metric':<20} {'Batch':<15} {'Streaming':<15} {'Diff':<15}")
    print("-" * 65)
    print(
        f"{'Total Signals':<20} {batch_result.total_signals:<15} {streaming_result.total_signals:<15} {streaming_result.total_signals - batch_result.total_signals:<15}"
    )
    print(
        f"{'Win Rate':<20} {batch_result.win_rate * 100:.1f}%{'':<10} {streaming_result.win_rate * 100:.1f}%{'':<10} {(streaming_result.win_rate - batch_result.win_rate) * 100:+.1f}%"
    )
    print(
        f"{'Avg Return':<20} {batch_result.avg_return:.2f}%{'':<9} {streaming_result.avg_return:.2f}%{'':<9} {streaming_result.avg_return - batch_result.avg_return:+.2f}%"
    )
    print(
        f"{'Elapsed (s)':<20} {batch_result.elapsed_seconds:.2f}{'':<12} {streaming_result.elapsed_seconds:.2f}{'':<12} {streaming_result.elapsed_seconds - batch_result.elapsed_seconds:+.2f}s"
    )

    # Find unique signals
    batch_tickers = {s.ticker for s in batch_result.signals}
    streaming_tickers = {s.ticker for s in streaming_result.signals}

    only_batch = batch_tickers - streaming_tickers
    only_streaming = streaming_tickers - batch_tickers
    common = batch_tickers & streaming_tickers

    print(f"\n{'Signal Distribution':<30}")
    print(f"  Only in Batch: {len(only_batch)} tickers")
    print(f"  Only in Streaming: {len(only_streaming)} tickers")
    print(f"  Common: {len(common)} tickers")

    if only_batch:
        print(
            f"\n  Batch-only tickers: {sorted(only_batch)[:10]}{'...' if len(only_batch) > 10 else ''}"
        )
    if only_streaming:
        print(
            f"  Streaming-only tickers: {sorted(only_streaming)[:10]}{'...' if len(only_streaming) > 10 else ''}"
        )

    # Compare timing for common signals
    if common:
        print(f"\n{'Timing Comparison (common tickers)':<30}")
        batch_by_ticker = {s.ticker: s for s in batch_result.signals}
        streaming_by_ticker = {s.ticker: s for s in streaming_result.signals}

        timing_diffs = []
        for ticker in sorted(common):
            batch_sig = batch_by_ticker[ticker]
            streaming_sig = streaming_by_ticker[ticker]
            diff_ms = (
                streaming_sig.entry_time.timestamp() * 1000
                - batch_sig.entry_time.timestamp() * 1000
            )
            timing_diffs.append(diff_ms)

        if timing_diffs:
            avg_diff = sum(timing_diffs) / len(timing_diffs)
            print(f"  Avg timing diff: {avg_diff / 1000:.1f}s (streaming - batch)")
            print(f"  Min diff: {min(timing_diffs) / 1000:.1f}s")
            print(f"  Max diff: {max(timing_diffs) / 1000:.1f}s")

    return batch_result, streaming_result


def main():
    parser = argparse.ArgumentParser(
        description="Compare batch vs streaming backtest modes"
    )
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument(
        "--tickers", nargs="+", help="Optional list of tickers to filter"
    )

    args = parser.parse_args()

    compare_modes(args.date, args.tickers)


if __name__ == "__main__":
    main()
