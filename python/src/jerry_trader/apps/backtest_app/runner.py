"""Backtest Runner - Async task for backtest execution.

Pipeline: PreFilter → DataLoader → FactorEngine → EventEvaluator → ClickHouse

Pushes progress updates via WebSocket broadcast.

Uses asyncio.to_thread() to run blocking operations in thread pool,
keeping the async event loop responsive for WebSocket messages.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

from jerry_trader.apps.backtest_app.models import (
    BacktestProgress,
    generate_experiment_id,
)
from jerry_trader.services.backtest.event.pipeline import BacktestPipeline
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest_app.runner", log_to_file=True)

# Dedicated thread pool for backtest computation
_backtest_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="backtest-")


class BacktestRunner:
    """Async backtest execution with WebSocket progress broadcast.

    Uses asyncio.to_thread() to run blocking operations in thread pool,
    keeping the async event loop responsive.
    """

    def __init__(
        self,
        experiment_id: str,
        date: str,
        events: list[str],
        tickers: Optional[list[str]] = None,
        hold_duration_minutes: int = 10,
        progress_callback: Optional[Callable[[BacktestProgress], None]] = None,
    ):
        self.experiment_id = experiment_id
        self.date = date
        self.events = events
        self.tickers = tickers
        self.hold_duration_minutes = hold_duration_minutes
        self.progress_callback = progress_callback

        self._total_signals = 0
        self._status = "pending"
        # Queue for progress updates from sync thread to async loop
        self._progress_queue: asyncio.Queue[BacktestProgress] = asyncio.Queue()

    async def run(self) -> dict:
        """Execute backtest pipeline asynchronously.

        Runs blocking operations in thread pool, keeping event loop responsive.
        Monitors progress queue and broadcasts updates via WebSocket.

        Returns:
            Summary dict with experiment_id, total_signals, avg_return, win_rate.
        """
        self._status = "running"
        self._total_signals = 0

        # Start progress monitor task
        monitor_task = asyncio.create_task(self._monitor_progress())

        try:
            # Run blocking computation in thread pool
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                _backtest_executor,
                self._run_sync,
            )

            self._status = result.get("status", "completed")
            self._total_signals = result.get("total_signals", 0)

            return result

        except Exception as e:
            logger.exception(f"BacktestRunner error: {e}")
            self._status = "failed"
            await self._queue_progress(BacktestProgress(type="error", message=str(e)))
            raise

        finally:
            # Stop progress monitor
            await self._queue_progress(BacktestProgress(type="_stop_monitor"))
            await monitor_task

    def _run_sync(self) -> dict:
        """Synchronous backtest computation (runs in thread pool).

        Uses BacktestPipeline for unified evaluation logic.
        Progress updates are sent via the queue for async broadcasting.
        """
        try:
            self._emit_progress_sync("Pipeline", 0)

            # Use shared pipeline for unified evaluation
            pipeline = BacktestPipeline(
                experiment_id=self.experiment_id,
                hold_duration_minutes=self.hold_duration_minutes,
            )

            # Progress callback wrapper
            def on_progress(step: str, percent: int, message: str | None = None):
                self._emit_progress_sync(step, percent, message=message)

            # Run pipeline
            result = pipeline.run(
                date=self.date,
                events=self.events if self.events else None,
                tickers=self.tickers,
                progress_callback=on_progress,
            )

            self._total_signals = result.total_signals
            self._emit_progress_sync("complete", 100, total_signals=self._total_signals)

            return result.summary()

        except Exception as e:
            logger.exception(f"BacktestRunner sync error: {e}")
            self._emit_progress_sync("error", 0, message=str(e))
            return {
                "experiment_id": self.experiment_id,
                "total_signals": 0,
                "status": "failed",
                "error": str(e),
            }

    async def _monitor_progress(self):
        """Monitor progress queue and broadcast updates via WebSocket."""
        while True:
            progress = await self._progress_queue.get()

            # Stop signal
            if progress.type == "_stop_monitor":
                break

            # Broadcast to WebSocket
            if self.progress_callback:
                self.progress_callback(progress)

    async def _queue_progress(self, progress: BacktestProgress):
        """Queue a progress update for async broadcasting."""
        await self._progress_queue.put(progress)

    def _emit_progress_sync(
        self,
        step_or_type: str,
        percent: int,
        message: Optional[str] = None,
        total_signals: Optional[int] = None,
    ):
        """Emit progress from sync thread (puts in queue).

        Args:
            step_or_type: Either step name (for progress type) or type directly
            percent: Progress percentage
            message: Optional message
            total_signals: Optional total signals count for complete type
        """
        # Determine type based on step_or_type
        if step_or_type in ("error", "complete", "signal", "_stop_monitor"):
            progress_type = step_or_type
            step = None
        else:
            progress_type = "progress"
            step = step_or_type

        progress = BacktestProgress(
            type=progress_type,
            date=self.date if progress_type == "progress" else None,
            step=step,
            percent=percent,
            message=message,
            experiment_id=self.experiment_id if progress_type == "complete" else None,
            total_signals=total_signals,
        )

        # Use asyncio.run_coroutine_threadsafe to put in queue from sync thread
        try:
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(
                lambda: asyncio.ensure_future(self._progress_queue.put(progress))
            )
        except RuntimeError:
            # No running loop, just log
            logger.debug(f"Progress: {progress_type} - {step or message}")

    def _emit_signal_sync(self, ticker: str, entry_time: int, entry_price: float):
        """Emit signal found from sync thread."""
        progress = BacktestProgress(
            type="signal",
            ticker=ticker,
            entry_time=entry_time // 1000,
            entry_price=entry_price,
        )

        try:
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(
                lambda: asyncio.ensure_future(self._progress_queue.put(progress))
            )
        except RuntimeError:
            logger.debug(f"Signal: {ticker} @ {entry_price}")


async def run_backtest(
    date: str,
    events: list[str],
    tickers: Optional[list[str]] = None,
    hold_duration_minutes: int = 10,
    progress_callback: Optional[Callable[[BacktestProgress], None]] = None,
) -> dict:
    """Run backtest and return summary.

    Args:
        date: Date in YYYY-MM-DD format.
        events: List of event names to evaluate.
        tickers: Optional list of specific tickers.
        hold_duration_minutes: Hold duration in minutes.
        progress_callback: Callback for WebSocket progress updates.

    Returns:
        Summary dict with experiment_id, total_signals, avg_return, win_rate.
    """
    experiment_id = generate_experiment_id()
    runner = BacktestRunner(
        experiment_id=experiment_id,
        date=date,
        events=events,
        tickers=tickers,
        hold_duration_minutes=hold_duration_minutes,
        progress_callback=progress_callback,
    )
    return await runner.run()
