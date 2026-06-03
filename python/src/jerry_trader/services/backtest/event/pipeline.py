"""Backtest Pipeline - Unified evaluation for App and CLI.

Stage-based evaluation ensures max 1 entry signal per ticker per session.
Pipeline owns the full result lifecycle: compute → ClickHouse → validate.

Two execution modes:
- streaming (default): Simulates real-time triggering, processes trades chronologically
- batch: Computes all factors upfront, then checks conditions at each timestamp

Usage:
    pipeline = BacktestPipeline(mode="streaming")  # Default mode
    result = pipeline.run(date="2026-03-13")

    # Or with batch mode (backward compatibility)
    pipeline = BacktestPipeline(mode="batch")
    result = pipeline.run(date="2026-03-13")

    # Or with specific events
    result = pipeline.run(
        date="2026-03-13",
        events=["gap_up_watch", "momentum_entry"],
    )
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Literal

import pytz

from jerry_trader._rust import compute_signal_exits_batch
from jerry_trader.domain.event import Event, EventStage
from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.services.backtest.batch_engine import FactorEngineBatchAdapter
from jerry_trader.services.backtest.config import BacktestConfig, PreFilterConfig
from jerry_trader.services.backtest.data_loader import DataLoader
from jerry_trader.services.backtest.event.engine import StreamingBacktestEngine
from jerry_trader.services.backtest.event.exit_strategy import (
    ExitStrategyConfig,
    apply_exit_strategy,
    get_default_exit_strategy,
    get_simple_exit_strategy,
)
from jerry_trader.services.backtest.event_evaluator import (
    EventEvaluator,
    TickerState,
    add_signal_density_to_factor_ts,
)
from jerry_trader.services.backtest.pre_filter import PreFilter
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.utils.paths import PROJECT_ROOT

if TYPE_CHECKING:
    from jerry_trader.services.backtest.data_loader import TickerData
    from jerry_trader.services.backtest.event.engine import StreamingResult

logger = setup_logger("backtest_pipeline", log_to_file=True)

# Default events to evaluate (stage-based: WATCH → ENTRY chain)
# volume_strong_10: rel_vol>5 + trade_rate>10 — best OOS precision (19.9%), best streaming EV
# Previous momentum_entry (trade_rate>10 + rel_vol>2.0): OOS prec=15.4%, worse streaming EV
DEFAULT_EVENTS = ["gap_up_watch", "volume_strong_10"]


def _expand_events_with_preconditions(
    event_names: list[str],
    evaluator: EventEvaluator,
) -> list[str]:
    """Expand event list to include pre-condition events.

    If user passes ['momentum_entry'], we auto-include ['gap_up_watch']
    since momentum_entry requires gap_up_watch as pre-condition.
    """
    result = list(event_names)
    seen = set(result)

    for event_name in event_names:
        event = next((e for e in evaluator.events if e.name == event_name), None)
        if event and event.pre_condition and event.pre_condition not in seen:
            result.insert(0, event.pre_condition)  # Insert before the dependent event
            seen.add(event.pre_condition)

    return result


@dataclass
class BacktestSignal:
    """Signal with computed returns."""

    experiment_id: str
    date: str
    ticker: str
    event_name: str
    event_stage: str
    entry_time: datetime
    entry_price: float
    exit_time: datetime
    exit_price: float
    return_pct: float
    exit_reason: str
    factors: dict
    max_price: float = 0.0
    min_price: float = 0.0
    time_to_max_ms: int = 0
    time_to_min_ms: int = 0


@dataclass
class BacktestResult:
    """Result from pipeline execution."""

    experiment_id: str
    date: str
    signals: list[BacktestSignal] = field(default_factory=list)
    total_signals: int = 0
    avg_return: float = 0.0
    win_rate: float = 0.0
    elapsed_seconds: float = 0.0
    validation: dict | None = None  # EventValidationResult per event_name

    def summary(self) -> dict:
        return {
            "experiment_id": self.experiment_id,
            "total_signals": self.total_signals,
            "avg_return": round(self.avg_return, 2),
            "win_rate": round(self.win_rate, 2),
            "status": "completed",
        }


class BacktestPipeline:
    """Unified pipeline for stage-based backtest evaluation.

    Supports two execution modes:
    - streaming (default): Simulates real-time triggering like live trading
    - batch: Computes all factors upfront, then checks conditions

    Streaming mode is recommended as it aligns with live behavior:
    - Processes trades chronologically
    - Updates indicators incrementally on each bar
    - Evaluates events immediately when factors are ready
    """

    def __init__(
        self,
        experiment_id: str | None = None,
        hold_duration_minutes: int = 10,
        slippage: float = 0.001,
        mode: Literal["streaming", "batch"] = "streaming",  # Default to streaming
        exit_strategy: ExitStrategyConfig | None = None,  # Default: tp15_sl20
        watch_timeout_ms: int = 30 * 60 * 1000,  # Default: 30 min
    ):
        self.experiment_id = experiment_id or self._generate_experiment_id()
        self.hold_duration_minutes = hold_duration_minutes
        self.slippage = slippage
        self.mode = mode
        self.watch_timeout_ms = watch_timeout_ms
        self.exit_strategy = exit_strategy or get_default_exit_strategy()
        self._ch = get_clickhouse_client()

    def _generate_experiment_id(self) -> str:
        """Generate unique experiment ID."""
        from datetime import datetime

        return f"bt_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def run(
        self,
        date: str,
        events: list[str] | None = None,
        tickers: list[str] | None = None,
        progress_callback: Callable | None = None,
    ) -> BacktestResult:
        """Execute full pipeline based on mode.

        Args:
            date: Date in YYYY-MM-DD format
            events: List of event names (default: gap_up_watch + momentum_entry)
            tickers: Optional filter to specific tickers
            progress_callback: Optional callback for progress updates

        Returns:
            BacktestResult with signals and summary stats
        """
        if self.mode == "streaming":
            return self._run_streaming(date, events, tickers, progress_callback)
        else:
            return self._run_batch(date, events, tickers, progress_callback)

    def _run_streaming(
        self,
        date: str,
        events: list[str] | None = None,
        tickers: list[str] | None = None,
        progress_callback: Callable | None = None,
    ) -> BacktestResult:
        """Execute streaming pipeline: PreFilter → DataLoader → StreamingEngine.

        Streaming mode simulates real-time triggering:
        - Processes trades chronologically
        - Updates indicators incrementally on each bar
        - Evaluates events immediately when factors are ready
        """
        start_time = time.time()

        if events is None:
            events = DEFAULT_EVENTS

        logger.info(f"Pipeline (streaming) start: {date}, events={events}")

        # Step 1: PreFilter
        pre_filter = PreFilter(ch_client=self._ch)
        candidates = pre_filter.find(date, PreFilterConfig())

        # Filter to specific tickers if requested
        if tickers:
            candidates = [c for c in candidates if c.symbol in tickers]

        if not candidates:
            logger.warning(f"No candidates found for {date}")
            return BacktestResult(
                experiment_id=self.experiment_id,
                date=date,
                total_signals=0,
            )

        logger.info(f"PreFilter: {len(candidates)} candidates")

        # Step 2: DataLoader
        loader = DataLoader(ch_client=self._ch)
        bt_config = BacktestConfig(date=date)
        ticker_data_map = loader.load(candidates, bt_config)
        logger.info(f"DataLoader: {len(ticker_data_map)} tickers loaded")

        # Step 3: Get session bounds
        ny_tz = pytz.timezone("America/New_York")
        dt = ny_tz.localize(datetime.strptime(date, "%Y-%m-%d"))
        session_start_ms = int(
            dt.replace(hour=4, minute=0, second=0).timestamp() * 1000
        )
        session_end_ms = int(dt.replace(hour=9, minute=30, second=0).timestamp() * 1000)

        # Step 4: Build first_entry_map
        first_entry_map: dict[str, int] = {}
        for symbol, ticker_data in ticker_data_map.items():
            if ticker_data.candidate:
                first_entry_map[symbol] = ticker_data.candidate.first_entry_ms

        # Step 5: Expand events with pre-conditions and run StreamingBacktestEngine
        streaming_engine = StreamingBacktestEngine(
            watch_timeout_ms=self.watch_timeout_ms,
        )
        expanded_events = _expand_events_with_preconditions(
            events, streaming_engine.evaluator
        )
        if expanded_events != events:
            logger.info(f"Expanded events: {events} -> {expanded_events}")
        streaming_results = streaming_engine.run_multiple_tickers(
            ticker_data_map=ticker_data_map,
            date=date,
            session_start_ms=session_start_ms,
            session_end_ms=session_end_ms,
            first_entry_map=first_entry_map,
            progress_callback=progress_callback,
            events=expanded_events,
        )

        # Step 6: Convert StreamingResult to BacktestSignal

        # Step 6: Convert StreamingResult to BacktestSignal
        all_signals: list[BacktestSignal] = []

        # Log exit strategy
        logger.info(
            f"Exit strategy: {self.exit_strategy.name} - {self.exit_strategy.description}"
        )

        for result in streaming_results:
            if not result.signals:
                continue

            # Build price trajectory for exit strategy
            ticker_data = ticker_data_map.get(result.symbol)
            if not ticker_data:
                continue

            # Get all trades sorted chronologically
            sorted_trades: list[tuple[int, float]] = sorted(
                [(int(t[0]), float(t[1])) for t in ticker_data.trades],
                key=lambda x: x[0],
            )

            # Convert each streaming signal
            for sig in result.signals:
                # Filter trades after entry time
                entry_trades = [
                    (ts, price)
                    for ts, price in sorted_trades
                    if ts >= sig.trigger_time_ms
                ]

                # Find entry price (last trade before or at entry time)
                entry_price = sig.trigger_price
                # Try to find better entry price from trades
                for ts, price in sorted_trades:
                    if ts <= sig.trigger_time_ms:
                        entry_price = price
                    else:
                        break

                # Apply exit strategy
                exit_result = apply_exit_strategy(
                    trades=entry_trades,
                    entry_time_ms=sig.trigger_time_ms,
                    entry_price=entry_price,
                    strategy=self.exit_strategy,
                )

                # Build exit reason string
                exit_reason = ", ".join(
                    f"{reason}:{size:.0%}"
                    for reason, size in exit_result.exit_reasons.items()
                )

                # Find actual exit time (last exit event)
                last_exit_time_ms = sig.trigger_time_ms
                if exit_result.exits:
                    last_exit_time_ms = exit_result.exits[-1].time_ms

                signal = BacktestSignal(
                    experiment_id=self.experiment_id,
                    date=date,
                    ticker=result.symbol,
                    event_name=sig.event_name,
                    event_stage=sig.event_stage,
                    entry_time=datetime.fromtimestamp(sig.trigger_time_ms / 1000),
                    entry_price=entry_price,
                    exit_time=datetime.fromtimestamp(last_exit_time_ms / 1000),
                    exit_price=(
                        exit_result.exits[-1].price
                        if exit_result.exits
                        else entry_price
                    ),
                    return_pct=exit_result.total_return_pct,
                    exit_reason=exit_reason,
                    factors=sig.factors,
                    max_price=exit_result.max_price,
                    min_price=exit_result.min_price,
                    time_to_max_ms=exit_result.time_to_max_ms,
                    time_to_min_ms=exit_result.time_to_min_ms,
                )
                all_signals.append(signal)

        # Compute summary stats
        total_signals = len(all_signals)
        avg_return = (
            sum(s.return_pct for s in all_signals) / total_signals
            if total_signals > 0
            else 0
        )
        win_count = sum(1 for s in all_signals if s.return_pct > 0)
        win_rate = win_count / total_signals if total_signals > 0 else 0

        # Step 7: Write signals to ClickHouse
        if self._ch and all_signals:
            self._write_signals_to_clickhouse(all_signals, date)

        elapsed_seconds = time.time() - start_time
        logger.info(
            f"Pipeline (streaming) complete: {total_signals} signals, "
            f"avg_return={avg_return:.2f}%, win_rate={win_rate:.2f}, "
            f"elapsed={elapsed_seconds:.2f}s"
        )

        return BacktestResult(
            experiment_id=self.experiment_id,
            date=date,
            signals=all_signals,
            total_signals=total_signals,
            avg_return=avg_return,
            win_rate=win_rate,
            elapsed_seconds=elapsed_seconds,
        )

    def _run_batch(
        self,
        date: str,
        events: list[str] | None = None,
        tickers: list[str] | None = None,
        progress_callback: Callable | None = None,
    ) -> BacktestResult:
        """Execute batch pipeline: PreFilter → DataLoader → FactorEngine → Evaluator.

        Batch mode computes all factors upfront, then checks conditions at each timestamp.
        """
        start_time = time.time()

        if events is None:
            events = DEFAULT_EVENTS

        logger.info(f"Pipeline (batch) start: {date}, events={events}")

        # Step 1: PreFilter
        pre_filter = PreFilter(ch_client=self._ch)
        candidates = pre_filter.find(date, PreFilterConfig())

        # Filter to specific tickers if requested
        if tickers:
            candidates = [c for c in candidates if c.symbol in tickers]

        if not candidates:
            logger.warning(f"No candidates found for {date}")
            return BacktestResult(
                experiment_id=self.experiment_id,
                date=date,
                total_signals=0,
            )

        logger.info(f"PreFilter: {len(candidates)} candidates")

        # Step 2: DataLoader
        loader = DataLoader(ch_client=self._ch)
        bt_config = BacktestConfig(date=date)
        ticker_data_map = loader.load(candidates, bt_config)
        logger.info(f"DataLoader: {len(ticker_data_map)} tickers loaded")

        # Step 3: FactorEngine
        engine = FactorEngineBatchAdapter()

        # Step 4: Get session bounds
        ny_tz = pytz.timezone("America/New_York")
        dt = ny_tz.localize(datetime.strptime(date, "%Y-%m-%d"))
        session_start_ms = int(
            dt.replace(hour=4, minute=0, second=0).timestamp() * 1000
        )
        session_end_ms = int(dt.replace(hour=9, minute=30, second=0).timestamp() * 1000)

        # Step 5: Load events.yaml
        config_path = Path.cwd() / "config" / "events.yaml"
        if not config_path.exists():
            config_path = PROJECT_ROOT / "config" / "events.yaml"

        evaluator = EventEvaluator(config_path=str(config_path))
        logger.info(f"Loaded {len(evaluator.events)} events from {config_path}")

        # Expand events with pre-conditions
        events = _expand_events_with_preconditions(events, evaluator)
        logger.info(f"Evaluating events: {events}")

        # Step 6: Evaluate each ticker (stage-based)
        all_signals: list[BacktestSignal] = []
        hold_ms = self.hold_duration_minutes * 60 * 1000

        for symbol, ticker_data in ticker_data_map.items():
            ticker_start = time.time()

            # Compute factors
            factor_ts = engine.compute(ticker_data)

            # Add signal_density
            factor_ts = add_signal_density_to_factor_ts(factor_ts)

            # Build price index from trades
            price_index: dict[int, float] = {}
            for trade in ticker_data.trades:
                ts_ms, price, _ = trade
                price_index[ts_ms] = price

            # Initialize ticker state for stage tracking
            ticker_state = TickerState(symbol=symbol)

            # Get first_entry_ms
            first_entry_ms = (
                ticker_data.candidate.first_entry_ms if ticker_data.candidate else None
            )

            # Evaluate each event (stage-based: WATCH → ENTRY)
            raw_signals: list[dict] = []

            for event_name in events:
                event = next(
                    (e for e in evaluator.events if e.name == event_name), None
                )
                if not event:
                    logger.warning(f"Event {event_name} not found, skipping")
                    continue

                # Evaluate with state tracking (max 1 signal per event per ticker)
                signals, ticker_state = evaluator.evaluate_ticker(
                    event=event,
                    symbol=symbol,
                    factor_ts=factor_ts,
                    session_start_ms=session_start_ms,
                    session_end_ms=session_end_ms,
                    trigger_prices=price_index,
                    first_entry_ms=first_entry_ms,
                    ticker_state=ticker_state,
                )
                raw_signals.extend(signals)

            # Filter to ENTRY signals only (exclude WATCH stage)
            entry_signals = [
                sig for sig in raw_signals if sig.get("event_stage") == "entry"
            ]

            if not entry_signals:
                continue

            # Step 7: Compute returns via Python exit strategy
            # Use apply_exit_strategy (same as streaming mode) instead of
            # broken Rust compute_signal_exits_batch.
            sorted_trades: list[tuple[int, float]] = sorted(price_index.items())

            for sig in entry_signals:
                entry_time = sig["trigger_time_ms"]
                entry_price = sig.get("trigger_price", 0.0)

                # Find exact entry price from trades at or before trigger time
                for ts, price in sorted_trades:
                    if ts <= entry_time:
                        entry_price = price
                    else:
                        break

                # Filter trades after entry time
                entry_trades = [
                    (ts, price) for ts, price in sorted_trades if ts >= entry_time
                ]

                # Apply Python exit strategy
                exit_result = apply_exit_strategy(
                    trades=entry_trades,
                    entry_time_ms=entry_time,
                    entry_price=entry_price,
                    strategy=self.exit_strategy,
                )

                # Build exit reason string
                exit_reason = ", ".join(
                    f"{reason}:{size:.0%}"
                    for reason, size in exit_result.exit_reasons.items()
                )

                # Find actual exit time (last exit event)
                last_exit_time_ms = entry_time
                last_exit_price = entry_price
                if exit_result.exits:
                    last_exit_time_ms = exit_result.exits[-1].time_ms
                    last_exit_price = exit_result.exits[-1].price

                signal = BacktestSignal(
                    experiment_id=self.experiment_id,
                    date=date,
                    ticker=symbol,
                    event_name=sig.get("event_name", "unknown"),
                    event_stage=sig.get("event_stage", "entry"),
                    entry_time=datetime.fromtimestamp(entry_time / 1000),
                    entry_price=entry_price,
                    exit_time=datetime.fromtimestamp(last_exit_time_ms / 1000),
                    exit_price=last_exit_price,
                    return_pct=exit_result.total_return_pct,
                    exit_reason=exit_reason or "market_close",
                    factors={
                        k: v
                        for k, v in sig.items()
                        if k
                        not in [
                            "trigger_time_ms",
                            "trigger_price",
                            "trigger_time_ns",
                            "event_name",
                            "event_stage",
                        ]
                    },
                    max_price=exit_result.max_price,
                    min_price=exit_result.min_price,
                    time_to_max_ms=exit_result.time_to_max_ms,
                    time_to_min_ms=exit_result.time_to_min_ms,
                )
                all_signals.append(signal)

            ticker_elapsed = time.time() - ticker_start
            logger.debug(
                f"Ticker {symbol}: elapsed={ticker_elapsed:.2f}s, "
                f"signals={len(entry_signals)}"
            )

        # Compute summary stats
        total_signals = len(all_signals)
        avg_return = (
            sum(s.return_pct for s in all_signals) / total_signals
            if total_signals > 0
            else 0
        )
        win_count = sum(1 for s in all_signals if s.return_pct > 0)
        win_rate = win_count / total_signals if total_signals > 0 else 0

        # Step 8: Write signals to ClickHouse
        if self._ch and all_signals:
            self._write_signals_to_clickhouse(all_signals, date)

        elapsed_seconds = time.time() - start_time
        logger.info(
            f"Pipeline complete: {total_signals} signals, "
            f"avg_return={avg_return:.2f}%, win_rate={win_rate:.2f}, "
            f"elapsed={elapsed_seconds:.2f}s"
        )

        return BacktestResult(
            experiment_id=self.experiment_id,
            date=date,
            signals=all_signals,
            total_signals=total_signals,
            avg_return=avg_return,
            win_rate=win_rate,
            elapsed_seconds=elapsed_seconds,
        )

    def _write_signals_to_clickhouse(
        self, signals: list[BacktestSignal], date: str
    ) -> None:
        """Write signals to ClickHouse backtest_signals table."""
        try:
            from datetime import date as date_type

            rows = []
            for signal in signals:
                signal_date = signal.date
                if isinstance(signal_date, str):
                    signal_date = datetime.strptime(signal_date, "%Y-%m-%d").date()

                rows.append(
                    [
                        signal.experiment_id,
                        signal_date,
                        signal.ticker,
                        signal.event_name,
                        signal.entry_time,
                        signal.entry_price,
                        signal.exit_time,
                        signal.exit_price,
                        signal.return_pct,
                        signal.exit_reason,
                        json.dumps(signal.factors),
                        signal.max_price,
                        signal.min_price,
                        signal.time_to_max_ms,
                        signal.time_to_min_ms,
                        datetime.now(),
                    ]
                )

            self._ch.insert(
                "backtest_signals",
                rows,
                column_names=[
                    "experiment_id",
                    "date",
                    "ticker",
                    "event_name",
                    "entry_time",
                    "entry_price",
                    "exit_time",
                    "exit_price",
                    "return_pct",
                    "exit_reason",
                    "factors",
                    "max_price",
                    "min_price",
                    "time_to_max_ms",
                    "time_to_min_ms",
                    "created_at",
                ],
            )
            logger.info(f"Wrote {len(signals)} signals to ClickHouse")
        except Exception as e:
            logger.error(f"Failed to write signals to ClickHouse: {e}")
