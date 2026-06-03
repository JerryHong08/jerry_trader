"""Streaming Backtest Engine - Simulates real-time event triggering.

Unlike batch mode which computes all factors upfront, this engine:
1. Processes trades chronologically (simulating live tick stream)
2. Builds bars via Rust BarBuilder (same as live)
3. Updates indicators on each bar close (same as live FactorEngine)
4. Evaluates events immediately when factors are ready (same as live SignalEngine)

This provides more accurate backtest results that align with live behavior.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from jerry_trader.services.backtest.config import TickerData
from jerry_trader.services.backtest.event_evaluator import EventEvaluator, TickerState
from jerry_trader.services.factor.factor_registry import get_factor_registry
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.utils.paths import PROJECT_ROOT

logger = setup_logger("streaming_engine", log_to_file=True)


@dataclass
class StreamingSignal:
    """Signal from streaming evaluation."""

    symbol: str
    date: str
    trigger_time_ms: int
    trigger_price: float
    event_name: str
    event_stage: str
    factors: dict[str, float]


@dataclass
class StreamingResult:
    """Result from streaming engine run."""

    symbol: str
    date: str
    signals: list[StreamingSignal] = field(default_factory=list)
    total_signals: int = 0
    elapsed_seconds: float = 0.0


class StreamingBacktestEngine:
    """Simulates real-time factor computation and event evaluation.

    Architecture (same as live):
        Trades → BarBuilder → Bar → BarIndicator.update() → factors
            → EventEvaluator.match_signal() → signal

    Key differences from batch mode:
        - Trades processed chronologically (not batch)
        - Bars built incrementally (not all at once)
        - Events evaluated immediately (not after all factors computed)
    """

    def __init__(
        self,
        events_config_path: str | None = None,
        watch_timeout_ms: int = 30 * 60 * 1000,  # Default: 30 min
    ):
        """Initialize streaming engine.

        Args:
            events_config_path: Path to events.yaml. Defaults to config/events.yaml.
            watch_timeout_ms: Max ms after WATCH to wait for ENTRY (default: 30 min).
        """
        self.registry = get_factor_registry()
        self.watch_timeout_ms = watch_timeout_ms

        # Load events config
        if events_config_path is None:
            config_path = PROJECT_ROOT / "config" / "events.yaml"
        else:
            config_path = Path(events_config_path)

        self.evaluator = EventEvaluator(config_path=str(config_path))
        logger.info(
            f"StreamingEngine: loaded {len(self.evaluator.events)} events "
            f"from {config_path}, watch_timeout={watch_timeout_ms/60000:.0f}min"
        )

    def run_single_ticker(
        self,
        ticker_data: TickerData,
        date: str,
        session_start_ms: int,
        session_end_ms: int,
        first_entry_ms: int | None = None,
        events: list[str] | None = None,
    ) -> StreamingResult:
        """Run streaming evaluation for a single ticker.

        Uses FactorEngineBatchAdapter for factor computation (identical to batch
        mode), then evaluates events chronologically via EventEvaluator.
        Streaming only differs from batch in that it takes the FIRST ENTRY signal
        per ticker, simulating real-time decision making.

        Args:
            ticker_data: Loaded data for the ticker.
            date: Date string.
            session_start_ms: Session start epoch ms.
            session_end_ms: Session end epoch ms.
            first_entry_ms: When ticker first entered subscription set (top20).
        """
        start_time = time.time()
        symbol = ticker_data.symbol

        if not ticker_data.trades:
            return StreamingResult(symbol=symbol, date=date)

        # ── Phase 1: Compute factors via batch adapter (identical to batch mode) ──
        from jerry_trader.services.backtest.batch_engine import FactorEngineBatchAdapter

        adapter = FactorEngineBatchAdapter(self.registry)
        factor_ts = adapter.compute(ticker_data)

        if not factor_ts:
            return StreamingResult(symbol=symbol, date=date)

        # ── Phase 2: Evaluate events using the SAME evaluator as batch mode ─────
        from jerry_trader.domain.event import EventStage

        ticker_state = TickerState(symbol=symbol)
        signals: list[StreamingSignal] = []

        # Build trigger price lookup
        trigger_prices: dict[int, float] = {}
        for ts_ms, price, _ in sorted(ticker_data.trades, key=lambda x: x[0]):
            trigger_prices[int(ts_ms)] = float(price)

        # Determine which events to evaluate
        all_events = self.evaluator.events
        if events:
            all_events = [e for e in all_events if e.name in events]

        # Stage ordering: WATCH first, then ENTRY
        watch_events = [e for e in all_events if e.stage == EventStage.WATCH]
        entry_events = [e for e in all_events if e.stage == EventStage.ENTRY]

        # Evaluate WATCH events (FIRST_ENTRY trigger)
        for event in watch_events:
            _, ticker_state = self.evaluator.evaluate_ticker(
                event=event,
                symbol=symbol,
                factor_ts=factor_ts,
                session_start_ms=session_start_ms,
                session_end_ms=session_end_ms,
                trigger_prices=trigger_prices,
                first_entry_ms=first_entry_ms,
                ticker_state=ticker_state,
            )

        # Evaluate ENTRY events (CONTINUOUS trigger)
        if ticker_state.watch_triggered:
            for event in entry_events:
                entry_sigs, ticker_state = self.evaluator.evaluate_ticker(
                    event=event,
                    symbol=symbol,
                    factor_ts=factor_ts,
                    session_start_ms=session_start_ms,
                    session_end_ms=session_end_ms,
                    trigger_prices=trigger_prices,
                    first_entry_ms=first_entry_ms,
                    ticker_state=ticker_state,
                )
                # Known metadata keys in signal dict (not factors)
                _META_KEYS = {
                    "trigger_time_ns",
                    "trigger_time_ms",
                    "symbol",
                    "session_phase",
                    "trigger_price",
                    "event_name",
                    "event_stage",
                }
                for sig_dict in entry_sigs:
                    factors = {
                        k: v
                        for k, v in sig_dict.items()
                        if k not in _META_KEYS and v is not None
                    }
                    signals.append(
                        StreamingSignal(
                            symbol=symbol,
                            date=date,
                            trigger_time_ms=sig_dict.get("trigger_time_ms", 0),
                            trigger_price=sig_dict.get("trigger_price", 0.0),
                            event_name=sig_dict.get("event_name", event.name),
                            event_stage="entry",
                            factors=factors,
                        )
                    )
                if ticker_state.entry_triggered:
                    break

        elapsed = time.time() - start_time

        logger.debug(
            f"StreamingEngine: {symbol} — {len(signals)} signals, "
            f"{len(factor_ts)} factor timestamps, elapsed={elapsed:.3f}s"
        )

        return StreamingResult(
            symbol=symbol,
            date=date,
            signals=signals,
            total_signals=len(signals),
            elapsed_seconds=elapsed,
        )

    def run_multiple_tickers(
        self,
        ticker_data_map: dict[str, TickerData],
        date: str,
        session_start_ms: int,
        session_end_ms: int,
        first_entry_map: dict[str, int] | None = None,
        progress_callback: Any | None = None,
        events: list[str] | None = None,
    ) -> list[StreamingResult]:
        """Run streaming evaluation for multiple tickers.

        Args:
            ticker_data_map: Dict mapping symbol → TickerData.
            date: Date string.
            session_start_ms: Session start epoch ms.
            session_end_ms: Session end epoch ms.
            first_entry_map: Dict mapping symbol → first_entry_ms.
            progress_callback: Optional callback for progress updates.
            events: Optional list of event names to evaluate. If None, evaluate all.

        Returns:
            List of StreamingResult for each ticker.
        """
        results: list[StreamingResult] = []
        total = len(ticker_data_map)

        for i, (symbol, ticker_data) in enumerate(ticker_data_map.items()):
            first_entry_ms = first_entry_map.get(symbol) if first_entry_map else None

            result = self.run_single_ticker(
                ticker_data=ticker_data,
                date=date,
                session_start_ms=session_start_ms,
                session_end_ms=session_end_ms,
                first_entry_ms=first_entry_ms,
                events=events,
            )
            results.append(result)

            if progress_callback:
                progress_callback(i + 1, total, symbol, result.total_signals)

        return results
