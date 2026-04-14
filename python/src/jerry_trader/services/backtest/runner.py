"""Backtest runner — orchestrates the full pipeline.

Pipeline:
  1. PreFilter → candidates
  2. DataLoader → TickerData per candidate
  3. FactorEngineBatchAdapter → FactorTimeseries per ticker
  4. SignalEvaluator → TriggerPoints per ticker
  5. Metrics → SignalResults per ticker
  6. Aggregate → BacktestResult
  7. Output → console + ClickHouse
"""

from __future__ import annotations

import uuid
from typing import Any

from jerry_trader.domain.backtest.types import BacktestResult, Candidate, SignalResult
from jerry_trader.services.backtest.batch_engine import FactorEngineBatchAdapter
from jerry_trader.services.backtest.config import BacktestConfig
from jerry_trader.services.backtest.data_loader import DataLoader
from jerry_trader.services.backtest.evaluator import SignalEvaluator
from jerry_trader.services.backtest.metrics import compute_batch_metrics
from jerry_trader.services.backtest.output import persist_results, print_summary
from jerry_trader.services.backtest.pre_filter import PreFilter
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.runner", log_to_file=True)


def _aggregate_result(
    date: str,
    rules_tested: list[str],
    signals: list[SignalResult],
    run_id: str | None = None,
) -> BacktestResult:
    """Aggregate individual signal results into a BacktestResult.

    Computes win rate, avg return, profit factor, and other aggregate metrics.
    """
    if not signals:
        return BacktestResult(
            date=date,
            rules_tested=rules_tested,
            total_signals=0,
            signals=[],
            run_id=run_id,
        )

    # Collect all return horizons present
    all_horizons: set[str] = set()
    for sig in signals:
        all_horizons.update(sig.returns.keys())

    # Per-horizon stats
    win_rate: dict[str, float] = {}
    avg_return: dict[str, float] = {}

    for horizon in sorted(all_horizons):
        values = [sig.returns[horizon] for sig in signals if horizon in sig.returns]
        if not values:
            continue
        wins = [v for v in values if v > 0]
        win_rate[horizon] = len(wins) / len(values) if values else 0.0
        avg_return[horizon] = sum(values) / len(values)

    # Profit factor
    all_returns = []
    for sig in signals:
        # Use the first available horizon's return for profit factor
        for h in sorted(all_horizons):
            if h in sig.returns:
                all_returns.append(sig.returns[h])
                break

    gross_wins = sum(r for r in all_returns if r > 0)
    gross_losses = abs(sum(r for r in all_returns if r < 0))
    profit_factor = gross_wins / gross_losses if gross_losses > 0 else 0.0

    # Averages
    slippages = [sig.slippage_pct for sig in signals]
    mfes = [sig.mfe for sig in signals if sig.mfe is not None]
    maes = [sig.mae for sig in signals if sig.mae is not None]
    peaks = [sig.time_to_peak_ms for sig in signals if sig.time_to_peak_ms is not None]

    return BacktestResult(
        date=date,
        rules_tested=rules_tested,
        total_signals=len(signals),
        signals=signals,
        run_id=run_id,
        win_rate=win_rate,
        avg_return=avg_return,
        profit_factor=profit_factor,
        avg_slippage=sum(slippages) / len(slippages) if slippages else 0.0,
        avg_mfe=sum(mfes) / len(mfes) if mfes else 0.0,
        avg_mae=sum(maes) / len(maes) if maes else 0.0,
        avg_time_to_peak_ms=sum(peaks) / len(peaks) if peaks else 0.0,
    )


class BacktestRunner:
    """Orchestrates a complete backtest run.

    Usage:
        config = BacktestConfig(date="2026-03-13", rules_dir="config/rules/")
        runner = BacktestRunner(config)
        result = runner.run()
    """

    def __init__(
        self,
        config: BacktestConfig,
        ch_client: Any | None = None,
    ):
        self._config = config
        self._ch = ch_client

    def run(self) -> BacktestResult:
        """Execute the full backtest pipeline.

        Returns:
            BacktestResult with all signals and aggregate metrics.
        """
        config = self._config
        start_ms, end_ms = config.session_range_ms()
        logger.info(
            f"BacktestRunner: starting for {config.date} "
            f"session {config.session_start}-{config.session_end} ET"
        )

        # Step 1: Pre-filter candidates
        candidates = self._step_prefilter(config)
        if not candidates:
            logger.warning("No candidates found, aborting")
            return BacktestResult(
                date=config.date,
                rules_tested=[],
                total_signals=0,
                signals=[],
            )

        # Dry-run: stop after pre-filter
        if config.candidates_only:
            self._print_candidates(candidates, config)
            return BacktestResult(
                date=config.date,
                rules_tested=[],
                total_signals=0,
                signals=[],
            )

        # Step 2: Load data
        ticker_data_map = self._step_load(candidates, config)

        # Step 3: Load rules
        evaluator = self._step_load_rules(config)

        # Step 4: Compute factors + evaluate signals + compute metrics
        all_signals = self._step_compute(ticker_data_map, evaluator, config)

        # Step 5: Aggregate
        rules_tested = [r.id for r in evaluator.rules]

        # Generate run_id before aggregating (links experiment to ClickHouse)
        run_id = f"{config.date}_{uuid.uuid4().hex[:8]}"
        result = _aggregate_result(
            config.date, rules_tested, all_signals, run_id=run_id
        )

        # Step 6: Output
        if config.output_console:
            print_summary(result, detailed=config.detailed)

        if config.output_clickhouse and self._ch:
            persist_results(result, self._ch, run_id)

        logger.info(
            f"BacktestRunner: done — {result.total_signals} signals, "
            f"{len(candidates)} candidates"
        )

        return result

    # ─────────────────────────────────────────────────────────────────────────
    # Pipeline Steps
    # ─────────────────────────────────────────────────────────────────────────

    def _step_prefilter(self, config: BacktestConfig):
        """Step 1: Pre-filter candidates from market_snapshot_collector.

        If collector has no data for this date, auto-runs build()
        to populate collector + process to market_snapshot.
        """
        if not self._ch:
            logger.warning("No ClickHouse client — cannot pre-filter")
            return []

        database = (
            config.clickhouse_config.get("database", "jerry_trader")
            if config.clickhouse_config
            else "jerry_trader"
        )

        logger.info("Step 1: Pre-filtering candidates...")
        prefilter = PreFilter(ch_client=self._ch, database=database)

        try:
            candidates = prefilter.find(config.date, config.pre_filter)
        except RuntimeError:
            # Collector empty — auto-build collector + process to market_snapshot
            logger.info("  collector empty, auto-building snapshot...")
            from jerry_trader.platform.config.session import make_session_id
            from jerry_trader.services.backtest.data.snapshot_builder import build

            date_compact = config.date.replace("-", "")
            session_id = make_session_id(replay_date=date_compact)
            build(
                date=config.date,
                ch_client=self._ch,
                database=database,
                mode="replay",
                session_id=session_id,
            )
            candidates = prefilter.find(config.date, config.pre_filter)

        logger.info(f"  → {len(candidates)} candidates")
        return candidates

    def _step_load(self, candidates, config: BacktestConfig):
        """Step 2: Load trade/quote data for candidates."""
        logger.info(f"Step 2: Loading data for {len(candidates)} tickers...")
        database = (
            config.clickhouse_config.get("database", "jerry_trader")
            if config.clickhouse_config
            else "jerry_trader"
        )
        loader = DataLoader(ch_client=self._ch, database=database)
        ticker_data_map = loader.load(candidates, config)

        # Filter out tickers with no trades
        with_data = {s: td for s, td in ticker_data_map.items() if td.trades}
        logger.info(
            f"  → {len(with_data)}/{len(ticker_data_map)} tickers have trade data"
        )
        return with_data

    def _step_load_rules(self, config: BacktestConfig) -> SignalEvaluator:
        """Step 3: Load DSL rules.

        If config.rules is provided, use them directly (no file I/O).
        Otherwise, load from config.rules_dir.
        """
        if config.rules:
            logger.info(
                f"Step 3: Using {len(config.rules)} rules from config (no file I/O)"
            )
            evaluator = SignalEvaluator(rules=config.rules)
        else:
            logger.info(f"Step 3: Loading rules from {config.rules_dir}...")
            evaluator = SignalEvaluator(rules_dir=config.rules_dir)
            evaluator.load_rules()
        return evaluator

    def _step_compute(
        self,
        ticker_data_map: dict,
        evaluator: SignalEvaluator,
        config: BacktestConfig,
    ) -> list[SignalResult]:
        """Step 4: Compute factors → evaluate signals → compute metrics."""
        logger.info(
            f"Step 4: Computing factors + evaluating signals "
            f"for {len(ticker_data_map)} tickers..."
        )
        session_start_ms, session_end_ms = config.session_range_ms()
        batch_engine = FactorEngineBatchAdapter()
        all_signals: list[SignalResult] = []

        # Progress bar
        try:
            from tqdm import tqdm

            items = tqdm(
                ticker_data_map.items(), desc="Computing factors", unit="ticker"
            )
        except ImportError:
            items = ticker_data_map.items()

        for symbol, td in items:
            # 4a. Compute factors
            try:
                factor_ts = batch_engine.compute(td)
            except Exception as e:
                logger.warning(f"Factor computation failed for {symbol}: {e}")
                continue

            if not factor_ts:
                logger.debug(f"  {symbol}: no factors computed, skipping")
                continue

            # 4b. Evaluate signals
            eval_result = evaluator.evaluate(
                symbol, factor_ts, cooldown_ms=config.cooldown_ms
            )
            if not eval_result.triggers:
                logger.debug(f"  {symbol}: no triggers")
                continue

            # 4b-filter: Only keep triggers within session window
            session_triggers = [
                t
                for t in eval_result.triggers
                if session_start_ms <= t.trigger_time_ms < session_end_ms
            ]
            if not session_triggers:
                logger.debug(
                    f"  {symbol}: {len(eval_result.triggers)} triggers, all outside session"
                )
                continue

            # 4c. Compute metrics
            signals = compute_batch_metrics(
                session_triggers,
                td,
                factor_ts,
                config.horizons_ms,
                config.slippage_buffer,
                config.default_slippage,
            )
            all_signals.extend(signals)

        logger.info(f"  → {len(all_signals)} total signals across all tickers")
        return all_signals

    @staticmethod
    def _print_candidates(
        candidates: list[Candidate],
        config: BacktestConfig,
    ) -> None:
        """Print pre-filter candidates for dry-run mode."""
        from datetime import datetime
        from zoneinfo import ZoneInfo

        ET = ZoneInfo("America/New_York")

        def fmt_vol(v: float) -> str:
            if v >= 1_000_000:
                return f"{v/1_000_000:.1f}M"
            if v >= 1_000:
                return f"{v/1_000:.0f}K"
            return f"{v:.0f}"

        print(f"\n{'='*82}")
        print(f"  PRE-FILTER CANDIDATES — {config.date}")
        print(f"{'='*82}")
        print(
            f"  {len(candidates)} tickers (new_entry_only={config.pre_filter.new_entry_only}, "
            f"min_gain={config.pre_filter.min_gain_pct}%)"
        )
        print()
        print(
            f"  {'Ticker':8s} {'Entry':>10s} {'Entry%':>7s} {'Max%':>7s} "
            f"{'Price':>7s} {'Vol':>8s} {'PeakVol':>8s}"
        )
        print(f"  {'-'*8} {'-'*10} {'-'*7} {'-'*7} {'-'*7} {'-'*8} {'-'*8}")

        for c in candidates[:50]:
            entry_time = datetime.fromtimestamp(
                c.first_entry_ms / 1000, tz=ET
            ).strftime("%H:%M:%S")
            print(
                f"  {c.symbol:8s} {entry_time:>10s} {c.gain_at_entry:>6.1f}% {c.max_gain:>6.1f}% "
                f"{c.price_at_entry:>7.2f} {fmt_vol(c.volume_at_entry):>8s} {fmt_vol(c.peak_volume):>8s}"
            )

        if len(candidates) > 50:
            print(f"  ... and {len(candidates) - 50} more")

        if candidates:
            avg_gain = sum(c.gain_at_entry for c in candidates) / len(candidates)
            avg_max = sum(c.max_gain for c in candidates) / len(candidates)
            avg_relvol = sum(c.relative_volume for c in candidates) / len(candidates)
            avg_vol = sum(c.volume_at_entry for c in candidates) / len(candidates)
            min_time = min(c.first_entry_ms for c in candidates)
            max_time = max(c.first_entry_ms for c in candidates)
            min_str = datetime.fromtimestamp(min_time / 1000, tz=ET).strftime(
                "%H:%M:%S"
            )
            max_str = datetime.fromtimestamp(max_time / 1000, tz=ET).strftime(
                "%H:%M:%S"
            )
            print()
            print(f"  Summary:")
            print(f"    Avg entry gain:  {avg_gain:.1f}%")
            print(f"    Avg max gain:    {avg_max:.1f}%")
            print(
                f"    Avg peak vol:    {fmt_vol(sum(c.peak_volume for c in candidates) / len(candidates))}"
            )
            print(f"    Entry range:     {min_str} - {max_str} ET")
        print(f"{'='*82}\n")
