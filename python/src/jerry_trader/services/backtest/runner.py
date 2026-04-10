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

from jerry_trader.domain.backtest.types import BacktestResult, SignalResult
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
        logger.info(f"BacktestRunner: starting for {config.date}")

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

        # Step 2: Load data
        ticker_data_map = self._step_load(candidates, config)

        # Step 3: Load rules
        evaluator = self._step_load_rules(config)

        # Step 4: Compute factors + evaluate signals + compute metrics
        all_signals = self._step_compute(ticker_data_map, evaluator, config)

        # Step 5: Aggregate
        rules_tested = [r.id for r in evaluator.rules]
        result = _aggregate_result(config.date, rules_tested, all_signals)

        # Step 6: Output
        if config.output_console:
            print_summary(result)

        if config.output_clickhouse and self._ch:
            run_id = f"{config.date}_{uuid.uuid4().hex[:8]}"
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
        """Step 1: Pre-filter candidates from market_snapshot."""
        if not self._ch:
            logger.warning("No ClickHouse client — cannot pre-filter")
            return []

        logger.info("Step 1: Pre-filtering candidates...")
        prefilter = PreFilter(
            ch_client=self._ch,
            database=(
                config.clickhouse_config.get("database", "jerry_trader")
                if config.clickhouse_config
                else "jerry_trader"
            ),
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
        """Step 3: Load DSL rules."""
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
        batch_engine = FactorEngineBatchAdapter()
        all_signals: list[SignalResult] = []

        for i, (symbol, td) in enumerate(ticker_data_map.items()):
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
            eval_result = evaluator.evaluate(symbol, factor_ts)
            if not eval_result.triggers:
                logger.debug(f"  {symbol}: no triggers")
                continue

            # 4c. Compute metrics
            signals = compute_batch_metrics(
                eval_result.triggers,
                td,
                factor_ts,
                config.horizons_ms,
                config.slippage_buffer,
                config.default_slippage,
            )
            all_signals.extend(signals)

            if signals:
                logger.info(
                    f"  [{i+1}/{len(ticker_data_map)}] {symbol}: "
                    f"{len(eval_result.triggers)} triggers → {len(signals)} signals"
                )

        logger.info(f"  → {len(all_signals)} total signals across all tickers")
        return all_signals
