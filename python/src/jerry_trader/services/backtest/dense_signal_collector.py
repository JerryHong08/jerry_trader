"""Dense Signal Collector — samples all mid-phase timestamps for ML training.

Unlike the event-driven BacktestPipeline (1 entry per ticker per session),
this module performs dense, uniform sampling every 2 minutes during MID session.
Each sample includes factors and 30-minute forward returns.

Used by:
- ml_model.py: ReturnPredictor.train()
- training_data/collector.py: Training data collection
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytz

from jerry_trader._rust import compute_signal_exits_batch
from jerry_trader.domain.session import SessionPhase, get_session_phase_from_epoch_ms
from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.services.backtest.batch_engine import FactorEngineBatchAdapter
from jerry_trader.services.backtest.config import BacktestConfig, PreFilterConfig
from jerry_trader.services.backtest.data_loader import DataLoader
from jerry_trader.services.backtest.pre_filter import PreFilter
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("dense_signal_collector", log_to_file=True)

FACTOR_NAMES = [
    "relative_volume",
    "price_direction",
    "trade_rate",
    "bid_ask_spread",
    "entry_gap_pct",
    "gap_percent",
    "order_imbalance",
    "quote_rate",
    "vol_accel_5_15",
    "ema_20",
]


def collect_signals(date: str) -> pd.DataFrame:
    """Collect all mid-phase signals with factors and returns.

    Samples every 2 minutes during mid phase, computes 30-min forward returns
    using the Rust batch exit computation.

    Args:
        date: Date string in YYYY-MM-DD format.

    Returns:
        DataFrame with columns: ticker, entry_time_ms, return_pct,
        max_return_pct, entry_price, exit_price, max_price, min_price,
        entry_gap_pct, session_phase, plus all FACTOR_NAMES columns.
    """
    ch = get_clickhouse_client()

    # Pre-filter candidates
    pre_filter = PreFilter(ch_client=ch)
    candidates = pre_filter.find(date, PreFilterConfig())

    if not candidates:
        return pd.DataFrame()

    # Load data
    config = BacktestConfig(date=date)
    data_loader = DataLoader(ch_client=ch)
    ticker_data_map = data_loader.load(candidates, config)

    # Factor engine
    engine = FactorEngineBatchAdapter()

    # Session window
    ny_tz = pytz.timezone("America/New_York")
    session_start_ms = int(
        ny_tz.localize(datetime.strptime(f"{date} 04:00", "%Y-%m-%d %H:%M")).timestamp()
        * 1000
    )
    session_end_ms = int(
        ny_tz.localize(datetime.strptime(f"{date} 09:30", "%Y-%m-%d %H:%M")).timestamp()
        * 1000
    )

    hold_ms = 30 * 60 * 1000  # 30 minutes

    all_signals = []

    for symbol, ticker_data in ticker_data_map.items():
        factor_ts = engine.compute(ticker_data)
        price_index = {ts: price for ts, price, _ in ticker_data.trades}

        candidate = next((c for c in candidates if c.symbol == symbol), None)
        entry_gap_pct = candidate.gain_at_entry if candidate else 0

        # Step 1: Collect valid timestamps (pre-filter)
        valid_timestamps = []
        valid_factors = []

        for ts_ms, factors in factor_ts.items():
            if not session_start_ms <= ts_ms < session_end_ms:
                continue

            phase = get_session_phase_from_epoch_ms(ts_ms)
            if phase != SessionPhase.MID:
                continue

            # Sample every 2 minutes
            if ts_ms % (2 * 60 * 1000) > 60 * 1000:
                continue

            valid_timestamps.append(ts_ms)
            valid_factors.append(factors)

        if not valid_timestamps:
            continue

        # Step 2: Batch compute returns (single Rust call)
        sorted_trades = sorted(price_index.items())
        timestamps = [(ts_ms, hold_ms) for ts_ms in valid_timestamps]

        try:
            exit_results = compute_signal_exits_batch(sorted_trades, timestamps)
        except Exception as e:
            logger.warning(f"{symbol}: batch compute failed: {e}")
            continue

        # Step 3: Process results in batch
        for i, (ts_ms, factors) in enumerate(zip(valid_timestamps, valid_factors)):
            if i < len(exit_results) and exit_results[i]:
                entry_price, exit_price, max_p, min_p, _, _, return_pct = exit_results[
                    i
                ]

                max_return_pct = (
                    (max_p - entry_price) / entry_price * 100.0
                    if entry_price > 0
                    else 0.0
                )

                signal = {
                    "ticker": symbol,
                    "entry_time_ms": ts_ms,
                    "return_pct": return_pct,
                    "max_return_pct": max_return_pct,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "max_price": max_p,
                    "min_price": min_p,
                    "entry_gap_pct": entry_gap_pct,
                    "session_phase": SessionPhase.MID.value,
                }

                for factor in FACTOR_NAMES:
                    signal[factor] = factors.get(factor, 0)

                all_signals.append(signal)

    return pd.DataFrame(all_signals)
