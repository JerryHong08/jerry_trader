"""ExitLab -- exit timing research for premarket momentum candidates.

Provides tradability scoring, factor-snapshot-at-entry, and mechanical
exit strategy simulation (tp/sl/trail) for prefiltered candidates.

Usage:
    lab = ExitLab("2026-03-06")
    lab.load_candidates()
    df = lab.tradability_score()
    reports = lab.sweep()
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from jerry_trader.services.backtest.data_loading import (
    _load_profiles,
    _load_quotes_ch,
    _load_trades_ch,
)
from jerry_trader.services.backtest.layer_lab.analysis import _ExitLabAnalysis
from jerry_trader.services.backtest.layer_lab.types import (
    ExitResult,
    ExitStrategyReport,
)


class ExitLab(_ExitLabAnalysis):
    """Exit timing research -- no factors, pure price-path analysis.

    For each prefilter candidate, walks forward through the trade stream
    from entry to session end, simulating mechanical exit rules.

    Usage:
        lab = ExitLab("2026-03-06")
        lab.load_candidates()
        print(lab.mfe_summary())
        reports = lab.sweep()
        for r in reports:
            print(r.summary())
    """

    def __init__(self, date: str):
        self.date = date
        self.candidates: list = []
        self._trades_cache: dict[str, pd.DataFrame] = {}
        self._quotes_cache: dict[str, pd.DataFrame] = {}
        self._profiles: dict[str, dict] = _load_profiles(date)

    def load_candidates(
        self, top_n: int = 20, min_gain_pct: float = 2.0, new_entry_only: bool = True
    ):
        """Load prefilter candidates for the date."""
        from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
        from jerry_trader.services.backtest.config import PreFilterConfig
        from jerry_trader.services.backtest.pre_filter import PreFilter

        ch = get_clickhouse_client()
        config = PreFilterConfig(
            top_n=top_n, min_gain_pct=min_gain_pct, new_entry_only=new_entry_only
        )
        self.candidates = PreFilter(ch_client=ch).find(self.date, config)
        print(f"Loaded {len(self.candidates)} candidates for {self.date}")
        return self

    def _load_trades(self, ticker: str) -> pd.DataFrame:
        """Load trades for a ticker, cached."""
        if ticker not in self._trades_cache:
            self._trades_cache[ticker] = _load_trades_ch(ticker, self.date)
        return self._trades_cache[ticker]

    def _load_quotes(self, ticker: str) -> pd.DataFrame:
        """Load quotes for a ticker, cached. Returns empty DataFrame if unavailable."""
        if ticker not in self._quotes_cache:
            try:
                self._quotes_cache[ticker] = _load_quotes_ch(ticker, self.date)
            except Exception:
                self._quotes_cache[ticker] = pd.DataFrame(
                    columns=["ts_ms", "bid", "ask", "bid_size", "ask_size"]
                )
        return self._quotes_cache[ticker]

    @staticmethod
    def find_entry_price(trades_df: pd.DataFrame, entry_ms: int) -> float | None:
        """Last trade price at or before entry_ms.

        If no trade exists before entry_ms (e.g., ticker was ranked premarket
        but trades only start later), falls back to the first trade within a
        2-minute window after entry_ms.
        """
        before = trades_df[trades_df["ts_ms"] <= entry_ms]
        if not before.empty:
            return float(before["price"].iloc[-1])

        # Fallback: first trade within 2 minutes after entry_ms
        after = trades_df[
            (trades_df["ts_ms"] > entry_ms) & (trades_df["ts_ms"] <= entry_ms + 120_000)
        ]
        if not after.empty:
            return float(after["price"].iloc[0])

        return None

    @staticmethod
    def simulate_one(
        trades_df: pd.DataFrame,
        entry_ms: int,
        entry_price: float,
        session_end_ms: int,
        *,
        tp_pct: float | None = None,
        sl_pct: float | None = None,
        trail_pct: float | None = None,
        time_stop_min: float | None = None,
    ) -> ExitResult | None:
        """Walk forward through trade stream, checking exit conditions.

        At each trade after entry, check TP, SL, trailing stop, time stop,
        and session end. First condition triggered wins.
        Returns ExitResult or None if no valid forward trades.
        """
        fwd = trades_df[trades_df["ts_ms"] > entry_ms]
        fwd = fwd[fwd["ts_ms"] <= session_end_ms]
        if fwd.empty:
            return None

        time_stop_ms = None
        if time_stop_min is not None:
            time_stop_ms = entry_ms + int(time_stop_min * 60 * 1000)

        peak_price = entry_price
        exit_reason = "session_end"
        exit_ms = session_end_ms
        exit_price = float(fwd["price"].iloc[-1])

        for row in fwd.itertuples():
            ts = int(row.ts_ms)
            price = float(row.price)

            if price > peak_price:
                peak_price = price

            # Check conditions in priority order
            triggered = False

            # 1. Stop loss (worst outcome, check first)
            if sl_pct is not None and price <= entry_price * (1 + sl_pct / 100):
                exit_reason = "stop_loss"
                exit_ms = ts
                exit_price = price
                triggered = True

            # 2. Take profit
            if (
                not triggered
                and tp_pct is not None
                and price >= entry_price * (1 + tp_pct / 100)
            ):
                exit_reason = "take_profit"
                exit_ms = ts
                exit_price = price
                triggered = True

            # 3. Trailing stop
            if not triggered and trail_pct is not None:
                trail_price = peak_price * (1 - trail_pct / 100)
                if price <= trail_price:
                    exit_reason = "trailing_stop"
                    exit_ms = ts
                    exit_price = price
                    triggered = True

            # 4. Time stop
            if not triggered and time_stop_ms is not None and ts >= time_stop_ms:
                exit_reason = "time_stop"
                exit_ms = ts
                exit_price = price
                triggered = True

            if triggered:
                break

        # Compute MFE/MAE over the full forward path (not just until exit)
        all_prices = fwd["price"].values
        # Prices up to exit point
        exit_idx = fwd["ts_ms"].searchsorted(exit_ms)
        relevant_prices = (
            all_prices[: exit_idx + 1] if exit_idx < len(all_prices) else all_prices
        )
        mfe = (np.max(relevant_prices) - entry_price) / entry_price * 100
        mae = (np.min(relevant_prices) - entry_price) / entry_price * 100
        pnl = (exit_price - entry_price) / entry_price * 100
        duration = (exit_ms - entry_ms) / 60000.0

        return ExitResult(
            ticker="",  # filled by caller
            entry_ms=entry_ms,
            entry_price=round(entry_price, 4),
            exit_ms=exit_ms,
            exit_price=round(exit_price, 4),
            exit_reason=exit_reason,
            pnl_pct=round(pnl, 4),
            mfe_pct=round(mfe, 2),
            mae_pct=round(mae, 2),
            duration_min=round(duration, 2),
        )

    def run_strategy(
        self,
        *,
        tp_pct: float | None = None,
        sl_pct: float | None = None,
        trail_pct: float | None = None,
        time_stop_min: float | None = None,
    ) -> ExitStrategyReport:
        """Run one exit strategy across all candidates."""
        dt_et = datetime.strptime(self.date, "%Y-%m-%d").replace(
            hour=9, minute=30, tzinfo=ZoneInfo("America/New_York")
        )
        session_end_ms = int(dt_et.timestamp() * 1000)

        results: list[ExitResult] = []
        skipped = 0

        for cand in self.candidates:
            trades = self._load_trades(cand.symbol)
            if trades.empty:
                skipped += 1
                continue

            entry_price = self.find_entry_price(trades, cand.first_entry_ms)
            if entry_price is None:
                skipped += 1
                continue

            result = self.simulate_one(
                trades,
                cand.first_entry_ms,
                entry_price,
                session_end_ms,
                tp_pct=tp_pct,
                sl_pct=sl_pct,
                trail_pct=trail_pct,
                time_stop_min=time_stop_min,
            )
            if result is None:
                skipped += 1
                continue

            result.ticker = cand.symbol
            results.append(result)

        if not results:
            return ExitStrategyReport(
                strategy_name="empty",
                params={},
                n_trades=0,
                n_profitable=0,
                win_rate=0.0,
                avg_win_pct=0.0,
                avg_loss_pct=0.0,
                expectancy_pct=0.0,
                avg_mfe_pct=0.0,
                avg_duration_min=0.0,
            )

        wins = [r for r in results if r.pnl_pct > 0]
        losses = [r for r in results if r.pnl_pct <= 0]
        n = len(results)

        name_parts = []
        params = {}
        if tp_pct is not None:
            name_parts.append(f"tp{tp_pct:.0f}")
            params["tp_pct"] = tp_pct
        if sl_pct is not None:
            name_parts.append(f"sl{sl_pct:.0f}")
            params["sl_pct"] = sl_pct
        if trail_pct is not None:
            name_parts.append(f"trail{trail_pct:.0f}")
            params["trail_pct"] = trail_pct
        if time_stop_min is not None:
            name_parts.append(f"time{time_stop_min:.0f}m")
            params["time_stop_min"] = time_stop_min

        return ExitStrategyReport(
            strategy_name="_".join(name_parts) if name_parts else "hold_to_end",
            params=params,
            n_trades=n,
            n_profitable=len(wins),
            win_rate=len(wins) / n,
            avg_win_pct=float(np.mean([r.pnl_pct for r in wins])) if wins else 0.0,
            avg_loss_pct=float(np.mean([r.pnl_pct for r in losses])) if losses else 0.0,
            expectancy_pct=float(np.mean([r.pnl_pct for r in results])),
            avg_mfe_pct=float(np.mean([r.mfe_pct for r in results])),
            avg_duration_min=float(np.mean([r.duration_min for r in results])),
            per_ticker=results,
        )

    def _snapshot_factors_at_entry(self) -> list[dict]:
        """Compute trade_rate at each candidate's first_entry_ms.

        Returns list of dicts with ticker, entry_ms, entry_price, gain_pct,
        n_fwd_trades, and factor values at the nearest grid point before entry.
        Includes all candidates where FactorLab.load() succeeds,
        even if trade_rate is NaN (recorded as missing key).
        """
        from jerry_trader.services.backtest.factor_lab import FactorLab

        dt_et = datetime.strptime(self.date, "%Y-%m-%d").replace(
            hour=9, minute=30, tzinfo=ZoneInfo("America/New_York")
        )
        session_end_ms = int(dt_et.timestamp() * 1000)

        snapshots: list[dict] = []
        for cand in self.candidates:
            trades = self._load_trades(cand.symbol)
            if trades.empty:
                continue
            entry_price = self.find_entry_price(trades, cand.first_entry_ms)
            if entry_price is None:
                continue

            rec = {
                "ticker": cand.symbol,
                "entry_ms": cand.first_entry_ms,
                "entry_price": entry_price,
                "gain_pct": cand.gain_at_entry,
            }

            try:
                fl = FactorLab(self.date)
                fl.load(cand.symbol)
            except Exception:
                continue

            # Only compute factors relevant for tradability
            for fn in ["trade_rate", "large_trade_ratio", "aggressor_ratio"]:
                try:
                    fl.compute_trade_factor(fn)
                except Exception:
                    pass
            for fn in ["bid_ask_spread"]:
                try:
                    fl.compute_quote_factor(fn)
                except Exception:
                    pass

            df = fl.factors_df()
            idx = df["ts_ms"].searchsorted(cand.first_entry_ms, side="right") - 1
            if idx < 0:
                idx = 0
            max_lookback = min(idx, 120)
            for fn in fl._factors:
                for offset in range(max_lookback + 1):
                    val = df.iloc[idx - offset].get(fn)
                    if val is not None and not (
                        isinstance(val, float) and np.isnan(val)
                    ):
                        rec[fn] = float(val)
                        break

            fwd = trades[trades["ts_ms"] > cand.first_entry_ms]
            fwd = fwd[fwd["ts_ms"] <= session_end_ms]
            rec["n_fwd_trades"] = len(fwd)
            snapshots.append(rec)

        return snapshots
