"""Factor Lab -- lightweight strategy research tool.

Zero-config experiment loop:
    data → factors → conditions → signals → PnL → plot

No ClickHouse/Redis/YAML/config required for the core factor computation.
Data access is through ClickHouse (fast ticker-filtered queries) with an
optional offline path via per-ticker Parquet or CSV.gz files.

Usage:
    lab = FactorLab("2026-03-02")
    lab.load("AAPL")
    lab.compute_trade_factor("aggressor_ratio", {"window_ms": 20000.0, "min_trades": 5.0})
    lab.compute_quote_factor("order_imbalance", {"window_ms": 5000.0})
    signals = lab.match(aggressor_ratio__gt=0.6, order_imbalance__gt=0.3)
    trades = lab.simulate(signals, hold_minutes=5, stop_loss_pct=-0.5, take_profit_pct=1.0)
    lab.plot(trades)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
from scipy import stats

from jerry_trader.services.backtest.data_loading import (
    DEFAULT_COMPUTE_INTERVAL_MS,
    POLYGON_DATA_ROOT,
    SESSION_END_MS,
    SESSION_START_MS,
    _load_quotes_ch,
    _load_quotes_csv,
    _load_trades_ch,
    _load_trades_csv,
    _ns_to_ms,
)

# ══════════════════════════════════════════════════════════════════════════
# Data loading (extracted to jerry_trader.services.backtest.data_loading)
# ══════════════════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════════════════
# Core lab
# ══════════════════════════════════════════════════════════════════════════


@dataclass
class Signal:
    """A matched entry signal."""

    ts_ms: int
    signal_pct: float = 0.0  # composite signal strength


@dataclass
class TradeLog:
    """Result of a simulated trade."""

    entry_ts: int
    exit_ts: int
    entry_price: float
    exit_price: float
    return_pct: float
    hold_minutes: float
    exit_reason: str  # "hold_expired", "stop_loss", "take_profit"


class FactorLab:
    """Lightweight strategy research environment.

    Loads data for one ticker on one date, computes factors, matches
    conditions, simulates exits, and plots results. No YAML, no pipeline,
    no event evaluation framework.

    Factor values are aligned on a regular compute grid (default 2s).

    eval_start_ms controls when signal evaluation begins. Data before this
    timestamp is still used for factor warmup (EMA, relative_volume, etc.),
    but match() only returns signals at or after this time. Set this to the
    ticker's first_entry_ms from PreFilter to align with live behavior.
    """

    def __init__(
        self,
        date: str,
        eval_start_ms: Optional[int] = None,
        eval_end_ms: Optional[int] = None,
    ):
        """
        Args:
            date: Date string "YYYY-MM-DD".
            eval_start_ms: If set, match() only returns signals with
                ts_ms >= eval_start_ms. Use PreFilter's first_entry_ms.
            eval_end_ms: If set, match() only returns signals with
                ts_ms <= eval_end_ms. Use session end (09:30 ET for premarket).
        """
        self.date = date
        self.ticker: Optional[str] = None
        self.eval_start_ms = eval_start_ms
        self.eval_end_ms = eval_end_ms

        # Raw data DataFrames
        self._trades: Optional[pd.DataFrame] = None
        self._quotes: Optional[pd.DataFrame] = None

        # Factor values indexed by compute_ts (ms)
        self._compute_ts: Optional[np.ndarray] = None
        self._factors: dict[str, np.ndarray] = {}  # factor_name → array (with NaN)

        # Derived
        self._price_at_ts: Optional[pd.Series] = (
            None  # compute_ts → nearest trade price
        )

    # ── PreFilter ──────────────────────────────────────────────────────────

    @staticmethod
    def prefilter_candidates(
        date: str,
        min_gain_pct: float = 2.0,
        new_entry_only: bool = True,
        top_n: int = 20,
    ) -> list:
        """Run PreFilter to get top-N entry candidates for a date.

        Uses the SAME subscription logic as the live processor and backtest
        pipeline — reads from market_snapshot_collector, partitions by window,
        computes ordinal rank, and tracks when each ticker first entered the
        top N.

        Args:
            date: Date string "YYYY-MM-DD".
            min_gain_pct: Minimum changePercent at entry.
            new_entry_only: Exclude tickers already in top N at first window.
            top_n: Rank threshold (default 20, must match processor.TOP_N).

        Returns:
            List of Candidate objects, sorted by first_entry_ms.
        """
        from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
        from jerry_trader.services.backtest.config import PreFilterConfig
        from jerry_trader.services.backtest.pre_filter import PreFilter

        ch = get_clickhouse_client()
        config = PreFilterConfig(
            top_n=top_n,
            min_gain_pct=min_gain_pct,
            new_entry_only=new_entry_only,
        )
        return PreFilter(ch_client=ch).find(date, config)

    @classmethod
    def from_candidate(
        cls, date: str, candidate, eval_end_ms: Optional[int] = None
    ) -> "FactorLab":
        """Create a FactorLab from a PreFilter Candidate.

        Sets eval_start_ms to candidate.first_entry_ms so match() only
        returns signals after the ticker entered the top N.
        """
        return cls(
            date, eval_start_ms=candidate.first_entry_ms, eval_end_ms=eval_end_ms
        )

    # ── Data ──────────────────────────────────────────────────────────────

    def load(
        self,
        ticker: str,
        offline: bool = False,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> "FactorLab":
        """Load trades and quotes for the given ticker/date.

        Args:
            ticker: Stock ticker symbol.
            offline: If True, load from CSV.gz files instead of ClickHouse.
            start_ms: Filter data to start time (epoch ms).
            end_ms: Filter data to end time (epoch ms).

        Returns self for chaining.
        """
        self.ticker = ticker.upper()

        if offline:
            self._trades = _load_trades_csv(self.ticker, self.date)
            self._quotes = _load_quotes_csv(self.ticker, self.date)
        else:
            self._trades = _load_trades_ch(self.ticker, self.date)
            self._quotes = _load_quotes_ch(self.ticker, self.date)

        if start_ms is not None:
            self._trades = self._trades[self._trades["ts_ms"] >= start_ms]
            self._quotes = self._quotes[self._quotes["ts_ms"] >= start_ms]
        if end_ms is not None:
            self._trades = self._trades[self._trades["ts_ms"] <= end_ms]
            self._quotes = self._quotes[self._quotes["ts_ms"] <= end_ms]

        return self

    def trades(self) -> pd.DataFrame:
        """Return raw trades DataFrame."""
        if self._trades is None:
            raise RuntimeError("Call load() first.")
        return self._trades.copy()

    def quotes(self) -> pd.DataFrame:
        """Return raw quotes DataFrame."""
        if self._quotes is None:
            raise RuntimeError("Call load() first.")
        return self._quotes.copy()

    # ── Compute grid ──────────────────────────────────────────────────────

    def _ensure_compute_grid(self, interval_ms: int = DEFAULT_COMPUTE_INTERVAL_MS):
        """Build a regular compute timestamp grid covering the loaded data."""
        if self._compute_ts is not None:
            return

        trades = self._trades
        quotes = self._quotes
        if trades is None or len(trades) == 0:
            raise RuntimeError("No trade data loaded. Call load() first.")

        t_min = trades["ts_ms"].min()
        t_max = trades["ts_ms"].max()
        if quotes is not None and len(quotes) > 0:
            t_min = min(t_min, quotes["ts_ms"].min())
            t_max = max(t_max, quotes["ts_ms"].max())

        self._compute_ts = np.arange(
            t_min, t_max + interval_ms, interval_ms, dtype=np.int64
        )

        # Nearest trade price at each compute_ts (deduplicate on index)
        price_series = trades.set_index("ts_ms")["price"]
        price_series = price_series[~price_series.index.duplicated(keep="last")]
        self._price_at_ts = price_series.reindex(self._compute_ts, method="ffill")

    @property
    def compute_ts(self) -> np.ndarray:
        self._ensure_compute_grid()
        return self._compute_ts

    # ── Factor computation ────────────────────────────────────────────────

    def compute_trade_factor(
        self,
        name: str,
        params: Optional[dict] = None,
    ) -> np.ndarray:
        """Compute a trade factor on the data grid.

        Args:
            name: Factor name (e.g., "aggressor_ratio", "trade_rate").
            params: Optional parameter dict (e.g., {"window_ms": 20000.0}).

        Returns:
            np.ndarray of factor values aligned with self.compute_ts (NaN where not ready).
        """
        from jerry_trader._rust import PyFactorEngine, PyTrade

        self._ensure_compute_grid()
        if self._trades is None:
            raise RuntimeError("Call load() first.")

        engine = PyFactorEngine()
        py_trades = [
            PyTrade(int(row.ts_ms), float(row.price), int(row.size))
            for row in self._trades.itertuples()
        ]
        result = engine.compute_batch_trade(
            name, py_trades, self._compute_ts.tolist(), params
        )
        values = np.array(
            [v if v is not None else np.nan for v in result], dtype=np.float64
        )
        self._factors[name] = values
        return values

    def _build_bars(self) -> list:
        """Build 1m OHLCV bars from trades using Rust BarBuilder."""
        from jerry_trader._rust import BarBuilder as RustBarBuilder

        builder = RustBarBuilder(["1m"])
        builder.configure_watermark(late_arrival_ms=0, idle_close_ms=1)

        batch_trades = [
            (ts, price, float(size))
            for ts, price, size in zip(
                self._trades["ts_ms"], self._trades["price"], self._trades["size"]
            )
        ]
        builder.ingest_trades_batch(self.ticker, batch_trades)
        builder.flush()
        return builder.drain_completed()

    def compute_bar_factor(
        self,
        name: str,
        params: Optional[dict] = None,
    ) -> np.ndarray:
        """Compute a bar factor on 1m bars.

        Uses Rust BarBuilder to build 1m bars from trades, then computes
        the factor via PyFactorEngine.compute_batch_bar().

        Args:
            name: Factor rust_key (e.g., "relative_volume", "price_direction").
            params: Optional parameter dict (e.g., {"lookback": 20.0}).

        Returns:
            np.ndarray of factor values aligned with self.compute_ts
            (forward-filled from bar_end timestamps).
        """
        from jerry_trader._rust import PyBar, PyFactorEngine

        self._ensure_compute_grid()
        if self._trades is None:
            raise RuntimeError("Call load() first.")

        raw_bars = self._build_bars()
        if not raw_bars:
            raise RuntimeError("No bars built from trades.")

        py_bars = [
            PyBar(
                b["bar_start"],
                b["open"],
                b["high"],
                b["low"],
                b["close"],
                int(b["volume"]),
            )
            for b in raw_bars
        ]

        engine = PyFactorEngine()
        values = engine.compute_batch_bar(name, py_bars, params)

        # Map bar_end → value, then forward-fill to compute grid
        bar_end_ts = [b["bar_end"] for b in raw_bars]
        bar_series = pd.Series(
            [v if v is not None else np.nan for v in values],
            index=bar_end_ts,
            dtype=np.float64,
        )
        bar_series = bar_series[~bar_series.index.duplicated(keep="last")]
        mapped = bar_series.reindex(self._compute_ts, method="ffill")
        arr = mapped.to_numpy(dtype=np.float64)
        self._factors[name] = arr
        return arr

    def compute_quote_factor(
        self,
        name: str,
        params: Optional[dict] = None,
    ) -> np.ndarray:
        """Compute a quote factor on the data grid.

        Args:
            name: Factor name (e.g., "order_imbalance", "bid_ask_spread").
            params: Optional parameter dict.

        Returns:
            np.ndarray of factor values aligned with self.compute_ts (NaN where not ready).
        """
        self._ensure_compute_grid()
        if self._quotes is None:
            raise RuntimeError("Call load() first.")

        from jerry_trader._rust import PyFactorEngine

        engine = PyFactorEngine()
        quote_tuples = [
            (
                int(row.ts_ms),
                float(row.bid),
                float(row.ask),
                int(row.bid_size),
                int(row.ask_size),
            )
            for row in self._quotes.itertuples()
        ]
        result = engine.compute_batch_quote(name, quote_tuples, params)

        # Quote factors generate their own 1s compute grid. Forward-fill
        # onto our grid so each compute_ts gets the most recent factor value.
        result_df = pd.DataFrame(result, columns=["ts_ms", "value"])
        result_df = result_df.set_index("ts_ms")
        result_df = result_df[~result_df.index.duplicated(keep="last")]
        mapped = result_df.reindex(self._compute_ts, method="ffill")
        values = mapped["value"].to_numpy(dtype=np.float64)
        self._factors[name] = values
        return values

    def factors_df(self) -> pd.DataFrame:
        """Return all computed factors as a DataFrame indexed by compute_ts."""
        self._ensure_compute_grid()
        df = pd.DataFrame({"ts_ms": self._compute_ts})
        for name, values in self._factors.items():
            df[name] = values
        if self._price_at_ts is not None:
            df["price"] = self._price_at_ts.values
        return df

    # ── Statistical validation (no thresholds, no simulation) ────────────

    @staticmethod
    def _forward_return(
        entry_ts: int,
        entry_price: float,
        trades_df: pd.DataFrame,
        horizon_ms: int,
    ) -> Optional[float]:
        """Compute forward return at horizon from actual trade prices.

        Uses the last trade price at or before entry_ts + horizon_ms.
        Returns None if no trade data within the horizon.
        """
        horizon_end = entry_ts + horizon_ms
        future = trades_df[trades_df.index > entry_ts]
        future = future[future.index <= horizon_end]
        if future.empty:
            return None
        return (float(future["price"].iloc[-1]) - entry_price) / entry_price * 100

    def forward_returns(
        self,
        horizons: Optional[dict[str, int]] = None,
    ) -> pd.DataFrame:
        """Compute forward returns at each compute grid point.

        Uses actual trade prices, not compute grid interpolation.

        Args:
            horizons: dict of label → horizon_ms.
                Default: {'30s': 30_000, '1m': 60_000, '2m': 120_000,
                          '5m': 300_000, '10m': 600_000}

        Returns DataFrame with compute grid ts_ms, price, factor values,
        and forward return columns (fwd_30s, fwd_1m, etc.).
        """
        if horizons is None:
            horizons = {
                "30s": 30_000,
                "1m": 60_000,
                "2m": 120_000,
                "5m": 300_000,
                "10m": 600_000,
            }

        self._ensure_compute_grid()
        df = self.factors_df()

        trades_df = self._trades.set_index("ts_ms")
        trades_df = trades_df[~trades_df.index.duplicated(keep="last")]

        for label, horizon_ms in horizons.items():
            col = f"fwd_{label}"
            df[col] = df.apply(
                lambda row: self._forward_return(
                    int(row["ts_ms"]), float(row["price"]), trades_df, horizon_ms
                ),
                axis=1,
            )

        return df

    def factor_correlation(
        self,
        factor_name: str,
        horizon: str = "5m",
        method: str = "spearman",
    ) -> dict:
        """Compute rank correlation between factor values and forward returns.

        Uses only timestamps within the eval window (eval_start_ms / eval_end_ms).
        NaN values in either factor or forward return are dropped.

        Args:
            factor_name: Factor column name.
            horizon: Forward return column label (e.g., '5m').
            method: 'spearman' (default) or 'pearson'.

        Returns dict with rho, p_value, n_samples.
        """
        df = self.forward_returns()
        fwd_col = f"fwd_{horizon}"

        # Filter to eval window
        if self.eval_start_ms is not None:
            df = df[df["ts_ms"] >= self.eval_start_ms]
        if self.eval_end_ms is not None:
            df = df[df["ts_ms"] <= self.eval_end_ms]

        mask = df[factor_name].notna() & df[fwd_col].notna()
        n = mask.sum()
        if n < 10:
            return {"rho": 0.0, "p_value": 1.0, "n_samples": n}

        if method == "spearman":
            rho, p = stats.spearmanr(df.loc[mask, factor_name], df.loc[mask, fwd_col])
        else:
            rho, p = stats.pearsonr(df.loc[mask, factor_name], df.loc[mask, fwd_col])
        return {"rho": float(rho), "p_value": float(p), "n_samples": int(n)}

    def factor_quantile(
        self,
        factor_name: str,
        horizon: str = "5m",
        n_quantiles: int = 5,
    ) -> dict:
        """Compare forward return distributions across factor quantiles.

        For each quantile of the factor, compute the forward return distribution:
        mean, median, std, positive_rate, extreme_up, extreme_down.

        Also includes a baseline (all data) for comparison.

        Args:
            factor_name: Factor column name.
            horizon: Forward return column label.
            n_quantiles: Number of quantile bins (default 5).

        Returns dict: {quantile_label: {mean, median, std, pos_rate, ...}}
        """
        df = self.forward_returns()
        fwd_col = f"fwd_{horizon}"

        if self.eval_start_ms is not None:
            df = df[df["ts_ms"] >= self.eval_start_ms]
        if self.eval_end_ms is not None:
            df = df[df["ts_ms"] <= self.eval_end_ms]

        valid = df[factor_name].notna() & df[fwd_col].notna()
        df = df[valid].copy()
        if len(df) < 20:
            return {"error": f"Only {len(df)} valid samples"}

        returns = df[fwd_col].values

        def _describe(arr):
            if len(arr) == 0:
                return {}
            return {
                "n": len(arr),
                "mean": float(np.mean(arr)),
                "median": float(np.median(arr)),
                "std": float(np.std(arr)),
                "pos_rate": float(np.sum(arr > 0) / len(arr)),
                "extreme_up": float(np.percentile(arr, 95)),
                "extreme_down": float(np.percentile(arr, 5)),
            }

        result = {"baseline": _describe(returns)}

        df["q"] = pd.qcut(df[factor_name], n_quantiles, labels=False, duplicates="drop")
        for q in range(n_quantiles):
            q_data = df[df["q"] == q][fwd_col].values
            if len(q_data) > 0:
                # Describe the quantile by its factor value range
                q_factors = df[df["q"] == q][factor_name]
                lo = float(q_factors.min())
                hi = float(q_factors.max())
                label = f"Q{q} [{lo:.3f}, {hi:.3f}]"
                result[label] = _describe(q_data)

        return result

    # ── Signal matching ───────────────────────────────────────────────────

    def match(self, **conditions: float) -> pd.DataFrame:
        """Match rows where factor values satisfy threshold conditions.

        Conditions use suffix suffixes:
            ___gt  → factor > threshold
            ___lt  → factor < threshold
            ___gte → factor >= threshold
            ___lte → factor <= threshold

        If eval_start_ms is set, only returns signals with ts_ms >= eval_start_ms.
        Data before eval_start_ms is still used for factor warmup.

        Example:
            signals = lab.match(aggressor_ratio__gt=0.6, order_imbalance__gt=0.3)

        Returns DataFrame with columns: ts_ms, price, signal, {factor columns...}
        """
        self._ensure_compute_grid()
        df = self.factors_df()

        # Filter to eval window (post-subscription, pre-session-end)
        if self.eval_start_ms is not None:
            df = df[df["ts_ms"] >= self.eval_start_ms]
        if self.eval_end_ms is not None:
            df = df[df["ts_ms"] <= self.eval_end_ms]

        if df.empty:
            return pd.DataFrame()

        mask = pd.Series(True, index=df.index)
        for spec, threshold in conditions.items():
            parts = spec.rsplit("__", 1)
            if len(parts) != 2:
                raise ValueError(
                    f"Condition '{spec}' missing suffix (__gt, __lt, etc.)"
                )

            factor_name, op = parts
            if factor_name not in df.columns:
                raise KeyError(
                    f"Factor '{factor_name}' not computed. Available: {list(self._factors.keys())}"
                )

            col = df[factor_name]

            if op == "gt":
                mask &= col > threshold
            elif op == "lt":
                mask &= col < threshold
            elif op == "gte":
                mask &= col >= threshold
            elif op == "lte":
                mask &= col <= threshold
            elif op == "between":
                lo, hi = (
                    threshold
                    if isinstance(threshold, tuple)
                    else (threshold, threshold)
                )
                mask &= (col >= lo) & (col <= hi)
            else:
                raise ValueError(
                    f"Unknown operator '{op}'. Use: gt, lt, gte, lte, between"
                )

        signals = df[mask].copy()
        signals["signal"] = 1
        return signals

    # ── Simulation ────────────────────────────────────────────────────────

    def simulate(
        self,
        signals: pd.DataFrame,
        hold_minutes: float = 5.0,
        stop_loss_pct: float = -1.0,
        take_profit_pct: float = 2.0,
        max_trades: int = 9999,
        min_interval_ms: int = 0,
    ) -> list[TradeLog]:
        """Simulate trades from a signal DataFrame.

        Entry: at each signal's ts_ms, using nearest price.
        Exit: when hold expires, stop-loss, or take-profit.

        Args:
            min_interval_ms: Skip signals within this distance of last entry.
                Set to e.g. 60_000 to avoid signal clusters (1 trade/minute).

        Returns list of TradeLog entries.
        """
        if self._trades is None or len(self._trades) == 0:
            return []

        hold_ms = int(hold_minutes * 60 * 1000)
        trades_prices: pd.DataFrame = self._trades.set_index("ts_ms")
        trades_prices = trades_prices[~trades_prices.index.duplicated(keep="last")]

        all_ts = trades_prices.index.values
        trades_list: list[TradeLog] = []
        last_entry_ts: int = 0

        for _, row in signals.iterrows():
            if len(trades_list) >= max_trades:
                break
            entry_ts = int(row["ts_ms"])
            if min_interval_ms > 0 and entry_ts - last_entry_ts < min_interval_ms:
                continue
            last_entry_ts = entry_ts

            entry_ts = int(row["ts_ms"])
            entry_price = float(row["price"])
            deadline = entry_ts + hold_ms

            exit_price = entry_price
            exit_reason = "hold_expired"
            exit_ts = deadline

            # Walk forward through trades to find exit
            mask = (all_ts > entry_ts) & (all_ts <= deadline)
            future_ts = all_ts[mask]
            future_prices = trades_prices.loc[future_ts, "price"].values

            for i, ts in enumerate(future_ts):
                p = float(future_prices[i])
                if np.isnan(p) or p <= 0:
                    continue
                return_pct = (p - entry_price) / entry_price * 100

                if return_pct <= stop_loss_pct:
                    exit_price = p
                    exit_ts = int(ts)
                    exit_reason = "stop_loss"
                    break
                if return_pct >= take_profit_pct:
                    exit_price = p
                    exit_ts = int(ts)
                    exit_reason = "take_profit"
                    break

            if exit_reason == "hold_expired" and len(future_ts) > 0:
                exit_price = (
                    float(future_prices[-1]) if len(future_prices) > 0 else entry_price
                )
                exit_ts = int(future_ts[-1]) if len(future_ts) > 0 else deadline

            return_pct = (exit_price - entry_price) / entry_price * 100
            trades_list.append(
                TradeLog(
                    entry_ts=entry_ts,
                    exit_ts=exit_ts,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    return_pct=round(return_pct, 4),
                    hold_minutes=round((exit_ts - entry_ts) / 60000, 2),
                    exit_reason=exit_reason,
                )
            )

        return trades_list

    # ── Analysis ──────────────────────────────────────────────────────────

    def summary(self, trades: list[TradeLog]) -> dict:
        """Compute summary statistics for a list of trades."""
        if not trades:
            return {"total_trades": 0}

        returns = [t.return_pct for t in trades]
        wins = [r for r in returns if r > 0]
        arr = np.array(returns)
        return {
            "total_trades": len(trades),
            "win_rate": len(wins) / len(trades) if trades else 0,
            "avg_return": float(np.mean(arr)),
            "total_return": float(np.sum(arr)),
            "max_return": float(np.max(arr)),
            "min_return": float(np.min(arr)),
            "avg_hold_minutes": float(np.mean([t.hold_minutes for t in trades])),
            "exit_reasons": {
                reason: sum(1 for t in trades if t.exit_reason == reason)
                for reason in sorted(set(t.exit_reason for t in trades))
            },
        }

    # ── Plot ──────────────────────────────────────────────────────────────

    def plot(
        self,
        trades: Optional[list[TradeLog]] = None,
        factors: Optional[list[str]] = None,
    ):
        """Plot price, factor values, and optional trade markers.

        Args:
            trades: Optional list of TradeLog entries to mark on the chart.
            factors: Optional list of factor names to plot (default: all computed).
        """
        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt

        self._ensure_compute_grid()
        df = self.factors_df()

        # Convert ts_ms to datetime for readable x-axis
        df["time"] = pd.to_datetime(df["ts_ms"], unit="ms")

        factor_names = factors or list(self._factors.keys())
        n_panels = 1 + len(factor_names)
        fig, axes = plt.subplots(
            n_panels,
            1,
            figsize=(14, 2 + 1.8 * n_panels),
            sharex=True,
            gridspec_kw={"height_ratios": [2] + [1] * len(factor_names)},
        )
        if n_panels == 1:
            axes = [axes]

        # Price panel
        ax = axes[0]
        ax.plot(df["time"], df["price"], color="#1a1a2e", linewidth=0.8, label="Price")
        ax.set_ylabel("Price", fontsize=10)
        ax.legend(loc="upper left", fontsize=8)
        ax.grid(True, alpha=0.3)

        # Mark trades
        if trades:
            for t in trades:
                entry_time = pd.to_datetime(t.entry_ts, unit="ms")
                exit_time = pd.to_datetime(t.exit_ts, unit="ms")
                color = "#22c55e" if t.return_pct > 0 else "#ef4444"
                ax.scatter(
                    entry_time, t.entry_price, color=color, s=20, zorder=5, alpha=0.8
                )
                ax.scatter(
                    exit_time,
                    t.exit_price,
                    color=color,
                    s=12,
                    zorder=5,
                    alpha=0.6,
                    marker="x",
                )

        # Factor panels
        for i, name in enumerate(factor_names):
            ax = axes[i + 1]
            values = df[name].values
            ax.plot(df["time"], values, color="#3b82f6", linewidth=0.8)
            ax.axhline(y=0, color="gray", linewidth=0.5, linestyle="--")
            ax.set_ylabel(name, fontsize=10)
            ax.grid(True, alpha=0.3)

        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        axes[-1].set_xlabel("Time", fontsize=10)
        fig.suptitle(f"{self.ticker} · {self.date}", fontsize=13, fontweight="bold")
        plt.tight_layout()
        plt.show()

        return fig


# ══════════════════════════════════════════════════════════════════════════
# Convenience
# ══════════════════════════════════════════════════════════════════════════


def quick_study(
    date: str,
    ticker: str,
    trade_factors: Optional[dict] = None,
    quote_factors: Optional[dict] = None,
    conditions: Optional[dict] = None,
    hold_minutes: float = 5.0,
    stop_loss_pct: float = -1.0,
    take_profit_pct: float = 2.0,
    offline: bool = False,
    eval_start_ms: Optional[int] = None,
    eval_end_ms: Optional[int] = None,
) -> FactorLab:
    """One-shot experiment: load → compute → match → simulate → plot.

    Args:
        date: Date string "YYYY-MM-DD".
        ticker: Stock ticker.
        trade_factors: dict of factor_name → params (e.g., {"aggressor_ratio": {"window_ms": 20000.0}}).
        quote_factors: dict of factor_name → params.
        conditions: dict of "factor__op" → threshold for signal matching.
        hold_minutes: Max hold time per trade.
        stop_loss_pct: Stop-loss threshold (negative, e.g., -0.5).
        take_profit_pct: Take-profit threshold (positive, e.g., 1.0).
        offline: If True, load from CSV.gz files.
        eval_start_ms: If set, only match signals with ts_ms >= this.
        eval_end_ms: If set, only match signals with ts_ms <= this.

    Returns the FactorLab instance (lab.plot() called automatically).
    """
    lab = FactorLab(date, eval_start_ms=eval_start_ms, eval_end_ms=eval_end_ms).load(
        ticker, offline=offline
    )

    for name, params in (trade_factors or {}).items():
        lab.compute_trade_factor(name, params)

    for name, params in (quote_factors or {}).items():
        lab.compute_quote_factor(name, params)

    if conditions:
        signals = lab.match(**conditions)
        print(f"Signals: {len(signals)} rows matched")
        if len(signals) > 0:
            trades = lab.simulate(
                signals,
                hold_minutes=hold_minutes,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
            )
            print(f"Trades: {len(trades)} simulated")
            print(lab.summary(trades))
            lab.plot(trades)
        else:
            lab.plot()
    else:
        lab.plot()

    return lab


# ══════════════════════════════════════════════════════════════════════════
# Batch factor validation across prefilter candidates
# ══════════════════════════════════════════════════════════════════════════

DEFAULT_BATCH_FACTORS: dict[str, tuple[str, dict | None]] = {
    "relative_volume": ("bar", {"lookback": 20.0}),
    "price_direction": ("bar", {}),
    "gap_percent": ("bar", {}),
    "vwap_deviation": ("bar", {"period": 20.0}),
    "trade_rate": ("trade", {"window_ms": 20000.0, "min_trades": 5.0}),
    "large_trade_ratio": (
        "trade",
        {"window_ms": 20000.0, "min_trades": 5.0, "large_multiplier": 2.0},
    ),
    "aggressor_ratio": ("trade", {"window_ms": 20000.0, "min_trades": 5.0}),
}

DEFAULT_BATCH_HORIZONS: dict[str, int] = {
    "30s": 30_000,
    "1m": 60_000,
    "2m": 120_000,
    "5m": 300_000,
    "10m": 600_000,
}


@dataclass
class FactorValidationReport:
    """Aggregated factor validation across multiple tickers."""

    factor: str
    horizon: str
    method: str = "spearman"
    pooled_rho: float = 0.0
    pooled_p: float = 1.0
    pooled_n: int = 0
    per_ticker_rhos: list[float] = field(default_factory=list)
    n_tickers: int = 0
    n_significant: int = 0
    quantile: dict = field(default_factory=dict)

    def summary(self) -> str:
        lines = [
            f"── {self.factor} @ {self.horizon} ({self.method}) ──",
            f"  tickers: {self.n_tickers} | significant: {self.n_significant}",
            f"  pooled ρ={self.pooled_rho:.4f} (p={self.pooled_p:.4f}, n={self.pooled_n})",
        ]
        if self.per_ticker_rhos:
            mean_r = sum(self.per_ticker_rhos) / len(self.per_ticker_rhos)
            pos = sum(1 for r in self.per_ticker_rhos if r > 0)
            neg = sum(1 for r in self.per_ticker_rhos if r <= 0)
            lines.append(f"  per-ticker: mean ρ={mean_r:.4f} | pos={pos} neg={neg}")
        if self.quantile:
            lines.append("  quantiles (factor low → high):")
            for q in self.quantile.get("quantiles", []):
                lines.append(
                    f"    Q{q['q']}: fwd_ret mean={q['mean']:+.2f}% "
                    f"median={q['median']:+.2f}% pos={q['pos_rate']:.1%} "
                    f"n={q['n']}"
                )
            lines.append(
                f"    baseline: mean={self.quantile['baseline']['mean']:+.2f}% "
                f"pos={self.quantile['baseline']['pos_rate']:.1%}"
            )
        return "\n".join(lines)


def batch_factor_validation(
    date: str,
    max_tickers: int | None = 50,
    top_n: int = 20,
    min_gain_pct: float = 2.0,
    factors: dict | None = None,
    horizons: dict | None = None,
) -> dict[str, dict[str, FactorValidationReport]]:
    """Statistical factor validation across all prefilter candidates.

    For each candidate ticker:
      1. Load data + compute all bar/trade/quote factors
      2. Compute forward returns at multiple horizons
      3. Pool the per-ticker DataFrames

    Then for each factor × horizon combination:
      - Per-ticker Spearman correlation (averaged)
      - Pooled Spearman correlation
      - Quantile analysis (forward return by factor quintile)

    Returns:
        {factor_name: {horizon_label: FactorValidationReport}}
    """
    import time

    from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
    from jerry_trader.services.backtest.config import PreFilterConfig
    from jerry_trader.services.backtest.pre_filter import PreFilter

    factors = factors or DEFAULT_BATCH_FACTORS
    horizons = horizons or DEFAULT_BATCH_HORIZONS

    ch = get_clickhouse_client()
    config = PreFilterConfig(
        top_n=top_n, min_gain_pct=min_gain_pct, new_entry_only=True
    )
    candidates = PreFilter(ch_client=ch).find(date, config)
    if max_tickers:
        candidates = candidates[:max_tickers]

    print(
        f"Batch validation: {len(candidates)} tickers, "
        f"{len(factors)} factors, {len(horizons)} horizons"
    )

    # Pooled DataFrames per factor: {factor_name: [DataFrame, ...]}
    pooled_dfs: dict[str, list[pd.DataFrame]] = {fn: [] for fn in factors}

    # Per-ticker stats
    per_ticker_results: dict[str, dict[str, list[float]]] = {
        fn: {hl: [] for hl in horizons} for fn in factors
    }

    # 09:30 ET epoch ms for the given date
    from datetime import datetime
    from zoneinfo import ZoneInfo

    dt_et = datetime.strptime(date, "%Y-%m-%d").replace(
        hour=9, minute=30, tzinfo=ZoneInfo("America/New_York")
    )
    eval_end_epoch = int(dt_et.timestamp() * 1000)

    for ci, cand in enumerate(candidates):
        ticker = cand.symbol
        eval_start = cand.first_entry_ms

        try:
            lab = FactorLab(
                date, eval_start_ms=eval_start, eval_end_ms=eval_end_epoch
            ).load(ticker)
        except Exception as e:
            print(f"  [{ci+1}/{len(candidates)}] {ticker}: SKIP (load error: {e})")
            continue

        # Compute bar factors (need bars first)
        bar_specs = {k: v for k, v in factors.items() if v[0] == "bar"}
        if bar_specs:
            try:
                bars = lab._build_bars()
            except Exception:
                bars = []
            if bars:
                for name, (_, params) in bar_specs.items():
                    try:
                        lab.compute_bar_factor(name, params or {})
                    except Exception:
                        pass

        # Compute trade factors
        trade_specs = {k: v for k, v in factors.items() if v[0] == "trade"}
        for name, (_, params) in trade_specs.items():
            try:
                lab.compute_trade_factor(name, params or {})
            except Exception:
                pass

        # Compute quote factors
        quote_specs = {k: v for k, v in factors.items() if v[0] == "quote"}
        for name, (_, params) in quote_specs.items():
            try:
                lab.compute_quote_factor(name, params or {})
            except Exception:
                pass

        # Forward returns — returns a DataFrame with factor + fwd_* columns
        df = None
        try:
            df = lab.forward_returns()
        except Exception:
            pass

        if df is None or len(df) == 0:
            print(f"  [{ci+1}/{len(candidates)}] {ticker}: SKIP (no forward returns)")
            continue

        # Collect per-factor DataFrames
        for fn in factors:
            if fn not in df.columns:
                continue
            fwd_cols = [c for c in df.columns if c.startswith("fwd_")]
            if not fwd_cols:
                continue
            sub = df[[fn] + fwd_cols].dropna()
            if len(sub) < 5:
                continue
            sub = sub.copy()
            sub["ticker"] = ticker
            pooled_dfs[fn].append(sub)

        print(f"  [{ci+1}/{len(candidates)}] {ticker}: {len(df)} rows")

    # ── Build reports ──────────────────────────────────────────────────
    results: dict[str, dict[str, FactorValidationReport]] = {}

    for fn in factors:
        results[fn] = {}
        if not pooled_dfs[fn]:
            continue

        all_data = pd.concat(pooled_dfs[fn], ignore_index=True)
        factor_col = fn

        for hl, horizon_ms in horizons.items():
            fwd_col = f"fwd_{hl}"
            if fwd_col not in all_data.columns:
                continue

            valid = all_data[[factor_col, fwd_col, "ticker"]].dropna()
            if len(valid) < 10:
                continue

            # Per-ticker Spearman
            per_ticker_rhos = []
            for ticker, grp in valid.groupby("ticker"):
                if len(grp) < 5:
                    continue
                rho, p = stats.spearmanr(grp[factor_col], grp[fwd_col])
                if not np.isnan(rho):
                    per_ticker_rhos.append(rho)

            # Pooled Spearman
            pooled_rho, pooled_p = stats.spearmanr(valid[factor_col], valid[fwd_col])

            n_sig = sum(1 for r in per_ticker_rhos if r > 0)

            # Quantile analysis
            n_q = 5
            valid_sorted = valid.sort_values(factor_col)
            boundaries = [int(len(valid_sorted) * i / n_q) for i in range(n_q + 1)]
            quantiles = []
            for qi in range(n_q):
                q_slice = valid_sorted.iloc[boundaries[qi] : boundaries[qi + 1]]
                fwd = q_slice[fwd_col]
                quantiles.append(
                    {
                        "q": qi + 1,
                        "n": len(q_slice),
                        "mean": round(float(fwd.mean()), 4),
                        "median": round(float(fwd.median()), 4),
                        "std": round(float(fwd.std()), 4),
                        "pos_rate": round(float((fwd > 0).sum() / len(fwd)), 4),
                    }
                )

            baseline_fwd = valid[fwd_col]
            quantile_report = {
                "baseline": {
                    "mean": round(float(baseline_fwd.mean()), 4),
                    "median": round(float(baseline_fwd.median()), 4),
                    "std": round(float(baseline_fwd.std()), 4),
                    "pos_rate": round(
                        float((baseline_fwd > 0).sum() / len(baseline_fwd)), 4
                    ),
                    "n": len(valid),
                },
                "quantiles": quantiles,
            }

            report = FactorValidationReport(
                factor=fn,
                horizon=hl,
                pooled_rho=(
                    round(float(pooled_rho), 4) if not np.isnan(pooled_rho) else 0.0
                ),
                pooled_p=round(float(pooled_p), 4) if not np.isnan(pooled_p) else 1.0,
                pooled_n=len(valid),
                per_ticker_rhos=[round(r, 4) for r in per_ticker_rhos],
                n_tickers=len(per_ticker_rhos),
                n_significant=n_sig,
                quantile=quantile_report,
            )
            results[fn][hl] = report

    # ── Print summary ──────────────────────────────────────────────────
    print("\n" + "=" * 72)
    print("Factor Validation Summary (Spearman ρ: factor → forward return)")
    print("=" * 72)
    for fn in sorted(results):
        for hl in sorted(results[fn], key=lambda h: horizons.get(h, 0)):
            report = results[fn][hl]
            print(report.summary())
            print()

    return results
