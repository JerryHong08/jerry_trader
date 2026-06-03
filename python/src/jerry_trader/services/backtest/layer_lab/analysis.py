"""Analysis methods for ExitLab — MFE, exit sweeps, factor analysis, tradability scoring.

These methods are in a separate mixin class to keep ExitLab lean.
ExitLab inherits from _ExitLabAnalysis to gain all analysis functionality.
"""

from __future__ import annotations

import os
from typing import Optional

import numpy as np
import pandas as pd

from jerry_trader.services.backtest.layer_lab.ignition import (
    _IGNITION_METHODS,
    _detect_ignitions,
)
from jerry_trader.services.backtest.layer_lab.types import (
    ExitResult,
    ExitStrategyReport,
)


class _ExitLabAnalysis:
    """Mixin: analysis methods for ExitLab.

    Expects the inheriting class to provide:
      self.date, self.candidates, self._load_trades(),
      self.find_entry_price(), self.simulate_one(), self.run_strategy()
    """

    def mfe_summary(self) -> dict:
        """Compute MFE distribution across all candidates.

        Returns dict with keys: mean_mfe, median_mfe, pct_hit_5pct,
        pct_hit_10pct, pct_hit_15pct, pct_positive_close, per_ticker_mfe.
        """
        from datetime import datetime
        from zoneinfo import ZoneInfo

        dt_et = datetime.strptime(self.date, "%Y-%m-%d").replace(
            hour=9, minute=30, tzinfo=ZoneInfo("America/New_York")
        )
        session_end_ms = int(dt_et.timestamp() * 1000)

        mfes: list[float] = []
        closes: list[float] = []
        hit_5 = hit_10 = hit_15 = 0
        per_ticker: list[dict] = []

        for cand in self.candidates:
            trades = self._load_trades(cand.symbol)
            if trades.empty:
                continue

            entry_price = self.find_entry_price(trades, cand.first_entry_ms)
            if entry_price is None:
                continue

            # Forward trades only
            fwd = trades[trades["ts_ms"] > cand.first_entry_ms]
            fwd = fwd[fwd["ts_ms"] <= session_end_ms]
            if fwd.empty:
                continue

            prices = fwd["price"].values
            peak = float(np.max(prices))
            trough = float(np.min(prices))
            mfe = (peak - entry_price) / entry_price * 100
            mae = (trough - entry_price) / entry_price * 100
            close_ret = (float(prices[-1]) - entry_price) / entry_price * 100

            mfes.append(mfe)
            closes.append(close_ret)
            if mfe >= 5:
                hit_5 += 1
            if mfe >= 10:
                hit_10 += 1
            if mfe >= 15:
                hit_15 += 1

            per_ticker.append(
                {
                    "ticker": cand.symbol,
                    "entry_price": round(entry_price, 4),
                    "mfe_pct": round(mfe, 2),
                    "mae_pct": round(mae, 2),
                    "close_pct": round(close_ret, 2),
                }
            )

        n = len(mfes)
        mean_mfe = float(np.mean(mfes)) if mfes else 0.0
        median_mfe = float(np.median(mfes)) if mfes else 0.0
        mean_close = float(np.mean(closes)) if closes else 0.0
        median_close = float(np.median(closes)) if closes else 0.0

        print(f"\n── MFE Distribution ({n} tickers) ──")
        print(f"  mean MFE: {mean_mfe:+.2f}%  median MFE: {median_mfe:+.2f}%")
        print(f"  mean close return: {mean_close:+.2f}%  median: {median_close:+.2f}%")
        print(f"  hit +5%:  {hit_5}/{n} ({hit_5/n*100:.0f}%)")
        print(f"  hit +10%: {hit_10}/{n} ({hit_10/n*100:.0f}%)")
        print(f"  hit +15%: {hit_15}/{n} ({hit_15/n*100:.0f}%)")

        return {
            "n": n,
            "mean_mfe": mean_mfe,
            "median_mfe": median_mfe,
            "mean_close": mean_close,
            "median_close": median_close,
            "hit_5pct": hit_5,
            "hit_10pct": hit_10,
            "hit_15pct": hit_15,
            "per_ticker": per_ticker,
        }

    # ── Exit Simulation ───────────────────────────────────────────────

    def sweep(self) -> list[ExitStrategyReport]:
        """Sweep common exit strategies across all candidates."""
        strategies = [
            # Hold to session end (baseline)
            {},
            # Fixed TP/SL combos
            {"tp_pct": 5.0, "sl_pct": 5.0},
            {"tp_pct": 5.0, "sl_pct": 10.0},
            {"tp_pct": 5.0, "sl_pct": 15.0},
            {"tp_pct": 10.0, "sl_pct": 5.0},
            {"tp_pct": 10.0, "sl_pct": 10.0},
            {"tp_pct": 10.0, "sl_pct": 15.0},
            {"tp_pct": 15.0, "sl_pct": 5.0},
            {"tp_pct": 15.0, "sl_pct": 10.0},
            {"tp_pct": 15.0, "sl_pct": 15.0},
            # Trailing stops
            {"trail_pct": 2.0},
            {"trail_pct": 3.0},
            {"trail_pct": 5.0},
            {"trail_pct": 7.0},
            # Trailing + SL combo
            {"trail_pct": 3.0, "sl_pct": 10.0},
            {"trail_pct": 5.0, "sl_pct": 10.0},
            # Time-based
            {"time_stop_min": 30.0},
            {"time_stop_min": 60.0},
        ]

        reports = []
        for params in strategies:
            report = self.run_strategy(**params)
            reports.append(report)
            print(report.summary())

        return reports

    def best_strategies(
        self, reports: list[ExitStrategyReport] | None = None, top_n: int = 5
    ) -> list[ExitStrategyReport]:
        """Return top N strategies by expectancy."""
        if reports is None:
            reports = self.sweep()
        return sorted(reports, key=lambda r: r.expectancy_pct, reverse=True)[:top_n]

    # ── Factor-based exit analysis ─────────────────────────────────────

    def factor_exit_analysis(self, horizon_ms: int = 300_000):
        """At the +5% MFE decision point, test if factors predict continuation.

        For each candidate that hit +5% intra-session:
        1. Find the trade timestamp when price first reached entry_price * 1.05
        2. Compute all factors for that ticker, snapshot values at that moment
        3. Classify: runner (subsequently hit +10%) vs fader (reversed to ≤ 0%)
        4. Run t-tests + Spearman ρ on each factor

        Prints a ranked table of factors by their ability to separate runners
        from faders at the critical exit-or-hold decision point.
        """
        from datetime import datetime
        from zoneinfo import ZoneInfo

        from jerry_trader.services.backtest.factor_lab import FactorLab

        dt_et = datetime.strptime(self.date, "%Y-%m-%d").replace(
            hour=9, minute=30, tzinfo=ZoneInfo("America/New_York")
        )
        session_end_ms = int(dt_et.timestamp() * 1000)

        # FactorLab-derived factor names (exclude ema — redundant with price)
        factor_names = [
            "trade_rate",
            "large_trade_ratio",
            "aggressor_ratio",
            "relative_volume",
            "price_direction",
            "gap_percent",
            "vwap_deviation",
            "volume_acceleration",
            "bid_ask_spread",
            "order_imbalance",
            "quote_rate",
        ]

        # ── Step 1: Find +5% hit moment and classify each candidate ──
        records: list[dict] = []
        skipped_no_data = 0
        skipped_no_5pct = 0

        for cand in self.candidates:
            trades = self._load_trades(cand.symbol)
            if trades.empty:
                skipped_no_data += 1
                continue

            entry_price = self.find_entry_price(trades, cand.first_entry_ms)
            if entry_price is None:
                skipped_no_data += 1
                continue

            # Forward trades from entry to session end
            fwd = trades[trades["ts_ms"] > cand.first_entry_ms]
            fwd = fwd[fwd["ts_ms"] <= session_end_ms]
            if fwd.empty:
                skipped_no_data += 1
                continue

            # Find first trade at +5%
            tp_target = entry_price * 1.05
            hit5 = fwd[fwd["price"] >= tp_target]
            if hit5.empty:
                skipped_no_5pct += 1
                continue

            hit5_ts = int(hit5["ts_ms"].iloc[0])
            hit5_price = float(hit5["price"].iloc[0])

            # Classify runner vs fader from trades after the +5% moment
            after5 = fwd[fwd["ts_ms"] > hit5_ts]
            if after5.empty:
                skipped_no_data += 1
                continue

            after_prices = after5["price"].values
            hit_10 = bool(np.any(after_prices >= entry_price * 1.10))
            hit_be = bool(np.any(after_prices <= entry_price))

            if hit_10:
                label = "runner"
            elif hit_be:
                label = "fader"
            else:
                label = "drift"

            # Continuous: forward return at horizon from +5% point
            fwd_ret = FactorLab._forward_return(
                hit5_ts, hit5_price, trades.set_index("ts_ms"), horizon_ms
            )

            records.append(
                {
                    "ticker": cand.symbol,
                    "entry_ms": cand.first_entry_ms,
                    "entry_price": entry_price,
                    "hit5_ms": hit5_ts,
                    "hit5_price": hit5_price,
                    "label": label,
                    "fwd_ret_pct": fwd_ret,
                }
            )

        print(f"\n── Factor Exit Analysis ({self.date}) ──")
        print(f"  candidates: {len(self.candidates)}")
        print(
            f"  hit +5%: {len(records)}  |  no data: {skipped_no_data}  |  "
            f"never hit +5%: {skipped_no_5pct}"
        )
        if not records:
            print("  No tickers hit +5% — nothing to analyze.")
            return None

        runners = [r for r in records if r["label"] == "runner"]
        faders = [r for r in records if r["label"] == "fader"]
        drifts = [r for r in records if r["label"] == "drift"]
        print(
            f"  runners (→ +10%): {len(runners)}  |  "
            f"faders (→ BE): {len(faders)}  |  "
            f"drift (neither): {len(drifts)}"
        )

        # ── Step 2: Compute factors for each ticker, snapshot at +5% ──
        print(f"\n  Computing factors for {len(records)} tickers...")
        for i, rec in enumerate(records):
            if i % 10 == 0 or i == len(records) - 1:
                print(f"    {i+1}/{len(records)} {rec['ticker']}")
            try:
                fl = FactorLab(self.date)
                fl.load(rec["ticker"])
            except Exception:
                # Skip tickers where factor computation fails (e.g., no quotes)
                continue

            # Trade factors
            for fn in ["trade_rate", "large_trade_ratio", "aggressor_ratio"]:
                try:
                    fl.compute_trade_factor(fn)
                except Exception:
                    pass

            # Bar factors
            for fn in [
                "relative_volume",
                "price_direction",
                "gap_percent",
                "vwap_deviation",
                "volume_acceleration",
            ]:
                try:
                    fl.compute_bar_factor(fn)
                except Exception:
                    pass

            # Quote factors
            for fn in ["bid_ask_spread", "order_imbalance", "quote_rate"]:
                try:
                    fl.compute_quote_factor(fn)
                except Exception:
                    pass

            # Snapshot factor values at +5% moment.
            # Backward-search up to 120 grid points (~4 min at 2s intervals)
            # to find the last valid (non-NaN) value — handles early entries
            # where factor warmup hasn't completed yet.
            df = fl.factors_df()
            idx = df["ts_ms"].searchsorted(rec["hit5_ms"], side="right") - 1
            if idx < 0:
                idx = 0

            # Record price at the grid point (always take the closest)
            if idx < len(df):
                row = df.iloc[idx]
                p = row.get("price")
                rec["grid_price"] = (
                    float(p)
                    if p is not None and not (isinstance(p, float) and np.isnan(p))
                    else None
                )

            # For each factor, backward-search for last valid value
            max_lookback = min(idx, 120)
            for fn in fl._factors:
                found = None
                for offset in range(max_lookback + 1):
                    val = df.iloc[idx - offset].get(fn)
                    if val is not None and not (
                        isinstance(val, float) and np.isnan(val)
                    ):
                        found = float(val)
                        break
                if found is not None:
                    rec[fn] = found

        # ── Step 3: Statistical tests ──
        print(f"\n── Factor vs. Runner/Fader (t-test) ──")
        results: list[dict] = []
        for fn in factor_names:
            runner_vals = [r[fn] for r in records if fn in r and r["label"] == "runner"]
            fader_vals = [r[fn] for r in records if fn in r and r["label"] == "fader"]

            if len(runner_vals) < 5 or len(fader_vals) < 5:
                continue

            t_stat, p_val = stats.ttest_ind(runner_vals, fader_vals, equal_var=False)
            r_mean = float(np.mean(runner_vals))
            f_mean = float(np.mean(fader_vals))
            # Cohen's d (pooled std for interpretable effect size)
            pooled_std = np.sqrt((np.var(runner_vals) + np.var(fader_vals)) / 2)
            cohens_d = float((r_mean - f_mean) / pooled_std) if pooled_std > 0 else 0.0

            results.append(
                {
                    "factor": fn,
                    "runner_mean": round(r_mean, 4),
                    "fader_mean": round(f_mean, 4),
                    "diff": round(r_mean - f_mean, 4),
                    "cohens_d": round(cohens_d, 3),
                    "t_stat": round(t_stat, 3),
                    "p_value": round(p_val, 4),
                    "n_runner": len(runner_vals),
                    "n_fader": len(fader_vals),
                }
            )

        # Sort by |Cohen's d| (effect size matters more than p-value here)
        results.sort(key=lambda x: abs(x["cohens_d"]), reverse=True)

        for r in results:
            sig = " **" if r["p_value"] < 0.01 else " *" if r["p_value"] < 0.05 else ""
            direction = "↑ runner" if r["diff"] > 0 else "↓ fader"
            print(
                f"  {r['factor']:24s}  "
                f"runner={r['runner_mean']:+.4f}  fader={r['fader_mean']:+.4f}  "
                f"d={r['cohens_d']:+.3f}  p={r['p_value']:.4f}{sig}  "
                f"({direction}, n={r['n_runner']}/{r['n_fader']})"
            )

        # ── Step 4: Continuous — Spearman ρ between factor @ +5% and fwd_ret ──
        print(
            f"\n── Factor @ +5% vs. forward return {horizon_ms//60000}min (Spearman ρ) ──"
        )
        fwd_records = [
            r
            for r in records
            if r.get("fwd_ret_pct") is not None and not np.isnan(r["fwd_ret_pct"])
        ]
        for fn in factor_names:
            pairs = [(r[fn], r["fwd_ret_pct"]) for r in fwd_records if fn in r]
            if len(pairs) < 10:
                continue
            x = [p[0] for p in pairs]
            y = [p[1] for p in pairs]
            rho, p_val = stats.spearmanr(x, y)
            sig = " **" if p_val < 0.01 else " *" if p_val < 0.05 else ""
            print(f"  {fn:24s}  ρ={rho:+.4f}  p={p_val:.4f}{sig}  n={len(pairs)}")

        # ── Step 5: Runner vs fader label prediction baseline ──
        n_labeled = len(runners) + len(faders)
        if n_labeled > 0:
            print(f"\n── Baseline ──")
            print(
                f"  runner ratio: {len(runners)}/{n_labeled} "
                f"({len(runners)/n_labeled*100:.1f}%)"
            )
            print(f"  If always-hold → win {len(runners)/n_labeled*100:.1f}%")
            print(
                f"  If always-exit  → 100% of +5% captured, "
                f"forgo {len(runners)} runners (+{5:.0f}%→+10%+)"
            )

        return {
            "records": records,
            "factor_tests": results,
            "n_hit5": len(records),
            "n_runner": len(runners),
            "n_fader": len(faders),
            "n_drift": len(drifts),
        }

    # ── Factor-based exit strategies ───────────────────────────────────

    def _snapshot_factors_at_5pct(self) -> list[dict]:
        """Pre-compute factor snapshots for all candidates at their +5% moment.

        Returns list of dicts with ticker, entry_ms, entry_price, hit5_ms,
        hit5_price, and factor values (trade_rate, bid_ask_spread, etc.).
        Only includes tickers where at least trade_rate is available.
        """
        from datetime import datetime
        from zoneinfo import ZoneInfo

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
            fwd = trades[trades["ts_ms"] > cand.first_entry_ms]
            fwd = fwd[fwd["ts_ms"] <= session_end_ms]
            if fwd.empty:
                continue
            hit5 = fwd[fwd["price"] >= entry_price * 1.05]
            if hit5.empty:
                continue

            hit5_ts = int(hit5["ts_ms"].iloc[0])
            hit5_price = float(hit5["price"].iloc[0])

            rec = {
                "ticker": cand.symbol,
                "entry_ms": cand.first_entry_ms,
                "entry_price": entry_price,
                "hit5_ms": hit5_ts,
                "hit5_price": hit5_price,
            }

            # Compute factors for this ticker
            try:
                fl = FactorLab(self.date)
                fl.load(cand.symbol)
            except Exception:
                continue

            # Per-factor try/except — don't lose trade_rate if bar/quote fails
            for fn in ["trade_rate", "large_trade_ratio", "aggressor_ratio"]:
                try:
                    fl.compute_trade_factor(fn)
                except Exception:
                    pass
            for fn in [
                "relative_volume",
                "price_direction",
                "gap_percent",
                "vwap_deviation",
                "volume_acceleration",
            ]:
                try:
                    fl.compute_bar_factor(fn)
                except Exception:
                    pass
            for fn in ["bid_ask_spread", "order_imbalance", "quote_rate"]:
                try:
                    fl.compute_quote_factor(fn)
                except Exception:
                    pass

            # Snapshot at +5% moment with backward search for each factor
            df = fl.factors_df()
            idx = df["ts_ms"].searchsorted(hit5_ts, side="right") - 1
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

            # Only include if we have at least trade_rate
            if "trade_rate" in rec:
                snapshots.append(rec)

        print(f"  {len(snapshots)} tickers with factor data at +5%")
        return snapshots

    def factor_exit_strategies(self):
        """Test factor-based exit rules against mechanical baselines.

        For each candidate, pre-computes factor values at the +5% hit moment,
        then uses the factor to decide: exit at +5% (safe) or hold for +10%
        (aggressive). Reuses the well-tested simulate_one() for accurate
        trade-by-trade simulation.

        Compares multiple decision thresholds.
        """
        print("\n── Pre-computing factor snapshots at +5% moment ──")
        snapshots = self._snapshot_factors_at_5pct()
        snap_by_ticker = {s["ticker"]: s for s in snapshots}

        from datetime import datetime
        from zoneinfo import ZoneInfo

        dt_et = datetime.strptime(self.date, "%Y-%m-%d").replace(
            hour=9, minute=30, tzinfo=ZoneInfo("America/New_York")
        )
        session_end_ms = int(dt_et.timestamp() * 1000)

        # Decision rules: based on factor values at +5%, pick tp_pct.
        # "hold" → tp=10% (aim higher), "exit" → tp=5% (lock in).
        rules: list[tuple[str, callable]] = []

        for tr_thresh in [0.5, 1.0, 2.0, 3.0, 5.0]:
            t = tr_thresh
            rules.append(
                (
                    f"factor_tr>{t:.1f}",
                    lambda s, t=t: 10.0 if s.get("trade_rate", 0) > t else 5.0,
                )
            )

        for sp_thresh in [500, 1000, 2000]:
            t = sp_thresh
            rules.append(
                (
                    f"factor_sp<{t}",
                    lambda s, t=t: 10.0 if s.get("bid_ask_spread", 99999) < t else 5.0,
                )
            )

        for tr_thresh, sp_thresh in [(1.0, 1000), (1.0, 2000), (2.0, 1000)]:
            tr, sp = tr_thresh, sp_thresh
            rules.append(
                (
                    f"factor_tr>{tr:.0f}_sp<{sp:.0f}",
                    lambda s, tr=tr, sp=sp: (
                        10.0
                        if s.get("trade_rate", 0) > tr
                        and s.get("bid_ask_spread", 99999) < sp
                        else 5.0
                    ),
                )
            )

        # Baselines
        rules.append(("baseline_tp5", lambda s: 5.0))
        rules.append(("baseline_tp10", lambda s: 10.0))
        rules.append(("baseline_hold", lambda s: None))

        print(
            f"\n── Factor-based TP selection ({len(rules)} rules, "
            f"{len(snapshots)} tickers with factor data) ──"
        )

        for rule_name, pick_tp in rules:
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

                # Pick TP based on factor data (if available)
                snap = snap_by_ticker.get(cand.symbol)
                if snap is not None:
                    tp_pct = pick_tp(snap)
                else:
                    tp_pct = 5.0  # no factor data → play safe

                result = self.simulate_one(
                    trades,
                    cand.first_entry_ms,
                    entry_price,
                    session_end_ms,
                    tp_pct=tp_pct,
                )
                if result is None:
                    skipped += 1
                    continue

                result.ticker = cand.symbol
                results.append(result)

            if not results:
                continue

            wins = [r for r in results if r.pnl_pct > 0]
            losses = [r for r in results if r.pnl_pct <= 0]
            n = len(results)

            report = ExitStrategyReport(
                strategy_name=rule_name,
                params={"rule": rule_name},
                n_trades=n,
                n_profitable=len(wins),
                win_rate=len(wins) / n,
                avg_win_pct=float(np.mean([r.pnl_pct for r in wins])) if wins else 0.0,
                avg_loss_pct=(
                    float(np.mean([r.pnl_pct for r in losses])) if losses else 0.0
                ),
                expectancy_pct=float(np.mean([r.pnl_pct for r in results])),
                avg_mfe_pct=float(np.mean([r.mfe_pct for r in results])),
                avg_duration_min=float(np.mean([r.duration_min for r in results])),
                per_ticker=results,
            )
            print(report.summary())

    # ── Tradability filter — factor-based pre-entry quality gate ───────

    def tradability_filter(self):
        """Test whether trade_rate availability at entry filters out zombies.

        Splits candidates by whether trade_rate is computable at entry
        (≥5 trades in a 20s window). Runs exit strategies on each group
        to measure the "zombie drag" on overall performance.
        """
        import numpy as np

        print("\n── Snapshotting factors at entry time ──")
        entries = self._snapshot_factors_at_entry()
        print(f"  {len(entries)} candidates with factor data loaded")

        tradable = [e for e in entries if "trade_rate" in e]
        zombie = [e for e in entries if "trade_rate" not in e]
        n_no_data = len(self.candidates) - len(entries)

        print(f"\n── Tradability Split ──")
        print(
            f"  Tradable (trade_rate valid at entry):  {len(tradable)} "
            f"({len(tradable)/max(len(self.candidates),1)*100:.0f}%)"
        )
        print(
            f"  Zombie (trade_rate NaN at entry):      {len(zombie)} "
            f"({len(zombie)/max(len(self.candidates),1)*100:.0f}%)"
        )
        print(f"  No factor data:                         {n_no_data}")

        for label, group in [("Tradable", tradable), ("Zombie", zombie)]:
            if not group:
                continue
            n_trades = [e["n_fwd_trades"] for e in group]
            gains = [e["gain_pct"] for e in group]
            print(f"\n  {label}:")
            print(
                f"    fwd trades: median={np.median(n_trades):.0f}  "
                f"p25={np.percentile(n_trades, 25):.0f}  "
                f"p75={np.percentile(n_trades, 75):.0f}"
            )
            print(
                f"    entry gain: median={np.median(gains):+.1f}%  "
                f"mean={np.mean(gains):+.1f}%"
            )
            if any("bid_ask_spread" in e for e in group):
                spreads = [
                    e.get("bid_ask_spread")
                    for e in group
                    if e.get("bid_ask_spread") is not None
                    and not np.isnan(e.get("bid_ask_spread", np.nan))
                ]
                if spreads:
                    print(
                        f"    bid_ask_spread: median={np.median(spreads):.0f}bps  "
                        f"p75={np.percentile(spreads, 75):.0f}bps"
                    )

        # ── Run exit strategies on each group ──
        from datetime import datetime
        from zoneinfo import ZoneInfo

        dt_et = datetime.strptime(self.date, "%Y-%m-%d").replace(
            hour=9, minute=30, tzinfo=ZoneInfo("America/New_York")
        )
        session_end_ms = int(dt_et.timestamp() * 1000)

        original = self.candidates
        ticker_set_all = {c.symbol for c in original}
        tradable_set = {e["ticker"] for e in tradable}
        zombie_set = {e["ticker"] for e in zombie}

        for group_label, ticker_set in [
            ("ALL", ticker_set_all),
            ("TRADABLE", tradable_set),
            ("ZOMBIE", zombie_set),
        ]:
            print(f"\n── {group_label} ({len(ticker_set)} tickers) ──")
            self.candidates = [c for c in original if c.symbol in ticker_set]

            for params in [{}, {"tp_pct": 5.0}, {"tp_pct": 10.0}, {"trail_pct": 5.0}]:
                report = self.run_strategy(**params)
                print(f"  {report.summary()}")

        self.candidates = original

        # ── Zombie drag ──
        if tradable and zombie:
            self.candidates = [c for c in original if c.symbol in tradable_set]
            t_report = self.run_strategy(tp_pct=5.0)
            self.candidates = [c for c in original if c.symbol in zombie_set]
            z_report = self.run_strategy(tp_pct=5.0)
            self.candidates = original

            print(f"\n── Zombie Drag (tp5) ──")
            print(
                f"  Tradable only:  expectancy={t_report.expectancy_pct:+.2f}%  "
                f"win_rate={t_report.win_rate:.1%}  n={t_report.n_trades}"
            )
            print(
                f"  Zombie only:    expectancy={z_report.expectancy_pct:+.2f}%  "
                f"win_rate={z_report.win_rate:.1%}  n={z_report.n_trades}"
            )
            delta = t_report.expectancy_pct - z_report.expectancy_pct
            print(
                f"  Δ expectancy:   {delta:+.2f}% — filtering zombies "
                f"{'adds' if delta > 0 else 'costs'} {abs(delta):.2f}%"
            )

    def tradability_score(
        self, weights: dict[str, float] | None = None
    ) -> pd.DataFrame:
        """Multi-dimensional 0-1 tradability scoring for all candidates.

        Computes per-dimension scores and a weighted composite.  All scores
        gracefully handle missing data (NaN / missing → score 0).

        Dimensions:
          - score_trade_rate:   trade density at entry (linear, 15 trades/min = 1.0)
          - score_spread:       inverse of bid-ask spread in bps (200bps = 0.0)
          - score_price:        distance from $1–$20 sweet spot (Gaussian)
          - score_float:        float shares <20M preference (logistic decay)
          - score_country:      non-CN/HK = 1.0, CN/HK = 0.0
          - score_composite:    weighted average of the above

        Args:
            weights: Optional dict overriding default equal weights.
                     Keys: trade_rate, spread, price, float, country.
                     Default: all 0.20 (equal weight).

        Returns:
            DataFrame indexed by ticker with individual + composite scores
            and the entry-level stats (gain_at_entry, n_fwd_trades).
        """
        import os

        import numpy as np
        import polars as pl

        from jerry_trader.platform.config.config import float_shares_dir

        if weights is None:
            weights = {
                "trade_rate": 0.20,
                "spread": 0.20,
                "price": 0.20,
                "float": 0.20,
                "country": 0.20,
            }

        # ---- 1. Load factor snapshots at entry ----
        print("\n── Snapshotting factors at entry ──")
        entries = self._snapshot_factors_at_entry()
        print(f"  {len(entries)} candidates with factor data")

        # ---- 2. Load float shares ----
        float_map: dict[str, float] = {}
        try:
            fs_files = sorted(
                [
                    f
                    for f in os.listdir(float_shares_dir)
                    if f.startswith("float_shares_") and f.endswith(".parquet")
                ]
            )
            if fs_files:
                fs_df = pl.read_parquet(os.path.join(float_shares_dir, fs_files[-1]))
                for row in fs_df.iter_rows(named=True):
                    s = row["symbol"]
                    if s and row["floatShares"] is not None:
                        float_map[s] = float(row["floatShares"])
            print(f"  Float data loaded: {len(float_map)} tickers")
        except Exception:
            print(f"  Float data not available")

        # ---- 3. Score each entry ----
        rows: list[dict] = []
        for e in entries:
            ticker = e["ticker"]
            profile = self._profiles.get(ticker, {})

            # --- trade_rate (linear: 15 trades/min = 1.0) ---
            tr = e.get("trade_rate")
            score_tr = (
                min(float(tr) / 15.0, 1.0)
                if tr is not None and not np.isnan(tr)
                else 0.0
            )

            # --- spread (inverse linear: 200bps = 0.0, 0bps = 1.0) ---
            sp = e.get("bid_ask_spread")
            score_sp = (
                max(0.0, 1.0 - float(sp) / 200.0)
                if sp is not None and not np.isnan(sp)
                else 0.0
            )

            # --- price (Gaussian around $8, $1–$20 central band) ---
            entry_price = e.get("entry_price", 0)
            if entry_price and entry_price > 0:
                # Gaussian: peak at $8, σ so $1 and $20 are ~0.3 each
                z = (entry_price - 8.0) / 7.0  # $1→-1σ, $20→+1.7σ
                score_pr = float(np.exp(-0.5 * z * z))
            else:
                score_pr = 0.0

            # --- float (<20M ideal, logistic decay beyond) ---
            fs = float_map.get(ticker)
            if fs is not None and fs > 0:
                # Logistic: 1.0 at 10M, 0.5 at 20M, ~0 at 50M
                fs_m = fs / 1_000_000
                score_fl = float(1.0 / (1.0 + np.exp((fs_m - 20.0) / 8.0)))
            else:
                score_fl = 0.0

            # --- country (non-CN/HK = 1.0, missing = 0.0) ---
            country = profile.get("country")
            if country is None:
                score_ct = 0.0
            else:
                score_ct = 0.0 if country in ("CN", "HK") else 1.0

            # --- composite ---
            composite = (
                weights.get("trade_rate", 0) * score_tr
                + weights.get("spread", 0) * score_sp
                + weights.get("price", 0) * score_pr
                + weights.get("float", 0) * score_fl
                + weights.get("country", 0) * score_ct
            )
            total_w = sum(weights.values())
            composite /= total_w if total_w > 0 else 1.0

            rows.append(
                {
                    "ticker": ticker,
                    "entry_ms": e["entry_ms"],
                    "entry_price": entry_price,
                    "gain_pct": e.get("gain_pct", 0),
                    "n_fwd_trades": e.get("n_fwd_trades", 0),
                    "trade_rate": e.get("trade_rate"),
                    "spread_bps": e.get("bid_ask_spread"),
                    "float_shares": fs,
                    "country": country,
                    "sector": profile.get("sector", ""),
                    "score_trade_rate": round(score_tr, 3),
                    "score_spread": round(score_sp, 3),
                    "score_price": round(score_pr, 3),
                    "score_float": round(score_fl, 3),
                    "score_country": round(score_ct, 3),
                    "score_composite": round(composite, 3),
                }
            )

        if not rows:
            print(
                "\n  WARNING: No candidates with factor data — all FactorLab loads failed"
            )
            return pd.DataFrame()

        df = (
            pd.DataFrame(rows)
            .set_index("ticker")
            .sort_values("score_composite", ascending=False)
        )

        # ---- 4. Print score distribution ----
        score_cols = [
            "score_trade_rate",
            "score_spread",
            "score_price",
            "score_float",
            "score_country",
            "score_composite",
        ]
        print(f"\n── Score Distribution ({len(df)} candidates) ──")
        for col in score_cols:
            vals = df[col]
            n_nonzero = (vals > 0).sum()
            print(
                f"  {col:22s}  median={vals.median():.3f}  "
                f"mean={vals.mean():.3f}  "
                f">0={n_nonzero}/{len(vals)} ({n_nonzero/len(vals)*100:.0f}%)"
            )

        print(f"\n── Top 10 by composite score ──")
        top = df.head(10)
        for t, row in top.iterrows():
            print(
                f"  {t:6s}  composite={row['score_composite']:.3f}  "
                f"tr={row['trade_rate']:.1f}  sp={row['spread_bps']:.0f}bps  "
                f"price=${row['entry_price']:.1f}  float={row['float_shares']/1e6:.1f}M  "
                f"country={row['country']}"
            )

        return df

    # ── Layer 2: Tradability Experiments ─────────────────────────────────

    def tradability_analysis(self):
        """Layer 2 experiment: dimension tercile splits + composite sweep.

        Phase A — For each of 5 dimensions, split candidates into terciles
        (Low/Med/High) by score, run baseline hold + tp5 strategies on each
        tercile, and report expectancy / win_rate / median MFE.

        Phase B — Sweep composite score threshold from 0.0–1.0 with finer
        granularity in the 0.15–0.50 range where filtering is most likely
        to matter.  Reports the expectation vs retention trade-off curve.
        """
        import numpy as np

        # ── Compute scores once ──
        df = self.tradability_score()
        score_map = df.to_dict("index")
        scored_tickers = set(score_map.keys())
        original = self.candidates

        if len(scored_tickers) < 6:
            print("  Not enough scored candidates (<6) for analysis")
            return

        dimensions = {
            "trade_rate": "score_trade_rate",
            "spread": "score_spread",
            "price": "score_price",
            "float": "score_float",
            "country": "score_country",
        }

        # ═══════════════════════════════════════════════════════════════
        # Phase A: Single-dimension tercile analysis
        # ═══════════════════════════════════════════════════════════════
        print("\n" + "=" * 82)
        print("PHASE A: Single-Dimension Tercile Analysis")
        print("=" * 82)

        phase_a_rows: list[dict] = []

        for dim_name, dim_col in dimensions.items():
            scored = [(t, score_map[t][dim_col]) for t in scored_tickers]
            scored.sort(key=lambda x: x[1])
            n = len(scored)
            k = n // 3
            if k < 2:
                continue

            terciles = {
                "Low": scored[:k],
                "Med": scored[k : 2 * k],
                "High": scored[2 * k :],
            }

            print(f"\n── {dim_name} ({dim_col}) ──")
            header = (
                f"  {'Tercile':<7s} {'N':>4s}  "
                f"{'Baseline':>10s}  {'Win%':>6s}  "
                f"{'Tp5 Exp':>9s}  {'Tp5Win%':>7s}  {'MedMFE':>8s}"
            )
            print(header)
            print(
                f"  {'-'*7} {'-'*4}  {'-'*10}  {'-'*6}  " f"{'-'*9}  {'-'*7}  {'-'*8}"
            )

            for label, tickers in terciles.items():
                ticker_set = {t for t, _ in tickers}
                self.candidates = [c for c in original if c.symbol in ticker_set]

                if not self.candidates:
                    continue

                base_r = self.run_strategy()
                tp5_r = self.run_strategy(tp_pct=5.0)
                mfe = self.mfe_summary()

                phase_a_rows.append(
                    {
                        "dim": dim_name,
                        "tercile": label,
                        "n": tp5_r.n_trades,
                        "baseline_exp": base_r.expectancy_pct,
                        "baseline_win": base_r.win_rate,
                        "tp5_exp": tp5_r.expectancy_pct,
                        "tp5_win": tp5_r.win_rate,
                        "median_mfe": mfe.get("median_mfe", 0),
                        "mean_mfe": mfe.get("mean_mfe", 0),
                    }
                )

                print(
                    f"  {label:<7s} {tp5_r.n_trades:>4d}  "
                    f"{base_r.expectancy_pct:>+9.2f}%  "
                    f"{base_r.win_rate:>5.0%}  "
                    f"{tp5_r.expectancy_pct:>+8.2f}%  "
                    f"{tp5_r.win_rate:>6.0%}  "
                    f"{mfe.get('median_mfe', 0):>+7.2f}%"
                )

            # Delta: High - Low for tp5
            high_rows = [
                r
                for r in phase_a_rows
                if r["dim"] == dim_name and r["tercile"] == "High"
            ]
            low_rows = [
                r
                for r in phase_a_rows
                if r["dim"] == dim_name and r["tercile"] == "Low"
            ]
            if high_rows and low_rows:
                d_exp = high_rows[0]["tp5_exp"] - low_rows[0]["tp5_exp"]
                d_mfe = high_rows[0]["median_mfe"] - low_rows[0]["median_mfe"]
                print(
                    f"  Δ High-Low:  tp5 expectancy {d_exp:+.2f}%  "
                    f"median MFE {d_mfe:+.2f}%"
                )

        self.candidates = original

        # ═══════════════════════════════════════════════════════════════
        # Phase B: Composite threshold sweep
        # ═══════════════════════════════════════════════════════════════
        print("\n" + "=" * 82)
        print("PHASE B: Composite Threshold Sweep (tp_pct=5.0)")
        print("=" * 82)

        thresholds = [
            0.0,
            0.10,
            0.15,
            0.20,
            0.25,
            0.30,
            0.35,
            0.40,
            0.45,
            0.50,
            0.55,
            0.60,
            0.70,
            0.80,
        ]
        # Gather all composites for percentiles
        all_composites = sorted(
            [score_map[t]["score_composite"] for t in scored_tickers],
            reverse=True,
        )
        total_scored = len(all_composites)

        header2 = (
            f"  {'Thresh':>6s}  {'N':>4s}  {'Keep%':>5s}  "
            f"{'Expectancy':>10s}  {'WinRate':>8s}  "
            f"{'AvgWin':>8s}  {'AvgLoss':>8s}  {'MedMFE':>8s}"
        )
        print(header2)
        print(
            f"  {'-'*6}  {'-'*4}  {'-'*5}  {'-'*10}  {'-'*8}  "
            f"{'-'*8}  {'-'*8}  {'-'*8}"
        )

        best_exp = -999.0
        best_thresh = 0.0

        for thresh in thresholds:
            passing = [
                t for t in scored_tickers if score_map[t]["score_composite"] >= thresh
            ]
            if len(passing) < 3:
                continue
            ticker_set = set(passing)
            self.candidates = [c for c in original if c.symbol in ticker_set]

            tp5_r = self.run_strategy(tp_pct=5.0)
            mfe = self.mfe_summary()
            keep_pct = len(passing) / max(total_scored, 1) * 100

            marker = "  ← ALL" if thresh == 0.0 else ""
            if tp5_r.expectancy_pct > best_exp:
                best_exp = tp5_r.expectancy_pct
                best_thresh = thresh
                marker += " ★"

            print(
                f"  {thresh:>6.2f}  {tp5_r.n_trades:>4d}  "
                f"{keep_pct:>4.0f}%  "
                f"{tp5_r.expectancy_pct:>+9.2f}%  "
                f"{tp5_r.win_rate:>7.1%}  "
                f"{tp5_r.avg_win_pct:>+7.2f}%  "
                f"{tp5_r.avg_loss_pct:>+7.2f}%  "
                f"{mfe.get('median_mfe', 0):>+7.2f}%{marker}"
            )

        self.candidates = original

        # ── Summary ──
        print(f"\n── Layer 2 Summary ──")
        print(f"  Best threshold: {best_thresh:.2f} " f"(expectancy={best_exp:+.2f}%)")

        # Rank dimensions by High-Low delta
        dim_deltas = {}
        for dim_name in dimensions:
            dim_rows = [r for r in phase_a_rows if r["dim"] == dim_name]
            high = next((r for r in dim_rows if r["tercile"] == "High"), None)
            low = next((r for r in dim_rows if r["tercile"] == "Low"), None)
            if high and low:
                dim_deltas[dim_name] = high["tp5_exp"] - low["tp5_exp"]

        if dim_deltas:
            ranked = sorted(dim_deltas.items(), key=lambda x: x[1], reverse=True)
            print(f"  Dimension importance (High-Low tp5 expectancy Δ):")
            for name, delta in ranked:
                bar = "█" * max(1, int(abs(delta) * 5))
                sign = "+" if delta >= 0 else ""
                print(f"    {name:<15s} {sign}{delta:.2f}%  {bar}")

        return phase_a_rows

    def tradability_score_v2(self) -> pd.DataFrame:
        """Revised data-driven tradability scoring.

        V1 priors were rejected by Phase A data.  V2 inverts the mal-signed
        dimensions based on empirical tercile analysis:

          - float_v2:  HIGH float = higher score (large floats trend better)
          - price_v2:  FAR from $8 = higher score (extreme prices have bigger % moves)
          - country:   same as V1 (non-CN/HK = 1.0)
          - trade_rate / spread: retained but de-weighted (low data coverage)

        Composite uses only the three coverage-rich dimensions:
            0.333 * price_v2 + 0.333 * float_v2 + 0.333 * country
        """
        import numpy as np

        entries = self._snapshot_factors_at_entry()
        if not entries:
            print("\n  WARNING: No candidates with factor data")
            return pd.DataFrame()

        # Float data
        float_map: dict[str, float] = {}
        try:
            fs_files = sorted(
                f
                for f in os.listdir(float_shares_dir)
                if f.startswith("float_shares_") and f.endswith(".parquet")
            )
            if fs_files:
                fs_df = pl.read_parquet(os.path.join(float_shares_dir, fs_files[-1]))
                for row in fs_df.iter_rows(named=True):
                    s = row["symbol"]
                    if s and row["floatShares"] is not None:
                        float_map[s] = float(row["floatShares"])
        except Exception:
            pass

        rows: list[dict] = []
        for e in entries:
            ticker = e["ticker"]
            profile = self._profiles.get(ticker, {})

            # --- trade_rate (V1, kept for reference) ---
            tr = e.get("trade_rate")
            score_tr = (
                min(float(tr) / 15.0, 1.0)
                if tr is not None and not np.isnan(tr)
                else 0.0
            )

            # --- spread (V1, kept for reference) ---
            sp = e.get("bid_ask_spread")
            score_sp = (
                max(0.0, 1.0 - float(sp) / 200.0)
                if sp is not None and not np.isnan(sp)
                else 0.0
            )

            # --- price_v2: inverted Gaussian — further from $8 = better ---
            entry_price = e.get("entry_price", 0)
            if entry_price and entry_price > 0:
                z = (entry_price - 8.0) / 7.0
                score_pr_v1 = float(np.exp(-0.5 * z * z))  # original
                score_pr_v2 = 1.0 - score_pr_v1  # inverted
            else:
                score_pr_v2 = 0.0

            # --- float_v2: inverted logistic — larger float = better ---
            fs = float_map.get(ticker)
            if fs is not None and fs > 0:
                fs_m = fs / 1_000_000
                score_fl_v1 = float(1.0 / (1.0 + np.exp((fs_m - 20.0) / 8.0)))
                score_fl_v2 = 1.0 - score_fl_v1  # inverted
            else:
                score_fl_v2 = 0.0

            # --- country (same as V1) ---
            country = profile.get("country")
            if country is None:
                score_ct = 0.0
            else:
                score_ct = 0.0 if country in ("CN", "HK") else 1.0

            # --- composite_v2: only the 3 coverage-rich dimensions ---
            composite_v2 = (score_pr_v2 + score_fl_v2 + score_ct) / 3.0

            rows.append(
                {
                    "ticker": ticker,
                    "entry_price": entry_price,
                    "gain_pct": e.get("gain_pct", 0),
                    "trade_rate": e.get("trade_rate"),
                    "spread_bps": e.get("bid_ask_spread"),
                    "float_shares": fs,
                    "country": country,
                    # V1 scores (reference)
                    "score_trade_rate": round(score_tr, 3),
                    "score_spread": round(score_sp, 3),
                    "score_price_v1": round(1.0 - score_pr_v2, 3),  # original direction
                    "score_float_v1": round(1.0 - score_fl_v2, 3),
                    "score_country": round(score_ct, 3),
                    # V2 scores (data-driven)
                    "score_price_v2": round(score_pr_v2, 3),
                    "score_float_v2": round(score_fl_v2, 3),
                    "score_composite_v2": round(composite_v2, 3),
                }
            )

        df = (
            pd.DataFrame(rows)
            .set_index("ticker")
            .sort_values("score_composite_v2", ascending=False)
        )

        print(f"\n── V2 Score Distribution ({len(df)} candidates) ──")
        for col in [
            "score_price_v2",
            "score_float_v2",
            "score_country",
            "score_composite_v2",
        ]:
            vals = df[col]
            n_nonzero = (vals > 0).sum()
            print(
                f"  {col:22s}  median={vals.median():.3f}  "
                f"mean={vals.mean():.3f}  "
                f">0={n_nonzero}/{len(vals)} ({n_nonzero/len(vals)*100:.0f}%)"
            )

        print(f"\n── V2 Top 10 ──")
        for t, row in df.head(10).iterrows():
            print(
                f"  {t:6s}  composite={row['score_composite_v2']:.3f}  "
                f"price=${row['entry_price']:.1f}  "
                f"float={row['float_shares']/1e6:.1f}M  "
                f"country={row['country']}"
            )

        return df

    def tradability_analysis_v2(self):
        """Phase C: Revised data-driven scoring — tercile + threshold sweep.

        Uses V2 scoring where float and price dimensions are inverted based
        on Phase A findings.  Only the three coverage-rich dimensions
        (price_v2, float_v2, country) are used for the composite.
        """
        import numpy as np

        df = self.tradability_score_v2()
        if df.empty:
            return

        score_map = df.to_dict("index")
        scored_tickers = set(score_map.keys())
        original = self.candidates

        if len(scored_tickers) < 6:
            print("  Not enough scored candidates (<6) for analysis")
            return

        dims_v2 = {
            "price_v2": "score_price_v2",
            "float_v2": "score_float_v2",
            "country": "score_country",
        }

        # ═══════════════════════════════════════════════════════════════
        # Phase C-A: Single-dimension tercile analysis (V2 scoring)
        # ═══════════════════════════════════════════════════════════════
        print("\n" + "=" * 82)
        print("PHASE C-A: V2 Single-Dimension Tercile Analysis (inverted signs)")
        print("=" * 82)

        phase_c_rows: list[dict] = []

        for dim_name, dim_col in dims_v2.items():
            scored = [(t, score_map[t][dim_col]) for t in scored_tickers]
            scored.sort(key=lambda x: x[1])
            n = len(scored)
            k = n // 3
            if k < 2:
                continue

            terciles = {
                "Low": scored[:k],
                "Med": scored[k : 2 * k],
                "High": scored[2 * k :],
            }

            print(f"\n── {dim_name} ({dim_col}) ──")
            header = (
                f"  {'Tercile':<7s} {'N':>4s}  "
                f"{'Baseline':>10s}  {'Win%':>6s}  "
                f"{'Tp5 Exp':>9s}  {'Tp5Win%':>7s}  {'MedMFE':>8s}"
            )
            print(header)
            print(
                f"  {'-'*7} {'-'*4}  {'-'*10}  {'-'*6}  " f"{'-'*9}  {'-'*7}  {'-'*8}"
            )

            for label, tickers in terciles.items():
                ticker_set = {t for t, _ in tickers}
                self.candidates = [c for c in original if c.symbol in ticker_set]
                if not self.candidates:
                    continue

                base_r = self.run_strategy()
                tp5_r = self.run_strategy(tp_pct=5.0)
                mfe = self.mfe_summary()

                phase_c_rows.append(
                    {
                        "dim": dim_name,
                        "tercile": label,
                        "n": tp5_r.n_trades,
                        "baseline_exp": base_r.expectancy_pct,
                        "baseline_win": base_r.win_rate,
                        "tp5_exp": tp5_r.expectancy_pct,
                        "tp5_win": tp5_r.win_rate,
                        "median_mfe": mfe.get("median_mfe", 0),
                        "mean_mfe": mfe.get("mean_mfe", 0),
                    }
                )

                print(
                    f"  {label:<7s} {tp5_r.n_trades:>4d}  "
                    f"{base_r.expectancy_pct:>+9.2f}%  "
                    f"{base_r.win_rate:>5.0%}  "
                    f"{tp5_r.expectancy_pct:>+8.2f}%  "
                    f"{tp5_r.win_rate:>6.0%}  "
                    f"{mfe.get('median_mfe', 0):>+7.2f}%"
                )

            # Delta
            high_r = [
                r
                for r in phase_c_rows
                if r["dim"] == dim_name and r["tercile"] == "High"
            ]
            low_r = [
                r
                for r in phase_c_rows
                if r["dim"] == dim_name and r["tercile"] == "Low"
            ]
            if high_r and low_r:
                d_exp = high_r[0]["tp5_exp"] - low_r[0]["tp5_exp"]
                d_mfe = high_r[0]["median_mfe"] - low_r[0]["median_mfe"]
                print(
                    f"  Δ High-Low:  tp5 expectancy {d_exp:+.2f}%  "
                    f"median MFE {d_mfe:+.2f}%"
                )

        self.candidates = original

        # ═══════════════════════════════════════════════════════════════
        # Phase C-B: V2 Composite threshold sweep
        # ═══════════════════════════════════════════════════════════════
        print("\n" + "=" * 82)
        print("PHASE C-B: V2 Composite Threshold Sweep (tp_pct=5.0)")
        print("=" * 82)

        thresholds = [
            0.0,
            0.10,
            0.15,
            0.20,
            0.25,
            0.30,
            0.35,
            0.40,
            0.45,
            0.50,
            0.55,
            0.60,
            0.65,
            0.70,
            0.75,
            0.80,
        ]
        all_composites = sorted(
            [score_map[t]["score_composite_v2"] for t in scored_tickers],
            reverse=True,
        )
        total_scored = len(all_composites)

        header2 = (
            f"  {'Thresh':>6s}  {'N':>4s}  {'Keep%':>5s}  "
            f"{'Expectancy':>10s}  {'WinRate':>8s}  "
            f"{'AvgWin':>8s}  {'AvgLoss':>8s}  {'MedMFE':>8s}"
        )
        print(header2)
        print(
            f"  {'-'*6}  {'-'*4}  {'-'*5}  {'-'*10}  {'-'*8}  "
            f"{'-'*8}  {'-'*8}  {'-'*8}"
        )

        best_exp = -999.0
        best_thresh = 0.0

        for thresh in thresholds:
            passing = [
                t
                for t in scored_tickers
                if score_map[t]["score_composite_v2"] >= thresh
            ]
            if len(passing) < 3:
                continue
            ticker_set = set(passing)
            self.candidates = [c for c in original if c.symbol in ticker_set]

            tp5_r = self.run_strategy(tp_pct=5.0)
            mfe = self.mfe_summary()
            keep_pct = len(passing) / max(total_scored, 1) * 100

            marker = "  ← ALL" if thresh == 0.0 else ""
            if tp5_r.expectancy_pct > best_exp:
                best_exp = tp5_r.expectancy_pct
                best_thresh = thresh
                marker += " ★"

            print(
                f"  {thresh:>6.2f}  {tp5_r.n_trades:>4d}  "
                f"{keep_pct:>4.0f}%  "
                f"{tp5_r.expectancy_pct:>+9.2f}%  "
                f"{tp5_r.win_rate:>7.1%}  "
                f"{tp5_r.avg_win_pct:>+7.2f}%  "
                f"{tp5_r.avg_loss_pct:>+7.2f}%  "
                f"{mfe.get('median_mfe', 0):>+7.2f}%{marker}"
            )

        self.candidates = original

        # ── V1 vs V2 Comparison ──
        print(f"\n── V1 vs V2 Comparison ──")
        print(f"  V1 best threshold: N/A (monotonic decay — 0.00 was best)")
        print(
            f"  V2 best threshold: {best_thresh:.2f} "
            f"(expectancy={best_exp:+.2f}%, "
            f"n={sum(1 for t in scored_tickers if score_map[t]['score_composite_v2'] >= best_thresh)})"
        )

        # Rank V2 dimensions
        dim_deltas = {}
        for dim_name in dims_v2:
            dim_rows = [r for r in phase_c_rows if r["dim"] == dim_name]
            high = next((r for r in dim_rows if r["tercile"] == "High"), None)
            low = next((r for r in dim_rows if r["tercile"] == "Low"), None)
            if high and low:
                dim_deltas[dim_name] = high["tp5_exp"] - low["tp5_exp"]

        if dim_deltas:
            ranked = sorted(dim_deltas.items(), key=lambda x: x[1], reverse=True)
            print(f"\n  V2 Dimension importance (High-Low tp5 expectancy Δ):")
            for name, delta in ranked:
                bar = "█" * max(1, int(abs(delta) * 5))
                sign = "+" if delta >= 0 else ""
                print(f"    {name:<15s} {sign}{delta:.2f}%  {bar}")

        return phase_c_rows

    # ── Ticker profiles: data-driven layer analysis ─────────────────────

    def ticker_profiles(self, csv_path: str | None = None) -> pd.DataFrame:
        """One row per ticker: static + factor + outcome data for manual review.

        Computes factor snapshots at entry, loads float shares, runs baseline
        (hold) and tp5 exit strategies, then merges everything into a single
        DataFrame.  Use this to study per-ticker patterns before designing
        Boolean layer logic.

        Columns:
          Identity:    ticker
          Entry:       entry_time (HH:MM:SS ET), entry_price, gain_at_entry,
                       prev_close, volume_at_entry, relative_volume, max_gain
          Factors:     trade_rate, bid_ask_spread, large_trade_ratio,
                       aggressor_ratio, n_fwd_trades
          Static:      country, sector, industry, exchange, market_cap
          Float:       float_shares
          Outcome:     entry_to_peak_pct (max possible return entry→09:30 ET),
                       tp5_pnl_pct, tp5_mfe_pct, tp5_exit_reason,
                       tp5_duration_min, tp5_win (bool),
                       baseline_pnl_pct, baseline_mfe_pct, baseline_exit_reason,
                       hit_5pct, hit_10pct
          Ignition:    {method}_ignition_ms, {method}_ignition_time (HH:MM:SS ET),
                       {method}_ignition_price, {method}_ignition_delay_sec,
                       {method}_ignition_to_peak_pct (from ignition → 09:30 ET)
                       Methods: vol_exp, range_breakout, trade_rate_surge,
                       sustained_activity, spread_collapse (requires quotes)
          All returns are premarket only (entry → 09:30 ET).

        Args:
            csv_path: If set, saves the DataFrame to this path.

        Returns:
            DataFrame with one row per candidate that has at least entry_price.
        """
        import os
        from datetime import datetime
        from zoneinfo import ZoneInfo

        import numpy as np
        import polars as pl

        from jerry_trader.platform.config.config import float_shares_dir

        dt_et = datetime.strptime(self.date, "%Y-%m-%d").replace(
            hour=9, minute=30, tzinfo=ZoneInfo("America/New_York")
        )
        session_end_ms = int(dt_et.timestamp() * 1000)

        # ── 1. Factor snapshots at entry ──
        print("Computing factor snapshots at entry...")
        entries = self._snapshot_factors_at_entry()
        snap_by_ticker: dict[str, dict] = {e["ticker"]: e for e in entries}
        print(f"  {len(entries)} tickers with factor data")

        # ── 2. Float shares ──
        float_map: dict[str, float] = {}
        try:
            fs_files = sorted(
                f
                for f in os.listdir(float_shares_dir)
                if f.startswith("float_shares_") and f.endswith(".parquet")
            )
            if fs_files:
                fs_df = pl.read_parquet(os.path.join(float_shares_dir, fs_files[-1]))
                for row in fs_df.iter_rows(named=True):
                    s = row["symbol"]
                    if s and row["floatShares"] is not None:
                        float_map[s] = float(row["floatShares"])
        except Exception:
            pass

        # ── 3. Per-ticker outcome + merge ──
        rows: list[dict] = []
        skipped_no_trades = 0
        skipped_no_entry = 0

        for cand in self.candidates:
            ticker = cand.symbol
            snap = snap_by_ticker.get(ticker, {})
            profile = self._profiles.get(ticker, {})

            # --- Entry data ---
            trades = self._load_trades(ticker)
            if trades.empty:
                skipped_no_trades += 1
                continue

            entry_price = self.find_entry_price(trades, cand.first_entry_ms)
            if entry_price is None:
                skipped_no_entry += 1
                continue

            # --- Outcome: baseline (hold) ---
            base_result = self.simulate_one(
                trades,
                cand.first_entry_ms,
                entry_price,
                session_end_ms,
            )
            if base_result is None:
                base_pnl = None
                base_mfe = None
                base_exit = "no_fwd_trades"
            else:
                base_pnl = base_result.pnl_pct
                base_mfe = base_result.mfe_pct
                base_exit = base_result.exit_reason

            # --- Outcome: tp5 ---
            tp5_result = self.simulate_one(
                trades,
                cand.first_entry_ms,
                entry_price,
                session_end_ms,
                tp_pct=5.0,
            )
            if tp5_result is None:
                tp5_pnl = None
                tp5_mfe = None
                tp5_exit = "no_fwd_trades"
                tp5_dur = None
                tp5_win = False
            else:
                tp5_pnl = tp5_result.pnl_pct
                tp5_mfe = tp5_result.mfe_pct
                tp5_exit = tp5_result.exit_reason
                tp5_dur = tp5_result.duration_min
                tp5_win = tp5_result.pnl_pct > 0

            # --- MFE flags + entry-to-peak ---
            fwd = trades[trades["ts_ms"] > cand.first_entry_ms]
            fwd = fwd[fwd["ts_ms"] <= session_end_ms]
            if not fwd.empty:
                prices = fwd["price"].values
                times = fwd["ts_ms"].values
                hit_5 = bool(np.any(prices >= entry_price * 1.05))
                hit_10 = bool(np.any(prices >= entry_price * 1.10))
                peak_price = float(np.max(prices))
                entry_to_peak_pct = (peak_price - entry_price) / entry_price * 100

                # --- Breakout diagnostics: short-window peak returns ---
                peak_idx = int(np.argmax(prices))
                time_to_peak_sec = float(
                    (times[peak_idx] - cand.first_entry_ms) / 1000.0
                )
                close_price = float(prices[-1])
                fade_from_peak_pct = (peak_price - close_price) / entry_price * 100

                def _peak_in_window(duration_ms: int) -> float:
                    mask = times <= cand.first_entry_ms + duration_ms
                    if not mask.any():
                        return 0.0
                    win_peak = float(prices[mask].max())
                    return (win_peak - entry_price) / entry_price * 100

                peak_1min_pct = _peak_in_window(60_000)
                peak_3min_pct = _peak_in_window(180_000)
                peak_5min_pct = _peak_in_window(300_000)
                peak_10min_pct = _peak_in_window(600_000)
            else:
                hit_5 = False
                hit_10 = False
                entry_to_peak_pct = 0.0
                time_to_peak_sec = 0.0
                fade_from_peak_pct = 0.0
                peak_1min_pct = 0.0
                peak_3min_pct = 0.0
                peak_5min_pct = 0.0
                peak_10min_pct = 0.0

            # --- Human-readable entry time (ET) ---
            entry_dt = datetime.fromtimestamp(
                cand.first_entry_ms / 1000, tz=ZoneInfo("America/New_York")
            )
            entry_time = entry_dt.strftime("%H:%M:%S")

            # --- Continuous tradability: trade gaps after entry ---
            fwd_ts = fwd["ts_ms"].values if not fwd.empty else np.array([])
            if len(fwd_ts) >= 2:
                gaps_ms = np.diff(fwd_ts)
                max_gap_sec = float(np.max(gaps_ms)) / 1000.0
                # Gaps > threshold buckets
                gap_over_30s = int(np.sum(gaps_ms > 30_000))
                gap_over_60s = int(np.sum(gaps_ms > 60_000))
                gap_over_120s = int(np.sum(gaps_ms > 120_000))
            elif len(fwd_ts) == 1:
                # Only one trade — gap from entry to that trade
                max_gap_sec = float(fwd_ts[0] - cand.first_entry_ms) / 1000.0
                gap_over_30s = 1 if max_gap_sec > 30 else 0
                gap_over_60s = 1 if max_gap_sec > 60 else 0
                gap_over_120s = 1 if max_gap_sec > 120 else 0
            else:
                max_gap_sec = float(session_end_ms - cand.first_entry_ms) / 1000.0
                gap_over_30s = 0
                gap_over_60s = 0
                gap_over_120s = 0

            # Trades in first 5 minutes after entry
            first_5min_end = cand.first_entry_ms + 300_000
            trades_5min = fwd[fwd["ts_ms"] <= first_5min_end] if not fwd.empty else fwd
            n_trades_first_5min = len(trades_5min)

            # Is every 60s bucket in first 5min "active" (≥1 trade)?
            fully_active_5min = True
            if n_trades_first_5min == 0:
                fully_active_5min = False
            elif len(trades_5min) > 0:
                t5_ts = trades_5min["ts_ms"].values
                for bucket_start in range(cand.first_entry_ms, first_5min_end, 60_000):
                    bucket_end = bucket_start + 60_000
                    has_trade = np.any((t5_ts >= bucket_start) & (t5_ts < bucket_end))
                    if not has_trade:
                        fully_active_5min = False
                        break

            # --- Ignition detection: find transition to active trading ---
            quotes = self._load_quotes(ticker)
            ignitions = _detect_ignitions(
                trades, cand.first_entry_ms, session_end_ms, quotes=quotes
            )

            # --- Assemble row ---
            mcap = profile.get("marketCap")
            fs = float_map.get(ticker)
            row = {
                # Identity
                "ticker": ticker,
                # Entry
                "entry_time": entry_time,
                "entry_price": round(entry_price, 4),
                "gain_at_entry": round(cand.gain_at_entry, 2),
                "prev_close": round(cand.prev_close, 4),
                "volume_at_entry": int(cand.volume_at_entry),
                "relative_volume": round(cand.relative_volume, 2),
                "max_gain": round(cand.max_gain, 2),
                # Factors at entry
                "trade_rate": snap.get("trade_rate"),
                "bid_ask_spread": snap.get("bid_ask_spread"),
                "large_trade_ratio": snap.get("large_trade_ratio"),
                "aggressor_ratio": snap.get("aggressor_ratio"),
                "n_fwd_trades": snap.get("n_fwd_trades", 0),
                # Static
                "country": profile.get("country"),
                "sector": profile.get("sector"),
                "industry": profile.get("industry"),
                "exchange": profile.get("exchange"),
                "market_cap": int(mcap) if mcap is not None else None,
                # Float
                "float_shares": int(fs) if fs is not None else None,
                # Continuous tradability (post-entry trade stream)
                "max_trade_gap_sec": round(max_gap_sec, 1),
                "gap_over_30s": gap_over_30s,
                "gap_over_60s": gap_over_60s,
                "gap_over_120s": gap_over_120s,
                "n_trades_first_5min": n_trades_first_5min,
                "first_5min_fully_active": fully_active_5min,
                # Outcome — premarket (entry → 09:30 ET)
                "entry_to_peak_pct": round(entry_to_peak_pct, 2),
                "tp5_pnl_pct": round(tp5_pnl, 4) if tp5_pnl is not None else None,
                "tp5_mfe_pct": round(tp5_mfe, 2) if tp5_mfe is not None else None,
                "tp5_exit_reason": tp5_exit,
                "tp5_duration_min": round(tp5_dur, 2) if tp5_dur is not None else None,
                "tp5_win": tp5_win,
                # Outcome — baseline
                "baseline_pnl_pct": (
                    round(base_pnl, 4) if base_pnl is not None else None
                ),
                "baseline_mfe_pct": (
                    round(base_mfe, 2) if base_mfe is not None else None
                ),
                "baseline_exit_reason": base_exit,
                # Outcome — MFE flags
                "hit_5pct": hit_5,
                "hit_10pct": hit_10,
                # Breakout diagnostics — short-window peak + fade
                "peak_1min_pct": round(peak_1min_pct, 2),
                "peak_3min_pct": round(peak_3min_pct, 2),
                "peak_5min_pct": round(peak_5min_pct, 2),
                "peak_10min_pct": round(peak_10min_pct, 2),
                "time_to_peak_sec": round(time_to_peak_sec, 1),
                "fade_from_peak_pct": round(fade_from_peak_pct, 2),
                # Breakout label — manual review (empty by default)
                "break_label": "",
            }

            # --- Ignition columns: per-method ignition point + outcome ---
            for method in _IGNITION_METHODS:
                ig = ignitions.get(method) if ignitions else None
                if ig and ig.get("ms"):
                    ig_ms = ig["ms"]
                    ig_price = ig["price"]
                    row[f"{method}_ignition_ms"] = ig_ms
                    ig_dt = datetime.fromtimestamp(
                        ig_ms / 1000, tz=ZoneInfo("America/New_York")
                    )
                    row[f"{method}_ignition_time"] = ig_dt.strftime("%H:%M:%S")
                    row[f"{method}_ignition_price"] = round(ig_price, 4)
                    row[f"{method}_ignition_delay_sec"] = round(
                        (ig_ms - cand.first_entry_ms) / 1000.0, 1
                    )
                    # Peak after ignition → session end
                    after_ig = fwd[fwd["ts_ms"] > ig_ms] if not fwd.empty else fwd
                    if not after_ig.empty:
                        ig_peak = float(after_ig["price"].max())
                        ig_to_peak = (ig_peak - ig_price) / ig_price * 100
                    else:
                        ig_to_peak = 0.0
                    row[f"{method}_ignition_to_peak_pct"] = round(ig_to_peak, 2)
                else:
                    row[f"{method}_ignition_ms"] = None
                    row[f"{method}_ignition_time"] = None
                    row[f"{method}_ignition_price"] = None
                    row[f"{method}_ignition_delay_sec"] = None
                    row[f"{method}_ignition_to_peak_pct"] = None

            rows.append(row)

        df = pd.DataFrame(rows)
        if df.empty:
            print("No ticker profiles generated.")
            return df

        # Sort by tp5_pnl descending (best performers first)
        df = df.sort_values("tp5_pnl_pct", ascending=False, na_position="last")

        print(f"\n── Ticker Profiles ({self.date}, premarket 04:00–09:30 ET) ──")
        print(
            f"  {len(df)} tickers  |  skipped: {skipped_no_trades} no-trades, "
            f"{skipped_no_entry} no-entry-price"
        )
        print(f"  All returns limited to premarket session (entry → 09:30 ET)")
        n_tp5_win = df["tp5_win"].sum()
        n_tp5_total = df["tp5_pnl_pct"].notna().sum()
        print(
            f"  tp5 win rate: {n_tp5_win}/{n_tp5_total} "
            f"({n_tp5_win/max(n_tp5_total,1)*100:.1f}%)"
        )
        print(f"  tp5 expectancy: {df['tp5_pnl_pct'].mean():+.2f}%")
        print(
            f"  has trade_rate: {df['trade_rate'].notna().sum()}/{len(df)} "
            f"({df['trade_rate'].notna().sum()/len(df)*100:.0f}%)"
        )
        print(
            f"  has spread:     {df['bid_ask_spread'].notna().sum()}/{len(df)} "
            f"({df['bid_ask_spread'].notna().sum()/len(df)*100:.0f}%)"
        )
        print(
            f"  has country:    {df['country'].notna().sum()}/{len(df)} "
            f"({df['country'].notna().sum()/len(df)*100:.0f}%)"
        )
        print(
            f"  has float:      {df['float_shares'].notna().sum()}/{len(df)} "
            f"({df['float_shares'].notna().sum()/len(df)*100:.0f}%)"
        )
        print(f"\n── Ignition Detection ──")
        for method in _IGNITION_METHODS:
            col = f"{method}_ignition_ms"
            n_detected = df[col].notna().sum() if col in df.columns else 0
            if n_detected == 0:
                print(f"  {method:<22s} never triggered")
                continue
            delays = df[f"{method}_ignition_delay_sec"].dropna()
            ig_to_peak = df[f"{method}_ignition_to_peak_pct"].dropna()
            med_delay = delays.median()
            med_peak = ig_to_peak.median()
            print(
                f"  {method:<22s} {n_detected}/{len(df)} detected"
                f"  median_delay={med_delay:.0f}s"
                f"  median_ig→peak={med_peak:+.1f}%"
            )

        if csv_path:
            df.to_csv(csv_path, index=False)
            print(f"\n  Saved to {csv_path}")

        return df
