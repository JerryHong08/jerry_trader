"""Train GBM to predict E[return | factors] from dense signal data.

Method 3: Direct prediction of expected return from factor vector.
Replaces boolean events.yaml thresholds with a continuous model.

Usage:
    poetry run python scripts/train_ml_signal.py
    poetry run python scripts/train_ml_signal.py --target max_return_pct
"""

from __future__ import annotations

import argparse
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

# ── Factor columns used as features ──
FACTOR_COLS = [
    "rel_vol_20",
    "trade_rate",
    "quote_rate",
    "price_direction",
    "bid_ask_spread",
    "vol_accel_5_15",
    "order_imbalance",
    "gap_pct",
    "entry_gap_pct",
    "ema_20",
]

# Session phase encoding
PHASE_MAP = {"early": 0, "mid": 1, "late": 2}


def load_data(
    explore_path: str, validate_path: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load and preprocess dense signal parquet files."""
    explore = pd.read_parquet(explore_path)
    validate = pd.read_parquet(validate_path)

    for df in [explore, validate]:
        df["session_phase_enc"] = df["session_phase"].map(PHASE_MAP).fillna(-1)
        # Cap extreme outliers at 99.5th percentile (per date, so done globally)
        for col in FACTOR_COLS:
            if col in df.columns:
                upper = df[col].quantile(0.995)
                df[col] = df[col].clip(upper=upper)

    return explore, validate


def train_model(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
) -> "lgb.Booster":
    """Train a regularized LightGBM model."""
    import lightgbm as lgb

    # Strong regularization: shallow trees, many leaves constrained
    params = {
        "objective": "regression",
        "metric": "rmse",
        "boosting_type": "gbdt",
        "num_leaves": 15,  # ~2^(depth-1), depth≈4
        "max_depth": 4,
        "learning_rate": 0.03,
        "n_estimators": 300,
        "min_child_samples": 200,  # Heavy regularization
        "subsample": 0.7,
        "colsample_bytree": 0.7,
        "reg_alpha": 0.5,
        "reg_lambda": 1.0,
        "verbosity": -1,
        "random_state": 42,
    }

    callbacks = [
        lgb.early_stopping(stopping_rounds=30),
        lgb.log_evaluation(period=50),
    ]

    model = lgb.LGBMRegressor(**params)
    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        callbacks=callbacks,
    )
    return model


def evaluate_model(
    model: "lgb.Booster",
    df: pd.DataFrame,
    feature_names: list[str],
    target_col: str,
) -> dict:
    """Evaluate model predictions vs actual returns."""
    X = df[feature_names].values
    y_actual = df[target_col].values
    y_pred = model.predict(X)

    # Overall stats
    rmse = np.sqrt(np.mean((y_pred - y_actual) ** 2))
    corr = np.corrcoef(y_pred, y_actual)[0, 1]

    # Bucket predictions into deciles and compute realized returns
    df_eval = df.copy()
    df_eval["predicted"] = y_pred
    df_eval["pred_decile"] = pd.qcut(y_pred, q=10, labels=False, duplicates="drop")

    decile_stats = (
        df_eval.groupby("pred_decile")
        .agg(
            n=("predicted", "count"),
            mean_pred=("predicted", "mean"),
            mean_actual=("max_return_pct", "mean"),
            median_actual=("max_return_pct", "median"),
            win_rate=("max_return_pct", lambda x: (x > 0).mean()),
            mean_return=("return_pct", "mean"),
        )
        .reset_index()
    )

    # Per-date correlation
    date_corrs = {}
    for date in sorted(df["date"].unique()):
        mask = df["date"] == date
        if mask.sum() > 50:
            date_pred = y_pred[mask]
            date_actual = y_actual[mask]
            date_corrs[date] = np.corrcoef(date_pred, date_actual)[0, 1]

    return {
        "rmse": rmse,
        "correlation": corr,
        "decile_stats": decile_stats,
        "date_correlations": date_corrs,
        "n_samples": len(df),
    }


def evaluate_as_signal(
    df: pd.DataFrame,
    y_pred: np.ndarray,
    target_col: str,
) -> dict:
    """Evaluate predictions as trading signals: what if we buy top N%?"""
    df_eval = df.copy()
    df_eval["predicted"] = y_pred

    results = {}
    for pct in [5, 10, 20, 30, 50]:
        threshold = np.percentile(y_pred, 100 - pct)
        signals = df_eval[df_eval["predicted"] >= threshold]
        if len(signals) < 10:
            results[f"top_{pct}%"] = {"n": 0, "error": "insufficient signals"}
            continue

        results[f"top_{pct}%"] = {
            "n": len(signals),
            "mean_max_return": float(signals[target_col].mean()),
            "median_max_return": float(signals[target_col].median()),
            "win_rate": float((signals[target_col] > 0).mean()),
            "mean_return": float(signals["return_pct"].mean()),
            # Per-date consistency
            "date_std": float(signals.groupby("date")[target_col].mean().std()),
        }

    return results


def feature_importance(model, feature_names: list[str]) -> pd.DataFrame:
    """Extract feature importance with SHAP-like breakdown."""
    # Gain-based importance (how much each feature reduces loss)
    importance = model.booster_.feature_importance(importance_type="gain")
    total = importance.sum()

    df_imp = pd.DataFrame(
        {
            "feature": feature_names,
            "gain": importance,
            "gain_pct": importance / total * 100,
        }
    ).sort_values("gain_pct", ascending=False)

    # Also get split count (how many times feature is used)
    split_imp = model.booster_.feature_importance(importance_type="split")
    df_imp["splits"] = [split_imp[i] for i in range(len(feature_names))]
    df_imp["splits"] = df_imp["splits"].astype(int)

    return df_imp


def partial_dependence(
    model,
    df: pd.DataFrame,
    feature: str,
    n_points: int = 50,
) -> pd.DataFrame:
    """Compute partial dependence for a single feature."""
    all_features = FACTOR_COLS + ["session_phase_enc"]
    X = df[all_features].values
    col_idx = all_features.index(feature)

    low, high = np.percentile(X[:, col_idx], [1, 99])
    grid = np.linspace(low, high, n_points)
    predictions = []

    for val in grid:
        X_copy = X.copy()
        X_copy[:, col_idx] = val
        predictions.append(model.predict(X_copy).mean())

    return pd.DataFrame({feature: grid, "predicted_return": predictions})


def compare_to_boolean(
    model,
    df: pd.DataFrame,
    feature_names: list[str],
    target_col: str,
) -> dict:
    """Compare GBM top signals vs current boolean volume_strong_10."""
    from collections import OrderedDict

    results = OrderedDict()

    # ── GBM: top 10% ──
    y_pred = model.predict(df[feature_names].values)
    threshold_10 = np.percentile(y_pred, 90)
    gbm_mask = y_pred >= threshold_10
    gbm_signals = df[gbm_mask]
    results["GBM top 10%"] = {
        "n": len(gbm_signals),
        "mean_max_return": float(gbm_signals[target_col].mean()),
        "win_rate": float((gbm_signals[target_col] > 0).mean()),
    }

    # ── GBM: top 20% ──
    threshold_20 = np.percentile(y_pred, 80)
    gbm_mask_20 = y_pred >= threshold_20
    gbm_signals_20 = df[gbm_mask_20]
    results["GBM top 20%"] = {
        "n": len(gbm_signals_20),
        "mean_max_return": float(gbm_signals_20[target_col].mean()),
        "win_rate": float((gbm_signals_20[target_col] > 0).mean()),
    }

    # ── Boolean: volume_strong_10 (current best) ──
    mask_vs10 = (df["rel_vol_20"] > 5.0) & (df["trade_rate"] > 10)
    vs10 = df[mask_vs10]
    results["volume_strong_10 (bool)"] = {
        "n": len(vs10),
        "mean_max_return": float(vs10[target_col].mean()),
        "win_rate": float((vs10[target_col] > 0).mean()),
    }

    # ── Boolean: momentum_entry (original) ──
    mask_me = (df["trade_rate"] > 10) & (df["rel_vol_20"] > 2.0)
    me = df[mask_me]
    results["momentum_entry (bool)"] = {
        "n": len(me),
        "mean_max_return": float(me[target_col].mean()),
        "win_rate": float((me[target_col] > 0).mean()),
    }

    # ── Per-date consistency ──
    for name in list(results.keys()):
        date_returns = []
        if name.startswith("GBM"):
            pct = 10 if "10%" in name else 20
            thresh = np.percentile(y_pred, 100 - pct)
            mask = y_pred >= thresh
        elif "volume_strong_10" in name:
            mask = mask_vs10
        else:
            mask = mask_me

        for date in sorted(df["date"].unique()):
            date_mask = mask & (df["date"] == date)
            if date_mask.sum() >= 3:
                date_returns.append(df.loc[date_mask, target_col].mean())
        if date_returns:
            results[name]["date_returns"] = date_returns
            results[name]["date_std"] = float(np.std(date_returns))
            results[name]["date_mean"] = float(np.mean(date_returns))

    return results


def main():
    parser = argparse.ArgumentParser(description="Train GBM signal model")
    parser.add_argument(
        "--target",
        default="max_return_pct",
        choices=["max_return_pct", "return_pct"],
        help="Target variable to predict",
    )
    parser.add_argument(
        "--explore",
        default="data/dense_signals/explore.parquet",
        help="Path to exploration parquet",
    )
    parser.add_argument(
        "--validate",
        default="data/dense_signals/validate.parquet",
        help="Path to validation parquet",
    )
    parser.add_argument(
        "--save-model",
        action="store_true",
        help="Save trained model to disk",
    )
    args = parser.parse_args()

    print(f"Loading data for target: {args.target}")
    explore, validate = load_data(args.explore, args.validate)

    # ── Prepare features ──
    feature_names = FACTOR_COLS + ["session_phase_enc"]
    X_explore = explore[feature_names].values
    y_explore = explore[args.target].values
    X_validate = validate[feature_names].values
    y_validate = validate[args.target].values

    # ── Split explore by date for early stopping ──
    explore_dates = sorted(explore["date"].unique())
    train_dates = explore_dates[:4]  # First 4 dates for training
    val_dates = explore_dates[4:]  # Last date for validation

    train_mask = explore["date"].isin(train_dates)
    val_mask = explore["date"].isin(val_dates)

    print(
        f"Train: {train_mask.sum():,} samples ({train_dates})\n"
        f"Val:   {val_mask.sum():,} samples ({val_dates})\n"
        f"Test:  {len(validate):,} samples ({sorted(validate['date'].unique())})"
    )

    # ── Train ──
    print("\nTraining LightGBM...")
    model = train_model(
        X_explore[train_mask],
        y_explore[train_mask],
        X_explore[val_mask],
        y_explore[val_mask],
    )

    # ── Evaluate on explore (in-sample) ──
    print("\n" + "=" * 70)
    print("IN-SAMPLE (explore set)")
    print("=" * 70)
    explore_eval = evaluate_model(model, explore, feature_names, args.target)
    print(f"  RMSE: {explore_eval['rmse']:.4f}")
    print(f"  Correlation: {explore_eval['correlation']:.4f}")
    print(f"  Date correlations: {explore_eval['date_correlations']}")
    print("\n  Decile analysis:")
    print(explore_eval["decile_stats"].to_string(index=False))

    # ── Evaluate on validate (out-of-sample) ──
    print("\n" + "=" * 70)
    print("OUT-OF-SAMPLE (validate set)")
    print("=" * 70)
    validate_eval = evaluate_model(model, validate, feature_names, args.target)
    print(f"  RMSE: {validate_eval['rmse']:.4f}")
    print(f"  Correlation: {validate_eval['correlation']:.4f}")
    print(f"  Date correlations: {validate_eval['date_correlations']}")
    print("\n  Decile analysis:")
    print(validate_eval["decile_stats"].to_string(index=False))

    # ── Signal evaluation ──
    print("\n" + "=" * 70)
    print("SIGNAL SIMULATION (validate set) — top N% as buy signals")
    print("=" * 70)
    y_val_pred = model.predict(X_validate)
    signal_results = evaluate_as_signal(validate, y_val_pred, args.target)
    for tier, r in signal_results.items():
        if "error" in r:
            print(f"  {tier}: {r['error']}")
        else:
            print(
                f"  {tier}: n={r['n']:,}, mean_max={r['mean_max_return']:.1f}%, "
                f"median_max={r['median_max_return']:.1f}%, win_rate={r['win_rate']:.1%}, "
                f"date_std={r['date_std']:.1f}%"
            )

    # ── Feature importance ──
    print("\n" + "=" * 70)
    print("FEATURE IMPORTANCE (gain-based)")
    print("=" * 70)
    imp = feature_importance(model, feature_names)
    print(imp.to_string(index=False))

    # ── Partial dependence for top 3 factors ──
    print("\n" + "=" * 70)
    print("PARTIAL DEPENDENCE (top 3 factors)")
    print("=" * 70)
    top3 = imp["feature"].head(3).tolist()
    for feat in top3:
        pd_df = partial_dependence(model, validate, feat)
        low_val = pd_df[pd_df["predicted_return"] == pd_df["predicted_return"].max()][
            feat
        ].values
        high_val = pd_df[pd_df["predicted_return"] == pd_df["predicted_return"].min()][
            feat
        ].values
        print(
            f"  {feat}: best region ~" f"{low_val[0]:.1f}" if len(low_val) > 0 else "?",
            f", worst region ~{high_val[0]:.1f}" if len(high_val) > 0 else "",
        )

    # ── Compare to boolean ──
    print("\n" + "=" * 70)
    print("GBM vs BOOLEAN (validate set)")
    print("=" * 70)
    comparison = compare_to_boolean(model, validate, feature_names, args.target)
    print(f"  {'Strategy':<25} {'N':>6} {'MeanMax':>10} {'WinRate':>8} {'DateStd':>8}")
    print(f"  {'-'*25} {'-'*6} {'-'*10} {'-'*8} {'-'*8}")
    for name, r in comparison.items():
        ds = r.get("date_std", 0)
        print(
            f"  {name:<25} {r['n']:>6,} {r['mean_max_return']:>9.1f}% "
            f"{r['win_rate']:>7.1%} {ds:>7.1f}%"
        )

    # ── Correlation with boolean signal ──
    bool_mask = (validate["rel_vol_20"] > 5.0) & (validate["trade_rate"] > 10)
    bool_signal = bool_mask.astype(float)
    gbm_bool_corr = np.corrcoef(y_val_pred, bool_signal)[0, 1]
    print(f"\n  GBM-vs-boolean correlation: {gbm_bool_corr:.4f}")
    print(f"  (low = GBM captures information boolean misses)")

    # ── Save model ──
    if args.save_model:
        model_path = Path("data/models")
        model_path.mkdir(parents=True, exist_ok=True)
        model_file = model_path / f"gbm_signal_{args.target}.pkl"
        with open(model_file, "wb") as f:
            pickle.dump(model, f)
        print(f"\nModel saved to {model_file}")


if __name__ == "__main__":
    main()
