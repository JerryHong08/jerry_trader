"""ML Model Training Script - Train models using collected training data.

Usage:
    # Train on backtest data (slow but comprehensive)
    poetry run python -m jerry_trader.services.training_data.train_model --dates 2026-03-11 2026-03-12

    # Train on collected parquet file
    poetry run python -m jerry_trader.services.training_data.train_model --input data/training_samples.parquet

    # Train and save to specific version
    poetry run python -m jerry_trader.services.training_data.train_model --input data/training_samples.parquet --output models/v3/predictor.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.model_selection import cross_val_score, train_test_split

from jerry_trader.services.training_data.collector import (
    ALL_FACTORS,
    TrainingDataCollector,
    TrainingSample,
)
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.utils.paths import PROJECT_ROOT

logger = setup_logger("train_model", log_to_file=True)


def train_model(
    samples: list[TrainingSample],
    model_type: str = "ridge",
    test_ratio: float = 0.3,
) -> dict:
    """Train ML model on collected samples.

    Args:
        samples: List of TrainingSample
        model_type: "ridge" or "gradient_boosting"
        test_ratio: Ratio for test set

    Returns:
        Dict with model and metrics
    """
    if len(samples) < 10:
        raise ValueError(f"Need at least 10 samples, got {len(samples)}")

    # Convert to DataFrame
    df = pd.DataFrame([s.to_dict() for s in samples])

    # Features to use
    features = ["relative_volume", "trade_rate", "price_direction", "gap_percent"]

    # Prepare X and y
    X = df[features].values
    y = df["return_pct"].values

    # Remove NaN
    valid_mask = ~np.isnan(X).any(axis=1) & ~np.isnan(y)
    X = X[valid_mask]
    y = y[valid_mask]

    logger.info(f"Training on {len(X)} samples")

    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_ratio, random_state=42
    )

    # Create model
    if model_type == "ridge":
        model = Ridge(alpha=10.0)
    else:
        model = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=3,
            learning_rate=0.1,
            min_samples_split=10,
            min_samples_leaf=5,
            random_state=42,
        )

    # Train
    model.fit(X_train, y_train)

    # Metrics
    train_r2 = model.score(X_train, y_train)
    test_r2 = model.score(X_test, y_test)
    y_pred = model.predict(X_test)
    mse = float(np.mean((y_test - y_pred) ** 2))

    # Cross-validation
    cv_scores = cross_val_score(model, X, y, cv=min(5, len(X) // 3), scoring="r2")
    cv_mean = float(cv_scores.mean())
    cv_std = float(cv_scores.std())

    # Feature importance
    if hasattr(model, "feature_importances_"):
        importance = dict(zip(features, model.feature_importances_))
    else:
        # For Ridge, use absolute coefficients
        importance = dict(zip(features, np.abs(model.coef_)))
        # Normalize
        total = sum(importance.values())
        if total > 0:
            importance = {k: v / total for k, v in importance.items()}

    metrics = {
        "train_r2": train_r2,
        "test_r2": test_r2,
        "mse": mse,
        "cv_mean": cv_mean,
        "cv_std": cv_std,
        "n_samples": len(X),
        "n_train": len(X_train),
        "n_test": len(X_test),
    }

    logger.info(
        f"Training complete: train_r2={train_r2:.3f}, test_r2={test_r2:.3f}, "
        f"cv={cv_mean:.3f}±{cv_std:.3f}"
    )

    return {
        "model": model,
        "features": features,
        "metrics": metrics,
        "importance": importance,
    }


def save_model(
    model,
    features: list[str],
    metrics: dict,
    importance: dict,
    output_path: Path,
    model_type: str = "ridge",
) -> None:
    """Save model to JSON and joblib files."""
    import joblib

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save config
    config = {
        "model_type": model_type,
        "features": features,
        "metrics": {
            "train_r2": round(metrics["train_r2"], 4),
            "test_r2": round(metrics["test_r2"], 4),
            "mse": round(metrics["mse"], 6),
            "cv_mean": round(metrics["cv_mean"], 4),
            "cv_std": round(metrics["cv_std"], 4),
        },
        "feature_importance": {k: round(v, 4) for k, v in importance.items()},
        "n_samples": metrics["n_samples"],
        "trained_at": pd.Timestamp.now().isoformat(),
    }

    with open(output_path, "w") as f:
        json.dump(config, f, indent=2)

    # Save model
    model_path = output_path.with_suffix(".joblib")
    joblib.dump(model, model_path)

    logger.info(f"Model saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Train ML model")

    parser.add_argument(
        "--input",
        "-i",
        help="Input parquet file with training samples",
    )
    parser.add_argument(
        "--dates",
        nargs="+",
        help="Dates to collect from backtest",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="models/v3/predictor.json",
        help="Output model path",
    )
    parser.add_argument(
        "--model-type",
        choices=["ridge", "gradient_boosting"],
        default="ridge",
        help="Model type to train",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10000,
        help="Max samples to collect",
    )

    args = parser.parse_args()

    # Collect samples
    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"Input file not found: {input_path}")
            return 1

        df = pd.read_parquet(input_path)
        samples = []
        for _, row in df.iterrows():
            factors = {f: row.get(f, 0.0) for f in ALL_FACTORS}
            samples.append(
                TrainingSample(
                    sample_id=str(row.get("sample_id", "")),
                    ticker=str(row.get("ticker", "")),
                    trigger_time_ns=int(row.get("trigger_time_ns", 0)),
                    event_name=str(row.get("event_name", "")),
                    factors=factors,
                    return_pct=float(row.get("return_pct", 0)),
                )
            )
        print(f"Loaded {len(samples)} samples from {input_path}")

    elif args.dates:
        collector = TrainingDataCollector()
        samples = collector.collect_from_backtest(dates=args.dates)
        print(f"Collected {len(samples)} samples from backtest")

    else:
        # Default: collect from recent dates
        collector = TrainingDataCollector()
        samples = collector.collect_from_backtest(
            dates=["2026-03-11", "2026-03-12", "2026-03-13"]
        )
        print(f"Collected {len(samples)} samples from default dates")

    if not samples:
        print("No samples collected")
        return 1

    # Train
    try:
        result = train_model(samples, model_type=args.model_type)
    except ValueError as e:
        print(f"Training error: {e}")
        return 1

    # Save
    output_path = PROJECT_ROOT / args.output
    save_model(
        result["model"],
        result["features"],
        result["metrics"],
        result["importance"],
        output_path,
        model_type=args.model_type,
    )

    # Print report
    print("\n" + "=" * 60)
    print("Training Report")
    print("=" * 60)
    print(f"Model type: {args.model_type}")
    print(
        f"Samples: {result['metrics']['n_samples']} "
        f"(train: {result['metrics']['n_train']}, test: {result['metrics']['n_test']})"
    )
    print(f"\nMetrics:")
    print(f"  Train R²: {result['metrics']['train_r2']:.4f}")
    print(f"  Test R²:  {result['metrics']['test_r2']:.4f}")
    print(
        f"  CV:       {result['metrics']['cv_mean']:.4f} ± {result['metrics']['cv_std']:.4f}"
    )
    print(f"  MSE:      {result['metrics']['mse']:.6f}")
    print(f"\nFeature Importance:")
    for feat, imp in sorted(
        result["importance"].items(), key=lambda x: x[1], reverse=True
    ):
        print(f"  {feat}: {imp:.4f}")
    print(f"\nModel saved to: {output_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
