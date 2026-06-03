"""ML Model for Return Prediction - GradientBoostingRegressor.

Predicts expected return from factor values, replacing hardcoded thresholds
with continuous prediction.

Usage:
    predictor = ReturnPredictor()
    predictor.train(dates=["2026-03-13", "2026-03-12"])
    expected_return = predictor.predict({"relative_volume": 2.5, "trade_rate": 20})
    feature_importance = predictor.get_feature_importance()
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.model_selection import cross_val_score

from jerry_trader.services.backtest.dense_signal_collector import collect_signals
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("ml_model", log_to_file=True)

# Default factors for prediction
DEFAULT_FEATURES = [
    "relative_volume",
    "price_direction",
    "trade_rate",
    "bid_ask_spread",
    "entry_gap_pct",
    "gap_percent",
    "order_imbalance",
    "quote_rate",
]

# Minimal features for backtest_results (limited factor set)
MINIMAL_FEATURES = [
    "relative_volume",
    "trade_rate",
    "price_direction",
    "gap_percent",
]


@dataclass
class TrainingResult:
    """Results from model training."""

    dates: list[str]
    total_samples: int
    features_used: list[str]

    # Model metrics
    r2_score: float = 0.0
    mse: float = 0.0
    cv_scores: list[float] = field(default_factory=list)
    cv_mean: float = 0.0
    cv_std: float = 0.0

    # Feature importance
    feature_importance: dict[str, float] = field(default_factory=dict)

    # Training timestamp
    trained_at: str = ""


class ReturnPredictor:
    """ML model for predicting expected return from factors.

    Uses GradientBoostingRegressor for tabular factor-return prediction.
    Suitable for:
    - Small-medium datasets (thousands of samples)
    - Interpretable feature importance
    - No deep learning needed (avoids overfitting)

    Example:
        predictor = ReturnPredictor()
        predictor.train(["2026-03-13"])

        # Predict expected return
        factors = {"relative_volume": 2.5, "trade_rate": 20}
        expected_return = predictor.predict(factors)

        # Get feature importance
        importance = predictor.get_feature_importance()

        # Save/load model
        predictor.save("models/return_predictor_v1.json")
        predictor.load("models/return_predictor_v1.json")
    """

    def __init__(
        self,
        features: list[str] | None = None,
        model_params: dict[str, Any] | None = None,
        use_gpu: bool = True,
    ):
        """Initialize predictor.

        Args:
            features: List of factor names to use as features
            model_params: Model parameters (XGBoost or GradientBoosting)
            use_gpu: Whether to use GPU acceleration (requires XGBoost with CUDA)
        """
        self.features = features or DEFAULT_FEATURES
        self.use_gpu = use_gpu
        self.model_params = model_params
        self.model: Any = None
        self.training_result: TrainingResult | None = None

        # Initialize model based on GPU preference
        if use_gpu:
            try:
                import xgboost as xgb

                # Verify CUDA support
                info = xgb.build_info()
                if not info.get("USE_CUDA", False):
                    logger.warning("XGBoost CUDA not available, falling back to CPU")
                    self.use_gpu = False
                else:
                    # Default XGBoost GPU params
                    if model_params is None:
                        self.model_params = {
                            "tree_method": "hist",
                            "device": "cuda",
                            "n_estimators": 200,
                            "max_depth": 4,
                            "learning_rate": 0.05,
                            "min_child_weight": 10,
                            "subsample": 0.8,
                            "colsample_bytree": 0.8,
                            "random_state": 42,
                        }
                    logger.info("Using XGBoost with CUDA acceleration")
            except ImportError:
                logger.warning("XGBoost not installed, falling back to sklearn CPU")
                self.use_gpu = False

        if not self.use_gpu:
            # Fallback to sklearn GradientBoostingRegressor
            if model_params is None:
                self.model_params = {
                    "n_estimators": 100,
                    "max_depth": 3,
                    "learning_rate": 0.1,
                    "min_samples_split": 10,
                    "min_samples_leaf": 5,
                    "random_state": 42,
                }
            logger.info("Using sklearn GradientBoostingRegressor (CPU)")

    def train(
        self,
        dates: list[str],
        cv_folds: int = 5,
    ) -> TrainingResult:
        """Train model on signals from specified dates.

        Args:
            dates: List of date strings YYYY-MM-DD
            cv_folds: Cross-validation folds

        Returns:
            TrainingResult with metrics and feature importance
        """
        logger.info(f"Training model on dates: {dates}")

        # Collect signals from all dates
        all_signals = []

        for date in dates:
            signals_df = collect_signals(date)
            if not signals_df.empty:
                # Filter valid signals
                valid_df = signals_df[signals_df["relative_volume"] > 0]
                all_signals.append(valid_df)
                logger.info(f"  {date}: {len(valid_df)} valid signals")

        if not all_signals:
            logger.warning("No valid signals collected")
            return TrainingResult(
                dates=dates,
                total_samples=0,
                features_used=self.features,
            )

        # Combine all data
        import pandas as pd

        combined_df = pd.concat(all_signals, ignore_index=True)
        logger.info(f"Combined: {len(combined_df)} total signals")

        # Prepare features and target
        X = combined_df[self.features].values
        # Use max_return_pct as target (max gain during hold period)
        # Falls back to return_pct if max_return_pct not available
        if "max_return_pct" in combined_df.columns:
            y = combined_df["max_return_pct"].values
            logger.info(
                "Using max_return_pct as training target (max gain during hold)"
            )
        else:
            y = combined_df["return_pct"].values
            logger.info("Using return_pct as training target (exit price return)")

        # Remove NaN rows
        valid_mask = ~np.isnan(X).any(axis=1) & ~np.isnan(y)
        X = X[valid_mask]
        y = y[valid_mask]

        logger.info(f"After NaN removal: {len(X)} samples")

        # Initialize and train model
        if self.use_gpu:
            import xgboost as xgb

            self.model = xgb.XGBRegressor(**self.model_params)
        else:
            from sklearn.ensemble import GradientBoostingRegressor

            self.model = GradientBoostingRegressor(**self.model_params)

        self.model.fit(X, y)

        # Compute metrics
        y_pred = self.model.predict(X)
        mse = float(np.mean((y - y_pred) ** 2))

        # R² score
        if hasattr(self.model, "score"):
            r2 = float(self.model.score(X, y))
        else:
            # XGBoost doesn't have score method, compute manually
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

        # Cross-validation
        cv_scores = cross_val_score(self.model, X, y, cv=cv_folds, scoring="r2")
        cv_mean = float(cv_scores.mean())
        cv_std = float(cv_scores.std())

        # Feature importance
        if hasattr(self.model, "feature_importances_"):
            importance = {
                feat: float(imp)  # Convert to Python float
                for feat, imp in zip(self.features, self.model.feature_importances_)
            }
        else:
            importance = {}

        # Store results
        self.training_result = TrainingResult(
            dates=dates,
            total_samples=len(X),
            features_used=self.features,
            r2_score=r2,
            mse=mse,
            cv_scores=list(cv_scores),
            cv_mean=cv_mean,
            cv_std=cv_std,
            feature_importance=importance,
            trained_at=datetime.now().isoformat(),
        )

        logger.info(f"Training complete: R²={r2:.3f}, CV={cv_mean:.3f}±{cv_std:.3f}")
        return self.training_result

    def predict(self, factors: dict[str, float]) -> float:
        """Predict expected return from factor values.

        Args:
            factors: Dict of factor_name -> value

        Returns:
            Expected return percentage
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")

        # Build feature vector in correct order
        X = np.array([[factors.get(f, 0) for f in self.features]])

        return float(self.model.predict(X)[0])

    def predict_batch(self, factors_list: list[dict[str, float]]) -> list[float]:
        """Predict expected returns for multiple signals.

        Args:
            factors_list: List of factor dicts

        Returns:
            List of expected return percentages
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")

        X = np.array(
            [[f.get(f_name, 0) for f_name in self.features] for f in factors_list]
        )

        return [float(v) for v in self.model.predict(X)]

    def get_feature_importance(self) -> dict[str, float]:
        """Get feature importance from trained model.

        Returns:
            Dict of factor_name -> importance score (normalized to sum=1)
        """
        if self.model is None or self.training_result is None:
            raise ValueError("Model not trained. Call train() first.")

        return self.training_result.feature_importance

    def get_ranked_features(self) -> list[tuple[str, float]]:
        """Get features ranked by importance.

        Returns:
            List of (factor_name, importance) sorted descending
        """
        importance = self.get_feature_importance()
        return sorted(importance.items(), key=lambda x: x[1], reverse=True)

    def explain(
        self, factors: dict[str, float], use_shap: bool = True
    ) -> dict[str, float]:
        """Explain prediction with feature contributions.

        Args:
            factors: Dict of factor_name -> value
            use_shap: Whether to use SHAP values (more accurate but slower)

        Returns:
            Dict of factor_name -> contribution to prediction
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")

        # Build feature vector
        X = np.array([[factors.get(f, 0) for f in self.features]])

        if use_shap:
            # Use SHAP for accurate explanation
            try:
                import shap

                # TreeExplainer for GradientBoosting
                explainer = shap.TreeExplainer(self.model)
                shap_values = explainer.shap_values(X)

                # SHAP values are contributions to the prediction
                contributions = dict(zip(self.features, shap_values[0]))

            except ImportError:
                logger.warning("SHAP not installed, falling back to simple explanation")
                return self._simple_explain(factors)
        else:
            return self._simple_explain(factors)

        return {k: float(v) for k, v in contributions.items()}

    def _simple_explain(self, factors: dict[str, float]) -> dict[str, float]:
        """Simple feature contribution approximation without SHAP.

        Uses feature importance * feature value as rough contribution estimate.
        """
        importance = self.get_feature_importance()

        # Normalize importance
        total_importance = sum(importance.values())
        normalized = {k: v / total_importance for k, v in importance.items()}

        # Contribution = importance * (value - mean_value)
        # This is a rough approximation, SHAP is preferred for accuracy
        contributions = {}
        for feature in self.features:
            value = factors.get(feature, 0)
            imp = normalized.get(feature, 0)
            # Rough estimate: importance * deviation from mean
            contributions[feature] = imp * value

        return contributions

    def explain_batch(
        self, factors_list: list[dict[str, float]], use_shap: bool = True
    ) -> list[dict[str, float]]:
        """Explain predictions for multiple signals.

        Args:
            factors_list: List of factor dicts
            use_shap: Whether to use SHAP values

        Returns:
            List of contribution dicts
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")

        if use_shap:
            try:
                import shap

                X = np.array(
                    [
                        [f.get(f_name, 0) for f_name in self.features]
                        for f in factors_list
                    ]
                )

                explainer = shap.TreeExplainer(self.model)
                shap_values = explainer.shap_values(X)

                return [
                    {f: float(v) for f, v in zip(self.features, row)}
                    for row in shap_values
                ]

            except ImportError:
                logger.warning("SHAP not installed, falling back to simple explanation")
                return [self._simple_explain(f) for f in factors_list]
        else:
            return [self._simple_explain(f) for f in factors_list]

    def save(self, path: str | Path) -> None:
        """Save model to JSON file.

        Args:
            path: File path for saving
        """
        if self.model is None or self.training_result is None:
            raise ValueError("Model not trained. Call train() first.")

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Save model parameters and feature importance
        # Note: We don't save the full sklearn model, just the config
        # For production, use pickle or joblib
        data = {
            "model_params": self.model_params,
            "features": self.features,
            "training_result": {
                "dates": self.training_result.dates,
                "total_samples": self.training_result.total_samples,
                "features_used": self.training_result.features_used,
                "r2_score": self.training_result.r2_score,
                "mse": self.training_result.mse,
                "cv_mean": self.training_result.cv_mean,
                "cv_std": self.training_result.cv_std,
                "feature_importance": self.training_result.feature_importance,
                "trained_at": self.training_result.trained_at,
            },
        }

        with open(path, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Model config saved to {path}")

        # Also save sklearn model with joblib
        import joblib

        model_path = path.with_suffix(".joblib")
        joblib.dump(self.model, model_path)
        logger.info(f"Sklearn model saved to {model_path}")

    def load(self, path: str | Path) -> None:
        """Load model from file.

        Args:
            path: File path for loading
        """
        path = Path(path)

        # Load config
        with open(path) as f:
            data = json.load(f)

        self.features = data["features"]

        # Handle both old and new config formats
        if "model_params" in data:
            # Old format from training
            self.model_params = data["model_params"]
        else:
            # New simpler format
            self.model_params = {}

        # Load sklearn model
        import joblib

        model_path = path.with_suffix(".joblib")
        self.model = joblib.load(model_path)

        # Restore training result (handle both formats)
        if "training_result" in data:
            tr = data["training_result"]
            self.training_result = TrainingResult(
                dates=tr["dates"],
                total_samples=tr["total_samples"],
                features_used=tr["features_used"],
                r2_score=tr["r2_score"],
                mse=tr["mse"],
                cv_mean=tr["cv_mean"],
                cv_std=tr["cv_std"],
                feature_importance=tr["feature_importance"],
                trained_at=tr["trained_at"],
            )
        elif "metrics" in data:
            # New format with metrics
            m = data["metrics"]
            self.training_result = TrainingResult(
                dates=[],
                total_samples=data.get("n_samples", 0),
                features_used=self.features,
                r2_score=m.get("test_r2", m.get("r2", 0)),
                mse=m.get("mse", 0),
                cv_mean=m.get("cv_mean", 0),
                cv_std=m.get("cv_std", 0),
                feature_importance=data.get("feature_importance", {}),
                trained_at=data.get("trained_at", ""),
            )
        else:
            # Minimal format
            self.training_result = TrainingResult(
                dates=[],
                total_samples=0,
                features_used=self.features,
            )

        logger.info(f"Model loaded from {path}")


def format_training_report(result: TrainingResult) -> str:
    """Format training result as readable report."""
    lines = [
        "# ML Model Training Report",
        "",
        "## Training Data",
        f"- Dates: {', '.join(result.dates)}",
        f"- Total samples: {result.total_samples}",
        f"- Features: {len(result.features_used)}",
        "",
        "## Model Performance",
        f"- R² score: {result.r2_score:.3f}",
        f"- MSE: {result.mse:.3f}",
        f"- Cross-validation: {result.cv_mean:.3f} ± {result.cv_std:.3f}",
        "",
        "## Feature Importance (ranked)",
    ]

    sorted_importance = sorted(
        result.feature_importance.items(),
        key=lambda x: x[1],
        reverse=True,
    )

    for feature, importance in sorted_importance:
        lines.append(f"- {feature}: {importance:.3f}")

    lines.extend(
        [
            "",
            f"Trained at: {result.trained_at}",
        ]
    )

    return "\n".join(lines)


__all__ = ["ReturnPredictor", "TrainingResult", "format_training_report"]
