"""
evaluate.py – Regression metrics + inference demo for Context v0.
"""

from __future__ import annotations

from typing import Dict

import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from .model import predict


# ── metric suite ─────────────────────────────────────────────────────────────
def evaluate_model(
    model,
    X: np.ndarray,
    y: np.ndarray,
) -> Dict[str, float]:
    """Compute regression metrics on (X, y). Returns dict of metric values."""
    y_pred = predict(model, X)
    return {
        "mse": float(mean_squared_error(y, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y, y_pred))),
        "mae": float(mean_absolute_error(y, y_pred)),
        "r2": float(r2_score(y, y_pred)),
    }


# ── single-sample inference demo ─────────────────────────────────────────────
def run_inference_demo(
    model,
    X_samples: np.ndarray,
    y_true: np.ndarray | None = None,
) -> None:
    """Print predicted vs actual for a handful of samples."""
    preds = predict(model, X_samples)
    for i, pred in enumerate(preds):
        actual = f"{y_true[i]:+.5f}" if y_true is not None else "n/a"
        print(f"  sample {i}: pred={pred:+.5f}  actual={actual}")
