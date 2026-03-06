"""
train.py – Training loop + end-to-end pipeline runner for Context v0.

Usage (from repo root):
    python -m jerry_trader.MachineLearning.train
"""

from __future__ import annotations

import time

import numpy as np

from jerry_trader.MachineLearning.dataset import build_feature_matrix, split_dataset
from jerry_trader.MachineLearning.evaluate import evaluate_model, run_inference_demo
from jerry_trader.MachineLearning.mock_data import FEATURE_COLS, generate_mock_dataset
from jerry_trader.MachineLearning.model import create_model, predict, save_model


# ── core training step ───────────────────────────────────────────────────────
def train(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    params: dict | None = None,
):
    """Train a LightGBM model and return (model, val_metrics)."""
    model = create_model(params)

    t0 = time.perf_counter()
    model.fit(X_train, y_train)
    elapsed = time.perf_counter() - t0

    print(f"[train] fit finished in {elapsed:.2f}s")

    # evaluate on validation set
    val_metrics = evaluate_model(model, X_val, y_val)
    return model, val_metrics


# ── end-to-end pipeline ─────────────────────────────────────────────────────
def run_training_pipeline(
    n_samples: int = 10_000,
    val_ratio: float = 0.2,
    seed: int = 42,
    save: bool = True,
) -> None:
    """Generate data → build dataset → train → evaluate → (save) → inference demo."""
    print("=" * 60)
    print("  Context v0  –  Stage 1 Training Pipeline")
    print("=" * 60)

    # 1. Generate mock data
    print("\n[1/5] Generating mock data …")
    df = generate_mock_dataset(n_samples, seed=seed)
    print(f"       {len(df)} samples, columns: {list(df.columns)}")

    # 2. Build feature matrix
    print("[2/5] Building feature matrix …")
    X, y = build_feature_matrix(df)
    X_train, X_val, y_train, y_val = split_dataset(X, y, val_ratio=val_ratio, seed=seed)
    print(f"       train: {X_train.shape[0]}  |  val: {X_val.shape[0]}")

    # 3. Train
    print("[3/5] Training LightGBM …")
    model, val_metrics = train(X_train, y_train, X_val, y_val)

    # 4. Evaluate
    print("\n[4/5] Validation metrics:")
    for k, v in val_metrics.items():
        print(f"       {k}: {v:.6f}")

    # 5. Feature importance
    importances = model.feature_importances_
    print("\n       Feature importances:")
    for col, imp in sorted(zip(FEATURE_COLS, importances), key=lambda x: -x[1]):
        print(f"         {col:>20s}  {imp:>5d}")

    # 6. Save
    if save:
        print("\n[5/5] Saving model …")
        save_model(model)

    # 7. Inference demo
    print("\n─── Inference demo (first 5 val samples) ───")
    run_inference_demo(model, X_val[:5], y_val[:5])

    print("\n✅  Pipeline complete.")


# ── entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_training_pipeline()
