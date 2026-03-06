"""
dataset.py – ContextV0 schema definition + feature-matrix helpers.

ContextV0 schema (7-dim):
    state_window:  r1 … r6   (6 × 10-second return buckets in a 60s window)
    meta:          seconds_from_4am

Label:  max_return_30s  (regression target)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Tuple

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

from jerry_trader.MachineLearning.mock_data import FEATURE_COLS, LABEL_COL


# ── schema dataclass (for documentation / type-safety) ───────────────────────
@dataclass
class ContextV0:
    """One observation in the v0 feature space."""

    # state_window – 6 × 10s returns covering a 60s lookback
    return_10s: List[float] = field(default_factory=lambda: [0.0] * 6)
    # meta
    seconds_from_4am: int = 0
    # label
    max_return_30s: float = 0.0

    def to_feature_vector(self) -> np.ndarray:
        """Return the flat 7-dim feature array (no label)."""
        return np.array(self.return_10s + [self.seconds_from_4am], dtype=np.float64)


# ── DataFrame → numpy arrays ─────────────────────────────────────────────────
def build_feature_matrix(
    df: pd.DataFrame,
    as_dataframe: bool = True,
) -> Tuple[pd.DataFrame | np.ndarray, np.ndarray]:
    """Convert a DataFrame (from mock_data) into (X, y).

    If *as_dataframe* is True (default), X is a DataFrame so that LightGBM
    receives feature names and avoids the "X does not have valid feature
    names" warning.  Otherwise X is a plain ndarray.

    X shape: (n_samples, 7)
    y shape: (n_samples,)
    """
    X = df[FEATURE_COLS]
    if not as_dataframe:
        X = X.values.astype(np.float64)
    y = df[LABEL_COL].values.astype(np.float64)
    return X, y


# ── train / validation split ─────────────────────────────────────────────────
def split_dataset(
    X: np.ndarray,
    y: np.ndarray,
    val_ratio: float = 0.2,
    seed: int = 42,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Return (X_train, X_val, y_train, y_val)."""
    return train_test_split(X, y, test_size=val_ratio, random_state=seed)


# ── quick smoke test ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    from .mock_data import generate_mock_dataset

    df = generate_mock_dataset(1000, seed=0)
    X, y = build_feature_matrix(df)
    X_train, X_val, y_train, y_val = split_dataset(X, y)

    print(f"X_train: {X_train.shape}  X_val: {X_val.shape}")
    print(f"y_train mean: {y_train.mean():.5f}  y_val mean: {y_val.mean():.5f}")
