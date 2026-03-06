"""
model.py – LightGBM wrapper for Context v0.

Provides: create / train / save / load / predict.
"""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any, Dict

import lightgbm as lgb
import numpy as np

# ── default hyper-parameters (deliberately simple) ───────────────────────────
DEFAULT_PARAMS: Dict[str, Any] = {
    "objective": "regression",
    "metric": "mse",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "max_depth": 6,
    "n_estimators": 200,
    "verbose": -1,
    "random_state": 42,
}


# ── factory ──────────────────────────────────────────────────────────────────
def create_model(params: Dict[str, Any] | None = None) -> lgb.LGBMRegressor:
    """Create an untrained LGBMRegressor with sensible defaults."""
    p = {**DEFAULT_PARAMS, **(params or {})}
    return lgb.LGBMRegressor(**p)


# ── persistence ──────────────────────────────────────────────────────────────
MODEL_DIR = Path(__file__).resolve().parent / "saved_models"


def save_model(model: lgb.LGBMRegressor, name: str = "context_v0") -> Path:
    """Pickle the model to *saved_models/<name>.pkl* and return the path."""
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    path = MODEL_DIR / f"{name}.pkl"
    with open(path, "wb") as f:
        pickle.dump(model, f)
    print(f"[model] saved → {path}")
    return path


def load_model(name: str = "context_v0") -> lgb.LGBMRegressor:
    """Load a pickled model by name."""
    path = MODEL_DIR / f"{name}.pkl"
    if not path.exists():
        raise FileNotFoundError(f"No saved model at {path}")
    with open(path, "rb") as f:
        model = pickle.load(f)
    print(f"[model] loaded ← {path}")
    return model


# ── inference ────────────────────────────────────────────────────────────────
def predict(model: lgb.LGBMRegressor, X: np.ndarray) -> np.ndarray:
    """Return model predictions.  X shape: (n, 7) or (7,)."""
    if X.ndim == 1:
        X = X.reshape(1, -1)
    return model.predict(X)
