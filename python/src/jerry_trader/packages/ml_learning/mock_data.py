"""
mock_data.py – Synthetic sample generator for Context v0.

Hidden rule (so LightGBM has something learnable):
    if sum(last 3 returns) > 0.01  →  label = +0.02 + noise
    else                           →  label = -0.01 + noise

Each sample has:
    - 6 × 10-second return buckets  (return_10s)
    - 1 × seconds_from_4am          (meta)
    - 1 × max_return_30s label       (regression target)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# ── constants ────────────────────────────────────────────────────────────────
RETURN_COLS = [f"r{i}" for i in range(1, 7)]  # r1 … r6
META_COL = "seconds_from_4am"
LABEL_COL = "max_return_30s"
FEATURE_COLS = RETURN_COLS + [META_COL]  # 7-dim feature vector

NOISE_SCALE = 0.003  # small Gaussian noise on the label


# ── single sample ────────────────────────────────────────────────────────────
def generate_mock_sample(rng: np.random.Generator | None = None) -> dict:
    """Return one ContextV0-shaped sample as a flat dict.

    Keys: r1..r6, seconds_from_4am, max_return_30s
    """
    if rng is None:
        rng = np.random.default_rng()

    print(rng)

    # 6 random 10-second returns (roughly in [-0.02, +0.02])
    returns = rng.normal(loc=0.0, scale=0.005, size=6)

    # Meta: random time between 04:00 (0s) and 20:00 (57600s)
    seconds_from_4am = int(rng.integers(0, 57_600))

    # ── hidden rule ──────────────────────────────────────────────────────────
    last3_sum = returns[3:].sum()
    if last3_sum > 0.01:
        label = 0.02
    else:
        label = -0.01
    label += rng.normal(0, NOISE_SCALE)

    sample = {col: val for col, val in zip(RETURN_COLS, returns)}
    sample[META_COL] = seconds_from_4am
    sample[LABEL_COL] = label
    return sample


# ── batch generation ─────────────────────────────────────────────────────────
def generate_mock_dataset(
    n_samples: int = 10_000,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate *n_samples* mock rows and return a pandas DataFrame.

    Columns: r1..r6, seconds_from_4am, max_return_30s
    """
    rng = np.random.default_rng(seed)
    rows = [generate_mock_sample(rng) for _ in range(n_samples)]
    return pd.DataFrame(rows)


# ── quick smoke test ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    df = generate_mock_dataset(20, seed=0)
    print(df.head(10).to_string(index=False))
    print(f"\nshape: {df.shape}")
    print(f"label mean: {df[LABEL_COL].mean():.5f}")
    print(f"label std : {df[LABEL_COL].std():.5f}")
