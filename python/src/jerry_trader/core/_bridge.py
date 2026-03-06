"""
Rust ↔ Python dispatch bridge for jerry_trader.core.

Tries to import computation functions from the Rust extension module
(``jerry_trader._rust``). Falls back to the pure-Python implementations
in the respective ``compute.py`` modules when the Rust extension is not
compiled or not available.

Usage (from any orchestration module):
    from jerry_trader.core._bridge import compute_ranks, VolumeTracker

The caller doesn't need to know whether Rust or Python is running.
"""

from jerry_trader.utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)

# ─── Snapshot compute functions ───────────────────────────────────────────
try:
    from jerry_trader._rust import (  # type: ignore[attr-defined]
        VolumeTracker,
        compute_derived_metrics,
        compute_ranks,
        compute_weighted_mid_price,
    )

    RUST_SNAPSHOT = True
    logger.info("_bridge - Using Rust snapshot compute")
except ImportError:
    from jerry_trader.core.snapshot.compute import (
        VolumeTracker,
        compute_derived_metrics,
        compute_ranks,
        compute_weighted_mid_price,
    )

    RUST_SNAPSHOT = False
    logger.info("_bridge - Using Python snapshot compute (Rust not available)")

# ─── Factor compute functions ────────────────────────────────────────────
try:
    from jerry_trader._rust import price_accel, z_score  # type: ignore[attr-defined]

    RUST_FACTORS = True
    logger.info("_bridge - Using Rust factor compute")
except ImportError:
    from jerry_trader.core.factors.compute import price_accel, z_score

    RUST_FACTORS = False
    logger.info("_bridge - Using Python factor compute (Rust not available)")


__all__ = [
    # Snapshot
    "VolumeTracker",
    "compute_ranks",
    "compute_derived_metrics",
    "compute_weighted_mid_price",
    # Factors
    "z_score",
    "price_accel",
    # Flags
    "RUST_SNAPSHOT",
    "RUST_FACTORS",
]
