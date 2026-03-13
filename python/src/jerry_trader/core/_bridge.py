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
# Partial dispatch is intentional: we can progressively migrate snapshot
# compute to Rust without requiring all symbols at once.
from jerry_trader.core.snapshot.compute import VolumeTracker as _PyVolumeTracker
from jerry_trader.core.snapshot.compute import (
    compute_derived_metrics as _py_compute_derived_metrics,
)
from jerry_trader.core.snapshot.compute import compute_ranks as _py_compute_ranks
from jerry_trader.core.snapshot.compute import (
    compute_weighted_mid_price as _py_compute_weighted_mid_price,
)

try:
    from jerry_trader._rust import (
        VolumeTracker as _RustVolumeTracker,  # type: ignore[attr-defined]
    )

    VolumeTracker = _RustVolumeTracker
    _rust_volume = True
    logger.info("_bridge - Using Rust VolumeTracker")
except ImportError:
    VolumeTracker = _PyVolumeTracker
    _rust_volume = False
    logger.info("_bridge - Using Python VolumeTracker (Rust not available)")

try:
    from jerry_trader._rust import (
        compute_ranks as _rust_compute_ranks,  # type: ignore[attr-defined]
    )

    compute_ranks = _rust_compute_ranks
    _rust_ranks = True
except ImportError:
    compute_ranks = _py_compute_ranks
    _rust_ranks = False

try:
    from jerry_trader._rust import (
        compute_derived_metrics as _rust_compute_derived_metrics,  # type: ignore[attr-defined]
    )

    compute_derived_metrics = _rust_compute_derived_metrics
    _rust_derived = True
except ImportError:
    compute_derived_metrics = _py_compute_derived_metrics
    _rust_derived = False

try:
    from jerry_trader._rust import (
        compute_weighted_mid_price as _rust_compute_weighted_mid_price,  # type: ignore[attr-defined]
    )

    compute_weighted_mid_price = _rust_compute_weighted_mid_price
    _rust_weighted_mid = True
except ImportError:
    compute_weighted_mid_price = _py_compute_weighted_mid_price
    _rust_weighted_mid = False

RUST_SNAPSHOT = _rust_volume or _rust_ranks or _rust_derived or _rust_weighted_mid
logger.info(
    "_bridge - Snapshot dispatch: rust(volume=%s, ranks=%s, derived=%s, weighted_mid=%s)",
    _rust_volume,
    _rust_ranks,
    _rust_derived,
    _rust_weighted_mid,
)

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
