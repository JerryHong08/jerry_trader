"""
Tests for jerry_trader.core._bridge

Covers:
    - Rust/Python dispatch: correct symbols exported
    - Fallback flags (RUST_SNAPSHOT, RUST_FACTORS)
    - All __all__ exports are importable
"""

import pytest

from jerry_trader.core._bridge import (
    RUST_FACTORS,
    RUST_SNAPSHOT,
    VolumeTracker,
    compute_derived_metrics,
    compute_ranks,
    compute_weighted_mid_price,
    price_accel,
    z_score,
)


class TestBridgeExports:
    """Verify the bridge module exports the correct symbols."""

    def test_snapshot_functions_callable(self):
        assert callable(compute_ranks)
        assert callable(compute_derived_metrics)
        assert callable(compute_weighted_mid_price)

    def test_volume_tracker_instantiable(self):
        vt = VolumeTracker()
        assert hasattr(vt, "compute_relative_volume_5min")
        assert hasattr(vt, "compute_batch")

    def test_factor_functions_callable(self):
        assert callable(z_score)
        assert callable(price_accel)


class TestBridgeFlags:
    """Verify the Rust availability flags."""

    def test_flags_are_booleans(self):
        assert isinstance(RUST_SNAPSHOT, bool)
        assert isinstance(RUST_FACTORS, bool)

    def test_rust_factors_active(self):
        """Rust factor compute (z_score, price_accel) should be active."""
        assert RUST_FACTORS is True, "Rust factors should be compiled and active"

    def test_snapshot_fallback_active(self):
        """Snapshot compute is still Python (Rust not yet implemented)."""
        # Update this once VolumeTracker / compute_ranks are ported to Rust
        assert RUST_SNAPSHOT is False, "Expected Python fallback (Rust not built yet)"


class TestBridgeAll:
    """Verify __all__ is consistent."""

    def test_all_exports_importable(self):
        import jerry_trader.core._bridge as bridge

        for name in bridge.__all__:
            assert hasattr(bridge, name), f"__all__ lists '{name}' but it's not defined"
