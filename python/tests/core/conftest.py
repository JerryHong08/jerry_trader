"""Shared fixtures for core tests."""

import pytest

try:
    from jerry_trader import clock

    HAS_CLOCK = True
except ImportError:
    HAS_CLOCK = False


@pytest.fixture(autouse=True)
def reset_clock_after_test():
    """Ensure clock.py is in live mode after every test (when available)."""
    yield
    if HAS_CLOCK:
        clock.set_live_mode()
