"""
Backward-compatibility shim.

All computation modules have moved to ``jerry_trader.core``.
This package re-exports via per-file shims so existing imports
like ``from jerry_trader.ComputeEngine.snapshot_processor import SnapshotProcessor``
continue to work.

Deprecated — prefer ``jerry_trader.core.*`` imports.
"""
