"""
jerry_trader.core — Computation core & Rust migration boundary.

Subpackages:
    snapshot/   Snapshot processing (ranks, derived metrics, volume tracking)
    factors/    Factor engine (trade_rate, accel, z-scores)
    signals/    State/signal detection (QUIET ↔ ACTIVE transitions)
    news/       News classification (LLM-based)

Each subpackage follows a compute/orchestration split:
    compute.py  — Pure functions & stateful trackers (Rust migration target)
    processor.py / engine.py — I/O orchestration (Redis, InfluxDB, threads)

The _bridge module provides a try/except dispatch that prefers the Rust
implementation (jerry_trader._rust) and falls back to pure-Python compute.
"""
