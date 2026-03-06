"""Benchmark: Rust vs Python factor compute functions."""

import random
import time
from collections import deque

from jerry_trader._rust import price_accel as rust_price_accel
from jerry_trader._rust import z_score as rust_z_score
from jerry_trader.core.factors.compute import price_accel as py_price_accel
from jerry_trader.core.factors.compute import z_score as py_z_score

random.seed(42)
iterations = 100_000

print("=" * 65)
print(f"z_score benchmark ({iterations:,} calls each)")
print("=" * 65)

for size in [10, 120, 1000]:
    hist_list = [random.gauss(50, 10) for _ in range(size)]
    hist_deque = deque(hist_list)
    value = 65.0

    t0 = time.perf_counter()
    for _ in range(iterations):
        py_z_score(value, hist_deque)
    py_time = time.perf_counter() - t0

    t0 = time.perf_counter()
    for _ in range(iterations):
        rust_z_score(value, hist_list)
    rust_time = time.perf_counter() - t0

    speedup = py_time / rust_time
    print(
        f"  history={size:>5d}:  Python {py_time:.3f}s  Rust {rust_time:.3f}s  -> {speedup:.1f}x"
    )

print()
print("=" * 65)
print(f"price_accel benchmark ({iterations:,} calls each)")
print("=" * 65)

for size in [10, 100, 500]:
    recent = [(1000 + i * 10, 100.0 + random.gauss(0, 0.5)) for i in range(size)]
    older = [(i * 10, 100.0 + random.gauss(0, 0.5)) for i in range(size)]

    t0 = time.perf_counter()
    for _ in range(iterations):
        py_price_accel(recent, older)
    py_time = time.perf_counter() - t0

    t0 = time.perf_counter()
    for _ in range(iterations):
        rust_price_accel(recent, older)
    rust_time = time.perf_counter() - t0

    speedup = py_time / rust_time
    print(
        f"  trades={size:>4d}:  Python {py_time:.3f}s  Rust {rust_time:.3f}s  -> {speedup:.1f}x"
    )

print()
print("Note: Rust times include PyO3 list->Vec conversion on each call.")
print("In production, factor_engine calls these ~1/sec per ticker, so the")
print("real win is consistency (no GC pauses) and headroom for more tickers.")
