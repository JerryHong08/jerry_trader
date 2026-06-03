"""Factor correctness snapshot tests.

Generates deterministic synthetic data for each factor type, runs batch
compute through PyFactorEngine, and compares results against gold files.

Gold files live in python/tests/core/factor_snapshots/<factor_name>.json

Regenerate gold files:
    FACTOR_GENERATE=1 poetry run pytest python/tests/core/test_factor_snapshots.py -v
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

SNAPSHOT_DIR = Path(__file__).parent / "factor_snapshots"

# ══════════════════════════════════════════════════════════════════════
# Synthetic data generators
# ══════════════════════════════════════════════════════════════════════


def _make_bars(n: int = 50) -> list:
    """Generate synthetic bars: mild uptrend with sinusoidal volume pattern.

    Returns list of PyBar instances.
    """
    from jerry_trader._rust import PyBar

    bars = []
    for i in range(n):
        ts_ms = 1_000_000 + i * 60_000  # 1-min bars
        base = 100.0 + i * 0.3  # steady uptrend
        noise = (i % 7) * 0.02  # deterministic "noise"
        open_p = base + noise
        close_p = base + 0.15 + ((i + 3) % 5) * 0.04
        high_p = max(open_p, close_p) + 0.05 + (i % 3) * 0.03
        low_p = min(open_p, close_p) - 0.05 - ((i + 1) % 3) * 0.03
        volume = 1000 + (i % 10) * 200 + ((i + 5) % 8) * 150  # oscillating
        bars.append(
            PyBar(
                ts_ms,
                round(open_p, 4),
                round(high_p, 4),
                round(low_p, 4),
                round(close_p, 4),
                volume,
            )
        )
    return bars


def _make_trades(n: int = 200) -> list:
    """Generate synthetic trades: 2/sec average, micro-bursts.

    Returns list of PyTrade instances.
    """
    from jerry_trader._rust import PyTrade

    trades = []
    ts_ms = 1_000_000
    for i in range(n):
        price = 100.0 + (i * 0.01) + ((i % 20) * 0.005)
        size = 100 + (i % 5) * 50
        trades.append(PyTrade(ts_ms, round(price, 4), size))
        # Varying intervals: mostly ~500ms, occasional 200ms bursts
        interval = 200 if i % 10 < 3 else 500
        ts_ms += interval
    return trades


def _make_quotes(n: int = 200) -> list:
    """Generate synthetic quotes: varying spread and size imbalance.

    Returns list of (ts_ms, bid, ask, bid_size, ask_size) tuples.
    """
    quotes = []
    ts_ms = 1_000_000
    for i in range(n):
        mid = 100.0 + (i * 0.005)
        # Spread oscillates: tight most of the time, wider every 20th quote
        half_spread = 0.005 if i % 20 != 0 else 0.025
        bid = round(mid - half_spread, 4)
        ask = round(mid + half_spread, 4)

        # Size imbalance oscillates between buy-heavy and sell-heavy
        if i % 30 < 15:
            bid_size = 800 + (i % 10) * 100
            ask_size = 300 + (i % 7) * 50
        else:
            bid_size = 300 + (i % 7) * 50
            ask_size = 800 + (i % 10) * 100

        quotes.append((ts_ms, bid, ask, bid_size, ask_size))
        ts_ms += 500  # 2 quotes/sec
    return quotes


def _make_compute_ts(n: int = 100) -> list[int]:
    """Generate 1-second compute timestamps aligned with trade data."""
    return [1_000_000 + i * 1000 for i in range(n)]


# ══════════════════════════════════════════════════════════════════════
# Gold file helpers
# ══════════════════════════════════════════════════════════════════════


def _should_generate() -> bool:
    return os.environ.get("FACTOR_GENERATE") == "1"


def _gold_path(factor_name: str) -> Path:
    return SNAPSHOT_DIR / f"{factor_name}.json"


def _serialize(value):
    """Make values JSON-serializable."""
    if value is None:
        return None
    if isinstance(value, float):
        return round(value, 10)
    return value


def _load_gold(factor_name: str):
    path = _gold_path(factor_name)
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


def _save_gold(factor_name: str, data):
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    with open(_gold_path(factor_name), "w") as f:
        json.dump(data, f, indent=2)


# ══════════════════════════════════════════════════════════════════════
# Bar factor tests
# ══════════════════════════════════════════════════════════════════════


def _compute_bar_factor(factor_name: str, params: dict | None = None):
    """Run batch bar factor computation and return serialized results."""
    from jerry_trader._rust import PyFactorEngine

    engine = PyFactorEngine()
    bars = _make_bars(50)
    results = engine.compute_batch_bar(factor_name, bars, params)
    return [_serialize(r) for r in results]


@pytest.mark.parametrize(
    "factor_name,params",
    [
        ("ema", {"period": 20.0}),
        ("relative_volume", {"lookback": 20.0}),
        ("price_direction", {}),
        ("gap_percent", {}),
        ("volume_acceleration", {"short_window": 5.0, "long_window": 15.0}),
        ("vwap_deviation", {"period": 20.0}),
    ],
)
def test_bar_factor_snapshot(factor_name, params):
    results = _compute_bar_factor(factor_name, params)

    if _should_generate():
        _save_gold(factor_name, results)
        pytest.skip(f"Generated gold file for {factor_name}")

    gold = _load_gold(factor_name)
    assert (
        gold is not None
    ), f"Gold file missing for {factor_name}. Run with FACTOR_GENERATE=1"
    assert results == gold, (
        f"Snapshot mismatch for {factor_name}.\n"
        f"First diff position: "
        f"{next((i for i, (a, b) in enumerate(zip(results, gold)) if a != b), 'none')}\n"
        f"Rerun with FACTOR_GENERATE=1 to update gold files."
    )


# ══════════════════════════════════════════════════════════════════════
# Trade factor tests
# ══════════════════════════════════════════════════════════════════════


def _compute_trade_factor(factor_name: str, params: dict | None = None):
    from jerry_trader._rust import PyFactorEngine

    engine = PyFactorEngine()
    trades = _make_trades(200)
    compute_ts = _make_compute_ts(100)
    results = engine.compute_batch_trade(factor_name, trades, compute_ts, params)
    return [_serialize(r) for r in results]


@pytest.mark.parametrize(
    "factor_name,params",
    [
        ("trade_rate", {"window_ms": 20000.0, "min_trades": 5.0}),
        (
            "large_trade_ratio",
            {"window_ms": 20000.0, "min_trades": 5.0, "large_multiplier": 2.0},
        ),
        ("aggressor_ratio", {"window_ms": 20000.0, "min_trades": 5.0}),
    ],
)
def test_trade_factor_snapshot(factor_name, params):
    results = _compute_trade_factor(factor_name, params)

    if _should_generate():
        _save_gold(factor_name, results)
        pytest.skip(f"Generated gold file for {factor_name}")

    gold = _load_gold(factor_name)
    assert (
        gold is not None
    ), f"Gold file missing for {factor_name}. Run with FACTOR_GENERATE=1"
    assert results == gold, (
        f"Snapshot mismatch for {factor_name}.\n"
        f"Rerun with FACTOR_GENERATE=1 to update gold files."
    )


# ══════════════════════════════════════════════════════════════════════
# Quote factor tests
# ══════════════════════════════════════════════════════════════════════


def _compute_quote_factor(factor_name: str, params: dict | None = None):
    from jerry_trader._rust import PyFactorEngine

    engine = PyFactorEngine()
    quotes = _make_quotes(200)
    results = engine.compute_batch_quote(factor_name, quotes, params)
    # results is List[(i64, Optional[f64])]
    return [[ts, _serialize(val)] for ts, val in results]


@pytest.mark.parametrize(
    "factor_name,params",
    [
        ("bid_ask_spread", {"window_ms": 5000.0}),
        ("order_imbalance", {"window_ms": 5000.0}),
        ("quote_rate", {"window_ms": 10000.0, "min_quotes": 10.0}),
    ],
)
def test_quote_factor_snapshot(factor_name, params):
    results = _compute_quote_factor(factor_name, params)

    if _should_generate():
        _save_gold(factor_name, results)
        pytest.skip(f"Generated gold file for {factor_name}")

    gold = _load_gold(factor_name)
    assert (
        gold is not None
    ), f"Gold file missing for {factor_name}. Run with FACTOR_GENERATE=1"
    assert results == gold, (
        f"Snapshot mismatch for {factor_name}.\n"
        f"First diff position: "
        f"{next((i for i, (a, b) in enumerate(zip(results, gold)) if a != b), 'none')}\n"
        f"Rerun with FACTOR_GENERATE=1 to update gold files."
    )
