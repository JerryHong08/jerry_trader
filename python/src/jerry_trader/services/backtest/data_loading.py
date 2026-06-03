"""Shared data loading helpers for backtest research modules.

Loads trades/quotes from ClickHouse (online) or Polygon CSV.gz (offline),
plus FMP company profiles with ticker-mapping for survivorship-bias correction.
"""

from __future__ import annotations

import gzip
from pathlib import Path

import pandas as pd

POLYGON_DATA_ROOT = Path("/mnt/blackdisk/quant_data/polygon_data/raw")

# Pre-market session in ET: 04:00–09:30 → rough UTC range
SESSION_START_MS: int = 4 * 3600 * 1000 + 9 * 3600 * 1000  # 09:00 ET in ms
SESSION_END_MS: int = SESSION_START_MS + 390 * 60 * 1000  # +390 min = 15:30 ET

DEFAULT_COMPUTE_INTERVAL_MS: int = 2000  # 2-second compute grid


def _ns_to_ms(ns: int) -> int:
    """Polygon sip_timestamp is nanoseconds since epoch."""
    return ns // 1_000_000


def _load_profiles(date_str: str) -> dict[str, dict]:
    """Load FMP company profiles keyed by all ticker names active on date_str.

    Uses FIGI-based ticker mapping so renamed tickers (LQR→YHC) and
    delisted tickers still resolve to their profile.  The returned dict
    is keyed by every ticker name that was active on the backtest date,
    translating old names to latest_ticker for the actual profile lookup.

    Returns empty dict if no profiles file exists (graceful degradation).
    """
    import os

    import polars as pl

    from jerry_trader.platform.config.config import fundamentals_profiles_dir

    date_compact = date_str.replace("-", "")

    # ---- 1. Load profiles parquet ----
    profiles_file = os.path.join(
        fundamentals_profiles_dir, f"profiles_{date_compact}.parquet"
    )

    if not os.path.exists(profiles_file):
        if not os.path.isdir(fundamentals_profiles_dir):
            return {}
        files = sorted(
            [
                f
                for f in os.listdir(fundamentals_profiles_dir)
                if f.startswith("profiles_") and f.endswith(".parquet")
            ]
        )
        if not files:
            return {}
        profiles_file = os.path.join(fundamentals_profiles_dir, files[-1])

    try:
        profile_df = pl.read_parquet(profiles_file)
    except Exception:
        return {}

    # ---- 2. Load mapped_tickers for rename + delisting universe ----
    try:
        from jerry_trader.services.market_data.bootstrap.ticker_utils import (
            get_mapped_tickers,
        )

        mapped = get_mapped_tickers()
    except Exception:
        # Fallback: simple symbol-keyed lookup without mapping
        profiles = {}
        for row in profile_df.iter_rows(named=True):
            profiles[row["symbol"]] = row
        return profiles

    # ---- 3. Build old_ticker → latest_ticker mapping ----
    old_to_latest: dict[str, str] = {}
    for row in mapped.select(["ticker", "latest_ticker"]).iter_rows(named=True):
        old_to_latest[row["ticker"]] = row["latest_ticker"]

    # Also collect delisted_utc per ticker (aligned with tickers list)
    ticker_delisted: dict[str, str | None] = {}
    for row in mapped.select(["tickers", "all_delisted_utc"]).iter_rows(named=True):
        for t, d in zip(row["tickers"], row["all_delisted_utc"]):
            if t not in ticker_delisted:  # first occurrence is the earliest
                ticker_delisted[t] = d

    # ---- 4. Build profile lookup keyed by latest_ticker ----
    latest_profiles: dict[str, dict] = {}
    for row in profile_df.iter_rows(named=True):
        latest_profiles[row["symbol"]] = row

    # ---- 5. Build result: all tickers active on date_str → profile ----
    profiles: dict[str, dict] = {}
    cutoff = date_str  # "2026-03-06"

    for old_ticker, latest_ticker in old_to_latest.items():
        # Check if this ticker was active on the backtest date
        d = ticker_delisted.get(old_ticker)
        if d is not None and d <= cutoff:
            continue  # delisted on or before backtest date

        # Translate old name → profile
        p = latest_profiles.get(latest_ticker)
        if p is None:
            continue

        profiles[old_ticker] = p
        # Also store under latest_ticker for direct lookups
        if latest_ticker not in profiles:
            profiles[latest_ticker] = p

    return profiles


def _load_trades_ch(ticker: str, date_str: str) -> pd.DataFrame:
    """Load trades for a single ticker/date from ClickHouse.

    Returns DataFrame with columns: ts_ms, price, size

    Filters out FINRA TRF (exchange=4) delayed reports where
    sip_timestamp - participant_timestamp > 1s. These stale trades
    pollute price-based analysis during the 8:00 ET batch release.
    """
    from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

    ch = get_clickhouse_client()
    if ch is None:
        raise RuntimeError("ClickHouse not available. Set CLICKHOUSE_PASSWORD env var.")

    db = "jerry_trader"
    result = ch.query(
        f"SELECT sip_timestamp, price, size "
        f"FROM {db}.trades FINAL "
        f"WHERE ticker = %(ticker)s AND date = %(date)s "
        f"AND (exchange != 4 "
        f"     OR sip_timestamp - participant_timestamp <= 1000000000) "
        f"ORDER BY sip_timestamp",
        parameters={"ticker": ticker, "date": date_str},
    )
    rows = result.result_rows
    if not rows:
        return pd.DataFrame(columns=["ts_ms", "price", "size"])

    data = [(int(r[0]) // 1_000_000, float(r[1]), int(r[2])) for r in rows]
    return pd.DataFrame(data, columns=["ts_ms", "price", "size"])


def _load_trades_batch_ch(tickers: list[str], date_str: str) -> dict[str, pd.DataFrame]:
    """Load trades for multiple tickers in a single ClickHouse query.

    Uses one client + one query to avoid per-ticker connection overhead.
    Returns dict keyed by ticker, each value a DataFrame with ts_ms, price, size.
    """
    from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

    ch = get_clickhouse_client()
    if ch is None:
        raise RuntimeError("ClickHouse not available. Set CLICKHOUSE_PASSWORD env var.")

    result: dict[str, pd.DataFrame] = {
        t: pd.DataFrame(columns=["ts_ms", "price", "size"]) for t in tickers
    }
    if not tickers:
        return result

    db = "jerry_trader"
    ticker_str = ", ".join(f"'{t}'" for t in tickers)
    rows = ch.query(
        f"SELECT ticker, sip_timestamp, price, size "
        f"FROM {db}.trades FINAL "
        f"WHERE ticker IN ({ticker_str}) AND date = %(date)s "
        f"ORDER BY ticker, sip_timestamp",
        parameters={"date": date_str},
    ).result_rows

    if not rows:
        return result

    data: dict[str, list[tuple[int, float, int]]] = {t: [] for t in tickers}
    for r in rows:
        ticker = r[0]
        data[ticker].append((int(r[1]) // 1_000_000, float(r[2]), int(r[3])))

    for ticker, entries in data.items():
        result[ticker] = pd.DataFrame(entries, columns=["ts_ms", "price", "size"])

    return result


def _load_quotes_ch(ticker: str, date_str: str) -> pd.DataFrame:
    """Load quotes for a single ticker/date from ClickHouse.

    Returns DataFrame with columns: ts_ms, bid, ask, bid_size, ask_size
    """
    from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

    ch = get_clickhouse_client()
    if ch is None:
        raise RuntimeError("ClickHouse not available. Set CLICKHOUSE_PASSWORD env var.")

    db = "jerry_trader"
    result = ch.query(
        f"SELECT sip_timestamp, bid_price, ask_price, bid_size, ask_size "
        f"FROM {db}.quotes FINAL "
        f"WHERE ticker = %(ticker)s AND date = %(date)s "
        f"ORDER BY sip_timestamp",
        parameters={"ticker": ticker, "date": date_str},
    )
    rows = result.result_rows
    if not rows:
        return pd.DataFrame(columns=["ts_ms", "bid", "ask", "bid_size", "ask_size"])

    data = [
        (
            int(r[0]) // 1_000_000,
            float(r[1]),
            float(r[2]),
            int(r[3]),
            int(r[4]),
        )
        for r in rows
    ]
    return pd.DataFrame(data, columns=["ts_ms", "bid", "ask", "bid_size", "ask_size"])


def _load_quotes_batch_ch(tickers: list[str], date_str: str) -> dict[str, pd.DataFrame]:
    """Load quotes for multiple tickers in a single ClickHouse query.

    Uses one client + one query to avoid per-ticker connection overhead.
    Returns dict keyed by ticker, each value a DataFrame with ts_ms, bid, ask, bid_size, ask_size.
    """
    from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

    ch = get_clickhouse_client()
    if ch is None:
        raise RuntimeError("ClickHouse not available. Set CLICKHOUSE_PASSWORD env var.")

    result: dict[str, pd.DataFrame] = {
        t: pd.DataFrame(columns=["ts_ms", "bid", "ask", "bid_size", "ask_size"])
        for t in tickers
    }
    if not tickers:
        return result

    db = "jerry_trader"
    ticker_str = ", ".join(f"'{t}'" for t in tickers)
    rows = ch.query(
        f"SELECT ticker, sip_timestamp, bid_price, ask_price, bid_size, ask_size "
        f"FROM {db}.quotes FINAL "
        f"WHERE ticker IN ({ticker_str}) AND date = %(date)s "
        f"ORDER BY ticker, sip_timestamp",
        parameters={"date": date_str},
    ).result_rows

    if not rows:
        return result

    data: dict[str, list[tuple[int, float, float, int, int]]] = {t: [] for t in tickers}
    for r in rows:
        ticker = r[0]
        data[ticker].append(
            (int(r[1]) // 1_000_000, float(r[2]), float(r[3]), int(r[4]), int(r[5]))
        )

    for ticker, entries in data.items():
        result[ticker] = pd.DataFrame(
            entries, columns=["ts_ms", "bid", "ask", "bid_size", "ask_size"]
        )

    return result


def _load_trades_csv(ticker: str, date_str: str) -> pd.DataFrame:
    """Offline path: stream a single-ticker trades CSV.gz from Polygon raw data."""
    year, month, day = date_str.split("-")
    path = (
        POLYGON_DATA_ROOT
        / "us_stocks_sip"
        / "trades_v1"
        / year
        / month
        / f"{date_str}.csv.gz"
    )
    if not path.exists():
        raise FileNotFoundError(f"Trades file not found: {path}")

    rows = []
    with gzip.open(path, "rt") as f:
        header = f.readline().strip().split(",")
        sip_idx = header.index("sip_timestamp")
        price_idx = header.index("price")
        size_idx = header.index("size")
        ticker_idx = header.index("ticker")

        for line in f:
            if ticker not in line:
                continue
            parts = line.strip().split(",")
            if parts[ticker_idx] != ticker:
                continue
            ts_ms = _ns_to_ms(int(parts[sip_idx]))
            price = float(parts[price_idx])
            size = int(float(parts[size_idx]))
            rows.append((ts_ms, price, size))

    return pd.DataFrame(rows, columns=["ts_ms", "price", "size"])


def _load_quotes_csv(ticker: str, date_str: str) -> pd.DataFrame:
    """Offline path: stream a single-ticker quotes CSV.gz from Polygon raw data."""
    year, month, day = date_str.split("-")
    path = (
        POLYGON_DATA_ROOT
        / "us_stocks_sip"
        / "quotes_v1"
        / year
        / month
        / f"{date_str}.csv.gz"
    )
    if not path.exists():
        raise FileNotFoundError(f"Quotes file not found: {path}")

    rows = []
    with gzip.open(path, "rt") as f:
        header = f.readline().strip().split(",")
        sip_idx = header.index("sip_timestamp")
        bid_p_idx = header.index("bid_price")
        ask_p_idx = header.index("ask_price")
        bid_s_idx = header.index("bid_size")
        ask_s_idx = header.index("ask_size")
        ticker_idx = header.index("ticker")

        for line in f:
            if ticker not in line:
                continue
            parts = line.strip().split(",")
            if parts[ticker_idx] != ticker:
                continue
            ts_ms = _ns_to_ms(int(parts[sip_idx]))
            bid = float(parts[bid_p_idx])
            ask = float(parts[ask_p_idx])
            bid_size = int(float(parts[bid_s_idx]))
            ask_size = int(float(parts[ask_s_idx]))
            rows.append((ts_ms, bid, ask, bid_size, ask_size))

    return pd.DataFrame(rows, columns=["ts_ms", "bid", "ask", "bid_size", "ask_size"])


# ══════════════════════════════════════════════════════════════════════════
# Core lab
# ══════════════════════════════════════════════════════════════════════════
