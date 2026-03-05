"""
Centralized Redis Key Schema.

Single source of truth for every session-scoped Redis key in the system.
All services import key-builder functions from here instead of hardcoding
f-string patterns, so a key rename is a one-line change.

Key Categories:
    Streams       – Append-only logs consumed via XREADGROUP / XREAD.
    Fixed keys    – One key per session (SET, ZSET, HSET).
    Per-symbol    – One key per (session, symbol) pair.
    Per-item      – One key per (session, news_id) pair.

Cleanup:
    clear_session_keys(r, session_id)   – SCAN + DELETE every key for a session.
    rollback_session_streams(r, sid, ms)– Trim all streams after a timestamp.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    import redis as _redis

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Streams
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def market_snapshot_stream(session_id: str) -> str:
    """Raw snapshot stream from collector / replayer → SnapshotProcessor."""
    return f"market_snapshot_stream:{session_id}"


def market_snapshot_processed(session_id: str) -> str:
    """Processed snapshot stream → BFF, StateEngine, NewsWorker, OverviewChart."""
    return f"market_snapshot_processed:{session_id}"


def movers_state_stream(session_id: str) -> str:
    """State engine output stream → BFF."""
    return f"movers_state:{session_id}"


def static_update_stream(session_id: str) -> str:
    """Static data update notifications → BFF."""
    return f"static_update_stream:{session_id}"


def news_article_stream(session_id: str) -> str:
    """Per-article news notifications → BFF, NewsProcessor."""
    return f"news_article_stream:{session_id}"


def factor_tasks_stream(session_id: str) -> str:
    """Factor computation task queue → FactorEngine."""
    return f"factor_tasks:{session_id}"


def signal_events_stream(session_id: str) -> str:
    """State-change signal events (ACTIVE / QUIET) → BFF, replay."""
    return f"signal_events:{session_id}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Fixed keys – one per session
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def movers_subscribed_set(session_id: str) -> str:
    """ZSET tracking all tickers that have appeared in the top-N."""
    return f"movers_subscribed_set:{session_id}"


def state_cursor(session_id: str) -> str:
    """HSET of per-ticker cursor timestamps for SnapshotProcessor / StateEngine recovery."""
    return f"state_cursor:{session_id}"


def static_pending(session_id: str) -> str:
    """SET of tickers awaiting static-data fetch (fundamentals, float)."""
    return f"static:pending:{session_id}"


def static_processing(session_id: str) -> str:
    """SET of tickers currently being processed by StaticDataWorker."""
    return f"static:processing:{session_id}"


def news_pending(session_id: str) -> str:
    """SET of tickers awaiting news fetch."""
    return f"static:pending:news:{session_id}"


def news_processing(session_id: str) -> str:
    """SET of tickers currently being processed by NewsWorker."""
    return f"static:processing:news:{session_id}"


def news_seen_articles(session_id: str) -> str:
    """SET of URL-hash fingerprints for deduplication."""
    return f"news:seen_articles:{session_id}"


def news_seen_titles(session_id: str) -> str:
    """SET of title fingerprints for cross-source deduplication."""
    return f"news:seen_titles:{session_id}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Per-symbol keys  (prefix + :{symbol})
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def static_ticker_summary(session_id: str, symbol: str) -> str:
    """HSET – ticker fundamentals summary."""
    return f"static:ticker:summary:{session_id}:{symbol}"


def static_ticker_summary_prefix(session_id: str) -> str:
    """Prefix for iterating / scanning all ticker summaries."""
    return f"static:ticker:summary:{session_id}"


def static_ticker_profile(session_id: str, symbol: str) -> str:
    """HSET – ticker company profile."""
    return f"static:ticker:profile:{session_id}:{symbol}"


def static_ticker_profile_prefix(session_id: str) -> str:
    return f"static:ticker:profile:{session_id}"


def news_ticker_zset(session_id: str, symbol: str) -> str:
    """ZSET – news IDs for a ticker, scored by published_time."""
    return f"news:ticker:{session_id}:{symbol}"


def news_ticker_prefix(session_id: str) -> str:
    return f"news:ticker:{session_id}"


def factor_hset(session_id: str, symbol: str) -> str:
    """HSET – latest factor snapshot per ticker (from FactorEngine)."""
    return f"factor:{session_id}:{symbol}"


def factor_hset_prefix(session_id: str) -> str:
    """Prefix for SCAN-discovering all factor HSETs."""
    return f"factor:{session_id}:"


def static_version(session_id: str, symbol: str, domain: str) -> str:
    """STRING counter – monotonic version per (symbol, domain)."""
    return f"static:version:{session_id}:{symbol}:{domain}"


def static_version_prefix(session_id: str) -> str:
    return f"static:version:{session_id}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Chart bars cache keys
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def chart_bars_cache(
    ticker: str, multiplier: int, timespan: str, from_date: str, to_date: str
) -> str:
    """STRING – cached OHLCV bars JSON (session-independent, same data for same params)."""
    return f"chart:bars:{ticker}:{multiplier}:{timespan}:{from_date}:{to_date}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Per-item keys  (prefix + :{id})
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def news_item(session_id: str, news_id: str) -> str:
    """HSET – cached news article detail."""
    return f"news:item:{session_id}:{news_id}"


def news_item_prefix(session_id: str) -> str:
    return f"news:item:{session_id}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Aggregate lists (used by cleanup / rollback helpers)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ALL_STREAM_BUILDERS = [
    market_snapshot_stream,
    market_snapshot_processed,
    movers_state_stream,
    static_update_stream,
    news_article_stream,
    factor_tasks_stream,
    signal_events_stream,
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Cleanup / rollback helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def scan_session_keys(r: "_redis.Redis", session_id: str) -> List[str]:
    """
    Return every Redis key whose name contains ``:{session_id}``.

    Uses ``SCAN`` so it never blocks the server.
    """
    pattern = f"*:{session_id}*"
    keys: List[str] = []
    cursor = 0
    while True:
        cursor, batch = r.scan(cursor, match=pattern, count=500)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys


def clear_session_keys(r: "_redis.Redis", session_id: str) -> int:
    """
    Delete **all** Redis keys belonging to *session_id*.

    Returns the number of keys deleted.
    """
    keys = scan_session_keys(r, session_id)
    if not keys:
        logger.info(f"clear_session_keys - No keys found for session {session_id}")
        return 0
    deleted = r.delete(*keys)
    logger.info(
        f"clear_session_keys - Deleted {deleted}/{len(keys)} keys for session {session_id}"
    )
    return deleted


def rollback_session_streams(
    r: "_redis.Redis", session_id: str, rollback_ts_ms: int
) -> int:
    """
    Remove entries **after** *rollback_ts_ms* from every session stream.

    Returns total number of entries deleted.
    """
    total_deleted = 0
    for builder in ALL_STREAM_BUILDERS:
        stream_name = builder(session_id)
        if not r.exists(stream_name):
            logger.info(
                f"rollback_session_streams - {stream_name} does not exist, skipping"
            )
            continue

        entries = r.xrange(stream_name)
        to_delete = [
            eid for eid, _ in entries if int(eid.split("-")[0]) > rollback_ts_ms
        ]
        if to_delete:
            r.xdel(stream_name, *to_delete)
            logger.info(
                f"rollback_session_streams - Deleted {len(to_delete)} entries from {stream_name}"
            )
            total_deleted += len(to_delete)
        else:
            logger.info(
                f"rollback_session_streams - No entries to delete from {stream_name}"
            )
    return total_deleted
