"""Observation sources — load trigger points from various data origins."""

from __future__ import annotations

import json as _json
import re
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jerry_trader.domain.analysis import Observation

# ── Log parser regexes (classifier-specific) ────────────────────────────────

_MODEL_LINE = re.compile(r"^model: (\S+)")
_RESULT_LINE = re.compile(r"^(\S+) (✅|❌) (.+)$")
_TITLE_LINE = re.compile(r"^Title:(.*)$")
_PUBLISHED_LINE = re.compile(r"^Published Time: (.*)$")
_CURRENT_LINE = re.compile(r"^Current Time: (.*)$")
_URL_LINE = re.compile(r"^Url: (.*)$")
_SOURCE_FROM_LINE = re.compile(r"^Source From: (.*)$")
_CONTENT_LINE = re.compile(r"^Content: (.*)$")


def _parse_isotime(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        ts = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


# ── Abstract source ─────────────────────────────────────────────────────────


class ObservationSource(ABC):
    """Load :class:`Observation` objects for a date range."""

    @abstractmethod
    def load(self, date_range: tuple[str, str]) -> list[Observation]:
        ...


# ── Classifier source ───────────────────────────────────────────────────────


class ClassifierObservationSource(ObservationSource):
    """Load classifier observations directly from ClickHouse ``news_classifications``.

    This is the preferred source once the NewsProcessor is writing to ClickHouse.
    No log parsing needed — just a SQL query.
    """

    def __init__(self, ch_client: Any):
        self._ch = ch_client

    def load(self, date_range: tuple[str, str]) -> list[Observation]:
        start, end = date_range
        query = (
            "SELECT symbol, current_time, is_catalyst, score, score_num, "
            "title, published_time, url, source_from, content_preview, "
            "model, explanation "
            "FROM jerry_trader.news_classifications "
            f"WHERE trade_date BETWEEN '{start}' AND '{end}' "
            "ORDER BY current_time"
        )
        try:
            result = self._ch.query(query)
            rows = result.result_rows
        except Exception:
            return []

        observations: list[Observation] = []
        for row in rows:
            (
                symbol, current_time, is_catalyst, score, score_num,
                title, published_time, url, source_from, content,
                model, explanation,
            ) = row

            trigger_time = current_time
            if isinstance(trigger_time, datetime) and trigger_time.tzinfo is None:
                trigger_time = trigger_time.replace(tzinfo=timezone.utc)

            observations.append(Observation(
                symbol=str(symbol).upper(),
                trigger_time=trigger_time,
                source="classifier",
                metadata={
                    "is_catalyst": bool(is_catalyst),
                    "score": str(score) if score else "0/10",
                    "score_num": int(score_num) if score_num else 0,
                    "title": str(title) if title else "",
                    "published_time": str(published_time) if published_time else "",
                    "url": str(url) if url else "",
                    "source_from": str(source_from) if source_from else "unknown",
                    "content": str(content) if content else "",
                    "model": str(model) if model else "",
                    "explanation": str(explanation) if explanation else "",
                },
            ))

        return observations


def _parse_processor_log(log_path: Path) -> list[Observation]:
    """Parse a single processor log file into Observations."""
    observations: list[Observation] = []
    current: dict[str, Any] | None = None

    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")

            m = _MODEL_LINE.match(line)
            if m:
                if current is not None and "symbol" in current:
                    obs = _dict_to_observation(current)
                    if obs:
                        observations.append(obs)
                current = {"model": m.group(1)}
                continue

            if current is None:
                continue

            m = _RESULT_LINE.match(line)
            if m:
                current["symbol"] = m.group(1)
                current["is_catalyst"] = m.group(2) == "✅"
                current["score"] = m.group(3)
                continue

            for pat, key in [
                (_TITLE_LINE, "title"),
                (_PUBLISHED_LINE, "published_time"),
                (_CURRENT_LINE, "current_time"),
                (_URL_LINE, "url"),
                (_SOURCE_FROM_LINE, "source_from"),
                (_CONTENT_LINE, "content"),
            ]:
                m = pat.match(line)
                if m:
                    current[key] = m.group(1).strip()
                    break
            else:
                if line.startswith("Explanation: "):
                    expl = line[len("Explanation: "):]
                    if expl.startswith("{"):
                        try:
                            current["explanation"] = _json.loads(expl)
                        except Exception:
                            current["explanation"] = expl
                    else:
                        current["explanation"] = expl

    # Last entry
    if current is not None and "symbol" in current:
        obs = _dict_to_observation(current)
        if obs:
            observations.append(obs)

    return observations


def _dict_to_observation(d: dict[str, Any]) -> Observation | None:
    """Convert a parsed log dict to an Observation."""
    symbol = d.get("symbol", "").upper()
    if not symbol:
        return None

    trigger_time = _parse_isotime(d.get("current_time", ""))
    if trigger_time is None:
        return None

    score_str = d.get("score", "0")
    try:
        score_num = int(score_str.split("/")[0])
    except (ValueError, IndexError):
        score_num = 0

    return Observation(
        symbol=symbol,
        trigger_time=trigger_time,
        source="classifier",
        metadata={
            "score": score_str,
            "score_num": score_num,
            "is_catalyst": d.get("is_catalyst", False),
            "title": d.get("title", ""),
            "published_time": d.get("published_time", ""),
            "url": d.get("url", ""),
            "source_from": d.get("source_from", "unknown"),
            "content": d.get("content", ""),
            "explanation": d.get("explanation", ""),
            "model": d.get("model", ""),
        },
    )
