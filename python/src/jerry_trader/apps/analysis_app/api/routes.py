"""Analysis API routes — classifier trace, observation sources, multi-date reports."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from jerry_trader.domain.analysis import (
    AnalysisConfig,
    Horizon,
    Metric,
    SliceConfig,
    SliceDimension,
)
from jerry_trader.services.analysis.analysis_lab import AnalysisLab

router = APIRouter()

# ── ClickHouse client (lazy) ───────────────────────────────────────────────

_ch_client = None


def _get_ch_client(request: Request):
    """Get ClickHouse client from app.state config (set by BackendStarter)."""
    ch_config = dict(getattr(request.app.state, "ch_config", None) or {})

    # Resolve password: the config may have 'password_env' (set by config builder)
    # instead of a literal 'password'. Read the env var if needed.
    if "password_env" in ch_config and "password" not in ch_config:
        ch_config["password"] = os.getenv(ch_config["password_env"], "")

    if not ch_config:
        ch_config = {
            "host": os.getenv("ANALYSIS_CH_HOST", "localhost"),
            "port": int(os.getenv("ANALYSIS_CH_PORT", "8123")),
            "user": os.getenv("ANALYSIS_CH_USER", "default"),
            "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
            "database": "jerry_trader",
        }
    if not ch_config.get("password"):
        raise HTTPException(status_code=500, detail="ClickHouse password not configured")
    from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

    client = get_clickhouse_client(ch_config)
    if client is None:
        raise HTTPException(status_code=500, detail="ClickHouse connection failed")
    return client


# ── Endpoints ─────────────────────────────────────────────────────────────


@router.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@router.get("/sources")
async def list_sources():
    """Return available observation sources."""
    return {
        "sources": [
            {"id": "classifier", "name": "LLM Classifier Trace",
             "description": "NewsProcessor classification performance"}
        ]
    }


@router.get("/classifier-trace")
async def classifier_trace(
    request: Request,
    date: str = Query(..., description="Date YYYYMMDD (e.g. 20260626)"),
    dedup: bool = Query(True, description="Keep best-scored per symbol"),
    horizon: str = Query("15m", description="Primary horizon: 5m, 15m, 30m"),
):
    """Run classifier trace for a single date. Returns slice results + top outcomes."""
    ch = _get_ch_client(request)
    h = Horizon(horizon)

    config = AnalysisConfig(
        source="classifier",
        date_range=(date, date),
        horizons=[Horizon.M5, Horizon.M15, Horizon.M30],
        slices=[SliceConfig(dimension=SliceDimension.SCORE_BUCKET)],
        metrics=[
            Metric.COUNT, Metric.AVG_MAX_RETURN,
            Metric.HIT_RATE_GT_5PCT, Metric.HIT_RATE_GT_10PCT,
            Metric.HIT_RATE_GT_20PCT, Metric.HIT_RATE_GT_50PCT,
        ],
        dedup=dedup,
    )

    try:
        lab = AnalysisLab(config, ch)
        report = lab.run()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Flatten to clean JSON
    result: dict[str, Any] = {
        "date": date,
        "dedup": dedup,
        "horizon": horizon,
        "total_outcomes": report.total_outcomes,
        "slices": [],
        "top_outcomes": [],
        "false_negatives": [],
    }

    for sl in report.slices:
        slice_data: dict[str, Any] = {
            "label": sl.label,
            "count": sl.count,
            "metrics": {k.value: v for k, v in sl.metrics.items()},
        }
        result["slices"].append(slice_data)

    # Top 10 outcomes by max return
    all_outcomes: list[Any] = []
    for sl in report.slices:
        all_outcomes.extend(sl.outcomes)

    seen = set()
    unique: list[Any] = []
    for o in all_outcomes:
        sid = (o.symbol, o.trigger_time)
        if sid not in seen:
            seen.add(sid)
            unique.append(o)

    sorted_o = sorted(unique, key=lambda o: o.max_return_at(h) or -99, reverse=True)
    for o in sorted_o[:10]:
        result["top_outcomes"].append({
            "symbol": o.symbol,
            "score": o.observation.metadata.get("score", ""),
            "is_catalyst": o.observation.metadata.get("is_catalyst", False),
            "title": o.observation.metadata.get("title", ""),
            "trigger_time": o.trigger_time.isoformat(),
            "trigger_price": o.trigger_price,
            "max_return": o.max_return_at(h),
            "return": o.return_at(h),
        })

    # False negatives (>+10% max return but not catalyst)
    fn = [o for o in unique
          if not o.observation.metadata.get("is_catalyst")
          and (o.max_return_at(h) or 0) >= 0.10]
    for o in sorted(fn, key=lambda o: o.max_return_at(h) or 0, reverse=True)[:10]:
        result["false_negatives"].append({
            "symbol": o.symbol,
            "score": o.observation.metadata.get("score", ""),
            "title": o.observation.metadata.get("title", ""),
            "max_return": o.max_return_at(h),
        })

    # Summary stats
    catalysts = [o for o in unique if o.observation.metadata.get("is_catalyst")]
    non_catalysts = [o for o in unique if not o.observation.metadata.get("is_catalyst")]
    with_returns = [o for o in all_outcomes if o.max_return_at(h) is not None]
    result["summary"] = {
        "total_classified": len(unique),
        "catalysts": len(catalysts),
        "non_catalysts": len(non_catalysts),
        "with_return_data": len(with_returns),
        "false_negatives_gt_10pct": len(fn),
    }

    return result


@router.get("/classifier-multi")
async def classifier_multi(
    request: Request,
    start: str = Query(..., description="Start date YYYYMMDD"),
    end: str = Query(..., description="End date YYYYMMDD"),
    dedup: bool = Query(True),
):
    """Multi-date classifier trace. Returns per-date slice results."""
    ch = _get_ch_client(request)
    log_dir = os.getenv("ANALYSIS_LOG_DIR", "logs/jerry_trader")

    config = AnalysisConfig(
        source="classifier",
        date_range=(start, end),
        horizons=[Horizon.M15],
        slices=[SliceConfig(dimension=SliceDimension.SCORE_BUCKET)],
        dedup=dedup,
    )

    try:
        lab = AnalysisLab(config, ch, log_dir)
        report = lab.run()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    result: dict[str, Any] = {
        "date_range": [start, end],
        "dedup": dedup,
        "total_outcomes": report.total_outcomes,
        "slices": [],
    }
    for sl in report.slices:
        result["slices"].append({
            "label": sl.label,
            "count": sl.count,
            "metrics": {k.value: v for k, v in sl.metrics.items()},
        })
    return result
