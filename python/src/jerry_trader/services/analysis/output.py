"""HTML / CSV / terminal output formatters for AnalysisReport."""

from __future__ import annotations

import csv
from datetime import datetime
from io import StringIO
from typing import Any

from jerry_trader.domain.analysis import AnalysisReport, Horizon, Metric, SliceResult


def _esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _pct(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value * 100:+.2f}%"


def _pct_raw(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value * 100:.1f}%"


# ── HTML ────────────────────────────────────────────────────────────────────


def render_html(report: AnalysisReport) -> str:
    """Generate a self-contained dark-themed HTML report."""
    cfg = report.config
    date_display = (
        f"{cfg.date_range[0]} → {cfg.date_range[1]}"
        if cfg.date_range[0] != cfg.date_range[1]
        else cfg.date_range[0]
    )
    dedup_note = " (per-symbol deduped)" if cfg.dedup else ""

    # ── Compute stats ──────────────────────────────────────────────────
    all_outcomes: list[Any] = []
    for sl in report.slices:
        all_outcomes.extend(sl.outcomes)
    # Deduplicate by observation id for counting catalysts
    seen = set()
    unique: list[Any] = []
    for o in all_outcomes:
        sid = (o.symbol, o.trigger_time)
        if sid not in seen:
            seen.add(sid)
            unique.append(o)

    catalysts = [o for o in unique if o.observation.metadata.get("is_catalyst")]
    non_catalysts = [o for o in unique if not o.observation.metadata.get("is_catalyst")]
    fn_10 = [o for o in non_catalysts if (o.max_return_at(Horizon.M15) or 0) >= 0.10]
    with_returns = [o for o in all_outcomes if o.max_return_at(Horizon.M15) is not None]

    # ── Slice tables ───────────────────────────────────────────────────
    slice_rows_all = _build_slice_rows(report.slices)
    cat_slices = [sl for sl in report.slices
                  if any(o.observation.metadata.get("is_catalyst") for o in sl.outcomes)]
    slice_rows_cat = _build_slice_rows(cat_slices)

    # ── Top 10 ─────────────────────────────────────────────────────────
    sorted_max = sorted(with_returns, key=lambda o: o.max_return_at(Horizon.M15) or -99, reverse=True)
    top10 = "\n".join(
        "<tr>"
        f"<td>{'✅' if o.observation.metadata.get('is_catalyst') else '❌'}</td>"
        f"<td><b>{_esc(o.symbol)}</b></td>"
        f"<td>{o.observation.metadata.get('score','?')}</td>"
        f"<td style='font-weight:700;color:#3fb950;'>{_pct(o.max_return_at(Horizon.M15))}</td>"
        f"<td>{_pct(o.return_at(Horizon.M15))}</td>"
        "<td style='max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'"
        " title='" + _esc(str(o.observation.metadata.get('title',''))) + "'>"
        + _esc(str(o.observation.metadata.get('title',''))[:55]) + "</td>"
        "</tr>"
        for o in sorted_max[:10]
    )

    # ── False negatives ────────────────────────────────────────────────
    fn_rows = "\n".join(
        "<tr>"
        f"<td><b>{_esc(o.symbol)}</b></td>"
        f"<td>{o.observation.metadata.get('score','?')}</td>"
        f"<td style='color:#f85149;font-weight:700;'>{_pct(o.max_return_at(Horizon.M15))}</td>"
        "<td style='max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'"
        " title='" + _esc(str(o.observation.metadata.get('title',''))) + "'>"
        + _esc(str(o.observation.metadata.get('title',''))[:50]) + "</td>"
        "</tr>"
        for o in sorted(fn_10, key=lambda o: o.max_return_at(Horizon.M15) or 0, reverse=True)[:10]
    ) if fn_10 else "<tr><td colspan='4'>None</td></tr>"

    # ── Assemble ───────────────────────────────────────────────────────
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Classifier Trace — {date_display}</title>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  background:#0d1117; color:#c9d1d9; padding:24px; }}
h1 {{ font-size:1.4em; color:#f0f6fc; margin-bottom:4px; }}
h2 {{ font-size:1.1em; color:#f0f6fc; margin:28px 0 12px 0; border-bottom:1px solid #21262d; padding-bottom:6px; }}
.subtitle {{ color:#8b949e; font-size:0.9em; margin-bottom:20px; }}
.card {{ background:#161b22; border:1px solid #30363d; border-radius:8px; padding:20px; margin-bottom:16px; }}
.stat-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:12px; }}
.stat {{ background:#0d1117; border:1px solid #21262d; border-radius:6px; padding:14px; text-align:center; }}
.stat-value {{ font-size:1.6em; font-weight:700; }}
.stat-label {{ font-size:0.78em; color:#8b949e; margin-top:2px; }}
.green {{ color:#3fb950; }} .yellow {{ color:#d29922; }} .red {{ color:#f85149; }} .gray {{ color:#8b949e; }}
table {{ width:100%; border-collapse:collapse; font-size:0.88em; }}
th {{ text-align:left; padding:8px 10px; border-bottom:2px solid #30363d; color:#8b949e;
  font-weight:600; font-size:0.82em; text-transform:uppercase; letter-spacing:0.5px; }}
td {{ padding:7px 10px; border-bottom:1px solid #21262d; }}
tr:hover td {{ background:rgba(255,255,255,0.02); }}
.footer {{ text-align:center; color:#484f58; font-size:0.78em; margin-top:32px; }}
</style></head>
<body>
<h1>&#x1f4ca; Classifier Trace — {date_display}{dedup_note}</h1>
<div class="subtitle">LLM news classification performance &middot; max return in 15min window &middot; generated {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}</div>

<div class="card">
  <div class="stat-grid">
    <div class="stat"><div class="stat-value">{len(unique)}</div><div class="stat-label">Total Unique</div></div>
    <div class="stat"><div class="stat-value green">{len(catalysts)}</div><div class="stat-label">Catalysts (&#x2705;)</div></div>
    <div class="stat"><div class="stat-value gray">{len(non_catalysts)}</div><div class="stat-label">Non-Catalysts (&#x274c;)</div></div>
    <div class="stat"><div class="stat-value">{len(with_returns)}</div><div class="stat-label">With Return Data</div></div>
    <div class="stat"><div class="stat-value red">{len(fn_10)}</div><div class="stat-label">False Negatives &gt;+10%</div></div>
  </div>
</div>

<h2>&#x26a1; Max Return 15min — All Tickers</h2>
<div class="card">
<table>
<thead><tr><th>Score</th><th>n</th><th>Avg Max</th>
<th>&gt; +5%</th><th>&gt; +10%</th><th>&gt; +20%</th><th>&gt; +50%</th></tr></thead>
<tbody>{slice_rows_all}</tbody>
</table>
</div>

<h2>&#x2705; Catalysts Only</h2>
<div class="card">
<table>
<thead><tr><th>Score</th><th>n</th><th>Avg Max</th>
<th>&gt; +5%</th><th>&gt; +10%</th><th>&gt; +20%</th><th>&gt; +50%</th></tr></thead>
<tbody>{slice_rows_cat if slice_rows_cat else '<tr><td colspan="7">No catalyst data</td></tr>'}</tbody>
</table>
</div>

<h2>&#x1f525; Top 10 — Highest Max Return</h2>
<div class="card"><table>
<thead><tr><th></th><th>Symbol</th><th>Score</th><th>Max 15m</th><th>Ret 15m</th><th>Title</th></tr></thead>
<tbody>{top10}</tbody>
</table></div>

<h2>&#x26a0;&#xfe0f; False Negatives — Classified &#x274c; but Spiked</h2>
<div class="card"><table>
<thead><tr><th>Symbol</th><th>Score</th><th>Max 15m</th><th>Title</th></tr></thead>
<tbody>{fn_rows}</tbody>
</table></div>

<div class="footer">Jerry Trader &middot; Analysis Engine &middot; {len(unique)} symbols &middot; {report.total_outcomes} outcomes</div>
</body></html>"""


def _build_slice_rows(slices: list[SliceResult]) -> str:
    score_colors = {"8-10": "#3fb950", "6-7": "#d29922", "4-5": "#e67e22", "1-3": "#8b949e"}
    rows = []
    for sl in slices:
        n = sl.count
        if n == 0:
            continue
        avg_max = sl.metrics.get(Metric.AVG_MAX_RETURN)
        gt5 = sl.metrics.get(Metric.HIT_RATE_GT_5PCT, 0)
        gt10 = sl.metrics.get(Metric.HIT_RATE_GT_10PCT, 0)
        gt20 = sl.metrics.get(Metric.HIT_RATE_GT_20PCT, 0)
        gt50 = sl.metrics.get(Metric.HIT_RATE_GT_50PCT, 0)
        color = score_colors.get(sl.label, "#8b949e")
        n5 = int(gt5 * n) if gt5 else 0
        n10 = int(gt10 * n) if gt10 else 0
        n20 = int(gt20 * n) if gt20 else 0
        n50 = int(gt50 * n) if gt50 else 0
        rows.append(
            f"<tr>"
            f'<td style="color:{color};font-weight:700;">{sl.label}</td>'
            f"<td>{n}</td><td>{_pct(avg_max)}</td>"
            f"<td><b>{n5}</b> <span style='color:#888;font-size:0.85em;'>({_pct_raw(gt5)})</span></td>"
            f"<td><b>{n10}</b> <span style='color:#888;font-size:0.85em;'>({_pct_raw(gt10)})</span></td>"
            f"<td><b>{n20}</b> <span style='color:#888;font-size:0.85em;'>({_pct_raw(gt20)})</span></td>"
            f"<td><b>{n50}</b> <span style='color:#888;font-size:0.85em;'>({_pct_raw(gt50)})</span></td>"
            f"</tr>"
        )
    return "\n".join(rows)


# ── Terminal ────────────────────────────────────────────────────────────────


def render_terminal(report: AnalysisReport, horizon: Horizon = Horizon.M15) -> str:
    """Print a human-readable summary (like the old script output)."""
    lines = [report.summary(horizon)]
    # Top 10
    all_o: list[Any] = []
    for sl in report.slices:
        all_o.extend(sl.outcomes)
    sorted_m = sorted(all_o, key=lambda o: o.max_return_at(horizon) or -99, reverse=True)
    if sorted_m:
        lines.append(f"\n  🔥 TOP 10 — Highest Max Return {horizon.value}:")
        for o in sorted_m[:10]:
            cat = "✅" if o.observation.metadata.get("is_catalyst") else "❌"
            lines.append(
                f"    {o.symbol:6s}  {cat} {str(o.observation.metadata.get('score','?')):>5s}  "
                f"max={_pct(o.max_return_at(horizon))}  "
                f"{str(o.observation.metadata.get('title',''))[:50]}"
            )
    return "\n".join(lines)


# ── CSV ────────────────────────────────────────────────────────────────────


def write_csv(report: AnalysisReport, path: str) -> None:
    """Write outcomes to a CSV file."""
    all_o: list[Any] = []
    for sl in report.slices:
        all_o.extend(sl.outcomes)
    if not all_o:
        return

    fieldnames = [
        "symbol", "is_catalyst", "score", "title", "trigger_time",
        "trigger_price",
        "return_5m", "return_15m", "return_30m",
        "max_return_5m", "max_return_15m", "max_return_30m",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for o in all_o:
            writer.writerow({
                "symbol": o.symbol,
                "is_catalyst": o.observation.metadata.get("is_catalyst"),
                "score": o.observation.metadata.get("score"),
                "title": o.observation.metadata.get("title"),
                "trigger_time": o.trigger_time.isoformat(),
                "trigger_price": o.trigger_price,
                "return_5m": o.return_at(Horizon.M5),
                "return_15m": o.return_at(Horizon.M15),
                "return_30m": o.return_at(Horizon.M30),
                "max_return_5m": o.max_return_at(Horizon.M5),
                "max_return_15m": o.max_return_at(Horizon.M15),
                "max_return_30m": o.max_return_at(Horizon.M30),
            })
    print(f"CSV written to {path}")
