#!/usr/bin/env python
"""Hindsight classifier performance analysis.

Thin CLI wrapper around the :class:`AnalysisLab` offline analysis engine.
All heavy lifting lives in ``services/analysis/`` and ``domain/analysis/``.

Usage:
    poetry run python scripts/classifier_trace.py --date 20260626 --dedup
    poetry run python scripts/classifier_trace.py --date 20260626 --dedup --html report.html
    poetry run python scripts/classifier_trace.py --date 20260626 --ssh-host mibuntu --ch-host 192.168.3.53
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from jerry_trader.domain.analysis import AnalysisConfig, Horizon, Metric, SliceConfig, SliceDimension
from jerry_trader.services.analysis.analysis_lab import AnalysisLab
from jerry_trader.services.analysis.output import render_html, render_terminal, write_csv

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def main():
    p = argparse.ArgumentParser(description="Trace LLM classifier performance")
    p.add_argument("--date", help="Date YYYYMMDD (e.g. 20260626)")
    p.add_argument("--input-file", help="Direct path to processor log")
    p.add_argument("--log-dir", default="logs/jerry_trader", help="Log directory root")
    p.add_argument("--ch-host", default="localhost", help="ClickHouse host")
    p.add_argument("--ch-port", type=int, default=8123)
    p.add_argument("--ch-user", default="default")
    p.add_argument("--ch-password", default=None)
    p.add_argument("--ssh-host", help="SSH host to fetch log + password from")
    p.add_argument("--ssh-remote-dir", default="/home/mibuntu/code-projects/jerry_trader")
    p.add_argument("--skip-returns", action="store_true")
    p.add_argument("--dedup", action="store_true", help="Keep best-scored per symbol")
    p.add_argument("--html", default=None, help="Output HTML path")
    p.add_argument("--csv", default=None, help="Output CSV path")
    p.add_argument("--limit", type=int, default=0, help="Limit entries (0=all)")
    args = p.parse_args()

    # ── Resolve log path ───────────────────────────────────────────────
    if args.input_file:
        log_path = args.input_file
    elif args.date:
        log_path = str(PROJECT_ROOT / args.log_dir / args.date / f"processor_{args.date}.log")
    else:
        print("ERROR: --date or --input-file required."); sys.exit(1)

    # Fetch log from remote if needed
    if args.ssh_host and not Path(log_path).exists() and args.date:
        remote = f"{args.ssh_remote_dir}/logs/jerry_trader/{args.date}/processor_{args.date}.log"
        print(f"Fetching: {args.ssh_host}:{remote}")
        tmp = tempfile.NamedTemporaryFile(suffix=".log", prefix=f"processor_{args.date}_", delete=False)
        tmp.close()
        r = subprocess.run(["scp", f"{args.ssh_host}:{remote}", tmp.name], capture_output=True, text=True)
        if r.returncode == 0:
            log_path = tmp.name
        else:
            print(f"WARNING: scp failed, trying local path")

    if not Path(log_path).exists():
        print(f"ERROR: Log not found: {log_path}"); sys.exit(1)

    # ── ClickHouse password ────────────────────────────────────────────
    ch_password = args.ch_password or os.getenv("CLICKHOUSE_PASSWORD", "")
    if not ch_password and args.ssh_host:
        r = subprocess.run(["ssh", args.ssh_host, "cat", f"{args.ssh_remote_dir}/.env"],
                           capture_output=True, text=True)
        if r.returncode == 0:
            for line in r.stdout.splitlines():
                if line.startswith("CLICKHOUSE_PASSWORD="):
                    ch_password = line.split("=", 1)[1].strip().strip("'").strip('"')
                    break

    # ── Analysis config ────────────────────────────────────────────────
    config = AnalysisConfig(
        source="classifier",
        date_range=(args.date, args.date),
        horizons=[Horizon.M5, Horizon.M15, Horizon.M30],
        slices=[SliceConfig(dimension=SliceDimension.SCORE_BUCKET)],
        metrics=[
            Metric.COUNT, Metric.AVG_MAX_RETURN,
            Metric.HIT_RATE_GT_5PCT, Metric.HIT_RATE_GT_10PCT,
            Metric.HIT_RATE_GT_20PCT, Metric.HIT_RATE_GT_50PCT,
        ],
        dedup=args.dedup,
    )

    # ── ClickHouse client ──────────────────────────────────────────────
    if not args.skip_returns:
        if not ch_password:
            print("ERROR: ClickHouse password required."); sys.exit(1)
        from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
        ch_client = get_clickhouse_client({
            "host": args.ch_host, "port": args.ch_port,
            "user": args.ch_user, "password": ch_password,
            "database": "jerry_trader",
        })
        if ch_client is None:
            print("ERROR: ClickHouse connection failed."); sys.exit(1)
    else:
        ch_client = None

    # ── Run ────────────────────────────────────────────────────────────
    print(f"Analyzing: {log_path}")
    lab = AnalysisLab(config, ch_client, args.log_dir)

    # Override log path for single-date classifier analysis
    lab._load_observations = lambda: _load_with_path(log_path, args.limit)

    if args.skip_returns:
        from jerry_trader.services.analysis.observation_source import ClassifierObservationSource
        src = ClassifierObservationSource(args.log_dir)
        observations = src.load((args.date, args.date) if args.date else ("", ""))
        if args.limit:
            observations = observations[:args.limit]
        if args.dedup:
            observations = AnalysisLab._dedup(observations)
        print(f"Parsed {len(observations)} observations")
        _print_classifier_stats(observations)
        return

    report = lab.run()
    print(render_terminal(report))
    if args.html:
        html = render_html(report)
        with open(args.html, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"HTML → {args.html}")
    if args.csv:
        write_csv(report, args.csv)
    ch_client.close()


def _load_with_path(log_path: str, limit: int = 0):
    """Load observations directly from a specific log path."""
    from jerry_trader.services.analysis.observation_source import _parse_processor_log
    obs = _parse_processor_log(Path(log_path))
    if limit:
        obs = obs[:limit]
    return obs


def _print_classifier_stats(observations):
    total = len(observations)
    catalysts = [o for o in observations if o.metadata.get("is_catalyst")]
    print(f"\n  Total: {total}  Catalysts: {len(catalysts)} ({len(catalysts)/total*100:.1f}%)" if total else "")
    by_score: dict[str, list] = {}
    for o in observations:
        s = o.metadata.get("score_num", 0)
        if s >= 8: k = "8-10"
        elif s >= 6: k = "6-7"
        elif s >= 4: k = "4-5"
        else: k = "1-3"
        by_score.setdefault(k, []).append(o)
    for k in ["8-10","6-7","4-5","1-3"]:
        b = by_score.get(k, [])
        if b:
            cats = sum(1 for o in b if o.metadata.get("is_catalyst"))
            print(f"  {k:>6s}  n={len(b):>4d}  catalysts={cats:>4d}  {cats/len(b)*100:>4.1f}%")


if __name__ == "__main__":
    main()
