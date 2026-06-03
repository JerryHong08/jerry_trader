"""Generate comprehensive ticker_profiles for all CH trading days + merge labels.

For each trading day with ClickHouse data between 2026-03-06 and 2026-03-13:
  1. Run PreFilter to find candidates (subscription-logic, same as live)
  2. Run ExitLab.ticker_profiles() to compute:
     - Pre-decision factors at entry (trade_rate, spread, large_trade_ratio, aggressor_ratio)
     - Static data (country, sector, market_cap, float_shares)
     - Outcome/evaluation metrics (entry_to_peak_pct, tp5_pnl, baseline_pnl, ignition)
  3. Merge existing labels from momo_labeling.csv
  4. Output one combined CSV with empty label/label_notes for new/unlabeled rows

Usage: poetry run python scripts/generate_labeling_profiles.py
"""

import csv
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python" / "src"))

from jerry_trader.services.backtest.layer_lab.lab import ExitLab

LABELING_CSV = Path(__file__).resolve().parent.parent / "data" / "momo_labeling.csv"
OUTPUT_CSV = (
    Path(__file__).resolve().parent.parent / "data" / "ticker_profiles_labeled.csv"
)

# All 2026 trading days with ClickHouse data (market_snapshot_collector + trades)
# Excludes the 2025-03-06 row (non-trading-day test data)
DATES = [
    "2026-03-02",
    "2026-03-03",
    "2026-03-04",
    "2026-03-05",
    "2026-03-06",
    "2026-03-09",
    "2026-03-10",
    "2026-03-11",
    "2026-03-12",
    "2026-03-13",
]

# ── Load existing labels ──
label_map: dict[tuple[str, str], dict[str, str]] = {}
if LABELING_CSV.exists():
    with open(LABELING_CSV) as f:
        for row in csv.DictReader(f):
            key = (row["date"].strip(), row["ticker"].strip())
            label_map[key] = {
                "label": row.get("label", "").strip(),
                "label_notes": row.get("label_notes", "").strip(),
                "break_label": row.get("break_label", "").strip(),
            }
    print(f"Loaded {len(label_map)} existing labels from {LABELING_CSV}")
else:
    print(f"No existing labels found at {LABELING_CSV}")

# ── Generate profiles per date ──
all_dfs = []

for date in DATES:
    print(f"\n{'=' * 70}")
    print(f"Processing {date}...")
    print(f"{'=' * 70}")

    lab = ExitLab(date)
    lab.load_candidates(top_n=999, min_gain_pct=0.0, new_entry_only=False)

    if not lab.candidates:
        print(f"  No candidates for {date}, skipping")
        continue

    df = lab.ticker_profiles()
    if df.empty:
        print(f"  No profiles generated for {date}, skipping")
        continue

    df.insert(0, "date", date)
    all_dfs.append(df)
    print(f"\n  {len(df)} profiles for {date}")

# ── Merge all dates ──
if not all_dfs:
    print("No data generated!")
    sys.exit(1)

combined = pd.concat(all_dfs, ignore_index=True)
print(f"\n{'=' * 70}")
print(f"Total profiles: {len(combined)} across {len(all_dfs)} dates")

# ── Merge labels ──
labels_col = []
notes_col = []
break_col = []
for _, row in combined.iterrows():
    key = (str(row["date"]), str(row["ticker"]))
    lbl = label_map.get(key, {})
    labels_col.append(lbl.get("label", ""))
    notes_col.append(lbl.get("label_notes", ""))
    break_col.append(lbl.get("break_label", ""))

n_already_labeled = sum(1 for l in labels_col if l)
n_break_labeled = sum(1 for b in break_col if b)
combined.insert(len(combined.columns), "label", labels_col)
combined.insert(len(combined.columns), "label_notes", notes_col)
# ticker_profiles() already includes break_label column — overwrite with saved values
if "break_label" in combined.columns:
    for i, val in enumerate(break_col):
        if val:
            combined.at[i, "break_label"] = val
else:
    combined.insert(len(combined.columns), "break_label", break_col)

print(
    f"Carried forward {n_already_labeled} existing MOMO labels, {n_break_labeled} break labels"
)

# ── Save ──
combined.to_csv(OUTPUT_CSV, index=False)
print(f"Saved {len(combined)} rows to {OUTPUT_CSV}")

# ── Quick summary ──
print(f"\nColumns ({len(combined.columns)}):")
for c in combined.columns:
    non_null = combined[c].notna().sum()
    print(f"  {c}: {non_null}/{len(combined)} non-null")

print(f"\nPer date:")
for date in DATES:
    dd = combined[combined["date"] == date]
    if len(dd):
        labeled = (dd["label"].notna() & (dd["label"] != "")).sum()
        print(f"  {date}: {len(dd)} profiles, {labeled} labeled")

label_counts = combined["label"].value_counts()
print(f"\nLabel distribution:")
for lbl, cnt in sorted(label_counts.items()):
    name = {"1": "True MOMO", "0": "False MOMO", "-1": "Skip"}.get(lbl, f"'{lbl}'")
    print(f"  {lbl} ({name}): {cnt}")
unlabeled = (combined["label"].isna() | (combined["label"] == "")).sum()
print(f"  (unlabeled): {unlabeled}")
