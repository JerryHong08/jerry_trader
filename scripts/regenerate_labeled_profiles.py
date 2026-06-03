"""Regenerate ticker_profiles for all labeling dates + merge labels.

Usage: poetry run python scripts/regenerate_labeled_profiles.py
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

# Extract unique dates from labeling CSV (in order)
dates = []
with open(LABELING_CSV) as f:
    for row in csv.DictReader(f):
        d = row["date"].strip()
        if d not in dates:
            dates.append(d)

print(f"Dates to process: {dates}")

all_dfs = []

for date in dates:
    print(f"\n{'='*60}")
    print(f"Processing {date}...")
    print(f"{'='*60}")

    lab = ExitLab(date)
    lab.load_candidates(top_n=999, min_gain_pct=0.0, new_entry_only=False)

    if not lab.candidates:
        print(f"  No candidates for {date}, skipping")
        continue

    df = lab.ticker_profiles()
    if df.empty:
        print(f"  No profiles generated for {date}, skipping")
        continue

    # Add date column for traceability
    df.insert(0, "date", date)

    all_dfs.append(df)
    print(f"  {len(df)} profiles for {date}")

# Merge all dates
if not all_dfs:
    print("No data generated!")
    sys.exit(1)

combined = pd.concat(all_dfs, ignore_index=True)
print(f"\nTotal profiles: {len(combined)} across {len(dates)} dates")

# --- Merge labels from momo_labeling.csv ---
label_map: dict[str, dict] = {}
with open(LABELING_CSV) as f:
    for row in csv.DictReader(f):
        key = f"{row['date'].strip()}:{row['ticker'].strip()}"
        label_map[key] = {
            "label": row.get("label", "").strip(),
            "label_notes": row.get("label_notes", "").strip(),
        }

labels_col = []
notes_col = []
for _, row in combined.iterrows():
    key = f"{row['date']}:{row['ticker']}"
    lbl = label_map.get(key, {})
    labels_col.append(lbl.get("label", ""))
    notes_col.append(lbl.get("label_notes", ""))

combined.insert(len(combined.columns), "label", labels_col)
combined.insert(len(combined.columns), "label_notes", notes_col)

# Save
combined.to_csv(OUTPUT_CSV, index=False)
print(f"\nSaved {len(combined)} rows to {OUTPUT_CSV}")

# Quick summary
label_counts = combined["label"].value_counts()
print(f"\nLabel distribution in output:")
for lbl, cnt in sorted(label_counts.items()):
    name = {"1": "True MOMO", "0": "False MOMO", "-1": "Skip"}.get(lbl, f"'{lbl}'")
    print(f"  {lbl} ({name}): {cnt}")
print(
    f"  (unlabeled/missing): {(combined['label'].isna() | (combined['label'] == '')).sum()}"
)
