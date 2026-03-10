#!/usr/bin/env python3
"""
Extract news processor log entries to JSON format.
Processes all news_processor_*.log files under logs/jerryib_trader/
"""

import re
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import ast


def parse_explanation(explanation_str: str) -> Optional[Dict]:
    """Parse the explanation string which is a dict literal."""
    try:
        # Try to evaluate as Python dict literal
        return ast.literal_eval(explanation_str)
    except (ValueError, SyntaxError):
        # If that fails, return as string
        return {"raw": explanation_str}


def extract_log_entries(log_file_path: Path) -> List[Dict]:
    """Extract structured entries from a news processor log file."""
    entries = []

    with open(log_file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Split by separator lines
    blocks = re.split(r'={50,}', content)

    for block in blocks:
        block = block.strip()
        if not block or 'model:' not in block:
            continue

        lines = block.split('\n')
        entry = {}

        for i, line in enumerate(lines):
            line = line.strip()

            if line.startswith('model:'):
                entry['model'] = line.split(':', 1)[1].strip()

            elif '✅' in line or '❌' in line:
                # Parse symbol, is_catalyst, score
                is_catalyst = '✅' in line
                parts = re.split(r'[✅❌]', line)
                symbol = parts[0].strip()
                score_part = parts[1].strip() if len(parts) > 1 else '0/10'

                entry['symbol'] = symbol
                entry['is_catalyst'] = is_catalyst
                entry['score'] = score_part

            elif line.startswith('Title:'):
                entry['title'] = line.split(':', 1)[1].strip()

            elif line.startswith('Published Time:'):
                entry['published_time'] = line.split(':', 1)[1].strip()

            elif line.startswith('Current Time:'):
                entry['current_time'] = line.split(':', 1)[1].strip()

            elif line.startswith('Explanation:'):
                explanation_str = line.split(':', 1)[1].strip()
                entry['explanation'] = parse_explanation(explanation_str)

            elif line.startswith('Url:'):
                entry['url'] = line.split(':', 1)[1].strip()

            elif line.startswith('Content:'):
                # Content is on the same line, after "Content: "
                entry['content'] = line.split(':', 1)[1].strip()
                break

        # Only add if we have the essential fields
        if 'symbol' in entry and 'title' in entry:
            # Add default values for fields that may not exist in old logs
            entry.setdefault('sources', 'unknown')
            entry.setdefault('source_from', 'unknown')
            entries.append(entry)

    return entries


def process_all_logs(base_dir: Path = Path('logs/jerry_trader')):
    """Process all news_processor logs and save to JSON."""

    if not base_dir.exists():
        print(f"Directory {base_dir} does not exist!")
        return

    # Find all date directories
    date_dirs = sorted([d for d in base_dir.iterdir() if d.is_dir() and d.name.isdigit()])

    total_entries = 0

    for date_dir in date_dirs:
        date = date_dir.name
        log_file = date_dir / f"news_processor_{date}.log"

        if not log_file.exists():
            print(f"Skipping {date}: no news_processor log found")
            continue

        print(f"Processing {log_file}...")
        entries = extract_log_entries(log_file)

        if entries:
            # Save to JSON in the same directory
            json_file = date_dir / f"news_processor_{date}.json"

            output = {
                "date": date,
                "log_file": str(log_file),
                "entry_count": len(entries),
                "entries": entries
            }

            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2, ensure_ascii=False)

            print(f"  ✓ Extracted {len(entries)} entries to {json_file}")
            total_entries += len(entries)
        else:
            print(f"  ✗ No entries found in {log_file}")

    print(f"\n{'='*60}")
    print(f"Total entries extracted: {total_entries}")
    print(f"Processed {len(date_dirs)} date directories")


if __name__ == "__main__":
    process_all_logs()
