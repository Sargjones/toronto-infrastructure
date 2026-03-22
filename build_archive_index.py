
"""
build_archive_index.py
======================
Scans the repo root for tii_data_weekly_*.json files and the archive/
directory for tii_brief_*.md files, then writes archive/index.json.

Run by GitHub Actions after the weekly snapshot step.
Can also be run locally to rebuild the index after manually adding files.

Output: archive/index.json
Schema:
{
  "generated_at": "2026-03-22T08:05:00Z",
  "weeks": [
    {
      "week":        "2026-W12",
      "data_date":   "2026-03-22",
      "run_ts":      "2026-03-22T06:02:14Z",
      "data_file":   "tii_data_weekly_2026-W12.json",
      "brief_file":  "archive/tii_brief_2026-W12.md",   // null if absent
      "brief_intro": "First 200 chars of brief text...", // null if absent
      "totals":      { "ok": 18, "warn": 3, "alert": 1, "error": 0, "manual": 9 },
      "alerts":      ["Brent Crude Price (92.4 USD/bbl)", "..."]
    },
    ...
  ]
}
Weeks are sorted newest-first.
"""

import glob
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT   = Path(__file__).parent
ARCHIVE_DIR = REPO_ROOT / "archive"
OUT_FILE    = ARCHIVE_DIR / "index.json"


def load_weekly_json(path: Path) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"  WARNING: could not read {path}: {e}", file=sys.stderr)
        return {}


def brief_intro(brief_path: Path, max_chars: int = 220) -> str | None:
    if not brief_path.exists():
        return None
    try:
        text = brief_path.read_text(encoding="utf-8").strip()
        # Strip the Week-of header line (first two lines are typically heading + blank)
        lines = text.splitlines()
        body_lines = [l for l in lines if not l.startswith("#") and l.strip()]
        body = " ".join(body_lines)
        return body[:max_chars].rsplit(" ", 1)[0] + "..." if len(body) > max_chars else body
    except Exception:
        return None


def build_index():
    ARCHIVE_DIR.mkdir(exist_ok=True)

    # Find all weekly snapshot files
    pattern = str(REPO_ROOT / "tii_data_weekly_*.json")
    weekly_files = sorted(glob.glob(pattern), reverse=True)

    if not weekly_files:
        print("No tii_data_weekly_*.json files found. Nothing to index.", file=sys.stderr)

    weeks = []
    for fpath in weekly_files:
        fname = Path(fpath).name
        # Extract week label: tii_data_weekly_2026-W12.json -> 2026-W12
        m = re.search(r"tii_data_weekly_(\d{4}-W\d{2})\.json", fname)
        if not m:
            print(f"  Skipping unrecognised filename: {fname}", file=sys.stderr)
            continue
        week = m.group(1)

        data = load_weekly_json(Path(fpath))

        # Core fields
        run_ts    = data.get("run_timestamp", "")
        run_date  = data.get("run_date", run_ts[:10] if run_ts else "")
        totals    = data.get("totals", {})
        results   = data.get("results", [])

        # Collect active alert indicators for the summary
        alerts = [
            f"{r['indicator']} ({r['value']} {r.get('unit','')})".strip()
            for r in results
            if r.get("status") == "alert" and r.get("value") is not None
        ]

        # Brief
        brief_fname = f"tii_brief_{week}.md"
        brief_path  = ARCHIVE_DIR / brief_fname
        brief_rel   = f"archive/{brief_fname}" if brief_path.exists() else None
        intro       = brief_intro(brief_path)

        weeks.append({
            "week":        week,
            "data_date":   run_date,
            "run_ts":      run_ts,
            "data_file":   fname,
            "brief_file":  brief_rel,
            "brief_intro": intro,
            "totals":      totals,
            "alerts":      alerts,
        })
        print(f"  Indexed {week}: {totals}  alerts={alerts}")

    index = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count":        len(weeks),
        "weeks":        weeks,
    }

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)

    print(f"\nWrote {OUT_FILE} ({len(weeks)} weeks)")
    return len(weeks)


if __name__ == "__main__":
    n = build_index()
    sys.exit(0 if n >= 0 else 1)
