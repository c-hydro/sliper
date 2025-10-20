#!/usr/bin/env python3
"""
sliper_app_time_reference.py — scan a date-based folder, pick the newest file, read JSON, and
return a timestamp derived from --date (for the date) and file-derived run_time (HH:MM),
plus the selected file path.

Folder template (default): /home/admin/soilslips-ws/data_dynamic/source/run/%Y/%m/%d

Usage examples
--------------
# print the timestamp and file path (YYYY-MM-DD HH:MM | /abs/path/to/file)
python sliper_app_time_reference.py --date 2025-10-01

# emit a shell-compatible export command and evaluate it in bash
# (so TIME_REF, FOLDER_REF, and FILE_REF are set in your current shell)
eval "$(python sliper_app_time_reference.py --date 2025-10-01 --export)"

echo "TIME_REF=$TIME_REF"
echo "FOLDER_REF=$FOLDER_REF"
echo "FILE_REF=$FILE_REF"

# write the export line to a file (useful for sourcing later)
python sliper_app_time_reference.py --date 2025-10-01 --write-env /tmp/time_ref.env
. /tmp/time_ref.env

Notes
-----
- The script limits its search strictly to the folder built from --date (no cross-date lookup).
- The output date ALWAYS comes from --date. Only the time (HH:MM) is extracted from the newest file.
- JSON keys checked: `run_time` (preferred). If missing, fallback to filename patterns like *_YYYYMMDD_HHMM.* or any HHMM chunk.
- If nothing valid is found, it prints/export:
    TIME_REF="NOT_FOUND"; FOLDER_REF="<folder>"; FILE_REF="NOT_FOUND"
  and exits with code 2 so bash can warn/exit cleanly.
- Timezone handling: output is a naive string formatted as %Y-%m-%d %H:%M; no conversion.
"""

from __future__ import annotations
import argparse
import datetime as dt
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional

FOLDER_TEMPLATE_DEFAULT = "/home/admin/soilslips-ws/data_dynamic/source/run/%Y/%m/%d"

# Strict pattern (YYYYMMDD_HHMM or YYYYMMDD-HHMM etc.) used first
FILENAME_DATETIME_RE = re.compile(r"(?P<date>\d{8})[_-]?(?P<time>\d{4})")
# Loose fallback: any HHMM group
FILENAME_TIME_RE = re.compile(r"(?P<time>\d{4})(?:\D|$)")


def _fmt_folder_for_date(d: dt.date, template: str) -> Path:
    # Use strftime on date; template should include %Y, %m, %d.
    return Path(d.strftime(template))


def _pick_newest_file(folder: Path) -> Path:
    try:
        candidates = [p for p in folder.iterdir() if p.is_file()]
    except FileNotFoundError:
        raise FileNotFoundError(f"Folder does not exist: {folder}")
    if not candidates:
        raise FileNotFoundError(f"No files found in folder: {folder}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _normalize_run_time(time_str: str) -> str:
    # Accept HHMM or HH:MM
    s = re.sub(r"[^0-9]", "", time_str)
    if len(s) != 4:
        raise ValueError(f"Invalid run_time: {time_str!r}")
    return f"{s[0:2]}:{s[2:4]}"


def _extract_time_from_json(fp: Path) -> Optional[str]:
    try:
        with fp.open("r", encoding="utf-8") as f:
            data = json.load(f)
        # tolerate nested dicts: look at top-level or one level down
        candidates = [data]
        if isinstance(data, dict):
            candidates.extend([v for v in data.values() if isinstance(v, dict)])
        for obj in candidates:
            if isinstance(obj, dict) and "run_time" in obj:
                return _normalize_run_time(str(obj["run_time"]))
    except Exception:
        return None
    return None


def _extract_time_from_filename(fp: Path) -> Optional[str]:
    m = FILENAME_DATETIME_RE.search(fp.stem)
    if m:
        return _normalize_run_time(m.group("time"))
    m2 = FILENAME_TIME_RE.search(fp.stem)
    if m2:
        return _normalize_run_time(m2.group("time"))
    return None


def build_time_ref(newest_file: Path, ref_date: dt.date) -> str:
    # Date is pinned to --date; only time is extracted.
    rt = _extract_time_from_json(newest_file)
    if not rt:
        rt = _extract_time_from_filename(newest_file)
    if not rt:
        raise ValueError(
            f"Could not extract run_time from JSON or filename: {newest_file}"
        )
    return f"{ref_date:%Y-%m-%d} {rt}"


def _emit_exports(time_ref: str, folder: Path, file_ref: str) -> str:
    return f'export TIME_REF="{time_ref}"; FOLDER_REF="{folder}"; FILE_REF="{file_ref}"'


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Select newest file for a date and return TIME_REF (and FILE_REF)."
    )
    ap.add_argument(
        "--date",
        required=True,
        help="Reference date in YYYY-MM-DD (used to build the folder path)",
    )
    ap.add_argument(
        "--folder-template",
        default=FOLDER_TEMPLATE_DEFAULT,
        help="Folder template with strftime tokens (default: %(default)s)",
    )
    ap.add_argument(
        "--export",
        action="store_true",
        help="Emit 'export TIME_REF=…; FOLDER_REF=…; FILE_REF=…' so you can eval it in a shell.",
    )
    ap.add_argument(
        "--write-env",
        metavar="FILE",
        help="Write a line 'export TIME_REF=…; FOLDER_REF=…; FILE_REF=…' to FILE for later sourcing.",
    )

    args = ap.parse_args()

    # Parse --date
    try:
        ref_date = dt.datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError as e:
        # Malformed input: standard argparse error (no exports)
        ap.error(f"--date must be YYYY-MM-DD: {e}")

    folder = _fmt_folder_for_date(ref_date, args.folder_template)

    try:
        newest = _pick_newest_file(folder)
        time_ref = build_time_ref(newest, ref_date)
        file_ref = str(newest.resolve())
        export_line = _emit_exports(time_ref, folder, file_ref)

        if args.write_env:
            Path(args.write_env).write_text(export_line + "\n", encoding="utf-8")

        if args.export:
            print(export_line)
        else:
            # Human-friendly default
            print(f"{time_ref} | {file_ref}")

        return 0

    except Exception as exc:
        # Sentinel values for bash + non-zero exit to allow clean detection
        time_ref = "NOT_FOUND"
        file_ref = "NOT_FOUND"
        export_line = _emit_exports(time_ref, folder, file_ref)

        if args.write_env:
            Path(args.write_env).write_text(export_line + "\n", encoding="utf-8")

        # Always print the export line so bash can eval it
        if args.export:
            print(export_line)
        else:
            # Also print a warning to stderr for logs
            print(f"Warning: {exc}", file=sys.stderr)
            print(export_line)

        return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit as e:
        raise

