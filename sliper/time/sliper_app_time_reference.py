#!/usr/bin/env python3
"""
sliper_app_time_reference.py — scan a date-based folder, pick the newest file, read JSON, and
return a timestamp derived from --date (for the date) and file-derived run_time (HH:MM),
plus the selected file path.

Examples
--------
# Default (now mode): use system current HH:MM as ceiling
python sliper_app_time_reference.py --date 2025-10-08

# Now mode with explicit override
python sliper_app_time_reference.py --date 2025-10-08 --mode now --max-time 13:30

# History mode: ignore system time, pick newest regardless
python sliper_app_time_reference.py --date 2025-10-01 --mode history

# History mode with specific cutoff
python sliper_app_time_reference.py --date 2025-10-01 --mode history --max-time 12:00

# Export usage (any mode)
eval "$(python sliper_app_time_reference.py --date 2025-10-08 --mode now --export)"

# Write environment file
python sliper_app_time_reference.py --date 2025-10-08 --mode now --write-env /tmp/time_ref.env
. /tmp/time_ref.env


Notes
-----
- The script limits its search strictly to the folder built from --date (no cross-date lookup).
- The output date ALWAYS comes from --date. Only the time (HH:MM) is extracted from the file.
- JSON keys checked: `run_time` (preferred). If missing, fallback to filename patterns like
  *_YYYYMMDD_HHMM.* or a standalone HHMM token.
- If nothing valid is found (or all files exceed the ceiling), it prints/export:
    TIME_REF="NOT_FOUND"; FOLDER_REF="<folder>"; FILE_REF="NOT_FOUND"
  and exits with code 2 so bash can warn/exit cleanly.
- Timezone handling: output is a naive string formatted as %Y-%m-%d %H:%M; no conversion.
"""

from __future__ import annotations
import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path
from typing import Iterable, Optional, Union

FOLDER_TEMPLATE_DEFAULT = "/home/admin/soilslips-ws/data_dynamic/source/run/%Y/%m/%d"

# Strict: ...YYYYMMDD[_-]HHMM... (e.g., foo_20251001_1340.json)
FILENAME_DATETIME_RE = re.compile(r"(?P<date>\d{8})[_-](?P<time>\d{4})")
# Loose fallback: any standalone 4-digit HHMM token
FILENAME_TIME_RE = re.compile(r"(?<!\d)(?P<time>\d{4})(?!\d)")


# ------------------------------ helpers ------------------------------ #

def _fmt_folder_for_date(d: dt.date, template: str) -> Path:
    return Path(d.strftime(template))


def _iter_candidate_files(folder: Path, pattern: str, json_only: bool) -> Iterable[Path]:
    if not folder.exists():
        raise FileNotFoundError(f"Folder does not exist: {folder}")
    if not folder.is_dir():
        raise NotADirectoryError(f"Not a directory: {folder}")

    for p in sorted(folder.glob(pattern)):
        if p.is_file():
            if json_only and p.suffix.lower() != ".json":
                continue
            yield p


def _pick_newest_file(folder: Path, pattern: str = "*", json_only: bool = False) -> Path:
    candidates = list(_iter_candidate_files(folder, pattern, json_only))
    if not candidates:
        raise FileNotFoundError(f"No files found in folder: {folder}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _validate_hhmm(hh: int, mm: int) -> None:
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise ValueError("hour/minute out of range")


def _normalize_run_time(time_str: str) -> str:
    """Accept 'HHMM', 'HH:MM', or 'HH:MM:SS', return 'HH:MM' with range checking."""
    s = str(time_str).strip()
    m = re.match(r"^(\d{1,2}):(\d{2})(?::\d{2})?$", s)
    if m:
        hh, mm = int(m.group(1)), int(m.group(2))
        _validate_hhmm(hh, mm)
        return f"{hh:02d}:{mm:02d}"
    digits = re.sub(r"[^0-9]", "", s)
    if len(digits) == 4:
        hh, mm = int(digits[:2]), int(digits[2:])
        _validate_hhmm(hh, mm)
        return f"{hh:02d}:{mm:02d}"
    raise ValueError(f"Invalid run_time: {time_str!r}")


def _extract_time_from_json(fp: Path, *, max_depth: int = 3) -> Optional[str]:
    try:
        with fp.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None

    def _walk(obj: Union[dict, list], depth: int) -> Optional[str]:
        if depth < 0:
            return None
        if isinstance(obj, dict):
            if "run_time" in obj:
                try:
                    return _normalize_run_time(obj["run_time"])  # type: ignore[index]
                except Exception:
                    return None
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    t = _walk(v, depth - 1)
                    if t:
                        return t
        elif isinstance(obj, list):
            for v in obj:
                if isinstance(v, (dict, list)):
                    t = _walk(v, depth - 1)
                    if t:
                        return t
        return None

    return _walk(data, max_depth)


def _extract_time_from_filename(fp: Path) -> Optional[str]:
    s = fp.stem
    m = FILENAME_DATETIME_RE.search(s)
    if m:
        try:
            return _normalize_run_time(m.group("time"))
        except Exception:
            return None
    m2 = FILENAME_TIME_RE.search(s)
    if m2:
        try:
            return _normalize_run_time(m2.group("time"))
        except Exception:
            return None
    return None


def _pick_newest_file_with_ceiling(
    folder: Path, pattern: str, json_only: bool, max_time: Optional[str]
) -> Path:
    """
    Pick the newest file whose extracted run time (HH:MM) is ≤ max_time if provided.
    If max_time is None, behaves like the regular newest selection.
    """
    candidates = sorted(
        _iter_candidate_files(folder, pattern, json_only),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No files found in folder: {folder}")

    if max_time is None:
        return candidates[0]

    def _hhmm_to_minutes(hhmm: str) -> int:
        h, m = map(int, hhmm.split(":"))
        return h * 60 + m

    ceiling_min = _hhmm_to_minutes(max_time)

    for fp in candidates:
        t = _extract_time_from_json(fp) or _extract_time_from_filename(fp)
        if not t:
            continue
        if _hhmm_to_minutes(t) <= ceiling_min:
            return fp

    raise ValueError("Files are not in the selected period (all run_time > ceiling)")


def build_time_ref(newest_file: Path, ref_date: dt.date) -> str:
    # Date is pinned to --date; only time is extracted.
    rt = _extract_time_from_json(newest_file)
    if not rt:
        rt = _extract_time_from_filename(newest_file)
    if not rt:
        raise ValueError(f"Could not extract run_time from JSON or filename: {newest_file}")
    return f"{ref_date:%Y-%m-%d} {rt}"


def _emit_exports(time_ref: str, folder: Path, file_ref: str) -> str:
    return f'export TIME_REF="{time_ref}"; FOLDER_REF="{folder}"; FILE_REF="{file_ref}"'


def _parse_hhmm_arg(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    return _normalize_run_time(s)


# ------------------------------ CLI ------------------------------ #

def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Select newest file for a date and return TIME_REF (and FILE_REF).\n"
            "Date comes from --date; time comes from JSON 'run_time' or filename."
        )
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
    ap.add_argument(
        "--pattern",
        default="*",
        help="Glob to select candidate files inside the folder (default: '*').",
    )
    ap.add_argument(
        "--json-only",
        action="store_true",
        help="Restrict to files with .json extension when picking the newest file.",
    )
    ap.add_argument(
        "--mode",
        choices=["now", "history"],
        default="now",
        help="Selection mode. 'now' uses system HH:MM as ceiling unless --max-time overrides.",
    )
    ap.add_argument(
        "--max-time",
        metavar="HH:MM",
        help=("Optional ceiling for run_time (HH:MM). In 'now' mode it overrides system time; "
              "in 'history' mode it enables a ceiling (otherwise newest wins)."),
    )

    args = ap.parse_args()

    # Parse --date
    try:
        ref_date = dt.datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError as e:
        ap.error(f"--date must be YYYY-MM-DD: {e}")

    folder = _fmt_folder_for_date(ref_date, args.folder_template)

    # Determine ceiling based on mode
    ceiling: Optional[str] = None
    if args.mode == "now":
        # default to system time; allow --max-time to override
        ceiling = _parse_hhmm_arg(args.max_time) or dt.datetime.now().strftime("%H:%M")
    else:  # history
        ceiling = _parse_hhmm_arg(args.max_time)  # may be None

    try:
        if ceiling is None:
            newest = _pick_newest_file(folder, pattern=args.pattern, json_only=args.json_only)
        else:
            newest = _pick_newest_file_with_ceiling(
                folder, pattern=args.pattern, json_only=args.json_only, max_time=ceiling
            )

        time_ref = build_time_ref(newest, ref_date)

        # Defensive check when ceiling is active
        if ceiling is not None:
            chosen_hhmm = time_ref.split()[1]
            ch = int(chosen_hhmm[:2]) * 60 + int(chosen_hhmm[3:])
            mh = int(ceiling[:2]) * 60 + int(ceiling[3:])
            if ch > mh:
                raise ValueError(
                    "Files are not in the selected period (post-ceiling time chosen unexpectedly)"
                )

        file_ref = str(newest.resolve())
        export_line = _emit_exports(time_ref, folder, file_ref)

        if args.write_env:
            Path(args.write_env).write_text(export_line + "\n", encoding="utf-8")

        if args.export:
            print(export_line)
        else:
            print(f"{time_ref} | {file_ref}")
        return 0

    except Exception as exc:
        # Sentinel values for bash + non-zero exit
        time_ref = "NOT_FOUND"
        file_ref = "NOT_FOUND"
        export_line = _emit_exports(time_ref, folder, file_ref)

        if args.write_env:
            try:
                Path(args.write_env).write_text(export_line + "\n", encoding="utf-8")
            except Exception:
                pass

        if args.export:
            print(export_line)
        else:
            print(f"Warning: {exc}", file=sys.stderr)
            print(export_line)
        return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise

