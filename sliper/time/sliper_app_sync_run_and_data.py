#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime, timedelta
import re
import sys
from typing import List, Optional, Tuple

INFO_RE = re.compile(r"^info_run_(\d{8})_(\d{4})\.json$")

def parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Resolve a data folder based on --time-run. "
            "Checks the exact hour, then scans backward by 1h within tolerance, "
            "then tries same-day info_run fallback if its derived data folder is non-empty. "
            "If nothing is found, returns empty values for Bash handling."
        )
    )
    p.add_argument("--time-run", required=True, help='Run time, e.g. "2025-10-20 09:23"')
    p.add_argument("--data-root", default="/", help="Root for data paths (ignored if pattern is absolute).")
    p.add_argument("--data-path-pattern",
                   default="/home/admin/soilslips-ws/data_dynamic/destination/data/soil_moisture/frc/{Y}/{m}/{d}/{HM}",
                   help="Data subpath pattern. Tokens: {Y},{m},{d},{H},{M},{HM} or %Y,%m,%d,%H,%M. Can be absolute.")
    p.add_argument("--run-root", default="/", help="Root for run paths (ignored if pattern is absolute).")
    p.add_argument("--run-path-pattern",
                   default="/home/admin/soilslips-ws/data_dynamic/source/run/{Y}/{m}/{d}",
                   help="Run subpath pattern. Tokens: {Y},{m},{d},{H},{M},{HM} or %Y,%m,%d,%H,%M. Can be absolute.")
    p.add_argument("--tolerance-hours", type=int, default=10,
                   help="Backward scan window (± hours, but used as 'previous N hours').")
    p.add_argument("--export", action="store_true",
                   help="Emit single-line shell exports (TIME_REF=... FOLDER_REF=...).")
    return p.parse_args()

def floor_to_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)

def convert_percent_to_brace(pattern: str) -> str:
    if pattern is None:
        return pattern
    s = pattern
    s = s.replace("%H%M", "{HM}")
    s = s.replace("%Y", "{Y}")
    s = s.replace("%m", "{m}")
    s = s.replace("%d", "{d}")
    s = s.replace("%H", "{H}")
    s = s.replace("%M", "{M}")
    return s

def path_from_pattern(root: Path, dt: datetime, pattern: str) -> Path:
    pattern = convert_percent_to_brace(pattern)
    tokens = {
        "Y": dt.strftime("%Y"),
        "m": dt.strftime("%m"),
        "d": dt.strftime("%d"),
        "H": dt.strftime("%H"),
        "M": dt.strftime("%M"),
        "HM": dt.strftime("%H%M"),
    }
    sub = pattern
    for k, v in tokens.items():
        sub = sub.replace(f"{{{k}}}", v)
    sub_path = Path(sub)
    return sub_path if sub_path.is_absolute() else (root / sub_path)

def is_non_empty_dir(p: Path) -> bool:
    try:
        return p.exists() and p.is_dir() and any(p.iterdir())
    except PermissionError:
        # If we cannot list, consider it not usable for selection
        return False

def list_info_runs_for_day(run_root: Path, run_pattern: str, day_dt: datetime) -> List[Path]:
    run_dir = path_from_pattern(run_root, day_dt, run_pattern)
    if not run_dir.exists() or not run_dir.is_dir():
        return []
    try:
        return [p for p in run_dir.iterdir() if p.is_file() and INFO_RE.match(p.name)]
    except PermissionError:
        return []

def info_run_to_dt(p: Path) -> Optional[datetime]:
    m = INFO_RE.match(p.name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M")
    except ValueError:
        return None

def pick_best_info_run(target_dt: datetime, candidates: List[Path], tolerance: timedelta) -> Optional[Path]:
    parsed: List[Tuple[Path, datetime]] = []
    for p in candidates:
        dt = info_run_to_dt(p)
        if dt:
            parsed.append((p, dt))
    if not parsed:
        return None

    # Exact match wins immediately
    for p, dt in parsed:
        if dt == target_dt:
            return p

    # Otherwise pick closest within tolerance; tie-breaker: later (newer) wins
    best = None
    best_delta = None
    for p, dt in parsed:
        delta = abs(dt - target_dt)
        if delta <= tolerance and (best is None or delta < best_delta or (delta == best_delta and dt > best[1])):
            best = (p, dt)
            best_delta = delta
    return best[0] if best else None

def emit_result(time_ref: Optional[datetime], data_folder: Optional[Path], export: bool):
    """
    Emit either selected values or empty values.
    - export=True: prints a single shell-safe assignment line.
    - export=False: prints RESULT lines (may be empty).
    """
    if time_ref is None or data_folder is None:
        if export:
            print("TIME_REF='' FOLDER_REF=''")
        else:
            print("STATUS: no suitable data folder found")
            print("RESULT: selected_time=")
            print("RESULT: selected_data_folder=")
        return

    if export:
        print(f"TIME_REF='{time_ref.strftime('%Y-%m-%d %H:%M')}' FOLDER_REF='{data_folder}'")
    else:
        print("STATUS: data directory exists and is non-empty")
        print(f"DATA:   {data_folder}")
        print(f"RUN:    {time_ref.isoformat(' ')}")
        print(f"RESULT: selected_time={time_ref.strftime('%Y-%m-%d %H:%M')}")
        print(f"RESULT: selected_data_folder={data_folder}")

def main():
    args = parse_args()
    try:
        run_dt_input = datetime.strptime(args.time_run, "%Y-%m-%d %H:%M")
    except ValueError:
        print(f"ERROR: --time-run must be like 'YYYY-MM-DD HH:MM' (got {args.time_run!r})", file=sys.stderr)
        sys.exit(2)

    run_dt = floor_to_hour(run_dt_input)

    # 1) Exact target hour
    data_target = path_from_pattern(Path(args.data_root), run_dt, args.data_path_pattern)
    if is_non_empty_dir(data_target):
        emit_result(run_dt, data_target, args.export)
        sys.exit(0)

    # 2) Backward scan by 1h within tolerance
    tolerance = timedelta(hours=args.tolerance_hours)
    found_dt: Optional[datetime] = None
    found_path: Optional[Path] = None

    for h in range(1, args.tolerance_hours + 1):
        dt = run_dt - timedelta(hours=h)
        p = path_from_pattern(Path(args.data_root), dt, args.data_path_pattern)
        if is_non_empty_dir(p):
            found_dt, found_path = dt, p
            break

    if found_dt is not None and found_path is not None:
        if not args.export:
            print(f"STATUS: target data missing/empty; selected previous data folder at -{h}h")
            print(f"DATA(target): {data_target}")
            print(f"PICK(DATA):   {found_path}  [{found_dt.strftime('%Y-%m-%d %H:%M')}]")
        emit_result(found_dt, found_path, args.export)
        sys.exit(0)

    # 3) Same-day info_run fallback (only if resulting data folder is non-empty)
    candidates = list_info_runs_for_day(Path(args.run_root), args.run_path_pattern, run_dt)
    pick = pick_best_info_run(run_dt, candidates, tolerance)

    if pick is not None:
        picked_dt = info_run_to_dt(pick)
        if picked_dt is not None:
            data_pick = path_from_pattern(Path(args.data_root), picked_dt, args.data_path_pattern)
            if is_non_empty_dir(data_pick):
                if not args.export:
                    reason = "exact match" if picked_dt == run_dt else f"closest within ±{args.tolerance_hours}h (Δ={abs(picked_dt - run_dt)})"
                    print("STATUS: data missing/empty; selected fallback info_run whose data folder is non-empty")
                    print(f"DATA(target): {data_target}")
                    print(f"PICK(info):   {pick}  [{picked_dt.strftime('%Y-%m-%d %H:%M')}, {reason}]")
                emit_result(picked_dt, data_pick, args.export)
                sys.exit(0)

    # 4) Nothing suitable found → emit empty values for Bash handling (exit 0)
    emit_result(None, None, args.export)
    sys.exit(0)

if __name__ == "__main__":
    main()

