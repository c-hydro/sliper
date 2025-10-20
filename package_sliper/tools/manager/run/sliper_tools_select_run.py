#!/usr/bin/env python3
"""
run_summary.py (flattened config)
---------------------------------
Create ONLY per-run/per-day summary JSON files with robust time utilities.
Config is simplified: each group under "files" contains *directly* the
source templates (no "source"/"dest" nesting). Example group:

"files": {
  "data": {
    "folder_template": "/path/{YYYY}/{MM}/{DD}/{RUN}",
    "glob": "Rain_*.tif"
  },
  "ancillary_dset_01": {
    "folder_template": "/path/{YYYY}/{MM}/{DD}/{RUN}",
    "glob": "ME_*.json"
  }
}

This script does NOT copy any assets; it only inspects and summarizes.

Enhancements:
- Adds a top-level 'timing' section (new) with fields formatted as "YYYY-MM-DD HH:MM"
  and with timezone provided separately in 'timing.tz' (and root 'tz' remains).
- 'timing' is computed ONLY from file groups whose key equals 'data' or starts with 'data'.
- 'timing.diff_hours_run_minus_latest' uses filesystem time in now-mode, and prefers
  filename-derived time (YYYYMMDDHHMM) in history-mode, falling back to filesystem time.
"""
from __future__ import annotations
import argparse
import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple

# zoneinfo (3.9+) with backport support
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore


# -----------------------------
# CLI
# -----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Create summary JSONs (no copies) via a flattened JSON config. "
            "Select the latest eligible run (<= time) for one day or iterate back N days."
        )
    )
    p.add_argument("--config", type=Path, required=True,
                   help="Path to JSON config with run_base_template, summary, and files (flattened groups).")
    p.add_argument("--tz", type=str, default=None, help="Override timezone in config.")
    p.add_argument("--when", type=str, default=None, help="ISO datetime in TZ, e.g. 2025-09-23T09:30.")
    p.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (used with --time).")
    p.add_argument("--time", type=str, default=None, help="HH:MM (used with --date).")
    # Multi-day iteration
    p.add_argument(
        "--n-days", type=int, default=0,
        help=(
            "Iterate from the chosen start time back N days (inclusive). "
            "0 = only the start day; 3 = start day plus 3 prior days."
        ),
    )
    p.add_argument(
        "--flatten-hour", type=str, default="00:00",
        help=(
            "Flatten the per-day time-of-day used to select runs (HH:MM). "
            "Example: 00:00 to select up to midnight runs; 09:00 to select morning runs."
        ),
    )
    p.add_argument(
        "--include-file-history",
        action="store_true",
        help="Include 'file_history' section in the summary JSON (default off unless overridden by config).",
    )
    p.add_argument(
        "--force-file-history",
        action="store_true",
        help="Force-enable 'file_history' regardless of config.",
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would be summarized without writing files.")
    return p.parse_args()


# -----------------------------
# Config helpers
# -----------------------------
def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Config not found: {path}")
    try:
        cfg = json.loads(path.read_text())
    except Exception as e:
        raise SystemExit(f"Invalid config JSON {path}: {e}")

    if "run_base_template" not in cfg:
        raise SystemExit("Config must include 'run_base_template' (day-level folder that may contain HHMM subfolders).")
    if "summary" not in cfg or "folder_template" not in cfg["summary"]:
        raise SystemExit("Config must include 'summary.folder_template'.")
    if ("filename_template" not in cfg["summary"]) and ("filename" not in cfg["summary"]):
        raise SystemExit("Config must include 'summary.filename_template' (preferred) or 'summary.filename'.")
    if "files" not in cfg or not isinstance(cfg["files"], dict) or not cfg["files"]:
        raise SystemExit("Config must include non-empty 'files' dict (file groups).")

    # Validate flattened file groups
    for key, spec in cfg["files"].items():
        if "folder_template" not in spec:
            raise SystemExit(f"files.{key} must include 'folder_template'.")
        # exactly one of glob or filename_template
        has_glob = "glob" in spec
        has_fname = "filename_template" in spec
        if (has_glob and has_fname) or (not has_glob and not has_fname):
            raise SystemExit(f"files.{key} must include exactly one of 'glob' or 'filename_template'.")
    return cfg


def config_uses_run(cfg: Dict[str, Any]) -> bool:
    """Return True if any template in the config contains {RUN}."""
    def has_run(s: Optional[str]) -> bool:
        return isinstance(s, str) and "{RUN}" in s

    if has_run(cfg.get("run_base_template")):
        return True
    summ = cfg.get("summary", {})
    if has_run(summ.get("folder_template")) or has_run(summ.get("filename_template")):
        return True
    for spec in cfg["files"].values():
        if has_run(spec.get("folder_template")) or has_run(spec.get("filename_template")):
            return True
    return False


def render_template(tmpl: str, yyyy: str, mm: str, dd: str,
                    eff_hh: str, eff_mi: str, run: Optional[str]) -> str:
    """
    Replace placeholders in templates.
    {RUN} is the selected run folder (e.g., '1400').
    {HH}/{MIN} default to the run's HH/MM if RUN provided; else fall back to effective time.
    """
    hh = (run[:2] if run else eff_hh)
    mi = (run[2:] if run else eff_mi)
    s = (tmpl
         .replace("{YYYY}", yyyy)
         .replace("{MM}", mm)
         .replace("{DD}", dd)
         .replace("{HH}", hh)
         .replace("{MIN}", mi))
    if "{RUN}" in s:
        s = s.replace("{RUN}", run or "")
    return s


# -----------------------------
# Time & run selection
# -----------------------------
def _parse_flat_hhmm(hhmm: str) -> Tuple[str, str]:
    try:
        dt = datetime.strptime(hhmm, "%H:%M")
        return dt.strftime("%H"), dt.strftime("%M")
    except ValueError:
        raise SystemExit("Invalid --flatten-hour. Expected HH:MM, e.g. 00:00 or 09:30.")


def resolve_effective_dt(tz_name: str, when: Optional[str], date: Optional[str], time: Optional[str]) -> datetime:
    tz = ZoneInfo(tz_name)
    if when:
        s = when.strip().replace(" ", "T")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            # support trailing Z/z shorthand for UTC
            if s.endswith(("Z", "z")):
                try:
                    dt = datetime.fromisoformat(s[:-1] + "+00:00")
                except ValueError:
                    raise SystemExit(f"Invalid --when format: {when}")
            else:
                raise SystemExit(f"Invalid --when format: {when}")
        return dt.replace(tzinfo=tz) if dt.tzinfo is None else dt.astimezone(tz)
    elif date and time:
        try:
            return datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
        except ValueError:
            raise SystemExit("Invalid --date/--time. Expected --date YYYY-MM-DD and --time HH:MM.")
    elif date or time:
        raise SystemExit("Provide both --date and --time, or use --when.")
    else:
        return datetime.now(tz)


def list_run_dirs(day_dir: Path) -> List[str]:
    try:
        it = list(day_dir.iterdir())
    except Exception:
        return []
    runs: List[str] = []
    for p in it:
        if p.is_dir() and len(p.name) == 4 and p.name.isdigit():
            runs.append(p.name)
    return sorted(runs)


def pick_latest_run(runs: List[str], hhmm: str) -> Optional[str]:
    elig = [r for r in runs if r <= hhmm]
    return elig[-1] if elig else None


# -----------------------------
# JSON & filename helpers
# -----------------------------
def try_load_json_strict(path: Path) -> Optional[Any]:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def try_load_json_robust(path: Path) -> Dict[str, Any]:
    """
    Robust JSON reader:
      1) strict json.loads
      2) if fail: take substring from first '{' to last '}', try again
      3) if still fail: return {"_parse_error": "...", "_raw_excerpt": "..."} (capped)
    """
    text = path.read_text(errors="replace")
    # 1) strict
    try:
        return json.loads(text)  # type: ignore
    except Exception as e1:
        pass
    # 2) slice between braces
    i, j = text.find("{"), text.rfind("}")
    if i != -1 and j != -1 and j > i:
        candidate = text[i:j+1]
        try:
            return json.loads(candidate)  # type: ignore
        except Exception as e2:
            return {"_parse_error": f"strict:{type(e1).__name__} / sliced:{type(e2).__name__}", "_raw_excerpt": candidate[:2000]}
    # 3) give up, include excerpt
    return {"_parse_error": "no_json_braces_found", "_raw_excerpt": text[:2000]}


def choose_json_for_run(candidates: List[Path], run_stamp: str) -> Optional[Path]:
    """
    Prefer a JSON whose name contains YYYYMMDDHHMM; otherwise newest by mtime (with tie-break on name).
    """
    if not candidates:
        return None
    exact = [p for p in candidates if run_stamp in p.name]
    pool = exact if exact else candidates
    try:
        return max(pool, key=lambda p: (p.stat().st_mtime, p.name))
    except Exception:
        return pool[0] if pool else None


def is_me_filename(name: str) -> bool:
    n = name.lower()
    return n.startswith("me") and n.endswith(".json")


def is_mmc_filename(name: str) -> bool:
    n = name.lower()
    return ("meteo" in n and "check" in n and n.endswith(".json"))


# New: pattern for filename timestamps YYYYMMDDHHMM
_TS12 = re.compile(r"(?P<ts>\d{12})")


def parse_dt_from_filename(name: str, tz: ZoneInfo) -> Optional[datetime]:
    """
    Try extracting YYYYMMDDHHMM from filename; return tz-aware datetime or None.
    """
    m = _TS12.search(name)
    if not m:
        return None
    s = m.group("ts")
    try:
        dt = datetime.strptime(s, "%Y%m%d%H%M")
        return dt.replace(tzinfo=tz)
    except Exception:
        return None


def file_best_datetime(p: Path, tz: ZoneInfo) -> datetime:
    """
    Best-effort 'creation' datetime for a file:
      - st_birthtime if present (macOS/BSD)
      - else st_mtime (POSIX 'mtime')
    Returned tz-aware in 'tz'.
    """
    st = p.stat()
    ts = getattr(st, "st_birthtime", None)
    if ts is None:
        ts = st.st_mtime
    base = datetime.fromtimestamp(ts)
    return base.replace(tzinfo=tz) if base.tzinfo is None else base.astimezone(tz)


def fmt_compact(dt: datetime, tz: ZoneInfo) -> str:
    """
    Format as 'YYYY-MM-DD HH:MM' in the target tz.
    """
    loc = dt.astimezone(tz)
    return loc.strftime("%Y-%m-%d %H:%M")


def is_data_group(label: str) -> bool:
    """
    Treat groups named 'data' or starting with 'data' (case-insensitive) as data groups.
    """
    return label.lower().startswith("data")


# -----------------------------
# Core (no copies; produce summary only)
# -----------------------------
def process_one_day(cfg: Dict[str, Any], tz_name: str, eff_dt_for_day: datetime, eff_hh: str, eff_mi: str,
                    include_file_history: bool, uses_run: bool, dry_run: bool, history_mode: bool) -> None:
    yyyy, mm, dd = eff_dt_for_day.strftime("%Y"), eff_dt_for_day.strftime("%m"), eff_dt_for_day.strftime("%d")
    hhmm_effective = eff_hh + eff_mi
    tz_obj = ZoneInfo(tz_name)

    # Determine run base and (optionally) choose run
    run_base = Path(render_template(cfg["run_base_template"], yyyy, mm, dd, eff_hh, eff_mi, run=None))

    if uses_run:
        runs = list_run_dirs(run_base)
        if not runs:
            print(f"[{eff_dt_for_day.isoformat()}] No HHMM run folders found in {run_base}")
            return
        candidate = pick_latest_run(runs, hhmm_effective)
        if not candidate:
            print(f"[{eff_dt_for_day.isoformat()}] No run folder <= chosen time ({hhmm_effective}). Found: {runs}")
            return
        run_hh, run_mi = candidate[:2], candidate[2:]
        print(f"Mode time (tz={tz_name}): {eff_dt_for_day.isoformat()}")
        print(f"Run base: {run_base}")
        print(f"Runs found: {runs} -> selected: {candidate}")
    else:
        candidate = None
        run_hh, run_mi = eff_hh, eff_mi
        print(f"Mode time (tz={tz_name}): {eff_dt_for_day.isoformat()}")
        print(f"Run base (no-run mode): {run_base}")

    run_stamp = f"{yyyy}{mm}{dd}{run_hh}{run_mi}"

    # Resolve summary path (per-run filename with timing reference)
    summary_cfg = cfg["summary"]
    if "filename_template" in summary_cfg:
        summary_name = render_template(summary_cfg["filename_template"], yyyy, mm, dd, eff_hh, eff_mi, run=candidate)
    else:
        summary_name = summary_cfg["filename"]

    summary_folder = Path(render_template(summary_cfg["folder_template"], yyyy, mm, dd, eff_hh, eff_mi, run=candidate))
    summary_path = summary_folder / summary_name

    # Inspect file groups (collect & list, no copy)
    results: Dict[str, Any] = {}
    all_seen_files: List[Path] = []

    # Timing accumulators (ONLY for data groups)
    algo_run_dt = eff_dt_for_day.replace(hour=int(run_hh), minute=int(run_mi), second=0, microsecond=0)
    data_fs_times: List[datetime] = []
    data_name_times: List[datetime] = []

    files_spec: Dict[str, Any] = cfg["files"]
    for label, spec in files_spec.items():
        src_folder = Path(render_template(spec["folder_template"], yyyy, mm, dd, eff_hh, eff_mi, run=candidate))

        # Collect sources
        if "glob" in spec:
            src_files = sorted(src_folder.glob(spec["glob"]))
        else:
            fname = render_template(spec["filename_template"], yyyy, mm, dd, eff_hh, eff_mi, run=candidate)
            p = src_folder / fname
            src_files = [p] if p.exists() else []

        all_seen_files.extend(src_files)

        # For timing, only consider "data" groups
        if is_data_group(label) and src_files:
            fs_times = [file_best_datetime(p, tz_obj) for p in src_files]
            data_fs_times.extend(fs_times)
            for p in src_files:
                dtp = parse_dt_from_filename(p.name, tz_obj)
                if dtp is not None:
                    data_name_times.append(dtp)

        # Optional: per-group history details (unchanged behavior, but richer if enabled)
        if include_file_history:
            # Also useful to show per-group oldest/newest (fs/name) even for non-data groups
            fs_times_hist = [file_best_datetime(p, tz_obj) for p in src_files]
            name_times_hist: List[datetime] = []
            for p in src_files:
                dtp = parse_dt_from_filename(p.name, tz_obj)
                if dtp is not None:
                    name_times_hist.append(dtp)
            results[label] = {
                "source_folder": str(src_folder),
                "seen_count": len(src_files),
                "seen_list": [p.name for p in src_files],
                "oldest_fs_time": (fmt_compact(min(fs_times_hist), tz_obj) if fs_times_hist else None),
                "newest_fs_time": (fmt_compact(max(fs_times_hist), tz_obj) if fs_times_hist else None),
                "oldest_name_time": (fmt_compact(min(name_times_hist), tz_obj) if name_times_hist else None),
                "newest_name_time": (fmt_compact(max(name_times_hist), tz_obj) if name_times_hist else None),
            }

    # Discover ME & MeteoModelCheck across all seen files (unchanged)
    me_candidates = [p for p in all_seen_files if is_me_filename(p.name)]
    mmc_candidates = [p for p in all_seen_files if is_mmc_filename(p.name)]

    def build_json_section(cands: List[Path], label: str) -> Dict[str, Any]:
        section: Dict[str, Any] = {"found": False}
        chosen = choose_json_for_run(cands, run_stamp)
        if not chosen:
            return section
        data = try_load_json_strict(chosen)
        parsed_strict = True
        if data is None:
            parsed_strict = False
            data = try_load_json_robust(chosen)
        section.update({
            "found": True,
            "file": chosen.name,
            "path": str(chosen),
            "parsed_strict": parsed_strict,
            "data": data
        })
        return section

    me_section = build_json_section(me_candidates, "ME")
    mmc_section = build_json_section(mmc_candidates, "MeteoModelCheck")

    # Compute timing summary ONLY from data groups
    if data_fs_times or data_name_times:
        overall_oldest_fs_dt = min(data_fs_times) if data_fs_times else None
        overall_newest_fs_dt = max(data_fs_times) if data_fs_times else None
        overall_oldest_name_dt = min(data_name_times) if data_name_times else None
        overall_newest_name_dt = max(data_name_times) if data_name_times else None
    else:
        overall_oldest_fs_dt = overall_newest_fs_dt = overall_oldest_name_dt = overall_newest_name_dt = None

    # Choose latest reference and diff-hours
    diff_hours = None
    diff_basis = None
    latest_ref_dt = None
    if history_mode:
        # Prefer filename-derived in history; fallback to fs
        latest_ref_dt = overall_newest_name_dt or overall_newest_fs_dt
        diff_basis = "filename" if overall_newest_name_dt is not None else ("filesystem" if latest_ref_dt else None)
    else:
        # Now mode: use filesystem time; fallback to filename if fs missing
        latest_ref_dt = overall_newest_fs_dt or overall_newest_name_dt
        diff_basis = "filesystem" if overall_newest_fs_dt is not None else ("filename" if latest_ref_dt else None)

    if latest_ref_dt is not None:
        diff_hours = (algo_run_dt - latest_ref_dt).total_seconds() / 3600.0

    # Compose summary (keeps previous keys; adds 'timing' with requested format)
    summary_doc: Dict[str, Any] = {
        "no_run_mode": not uses_run,
        "run_date": f"{yyyy}-{mm}-{dd}",
        "run_time": f"{run_hh}:{run_mi}",  # unchanged (HH:MM) for backward compatibility
        "run": (candidate if candidate is not None else ""),
        "tz": tz_name,  # timezone provided separately
        "effective_time": eff_dt_for_day.isoformat(),
        "run_base": str(run_base),
        "me": me_section,
        "mc": mmc_section,
        "timing": {
            "tz": tz_name,  # duplicate for convenience within timing section
            "algo_run_time": fmt_compact(algo_run_dt, tz_obj),
            "overall_oldest_fs_time": (fmt_compact(overall_oldest_fs_dt, tz_obj) if overall_oldest_fs_dt else None),
            "overall_newest_fs_time": (fmt_compact(overall_newest_fs_dt, tz_obj) if overall_newest_fs_dt else None),
            "overall_oldest_name_time": (fmt_compact(overall_oldest_name_dt, tz_obj) if overall_oldest_name_dt else None),
            "overall_newest_name_time": (fmt_compact(overall_newest_name_dt, tz_obj) if overall_newest_name_dt else None),
            "diff_hours_run_minus_latest": diff_hours,
            "diff_basis": diff_basis,  # "filesystem" or "filename" (or None if no data files)
            "data_groups_used": [k for k in files_spec.keys() if is_data_group(k)],
        }
    }
    if include_file_history:
        summary_doc["file_history"] = results
    # Expose group names to assist downstream tools
    summary_doc["_files_groups"] = list(files_spec.keys())

    # Write or print
    if dry_run:
        print(json.dumps(summary_doc, indent=2, sort_keys=True))
    else:
        summary_folder.mkdir(parents=True, exist_ok=True)
        tmp = summary_path.with_suffix(summary_path.suffix + ".tmp")
        tmp.write_text(json.dumps(summary_doc, indent=2, sort_keys=True))
        tmp.replace(summary_path)
        print(f"Summary written: {summary_path}")


# -----------------------------
# Main
# -----------------------------
def main():
    args = parse_args()
    cfg = load_config(args.config)
    tz_name = args.tz or cfg.get("tz", "Europe/Rome")

    # Determine include_file_history policy
    include_file_history = bool(
        args.force_file_history
        or args.include_file_history
        or cfg.get("include_file_history", False)
    )

    # Resolve the base start datetime in tz
    base_dt = resolve_effective_dt(tz_name, args.when, args.date, args.time)

    # Flatten time-of-day per user's choice (HH:MM) for each iterated day
    flat_hh, flat_mi = _parse_flat_hhmm(args.flatten_hour)

    # Detect whether config expects RUN subfolders
    uses_run = config_uses_run(cfg)

    # Iterate from start day back N days (inclusive)
    n_days = max(0, int(args.n_days))
    apply_flatten = n_days > 0  # history-mode signal

    for d in range(0, n_days + 1):
        day_dt = (base_dt - timedelta(days=d))
        if apply_flatten:
            day_dt = day_dt.replace(hour=int(flat_hh), minute=int(flat_mi), second=0, microsecond=0)
            eff_hh, eff_mi = flat_hh, flat_mi
        else:
            eff_hh, eff_mi = day_dt.strftime("%H"), day_dt.strftime("%M")

        process_one_day(
            cfg=cfg,
            tz_name=tz_name,
            eff_dt_for_day=day_dt,
            eff_hh=eff_hh,
            eff_mi=eff_mi,
            include_file_history=include_file_history,
            uses_run=uses_run,
            dry_run=args.dry_run,
            history_mode=apply_flatten,  # now mode (False) vs history mode (True)
        )


if __name__ == "__main__":
    main()

