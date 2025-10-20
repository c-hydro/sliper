#!/usr/bin/env bash
set -euo pipefail

# =============================
# Project paths (edit if needed)
# =============================
ROOT="${ROOT:-/home/admin/soilslips-system/manager_run}"
VENV_DIR="${VENV_DIR:-$ROOT/venv}"
PYTHON_SCRIPT="${PYTHON_SCRIPT:-$ROOT/sliper_tools_select_run.py}"
CONFIG_FILE_DEFAULT="${CONFIG_FILE_DEFAULT:-$ROOT/sliper_tools_select_run.json}"
LOCKFILE_DEFAULT="${LOCKFILE_DEFAULT:-$ROOT/sliper_tools_select_run.lock}"

VERBOSE=0

usage() {
  cat <<'EOF'
Usage:
  sliper_tools_sync_data_rain.sh --mode now [options]
  sliper_tools_sync_data_rain.sh --mode history (--when ISO | --date YYYY-MM-DD --time HH:MM) [options]

Modes:
  --mode now
  --mode history

Time selectors (history mode):
  --when 2025-09-23T09:30
  --date 2025-09-23 --time 09:30

Common options:
  --config /path/to/config.json        (default: ROOT/sliper_tools_sync_data_rain_frc.json)
  --script /path/to/sliper_tools_sync_data_rain.py
  --lockfile /path/to/lockfile         (default: ROOT/sliper_tools_sync_data_rain_frc.lock)
  --tz Europe/Rome                     (overrides config tz)
  --n-days INT                         (default: 0; iterate start day plus N previous days)
  --flatten-hour HH:MM                 (default: 00:00; per-day selection time)
  --include-file-history               (include detailed file history in summary JSON)
  --list-only                          (dry run: show planned copies only)
  --verbose | --quiet
  -h | --help

Notes:
- This script auto-creates and uses a local Python venv at ROOT/venv.
- It installs backports.zoneinfo automatically for Python < 3.9.
EOF
}

log() { [[ $VERBOSE -eq 1 ]] && echo "[INFO] $*" >&2 || true; }
err() { echo "[ERR ] $*" >&2; }
die() { err "$*"; exit 1; }

# =============================
# Parse args
# =============================
MODE=""
WHEN=""
DATE_ARG=""
TIME_ARG=""
CONFIG_FILE="$CONFIG_FILE_DEFAULT"
LOCKFILE="$LOCKFILE_DEFAULT"
LIST_ONLY=0
INCLUDE_FILE_HISTORY=0
OPT_TZ=""
N_DAYS="0"
FLATTEN_HOUR="00:00"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)                 MODE="${2:-}"; shift 2 ;;
    --when)                 WHEN="${2:-}"; shift 2 ;;
    --date)                 DATE_ARG="${2:-}"; shift 2 ;;
    --time)                 TIME_ARG="${2:-}"; shift 2 ;;
    --config)               CONFIG_FILE="${2:-}"; shift 2 ;;
    --script)               PYTHON_SCRIPT="${2:-}"; shift 2 ;;
    --lockfile)             LOCKFILE="${2:-}"; shift 2 ;;
    --tz)                   OPT_TZ="${2:-}"; shift 2 ;;
    --n-days)               N_DAYS="${2:-0}"; shift 2 ;;
    --flatten-hour)         FLATTEN_HOUR="${2:-00:00}"; shift 2 ;;
    --include-file-history) INCLUDE_FILE_HISTORY=1; shift ;;
    --list-only)            LIST_ONLY=1; shift ;;
    --verbose)              VERBOSE=1; shift ;;
    --quiet)                VERBOSE=0; shift ;;
    -h|--help)              usage; exit 0 ;;
    *)                      err "Unknown option: $1"; usage; exit 2 ;;
  esac
done

[[ -n "$MODE" ]] || die "Missing --mode (now|history)."
[[ "$MODE" == "now" || "$MODE" == "history" ]] || die "--mode must be now or history."
[[ -f "$PYTHON_SCRIPT" ]] || die "Python script not found: $PYTHON_SCRIPT"
[[ -f "$CONFIG_FILE" ]] || die "Config file not found: $CONFIG_FILE"

if [[ "$MODE" == "history" ]]; then
  if [[ -z "$WHEN" && ( -z "$DATE_ARG" || -z "$TIME_ARG" ) ]]; then
    die "History mode requires --when ISO or --date YYYY-MM-DD AND --time HH:MM."
  fi
else
  WHEN=""; DATE_ARG=""; TIME_ARG=""
fi

# Basic sanity on --flatten-hour (HH:MM)
if [[ ! "$FLATTEN_HOUR" =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]]; then
  die "Invalid --flatten-hour '$FLATTEN_HOUR'. Expected HH:MM (e.g., 00:00 or 09:30)."
fi

# Basic sanity on --n-days
if ! [[ "$N_DAYS" =~ ^[0-9]+$ ]]; then
  die "Invalid --n-days '$N_DAYS'. Must be a non-negative integer."
fi

# =============================
# Ensure venv
# =============================
ensure_venv() {
  if [[ ! -d "$VENV_DIR" ]]; then
    log "Creating venv at $VENV_DIR"
    python3 -m venv "$VENV_DIR"
  fi

  # shellcheck disable=SC1091
  source "$VENV_DIR/bin/activate"

  # Make sure pip is present/recent
  python -m ensurepip --upgrade >/dev/null 2>&1 || true
  python -m pip install --upgrade pip wheel setuptools >/dev/null

  # Install backports.zoneinfo if Python < 3.9
  NEED_BZ="$(python - <<'PY'
import sys
print(int(sys.version_info < (3,9)))
PY
)"
  if [[ "$NEED_BZ" -eq 1 ]]; then
    log "Installing backports.zoneinfo (Python < 3.9)"
    pip install -q backports.zoneinfo
  fi
}

ensure_venv

PYTHON_BIN="$VENV_DIR/bin/python"

# =============================
# Build command
# =============================
CMD=( "$PYTHON_BIN" "$PYTHON_SCRIPT" --config "$CONFIG_FILE" --flatten-hour "$FLATTEN_HOUR" --n-days "$N_DAYS" )

[[ $LIST_ONLY -eq 1 ]] && CMD+=( --list-only )
[[ $INCLUDE_FILE_HISTORY -eq 1 ]] && CMD+=( --include-file-history )
[[ -n "$OPT_TZ" ]] && CMD+=( --tz "$OPT_TZ" )

if [[ "$MODE" == "history" ]]; then
  if [[ -n "$WHEN" ]]; then
    CMD+=( --when "$WHEN" )
  else
    CMD+=( --date "$DATE_ARG" --time "$TIME_ARG" )
  fi
fi

log "Command: ${CMD[*]}"

run_cmd() { "${CMD[@]}"; }

# =============================
# Lock & execute
# =============================
if command -v flock >/dev/null 2>&1; then
  log "Using flock at $LOCKFILE"
  # Open FD 9 on lockfile, THEN acquire non-blocking lock
  exec 9>"$LOCKFILE"
  if ! flock -n 9; then
    die "Another run is in progress (lock: $LOCKFILE)."
  fi
  run_cmd
else
  log "flock not available; using naïve lock at $LOCKFILE"
  if [[ -e "$LOCKFILE" ]]; then
    die "Lockfile exists ($LOCKFILE). Remove it if stale."
  fi
  trap 'rm -f "$LOCKFILE"' EXIT
  : > "$LOCKFILE"
  run_cmd
fi

