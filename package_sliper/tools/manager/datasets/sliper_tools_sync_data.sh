#!/usr/bin/env bash
set -euo pipefail

# =============================
# Project paths (edit if needed)
# =============================
ROOT="${ROOT:-/home/admin/soilslips-system/manager_data}"
VENV_DIR="${VENV_DIR:-$ROOT/venv}"

# Python script + defaults
PYTHON_SCRIPT="${PYTHON_SCRIPT:-$ROOT/sliper_tools_sync_data.py}"
CONFIG_FILE_DEFAULT="${CONFIG_FILE_DEFAULT:-$ROOT/sliper_tools_sync_data_obs.json}"
LOCKFILE_DEFAULT="${LOCKFILE_DEFAULT:-$ROOT/sliper_tools_sync_data.lock}"

VERBOSE=0

usage() {
  cat <<'EOF'
Usage:
  sliper_tools_copy_from_run.sh --mode now [options]
  sliper_tools_copy_from_run.sh --mode history (--when ISO | --date YYYY-MM-DD --time HH:MM) [options]

Modes:
  --mode now
  --mode history

Time selectors (history mode):
  --when 2025-09-23T09:30
  --date 2025-09-23 --time 09:30

Options:
  --config /path/to/config.json
  --script /path/to/script.py
  --lockfile /path/to/lockfile
  --tz Europe/Rome
  --end-at-date-obs             Use [DateIni, DateObs] instead of [DateIni, DateEnd]
  --list-only                   Dry run (show actions only)
  --debug-gate                  Include per-file gating diagnostics (even without --list-only)
  --n-days N                    Extend copy window backwards by N full days
  --flatten-hour HH:MM          If now < HH:MM, shift window back 1 day
  --start-hour HH:MM            Floor start of window to HH:MM when --n-days>0
  --freq-minutes N              Keep only one file every N minutes
  --filename-date-regex REGEX   Regex for timestamp in filenames (default: Rain_(?P<ts>\d{12})\.tif for data group)
  --verbose | --quiet
  -h | --help

Notes:
- This script auto-creates and uses a local Python venv at ROOT/venv.
- Installs backports.zoneinfo automatically for Python < 3.9.
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
DEBUG_GATE=0
OPT_TZ=""
END_AT_DATE_OBS=0
N_DAYS=0
FLATTEN_HOUR=""
START_HOUR=""
FREQ_MINUTES=""
FILENAME_DATE_REGEX=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)                MODE="${2:-}"; shift 2 ;;
    --when)                WHEN="${2:-}"; shift 2 ;;
    --date)                DATE_ARG="${2:-}"; shift 2 ;;
    --time)                TIME_ARG="${2:-}"; shift 2 ;;
    --config)              CONFIG_FILE="${2:-}"; shift 2 ;;
    --script)              PYTHON_SCRIPT="${2:-}"; shift 2 ;;
    --lockfile)            LOCKFILE="${2:-}"; shift 2 ;;
    --tz)                  OPT_TZ="${2:-}"; shift 2 ;;
    --end-at-date-obs)     END_AT_DATE_OBS=1; shift ;;
    --list-only)           LIST_ONLY=1; shift ;;
    --debug-gate)          DEBUG_GATE=1; shift ;;
    --n-days)              N_DAYS="${2:-0}"; shift 2 ;;
    --flatten-hour)        FLATTEN_HOUR="${2:-}"; shift 2 ;;
    --start-hour)          START_HOUR="${2:-}"; shift 2 ;;
    --freq-minutes)        FREQ_MINUTES="${2:-}"; shift 2 ;;
    --filename-date-regex) FILENAME_DATE_REGEX="${2:-}"; shift 2 ;;
    --verbose)             VERBOSE=1; shift ;;
    --quiet)               VERBOSE=0; shift ;;
    -h|--help)             usage; exit 0 ;;
    *)                     err "Unknown option: $1"; usage; exit 2 ;;
  esac
done

# =============================
# Validate args
# =============================
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
  python -m ensurepip --upgrade >/dev/null 2>&1 || true
  python -m pip install --upgrade pip wheel setuptools >/dev/null
  NEED_BZ="$(python - <<'PY'
import sys
print(int(sys.version_info < (3,9)))
PY
)"
  if [[ "$NEED_BZ" -eq 1 ]]; then
    pip install -q backports.zoneinfo
  fi
}
ensure_venv
PYTHON_BIN="$VENV_DIR/bin/python"

# =============================
# Build command
# =============================
CMD=( "$PYTHON_BIN" "$PYTHON_SCRIPT" --config "$CONFIG_FILE" )
[[ $END_AT_DATE_OBS -eq 1 ]] && CMD+=( --limit-to-obs )
[[ $LIST_ONLY -eq 1 ]] && CMD+=( --dry-run )
[[ $DEBUG_GATE -eq 1 ]] && CMD+=( --debug-gate )
[[ -n "$OPT_TZ" ]] && CMD+=( --tz "$OPT_TZ" )
[[ $N_DAYS -gt 0 ]] && CMD+=( --n-days "$N_DAYS" )
[[ -n "$FLATTEN_HOUR" ]] && CMD+=( --flatten-hour "$FLATTEN_HOUR" )
[[ -n "$START_HOUR" ]] && CMD+=( --start-hour "$START_HOUR" )
[[ -n "$FREQ_MINUTES" ]] && CMD+=( --freq-minutes "$FREQ_MINUTES" )
[[ -n "$FILENAME_DATE_REGEX" ]] && CMD+=( --filename-date-regex "$FILENAME_DATE_REGEX" )

if [[ "$MODE" == "history" ]]; then
  if [[ -n "$WHEN" ]]; then
    CMD+=( --when "$WHEN" )
  else
    CMD+=( --date "$DATE_ARG" --time "$TIME_ARG" )
  fi
fi

log "Command: ${CMD[*]}"
run_copy() { "${CMD[@]}"; }

# =============================
# Lock & execute
# =============================
if command -v flock >/dev/null 2>&1; then
  log "Using flock at $LOCKFILE"
  exec 9>"$LOCKFILE"
  if ! flock -n 9; then
    die "Another run is in progress (lock: $LOCKFILE)."
  fi
  run_copy
else
  log "flock not available; using naïve lock at $LOCKFILE"
  if [[ -e "$LOCKFILE" ]]; then
    die "Lockfile exists ($LOCKFILE). Remove it if stale."
  fi
  trap 'rm -f "$LOCKFILE"' EXIT
  : > "$LOCKFILE"
  run_copy
fi

