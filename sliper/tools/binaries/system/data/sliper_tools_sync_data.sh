
#!/usr/bin/env bash
set -euo pipefail

# -------------------------------------------------------------------------------------
# Configuration
ROOT="${ROOT:-/home/admin/soilslips-system/manager_data}"
VENV_DIR="${VENV_DIR:-$ROOT/venv}"

PYTHON_SCRIPT="${PYTHON_SCRIPT:-$ROOT/sliper_tools_sync_data.py}"
CONFIG_FILE_DEFAULT="${CONFIG_FILE_DEFAULT:-$ROOT/sliper_tools_sync_data_obs.json}"
LOCKFILE_DEFAULT="${LOCKFILE_DEFAULT:-$ROOT/sliper_tools_sync_data.lock}"

VERBOSE=0

# -------------------------------------------------------------------------------------
# Usage
usage() {
cat <<'EOF'
Usage:
  sliper_tools_sync_data.sh --mode now [--fix HH:MM | --newest] [options]
  sliper_tools_sync_data.sh --mode history (--when ISO | --date YYYY-MM-DD --time HH:MM) [--fix HH:MM | --newest] [options]

Selection:
  --fix HH:MM              Require exact summary run hour (e.g. 04:00)
  --newest                  Select newest available summary (DEFAULT)

Options:
  --config PATH
  --script PATH
  --lockfile PATH
  --tz TIMEZONE
  --end-at-date-obs         Limit copying to DateObs
  --end-at-run-hour         Limit copying to selected run hour (inclusive)
  --list-only
  --debug-gate
  --n-days N
  --scan-source-days
  --flatten-hour HH:MM
  --start-hour HH:MM
  --freq-minutes N
  --filename-date-regex REGEX
  --verbose | --quiet
  --when ISO
  --date YYYY-MM-DD
  --time HH:MM
  -h | --help

Notes:
  --time in history mode specifies the date/time reference.
  --fix controls exact run selection.
  Without --fix, --time retains the existing cutoff behavior
  unless --newest is explicit.

  --end-at-run-hour uses the run date and hour selected
  from the summary.

  When combined with --end-at-date-obs, the Python
  synchronizer must use the earlier of the two cutoffs.
EOF
}

# -------------------------------------------------------------------------------------
# Logging methods
log() {
    [[ $VERBOSE -eq 1 ]] && echo "[INFO] $*" >&2 || true
}

err() {
    echo "[ERR ] $*" >&2
}

die() {
    err "$*"
    exit 1
}

# -------------------------------------------------------------------------------------
# Initialize arguments
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
END_AT_RUN_HOUR=0

SCAN_SOURCE_DAYS=0
N_DAYS=0
FLATTEN_HOUR=""
START_HOUR=""

FREQ_MINUTES=""
FILENAME_DATE_REGEX=""

FIX_HOUR=""
NEWEST=0

# -------------------------------------------------------------------------------------
# Parse arguments
while [[ $# -gt 0 ]]; do

    case "$1" in

        --mode)
            MODE="${2:-}"
            shift 2
            ;;

        --when)
            WHEN="${2:-}"
            shift 2
            ;;

        --date)
            DATE_ARG="${2:-}"
            shift 2
            ;;

        --time)
            TIME_ARG="${2:-}"
            shift 2
            ;;

        --fix)
            FIX_HOUR="${2:-}"
            shift 2
            ;;

        --newest)
            NEWEST=1
            shift
            ;;

        --config)
            CONFIG_FILE="${2:-}"
            shift 2
            ;;

        --script)
            PYTHON_SCRIPT="${2:-}"
            shift 2
            ;;

        --lockfile)
            LOCKFILE="${2:-}"
            shift 2
            ;;

        --tz)
            OPT_TZ="${2:-}"
            shift 2
            ;;

        --end-at-date-obs)
            END_AT_DATE_OBS=1
            shift
            ;;

        --end-at-run-hour)
            END_AT_RUN_HOUR=1
            shift
            ;;

        --scan-source-days)
            SCAN_SOURCE_DAYS=1
            shift
            ;;

        --list-only)
            LIST_ONLY=1
            shift
            ;;

        --debug-gate)
            DEBUG_GATE=1
            shift
            ;;

        --n-days)
            N_DAYS="${2:-0}"
            shift 2
            ;;

        --flatten-hour)
            FLATTEN_HOUR="${2:-}"
            shift 2
            ;;

        --start-hour)
            START_HOUR="${2:-}"
            shift 2
            ;;

        --freq-minutes)
            FREQ_MINUTES="${2:-}"
            shift 2
            ;;

        --filename-date-regex)
            FILENAME_DATE_REGEX="${2:-}"
            shift 2
            ;;

        --verbose)
            VERBOSE=1
            shift
            ;;

        --quiet)
            VERBOSE=0
            shift
            ;;

        -h|--help)
            usage
            exit 0
            ;;

        *)
            err "Unknown option: $1"
            usage
            exit 2
            ;;

    esac

done

# -------------------------------------------------------------------------------------
# Validate arguments
[[ "$MODE" == now || "$MODE" == history ]] || \
    die "--mode must be now or history"

[[ -f "$PYTHON_SCRIPT" ]] || \
    die "Python script not found: $PYTHON_SCRIPT"

[[ -f "$CONFIG_FILE" ]] || \
    die "Config file not found: $CONFIG_FILE"

[[ -z "$FIX_HOUR" || $NEWEST -eq 0 ]] || \
    die "--fix and --newest are mutually exclusive"

if [[ -n "$FIX_HOUR" && ! "$FIX_HOUR" =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]]; then
    die "--fix must use HH:MM"
fi

if [[ "$MODE" == history ]]; then

    [[ -n "$WHEN" || ( -n "$DATE_ARG" && -n "$TIME_ARG" ) ]] || \
        die "History requires --when or --date and --time"

else

    WHEN=""
    DATE_ARG=""
    TIME_ARG=""

fi

# -------------------------------------------------------------------------------------
# Initialize virtual environment
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
print(int(sys.version_info < (3, 9)))
PY
)"

    if [[ "$NEED_BZ" -eq 1 ]]; then
        pip install -q backports.zoneinfo
    fi
}

ensure_venv

# -------------------------------------------------------------------------------------
# Define Python command
PYTHON_BIN="$VENV_DIR/bin/python"

CMD=(
    "$PYTHON_BIN"
    "$PYTHON_SCRIPT"
    --config "$CONFIG_FILE"
)

# -------------------------------------------------------------------------------------
# Add optional arguments
[[ $END_AT_DATE_OBS -eq 1 ]] && CMD+=( --limit-to-obs )
[[ $END_AT_RUN_HOUR -eq 1 ]] && CMD+=( --end-at-run-hour )

[[ $LIST_ONLY -eq 1 ]] && CMD+=( --dry-run )
[[ $DEBUG_GATE -eq 1 ]] && CMD+=( --debug-gate )

[[ -n "$OPT_TZ" ]] && CMD+=( --tz "$OPT_TZ" )

[[ $N_DAYS -gt 0 ]] && CMD+=( --n-days "$N_DAYS" )
[[ $SCAN_SOURCE_DAYS -eq 1 ]] && CMD+=( --scan-source-days )

[[ -n "$FLATTEN_HOUR" ]] && CMD+=( --flatten-hour "$FLATTEN_HOUR" )
[[ -n "$START_HOUR" ]] && CMD+=( --start-hour "$START_HOUR" )

[[ -n "$FREQ_MINUTES" ]] && CMD+=( --freq-minutes "$FREQ_MINUTES" )

[[ -n "$FILENAME_DATE_REGEX" ]] && \
    CMD+=( --filename-date-regex "$FILENAME_DATE_REGEX" )

[[ -n "$FIX_HOUR" ]] && CMD+=( --fix "$FIX_HOUR" )
[[ $NEWEST -eq 1 ]] && CMD+=( --newest )

# -------------------------------------------------------------------------------------
# Add history arguments
if [[ "$MODE" == history ]]; then

    if [[ -n "$WHEN" ]]; then
        CMD+=( --when "$WHEN" )
    else
        CMD+=( --date "$DATE_ARG" --time "$TIME_ARG" )
    fi

fi

# -------------------------------------------------------------------------------------
# Print execution configuration
log "Mode               : $MODE"
log "Python script      : $PYTHON_SCRIPT"
log "Configuration      : $CONFIG_FILE"
log "Fix hour           : ${FIX_HOUR:-newest}"
log "End at DateObs     : $END_AT_DATE_OBS"
log "End at run hour    : $END_AT_RUN_HOUR"
log "Lock file          : $LOCKFILE"

if [[ $VERBOSE -eq 1 ]]; then
    printf '[INFO] Command: ' >&2
    printf '%q ' "${CMD[@]}" >&2
    printf '\n' >&2
fi

# -------------------------------------------------------------------------------------
# Initialize lock folder
mkdir -p "$(dirname "$LOCKFILE")"

# -------------------------------------------------------------------------------------
# Execute Python synchronizer with Bash lock
if command -v flock >/dev/null 2>&1; then

    log "Using flock at $LOCKFILE"

    exec 9>"$LOCKFILE"

    if ! flock -n 9; then
        die "Another run is in progress (lock: $LOCKFILE)"
    fi

    "${CMD[@]}"

else

    log "flock not available; using naive lock at $LOCKFILE"

    if [[ -e "$LOCKFILE" ]]; then
        die "Lockfile exists ($LOCKFILE). Remove it if stale."
    fi

    trap 'rm -f "$LOCKFILE"' EXIT

    : > "$LOCKFILE"

    "${CMD[@]}"

fi

# -------------------------------------------------------------------------------------

