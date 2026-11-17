#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="${ROOT:-/home/admin/soilslips-system/manager_run}"
VENV_DIR="${VENV_DIR:-$ROOT/venv}"
PYTHON_SCRIPT="${PYTHON_SCRIPT:-$ROOT/sliper_tools_select_run.py}"
CONFIG_FILE_DEFAULT="${CONFIG_FILE_DEFAULT:-$ROOT/sliper_tools_select_run.json}"
LOCK_FOLDER="/home/admin/lock/manager_run"
VERBOSE=0

usage() {
    cat <<'USAGE'
Usage:
  sliper_tools_select_run.sh --mode now [--run-time HH:MM] [options]
  sliper_tools_select_run.sh --mode history (--when ISO | --date YYYY-MM-DD --time HH:MM) [--run-time HH:MM] [options]

Options:
  --run-time HH:MM          Selected model cycle; translated to Python --when DATETHH:MM
  --config PATH            JSON configuration
  --script PATH            Python selection script
  --lockfile PATH          Override per-run lock (wrapper passes this)
  --tz TIMEZONE
  --n-days N
  --flatten-hour HH:MM
  --include-file-history
  --list-only
  --verbose | --quiet
  --when ISO | --date YYYY-MM-DD --time HH:MM  (history mode)
  -h | --help
USAGE
}
log() { if ((VERBOSE)); then printf '[INFO] %s\n' "$*" >&2; fi; }
die() { printf '[ERROR] %s\n' "$*" >&2; exit 1; }

MODE=""
WHEN=""
DATE_ARG=""
TIME_ARG=""
RUN_TIME=""
CONFIG_FILE="$CONFIG_FILE_DEFAULT"
LOCKFILE=""
LIST_ONLY=0
INCLUDE_FILE_HISTORY=0
OPT_TZ=""
N_DAYS=0
FLATTEN_HOUR="00:00"

while (($#)); do
    case "$1" in
        --mode|--when|--date|--time|--run-time|--config|--script|--lockfile|--tz|--n-days|--flatten-hour)
            (($# >= 2)) || die "$1 requires a value"
            case "$1" in
                --mode) MODE="$2" ;;
                --when) WHEN="$2" ;;
                --date) DATE_ARG="$2" ;;
                --time) TIME_ARG="$2" ;;
                --run-time) RUN_TIME="$2" ;;
                --config) CONFIG_FILE="$2" ;;
                --script) PYTHON_SCRIPT="$2" ;;
                --lockfile) LOCKFILE="$2" ;;
                --tz) OPT_TZ="$2" ;;
                --n-days) N_DAYS="$2" ;;
                --flatten-hour) FLATTEN_HOUR="$2" ;;
            esac
            shift 2 ;;
        --include-file-history) INCLUDE_FILE_HISTORY=1; shift ;;
        --list-only) LIST_ONLY=1; shift ;;
        --verbose) VERBOSE=1; shift ;;
        --quiet) VERBOSE=0; shift ;;
        -h|--help) usage; exit 0 ;;
        *) die "Unknown option: $1" ;;
    esac
done

[[ "$MODE" == now || "$MODE" == history ]] || die '--mode must be now or history'
[[ -f "$PYTHON_SCRIPT" ]] || die "Python script not found: $PYTHON_SCRIPT"
[[ -f "$CONFIG_FILE" ]] || die "Configuration not found: $CONFIG_FILE"
[[ -z "$RUN_TIME" || "$RUN_TIME" =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]] || die "Invalid --run-time '$RUN_TIME' (HH:MM required)"
[[ "$FLATTEN_HOUR" =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]] || die "Invalid --flatten-hour '$FLATTEN_HOUR'"
[[ "$N_DAYS" =~ ^[0-9]+$ ]] || die "Invalid --n-days '$N_DAYS'"
if [[ "$MODE" == history ]]; then
    [[ -n "$WHEN" || ( -n "$DATE_ARG" && -n "$TIME_ARG" ) ]] || die 'History requires --when or --date and --time'
    [[ -z "$WHEN" || ( -z "$DATE_ARG" && -z "$TIME_ARG" ) ]] || die 'Use --when or --date and --time, not both'
else
    [[ -z "$WHEN" && -z "$DATE_ARG" && -z "$TIME_ARG" ]] || die '--when/--date/--time require --mode history'
fi

# The wrapper supplies a cycle-specific lock. For standalone use, derive the
# same lock naming convention from the requested cycle or current clock time.
if [[ -z "$LOCKFILE" ]]; then
    RUN_HOUR="${RUN_TIME:-$(date '+%H:%M')}"
    if [[ "$MODE" == history ]]; then
        if [[ -n "$WHEN" ]]; then
            RUN_DATE="${WHEN:0:10}"
        else
            RUN_DATE="$DATE_ARG"
        fi
    else
        RUN_DATE="$(date '+%Y-%m-%d')"
    fi
    RUN_TAG="$(date -d "$RUN_DATE $RUN_HOUR:00" '+%Y%m%d%H%M')" || die 'Unable to derive lock time tag'
    LOCKFILE="$LOCK_FOLDER/sliper_tools_select_run_${RUN_TAG}.lock"
fi

command -v flock >/dev/null 2>&1 || die 'flock is required for safe execution'
mkdir -p "$(dirname "$LOCKFILE")"
exec 9>"$LOCKFILE"
if ! flock -n 9; then
    die "Another selection is running (lock: $LOCKFILE)"
fi

# Avoid repeated pip upgrades on every operational execution.
if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    log "Creating Python environment: $VENV_DIR"
    python3 -m venv "$VENV_DIR"
fi
PYTHON_BIN="$VENV_DIR/bin/python"
if "$PYTHON_BIN" - <<'PY'
import sys
sys.exit(0 if sys.version_info >= (3, 9) else 1)
PY
then
    :
else
    "$PYTHON_BIN" -m pip install -q backports.zoneinfo
fi

CMD=("$PYTHON_BIN" "$PYTHON_SCRIPT" --config "$CONFIG_FILE" --flatten-hour "$FLATTEN_HOUR" --n-days "$N_DAYS")
((LIST_ONLY == 0)) || CMD+=(--dry-run)
((INCLUDE_FILE_HISTORY == 0)) || CMD+=(--include-file-history)
[[ -z "$OPT_TZ" ]] || CMD+=(--tz "$OPT_TZ")
# Python accepts --when, not --run-time. Convert the requested cycle to
# an ISO date/time reference while keeping --run-time for wrapper/lock naming.
PYTHON_WHEN=""
if [[ -n "$RUN_TIME" ]]; then
    if [[ "$MODE" == history ]]; then
        if [[ -n "$WHEN" ]]; then
            RUN_DATE="${WHEN:0:10}"
        else
            RUN_DATE="$DATE_ARG"
        fi
    else
        RUN_DATE="$(date '+%Y-%m-%d')"
    fi
    PYTHON_WHEN="${RUN_DATE}T${RUN_TIME}"
    CMD+=(--when "$PYTHON_WHEN" --exact-run)
elif [[ "$MODE" == history ]]; then
    if [[ -n "$WHEN" ]]; then
        CMD+=(--when "$WHEN")
    else
        CMD+=(--date "$DATE_ARG" --time "$TIME_ARG")
    fi
fi

printf '[SEARCH MODEL RUN] Mode          : %s\n' "$MODE"
printf '[SEARCH MODEL RUN] Requested run : %s\n' "${RUN_TIME:-automatic selection}"
printf '[SEARCH MODEL RUN] Python --when : %s\n' "${PYTHON_WHEN:-not specified}"
printf '[SEARCH MODEL RUN] Selection     : %s\n' "$( [[ -n "$RUN_TIME" ]] && echo exact || echo latest-eligible )"
printf '[SEARCH MODEL RUN] Lock file     : %s\n' "$LOCKFILE"
printf '[SEARCH MODEL RUN] Python script : %s\n' "$PYTHON_SCRIPT"
printf '[SEARCH MODEL RUN] Command       : '
printf '%q ' "${CMD[@]}"
printf '\n'

if "${CMD[@]}"; then
    EXIT_CODE=0
else
    EXIT_CODE=$?
fi
printf '[SEARCH MODEL RUN] Exit code     : %s\n' "$EXIT_CODE"
exit "$EXIT_CODE"
