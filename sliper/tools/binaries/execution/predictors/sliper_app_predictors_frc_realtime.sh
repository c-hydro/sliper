#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------------------
# Cron-safe environment (helps when run via crontab)
export HOME=/home/admin
export SHELL=/bin/bash
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
umask 022
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Script information
SCRIPT_NAME='SLIPER - APP PREDICTORS - FRC REALTIME'
SCRIPT_VERSION="1.3.1"
SCRIPT_DATE='2026/10/23'
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Defaults (overridden by CLI)
SCRIPT_MODE="now"      # now|history
TIME_RUN=""            # user input
EXPLICIT_RUN=false     # true only when --date is supplied
TIME_ALG=""            # normalized datetime: YYYY-MM-DD HH:MM
TIME_ANC=""            # normalized date: YYYY-MM-DD

SKIP_ALGORITHM=false
SKIP_ANCILLARY=false

TZ_LOCAL=""
ROUND_HOUR=true
FORCE_RUN=false
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# User-configurable env/paths
CONDA_HOME="/home/admin/library/env_conda_sliper_data"
CONDA_ENV="sliper_data_libraries"

ANC_FOLDER="/home/admin/library/package_sliper/time"
ANC_SCRIPT="/home/admin/library/package_sliper/time/sliper_app_time_reference.py"
ANC_SETTINGS_FOLDER_TEMPLATE="/home/admin/soilslips-ws/data_dynamic/source/run/%Y/%m/%d"

ALG_FOLDER="/home/admin/library/package_sliper/apps/predictors"
ALG_SCRIPT="/home/admin/library/package_sliper/apps/predictors/sliper_app_predictors_main.py"
ALG_SETTINGS="/home/admin/soilslips-exec/predictors/sliper_app_predictors_frc_realtime.json"

# log setup
LOG_FOLDER="${LOG_FOLDER:-/home/admin/log/runner_predictors}"
LOG_BASENAME="${LOG_BASENAME:-bash_predictors_frc_realtime}"

# lock setup
LOCK_FOLDER="${LOCK_FOLDER:-/home/admin/lock/runner_predictors}"
LOCK_BASENAME="${LOCK_BASENAME:-bash_predictors_frc_realtime}"

# get timestamp from system
ts() {
    date '+%F %T %Z'
}
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Script usage
usage() {

    cat <<EOF
Usage: $(basename "$0") \
[--mode now|history] \
[--date 'YYYY-MM-DD'|'YYYY-MM-DD HH:MM'] \
[--skip-algorithm] \
[--skip-ancillary] \
[--round-hour] \
[--tz ZONE]

Options:
  --mode            Processing mode (default: now).
                    Use 'history' with --date.

  --date            Date/time to process.
                    Accepts:
                      YYYY-MM-DD
                      YYYY-MM-DD HH:MM

  --skip-algorithm  Run ancillary but skip algorithm execution.

  --skip-ancillary  Skip ancillary step.

  --round-hour      Truncate TIME_ALG minutes to HH:00.

  --tz ZONE         Timezone for time calculations.

  --force           Request forced execution; never bypass an active lock.

  -h, --help        Show this help.
EOF
}
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Parse CLI
while [[ ${1:-} ]]; do

    case "$1" in

        --mode)
            SCRIPT_MODE="${2:-}"
            shift 2
            ;;

        --date)
            TIME_RUN="${2:-}"
            EXPLICIT_RUN=true
            shift 2
            ;;

        --skip-algorithm)
            SKIP_ALGORITHM=true
            shift
            ;;

        --skip-ancillary)
            SKIP_ANCILLARY=true
            shift
            ;;

        --round-hour)
            ROUND_HOUR=true
            shift
            ;;

        --force)
            FORCE_RUN=true
            shift
            ;;

        --tz)
            TZ_LOCAL="${2:-}"
            shift 2
            ;;

        -h|--help)
            usage
            exit 0
            ;;

        *)
            echo "Unknown arg: $1" >&2
            usage
            exit 1
            ;;

    esac

done

# Validate script mode
if [[ "$SCRIPT_MODE" != "now" && "$SCRIPT_MODE" != "history" ]]; then
    echo " ===> ERROR: --mode must be 'now' or 'history'." >&2
    exit 1
fi
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Resolve selected execution time before log and lock filenames
if [[ "$SCRIPT_MODE" == "history" && -z "$TIME_RUN" ]]; then
    echo " ===> ERROR: --date is required in history mode." >&2
    exit 1
fi

if [[ -n "$TIME_RUN" ]]; then
    if [[ "$TIME_RUN" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        TIME_ANC="$TIME_RUN"
        TIME_ALG="$TIME_RUN 00:00"
    elif [[ "$TIME_RUN" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}[[:space:]][0-9]{2}:[0-9]{2}$ ]]; then
        TIME_ALG="$TIME_RUN"
        TIME_ANC="${TIME_RUN:0:10}"
    else
        echo " ===> ERROR: invalid --date format: $TIME_RUN" >&2
        exit 1
    fi
else
    if [[ -n "$TZ_LOCAL" ]]; then
        TIME_ALG="$(TZ="$TZ_LOCAL" date '+%F %H:%M')"
    else
        TIME_ALG="$(date '+%F %H:%M')"
    fi
    TIME_ANC="${TIME_ALG:0:10}"
fi

if [[ -n "$TZ_LOCAL" ]]; then
    TZ="$TZ_LOCAL" date -d "$TIME_ALG" '+%F %H:%M' >/dev/null 2>&1 || { echo " ===> ERROR: invalid date/time: $TIME_ALG" >&2; exit 1; }
    TZ="$TZ_LOCAL" date -d "$TIME_ANC" '+%F' >/dev/null 2>&1 || { echo " ===> ERROR: invalid date: $TIME_ANC" >&2; exit 1; }
else
    date -d "$TIME_ALG" '+%F %H:%M' >/dev/null 2>&1 || { echo " ===> ERROR: invalid date/time: $TIME_ALG" >&2; exit 1; }
    date -d "$TIME_ANC" '+%F' >/dev/null 2>&1 || { echo " ===> ERROR: invalid date: $TIME_ANC" >&2; exit 1; }
fi

if $ROUND_HOUR; then
    if [[ -n "$TZ_LOCAL" ]]; then
        TIME_ALG="$(TZ="$TZ_LOCAL" date -d "$TIME_ALG" '+%F %H:00')"
    else
        TIME_ALG="$(date -d "$TIME_ALG" '+%F %H:00')"
    fi
fi

TIME_RUN="${TIME_RUN:-$TIME_ALG}"
SELECTED_TIME_RUN="$TIME_RUN"
if [[ -n "$TZ_LOCAL" ]]; then
    TIME_RUN_TAG="$(TZ="$TZ_LOCAL" date -d "$TIME_RUN" '+%Y%m%d_%H%M')"
else
    TIME_RUN_TAG="$(date -d "$TIME_RUN" '+%Y%m%d_%H%M')"
fi
TIME_MACHINE_TAG="$(date +'%Y%m%d_%H%M%S')"
LOG_FILE="${LOG_FILE:-$LOG_FOLDER/${LOG_BASENAME}_${TIME_RUN_TAG}_${TIME_MACHINE_TAG}.log}"
LOCK_FILE="${LOCK_FILE:-$LOCK_FOLDER/${LOCK_BASENAME}_${TIME_RUN_TAG}.lock}"

mkdir -p "$(dirname "$LOG_FILE")"
if command -v tee >/dev/null 2>&1; then
    exec > >(tee -a "$LOG_FILE") 2>&1
else
    exec >>"$LOG_FILE" 2>&1
fi

echo " ==================================================================================="
echo " ==> $SCRIPT_NAME (Version: $SCRIPT_VERSION  Release_Date: $SCRIPT_DATE)"
echo " ==> START ..."
echo " ==> MODE: $SCRIPT_MODE | TIME_SYSTEM: $(ts) | TIME_RUN: $TIME_RUN | TIME_ALG: $TIME_ALG | TIME_ANC: $TIME_ANC | SKIP_ALGORITHM=$SKIP_ALGORITHM | SKIP_ANCILLARY=$SKIP_ANCILLARY | TZ=${TZ_LOCAL:-system}"
echo " ==> LOGGING TO: $LOG_FILE"
echo " ==> LOCK FILE: $LOCK_FILE"

# Never unlink or bypass a live flock lock, even with --force.
mkdir -p "$(dirname "$LOCK_FILE")"
exec 200>"$LOCK_FILE" || { echo " ===> ERROR: cannot open lockfile $LOCK_FILE"; exit 9; }
if ! /usr/bin/flock -n 200; then
    echo " ===> ERROR: another execution is active for TIME_RUN=$TIME_RUN (lock: $LOCK_FILE)"
    echo " ===> --force does not bypass an active lock"
    exit 9
fi
echo " ===> Lock acquired: $LOCK_FILE"
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# # CONDA STEP
echo " ===> [CONDA] 1/8 Locate conda"

if [[ -x "$CONDA_HOME/bin/conda" ]]; then
    CONDA_BIN="$CONDA_HOME/bin/conda"
else
    CONDA_BIN="$(command -v conda || true)"
fi

if [[ -z "${CONDA_BIN:-}" ]]; then
    echo " ===> [CONDA][WARN] conda not found on system PATHs"
fi

echo " ===> [CONDA] 2/8 Candidate direct interpreters"

CANDIDATES=(
    "$CONDA_HOME/envs/$CONDA_ENV/bin/python"
    "$CONDA_HOME/bin/python"
    "$(command -v python || true)"
)

PY=""

for p in "${CANDIDATES[@]}"; do

    if [[ -n "$p" && -x "$p" ]]; then
        PY="$p"
        break
    fi

done

echo "       candidates: ${CANDIDATES[*]}"

if [[ -n "$PY" ]]; then

    echo " ===> [CONDA] Testing direct python: $PY"

    if "$PY" -V >/dev/null 2>&1; then

        echo " ===> [CONDA] Using direct interpreter (no activate): $PY"

        PYRUN=( "$PY" )

        "${PYRUN[@]}" -V 2>&1 | sed 's/^/       /'

    else

        echo " ===> [CONDA][WARN] direct python exists but not runnable"

        PY=""

    fi

fi

if [[ -z "$PY" && -n "${CONDA_BIN:-}" ]]; then

    echo " ===> [CONDA] 3/8 Try 'conda run' (no shell activation)"

    if "$CONDA_BIN" run -n "$CONDA_ENV" --no-capture-output python -V >/dev/null 2>&1; then

        echo " ===> [CONDA] Using: conda run -n $CONDA_ENV python"

        PYRUN=(
            "$CONDA_BIN"
            run
            -n "$CONDA_ENV"
            --no-capture-output
            python
        )

        "${PYRUN[@]}" -V 2>&1 | sed 's/^/       /'

    else

        echo " ===> [CONDA][WARN] conda run failed; trying activate with timeout"
        echo " ===> [CONDA] 4/8 Probe activation in a sandboxed shell (8s timeout)"

        if timeout 8s env \
            CONDA_HOME="$CONDA_HOME" \
            CONDA_ENV="$CONDA_ENV" \
            bash -lc \
            'eval "$("$CONDA_HOME/bin/conda" shell.bash hook)"; conda activate "$CONDA_ENV"; python -V' \
            >/dev/null 2>&1; then

            echo " ===> [CONDA] 5/8 Activate here (current shell)"

            eval "$("$CONDA_HOME/bin/conda" shell.bash hook)"

            conda activate "$CONDA_ENV" >/dev/null 2>&1

            echo " ===> [CONDA] Env activated: $CONDA_ENV"

            PYRUN=( python )

            "${PYRUN[@]}" -V 2>&1 | sed 's/^/       /'

        else

            echo " ===> [CONDA][ERROR] Activation hangs or fails under cron"
            echo "       Tried: direct python, conda run, and activation probe"

            exit 2

        fi

    fi

fi

# Final guard
if [[ -z "${PYRUN[*]:-}" ]]; then
    echo " ===> [CONDA][ERROR] No working python runner determined"
    exit 2
fi

echo " ===> [CONDA] 6/8 Python runner OK: ${PYRUN[*]}"
echo " ===> [CONDA] 7/8 sys.executable / paths sanity"

"${PYRUN[@]}" - <<'PYCHK' 2>&1 | sed 's/^/       /'
import sys
import os
import site

print("python:", sys.version.replace("\n", " "))
print("executable:", sys.executable)
print("PATH(head):", os.environ.get("PATH", "")[:200] + "...")

try:
    print("site-packages:", site.getsitepackages())
except Exception as e:
    print("site-packages: n/a", e)
PYCHK

echo " ===> [CONDA] 8/8 Done"
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# PYTHONPATH STEP
export PYTHONPATH="${PYTHONPATH:-}:$ANC_FOLDER:$ALG_FOLDER"

echo " ===> PYTHONPATH FOLDERS:"

echo "$PYTHONPATH" \
    | tr ':' '\n' \
    | grep -v '^$' \
    | awk '{print " :::: [" NR "] " $0}'
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Ensure required Python entrypoints exist
for p in "$ANC_SCRIPT" "$ALG_SCRIPT"; do

    if [[ ! -f "$p" ]]; then
        echo " ===> ERROR: required script not found: $p" >&2
        exit 2
    fi

done
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# ANCILLARY STEP
# Explicit --date: exact hour only; never search or fall back.
# Automatic mode: preserve existing --max-time lookup.
#
echo " ===> RUN ANCILLARY SCRIPT [TIME_REF] ..."

TIME_DATE="$TIME_ANC"
if [[ -n "$TZ_LOCAL" ]]; then
    ANC_SETTINGS_FOLDER="$(TZ="$TZ_LOCAL" date -d "$TIME_DATE" +"$ANC_SETTINGS_FOLDER_TEMPLATE")"
else
    ANC_SETTINGS_FOLDER="$(date -d "$TIME_DATE" +"$ANC_SETTINGS_FOLDER_TEMPLATE")"
fi

echo " :::: REQUESTED TIME : $TIME_ALG"
echo " :::: ANCILLARY FOLDER: $ANC_SETTINGS_FOLDER"

if [[ "$EXPLICIT_RUN" == true ]]; then
    # The caller selected an exact cycle. No --max-time and no earlier-run fallback.
    TIME_REF="$TIME_ALG"
    TIME_REF_TAG="$(date -d "$TIME_REF" '+%Y%m%d_%H%M')"
    FOLDER_REF="$ANC_SETTINGS_FOLDER"
    FILE_REF="$FOLDER_REF/info_run_${TIME_REF_TAG}.json"

    echo " :::: EXACT RUN REQUIRED: $TIME_REF"
    echo " :::: EXACT RUN FILE:     $FILE_REF"

    if [[ ! -s "$FILE_REF" ]]; then
        echo " ===> ERROR: exact requested run is unavailable: $FILE_REF" >&2
        exit 3
    fi
    if ! command -v jq >/dev/null 2>&1; then
        echo " ===> ERROR: jq is required for exact-run validation" >&2
        exit 3
    fi
    if ! jq -e --arg run "$(date -d "$TIME_REF" '+%H%M')" \
                  --arg run_date "$(date -d "$TIME_REF" '+%Y-%m-%d')" '
        .run == $run and .run_date == $run_date and
        .me.found == true and
        (.me.data.DateIni | type == "string" and length > 0) and
        (.me.data.DateEnd | type == "string" and length > 0)
    ' "$FILE_REF" >/dev/null; then
        echo " ===> ERROR: exact-run JSON invalid/incomplete: $FILE_REF" >&2
        exit 3
    fi
    echo " ===> Exact requested run validated: $TIME_REF"

elif [[ "$SKIP_ANCILLARY" != true ]]; then
    # Automatic mode only: select the latest available cycle up to current hour.
    if [[ -n "$TZ_LOCAL" ]]; then
        TIME_MAX="$(TZ="$TZ_LOCAL" date -d "$TIME_ALG" '+%H:%M')"
    else
        TIME_MAX="$(date -d "$TIME_ALG" '+%H:%M')"
    fi
    CMD_ANC_DEF=(
        "${PYRUN[@]}" "$ANC_SCRIPT"
        --date "$TIME_DATE"
        --folder-template "$ANC_SETTINGS_FOLDER"
        --max-time "$TIME_MAX"
        --export
    )
    echo " :::: ANC CMD ${CMD_ANC_DEF[*]}"
    if ! CMD_ANC_EXPORT="$("${CMD_ANC_DEF[@]}")"; then
        echo " ===> ERROR: ancillary tool failed" >&2
        exit 3
    fi
    if [[ -z "$CMD_ANC_EXPORT" ]]; then
        echo " ===> ERROR: ancillary tool produced no output" >&2
        exit 3
    fi
    echo " :::: ANC EXPORT ${CMD_ANC_EXPORT}"
    eval "$CMD_ANC_EXPORT"
    TIME_RUN="$SELECTED_TIME_RUN"
    if [[ -z "${TIME_REF:-}" || "$TIME_REF" == NOT_FOUND ]]; then
        echo " ===> WARNING: no ancillary run found at or before $TIME_MAX"
        SKIP_ALGORITHM=true
    fi
else
    TIME_REF="$TIME_ALG"
    echo " ===> RUN ANCILLARY SCRIPT [TIME_REF] ... SKIPPED (BY FLAG)"
fi

echo " :::: SUMMARY_INFO_FOLDER_REF: ${FOLDER_REF:-<unset>}"
echo " :::: SUMMARY_INFO_FILE_REF:   ${FILE_REF:-<unset>}"
echo " :::: SUMMARY_INFO_TIME_REF:   ${TIME_REF:-<unset>}"
echo " :::: SUMMARY_INFO_TIME_RUN:   ${TIME_RUN:-<unset>}"
echo " ===> RUN ANCILLARY SCRIPT [TIME_REF] ... DONE"
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# ALGORITHM STEP
echo " ===> RUN ALGORITHM ..."

CMD_ALG_DEF=(
    "${PYRUN[@]}"
    "$ALG_SCRIPT"
    -settings_file "$ALG_SETTINGS"
    -time "${TIME_REF:-}"
)

echo " :::: ALG CMD ${CMD_ALG_DEF[*]}"

# Check TIME_REF validity
if [[ -z "${TIME_REF:-}" || "$TIME_REF" == "NOT_FOUND" ]]; then

    echo " ===> RUN ALGORITHM ... SKIPPED (invalid TIME_REF: ${TIME_REF:-<unset>})"

else

    if $SKIP_ALGORITHM; then

        echo " ===> RUN ALGORITHM ... SKIPPED (user flag --skip-algorithm)"

    else

        "${CMD_ALG_DEF[@]}"

        echo " ===> RUN ALGORITHM ... DONE"

    fi

fi
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Algorithm headers - end
echo " ==> $SCRIPT_NAME (Version: $SCRIPT_VERSION  Release_Date: $SCRIPT_DATE)"
echo " ==> ... END"
echo " ==> Bye, Bye"
echo " ==================================================================================="
# -----------------------------------------------------------------------------------------
