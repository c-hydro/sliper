
#!/usr/bin/env bash
set -Eeuo pipefail

# -----------------------------------------------------------------------------------------
# SLIPER - APP DATA PROCESSING RAIN - FRC REALTIME
# Version: 1.3.3
# -----------------------------------------------------------------------------------------

export HOME=/home/admin
export SHELL=/bin/bash
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
umask 022

SCRIPT_NAME="SLIPER - APP DATA PROCESSING RAIN - FRC REALTIME"
SCRIPT_VERSION="1.3.3"

SCRIPT_MODE="now"
TIME_RUN=""
TIME_ALG=""
TIME_ANC=""
TIME_REF=""
SKIP_ALGORITHM=false
SKIP_ANCILLARY=false
ROUND_HOUR=true
FORCE_RUN=false
TZ_LOCAL=""

CONDA_HOME="/home/admin/library/env_conda_sliper_data"
CONDA_ENV="sliper_data_libraries"

ANC_FOLDER="/home/admin/library/package_sliper/time"
ANC_SCRIPT="$ANC_FOLDER/sliper_app_time_reference.py"
ANC_SETTINGS_FOLDER_TEMPLATE="/home/admin/soilslips-ws/data_dynamic/source/run/%Y/%m/%d"

ALG_FOLDER="/home/admin/library/package_sliper/data/rain"
ALG_SCRIPT="$ALG_FOLDER/sliper_app_data_processing_rain_main.py"
ALG_SETTINGS="/home/admin/soilslips-exec/data/sliper_app_data_processing_rain_frc_realtime.json"

LOG_FOLDER="/home/admin/log/runner_data"
LOCK_FOLDER="/home/admin/lock/runner_data"
LOG_BASENAME="bash_data_preprocessing_rain_frc_realtime"
LOCK_BASENAME="bash_data_preprocessing_rain_frc_realtime"

usage() {
    echo "Usage: $0 [--mode now|history] [--date 'YYYY-MM-DD HH:MM'] [--skip-algorithm] [--skip-ancillary] [--tz ZONE] [--force]"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode) SCRIPT_MODE="${2:?Missing mode}"; shift 2 ;;
        --date) TIME_RUN="${2:?Missing date}"; shift 2 ;;
        --skip-algorithm) SKIP_ALGORITHM=true; shift ;;
        --skip-ancillary) SKIP_ANCILLARY=true; shift ;;
        --round-hour) ROUND_HOUR=true; shift ;;
        --tz) TZ_LOCAL="${2:?Missing timezone}"; shift 2 ;;
        --force|-f) FORCE_RUN=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "ERROR: Unknown option $1"; usage; exit 1 ;;
    esac
done

[[ "$SCRIPT_MODE" == "now" || "$SCRIPT_MODE" == "history" ]] || {
    echo "ERROR: Invalid mode"
    exit 1
}

EXPLICIT_RUN=false
[[ -n "$TIME_RUN" ]] && EXPLICIT_RUN=true

if [[ "$SCRIPT_MODE" == "history" && -z "$TIME_RUN" ]]; then
    echo "ERROR: --date is required in history mode"
    exit 1
fi

if [[ -n "$TZ_LOCAL" ]]; then
    export TZ="$TZ_LOCAL"
fi

if [[ -z "$TIME_RUN" ]]; then
    TIME_ALG="$(date '+%Y-%m-%d %H:%M')"
elif [[ "$TIME_RUN" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    TIME_ALG="$TIME_RUN 00:00"
elif [[ "$TIME_RUN" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}[[:space:]][0-9]{2}:[0-9]{2}$ ]]; then
    TIME_ALG="$TIME_RUN"
else
    echo "ERROR: Invalid --date format: $TIME_RUN"
    exit 1
fi

date -d "$TIME_ALG" '+%Y-%m-%d %H:%M' >/dev/null || exit 1

if $ROUND_HOUR; then
    TIME_ALG="$(date -d "$TIME_ALG" '+%Y-%m-%d %H:00')"
fi

TIME_ANC="${TIME_ALG:0:10}"
TIME_RUN="${TIME_RUN:-$TIME_ALG}"

TIME_RUN_TAG="$(date -d "$TIME_ALG" '+%Y%m%d_%H%M')"
TIME_MACHINE_TAG="$(date '+%Y%m%d_%H%M%S')"

mkdir -p "$LOG_FOLDER" "$LOCK_FOLDER"

LOG_FILE="$LOG_FOLDER/${LOG_BASENAME}_${TIME_RUN_TAG}_${TIME_MACHINE_TAG}.log"
LOCK_FILE="$LOCK_FOLDER/${LOCK_BASENAME}_${TIME_RUN_TAG}.lock"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "================================================================================"
echo " ==> $SCRIPT_NAME - v$SCRIPT_VERSION"
echo " ==> START"
echo " ==> MODE: $SCRIPT_MODE"
echo " ==> TIME_RUN: $TIME_RUN"
echo " ==> TIME_ALG: $TIME_ALG"
echo " ==> TIME_ANC: $TIME_ANC"
echo " ==> LOG: $LOG_FILE"
echo "================================================================================"

# -----------------------------------------------------------------------------------------
# Python environment
# -----------------------------------------------------------------------------------------

PY=""
PYRUN=()

for CANDIDATE in \
    "$CONDA_HOME/envs/$CONDA_ENV/bin/python" \
    "$CONDA_HOME/bin/python"; do

    if [[ -x "$CANDIDATE" ]] && "$CANDIDATE" -V >/dev/null 2>&1; then
        PY="$CANDIDATE"
        break
    fi
done

if [[ -n "$PY" ]]; then
    PYRUN=("$PY")
else
    CONDA_BIN="$CONDA_HOME/bin/conda"

    if [[ ! -x "$CONDA_BIN" ]]; then
        CONDA_BIN="$(command -v conda || true)"
    fi

    if [[ -z "$CONDA_BIN" ]]; then
        echo "ERROR: Conda and Python interpreter unavailable"
        exit 2
    fi

    PYRUN=("$CONDA_BIN" run -n "$CONDA_ENV" --no-capture-output python)

    if ! "${PYRUN[@]}" -V; then
        echo "ERROR: Cannot execute Python in Conda environment"
        exit 2
    fi
fi

export PYTHONPATH="${PYTHONPATH:-}:$ANC_FOLDER:$ALG_FOLDER"

echo " ==> PYTHON: ${PYRUN[*]}"
echo " ==> PYTHONPATH: $PYTHONPATH"

for REQUIRED in "$ANC_SCRIPT" "$ALG_SCRIPT" "$ALG_SETTINGS"; do
    if [[ ! -f "$REQUIRED" ]]; then
        echo "ERROR: Required file missing: $REQUIRED"
        exit 2
    fi
done

# -----------------------------------------------------------------------------------------
# Ancillary / exact cycle selection
# -----------------------------------------------------------------------------------------

echo "--------------------------------------------------------------------------------"
echo " ==> RUN ANCILLARY"
echo "--------------------------------------------------------------------------------"

ANC_SETTINGS_FOLDER="$(date -d "$TIME_ANC" +"$ANC_SETTINGS_FOLDER_TEMPLATE")"

if [[ "$SKIP_ANCILLARY" == false ]]; then

    if [[ "$EXPLICIT_RUN" == true ]]; then

        TIME_REF="$TIME_ALG"
        TIME_REF_TAG="$(date -d "$TIME_REF" '+%Y%m%d_%H%M')"
        FOLDER_REF="$ANC_SETTINGS_FOLDER"
        FILE_REF="$FOLDER_REF/info_run_${TIME_REF_TAG}.json"

        echo " ==> FIXED TIME_REF: $TIME_REF"
        echo " ==> FIXED FILE_REF: $FILE_REF"

        if [[ ! -s "$FILE_REF" ]]; then
            echo "ERROR: Requested run information missing: $FILE_REF"
            exit 3
        fi

        if ! command -v jq >/dev/null 2>&1; then
            echo "ERROR: jq is required"
            exit 3
        fi

        if ! jq -e \
            --arg run "$(date -d "$TIME_REF" '+%H%M')" \
            --arg run_date "$(date -d "$TIME_REF" '+%Y-%m-%d')" \
            '
            .run == $run and
            .run_date == $run_date and
            (.me.found == true) and
            (.me.data.DateIni | type == "string" and length > 0) and
            (.me.data.DateEnd | type == "string" and length > 0)
            ' "$FILE_REF" >/dev/null; then

            echo "ERROR: Invalid or incomplete run JSON: $FILE_REF"
            exit 3
        fi

    else

        CMD_ANC=(
            "${PYRUN[@]}"
            "$ANC_SCRIPT"
            --date "$TIME_ANC"
            --folder-template "$ANC_SETTINGS_FOLDER"
            --export
        )

        if ! ANC_EXPORT="$("${CMD_ANC[@]}")"; then
            echo "ERROR: Ancillary execution failed"
            exit 3
        fi

        if [[ -z "$ANC_EXPORT" ]]; then
            echo "ERROR: Ancillary returned empty output"
            exit 3
        fi

        TIME_RUN_SELECTED="$TIME_RUN"
        eval "$ANC_EXPORT"
        TIME_RUN="$TIME_RUN_SELECTED"

        if [[ "${TIME_REF:-}" == "NOT_FOUND" ]]; then
            echo "WARNING: No run available"
            SKIP_ALGORITHM=true
        fi
    fi
else
    TIME_REF="$TIME_ALG"
    echo " ==> Ancillary skipped"
fi

echo " ==> TIME_REF: ${TIME_REF:-NOT_FOUND}"
echo " ==> FILE_REF: ${FILE_REF:-NOT_FOUND}"

# -----------------------------------------------------------------------------------------
# Application execution lock
#
# IMPORTANT:
#   - Acquired only when the algorithm is ready to run
#   - Bash parent retains the lock
#   - Python child does NOT inherit the lock descriptor
#   - No forced bypass
# -----------------------------------------------------------------------------------------

APP_LOCK_FD=""

acquire_application_lock() {

    if ! command -v flock >/dev/null 2>&1; then
        echo "ERROR: flock unavailable"
        return 9
    fi

    if ! exec {APP_LOCK_FD}>"$LOCK_FILE"; then
        echo "ERROR: Cannot open application lock: $LOCK_FILE"
        return 9
    fi

    if ! flock -n "$APP_LOCK_FD"; then
        echo "ERROR: Another application execution is running"
        echo "LOCK: $LOCK_FILE"

        if command -v fuser >/dev/null 2>&1; then
            fuser -v "$LOCK_FILE" 2>&1 || true
        fi

        exec {APP_LOCK_FD}>&-
        APP_LOCK_FD=""
        return 9
    fi

    echo " ==> Application lock acquired: $LOCK_FILE"
}

release_application_lock() {

    if [[ -n "${APP_LOCK_FD:-}" ]]; then
        exec {APP_LOCK_FD}>&-
        APP_LOCK_FD=""
        echo " ==> Application lock released"
    fi
}

# -----------------------------------------------------------------------------------------
# Algorithm
# -----------------------------------------------------------------------------------------

echo "--------------------------------------------------------------------------------"
echo " ==> RUN ALGORITHM"
echo "--------------------------------------------------------------------------------"

if [[ -z "$TIME_REF" || "$TIME_REF" == "NOT_FOUND" ]]; then
    echo " ==> Algorithm skipped: invalid TIME_REF"

elif [[ "$SKIP_ALGORITHM" == true ]]; then
    echo " ==> Algorithm skipped by flag"

else
    CMD_ALG=(
        "${PYRUN[@]}"
        "$ALG_SCRIPT"
        -settings_file "$ALG_SETTINGS"
        -time "$TIME_REF"
    )

    printf ' ==> COMMAND: '
    printf '%q ' "${CMD_ALG[@]}"
    echo

    if ! acquire_application_lock; then
        exit 9
    fi

    # Keep the lock in this Bash process, but close it in the child.
    if "${CMD_ALG[@]}" {APP_LOCK_FD}>&-; then
        ALG_RC=0
    else
        ALG_RC=$?
    fi

    release_application_lock

    if (( ALG_RC != 0 )); then
        echo "ERROR: Algorithm failed with exit code $ALG_RC"
        exit "$ALG_RC"
    fi

    echo " ==> ALGORITHM DONE"
fi

echo "================================================================================"
echo " ==> $SCRIPT_NAME"
echo " ==> END - SUCCESS"
echo "================================================================================"

exit 0

