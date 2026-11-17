
#!/usr/bin/env bash
set -Eeuo pipefail

# ----------------------------------------------------------------------------------------------------------------------
# Configuration

ROOT="/home/admin/soilslips-system/manager_run"

CONFIG_FOLDER="$ROOT"
CONFIG_FILENAME="sliper_tools_select_run.json"

SCRIPT_FOLDER="$ROOT"
SCRIPT_FILENAME="sliper_tools_select_run.sh"

NAME_RUN="select_run"

INPUT_ROOT="/mnt/RainPercentileModels/risultati/MergeScenarioRegridded/Perc80"
OUTPUT_ROOT="/home/admin/soilslips-ws/data_dynamic/source/run"

LOG_FOLDER="/home/admin/log/manager_run"
INFO_FOLDER="/home/admin/info/manager_run"
LOCK_FOLDER="/home/admin/lock/manager_run"

# ----------------------------------------------------------------------------------------------------------------------
# Arguments

usage() {
    echo "Usage: $0 [--run-time HH:MM] [-f|--force]"
    echo
    echo "Options:"
    echo "  --run-time HH:MM   Search for the specified model run"
    echo "  -f, --force        Force execution even if output is valid"
    echo "  -h, --help         Show this help"
    echo
    echo "Examples:"
    echo "  $0"
    echo "  $0 --run-time 04:00"
    echo "  $0 --run-time 15:00"
    echo "  $0 --run-time 04:00 --force"
}

RUN_TIME=""
FORCE=false

while (($#)); do
    case "$1" in
        --run-time)
            if (($# < 2)); then
                echo "ERROR: --run-time requires HH:MM" >&2
                exit 1
            fi
            RUN_TIME="$2"
            shift 2
            ;;
        -f|--force)
            FORCE=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "ERROR: Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if [[ -n "$RUN_TIME" &&
      ! "$RUN_TIME" =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]]; then
    echo "ERROR: Invalid --run-time '$RUN_TIME'" >&2
    exit 1
fi

# ----------------------------------------------------------------------------------------------------------------------
# Resolve run time

TIME_NOW="$(date '+%Y-%m-%d %H:%M:%S')"
RUN_DATE="$(date '+%Y-%m-%d')"

if [[ -n "$RUN_TIME" ]]; then
    TIME_RUN="$RUN_DATE $RUN_TIME:00"
else
    TIME_RUN="$TIME_NOW"
fi

TIME_RUN_TAG="$(date -d "$TIME_RUN" '+%Y%m%d%H%M')" || exit 1

RUN_DATE_PATH="$(date -d "$TIME_RUN" '+%Y/%m/%d')" || exit 1
RUN_DATE_ISO="$(date -d "$TIME_RUN" '+%Y-%m-%d')" || exit 1
RUN_DATE_TAG="$(date -d "$TIME_RUN" '+%Y%m%d')" || exit 1
RUN_HHMM="$(date -d "$TIME_RUN" '+%H%M')" || exit 1

# Actual check time, independent of the model run time
TIME_CHECK="$(date '+%Y-%m-%d %H:%M:%S')"
TIME_CHECK_TAG="$(date '+%H%M')"

# ----------------------------------------------------------------------------------------------------------------------
# Resolve files

INPUT_FOLDER="$INPUT_ROOT/$RUN_DATE_PATH/$RUN_HHMM"
OUTPUT_FOLDER="$OUTPUT_ROOT/$RUN_DATE_PATH"

INPUT_FILE_ME="$INPUT_FOLDER/ME_${TIME_RUN_TAG}.json"
INPUT_FILE_METEO="$INPUT_FOLDER/MeteoModelCheck_${TIME_RUN_TAG}.json"

OUTPUT_FILE="$OUTPUT_FOLDER/info_run_${RUN_DATE_TAG}_${RUN_HHMM}.json"

SCRIPT_FILE="$SCRIPT_FOLDER/$SCRIPT_FILENAME"
CONFIG_FILE="$CONFIG_FOLDER/$CONFIG_FILENAME"

# One log per check
LOGFILE="$LOG_FOLDER/${NAME_RUN}_${TIME_RUN_TAG}_${TIME_CHECK_TAG}.txt"

# One info file per model cycle
INFOFILE="$INFO_FOLDER/${NAME_RUN}_${TIME_RUN_TAG}.info"

# One lock per model cycle
LOCKFILE="$LOCK_FOLDER/sliper_tools_select_run_${TIME_RUN_TAG}.lock"
WRAPPER_LOCKFILE="$LOCK_FOLDER/sliper_tools_select_run_${TIME_RUN_TAG}.wrapper.lock"

# ----------------------------------------------------------------------------------------------------------------------
# Ensure folders

mkdir -p "$LOG_FOLDER" "$INFO_FOLDER" "$LOCK_FOLDER"

# ----------------------------------------------------------------------------------------------------------------------
# Validate dependencies

if [[ ! -x "$SCRIPT_FILE" ]]; then
    echo "ERROR: Script missing or not executable: $SCRIPT_FILE" >&2
    exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "ERROR: Configuration missing: $CONFIG_FILE" >&2
    exit 1
fi

if ! command -v flock >/dev/null 2>&1; then
    echo "ERROR: flock is required" >&2
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "ERROR: jq is required" >&2
    exit 1
fi

# ----------------------------------------------------------------------------------------------------------------------
# Acquire wrapper lock

exec 200>"$WRAPPER_LOCKFILE"

if ! flock -n 200; then
    echo "WARNING: Another execution is already running."
    echo "Wrapper lock: $WRAPPER_LOCKFILE"
    exit 1
fi

# ----------------------------------------------------------------------------------------------------------------------
# Initialize log
# Append rather than truncate, so checks within the same minute
# do not overwrite each other's logs.

exec > >(tee -a "$LOGFILE") 2>&1

# ----------------------------------------------------------------------------------------------------------------------
# Validation functions

INPUT_ME_AVAILABLE=false
INPUT_METEO_AVAILABLE=false
OUTPUT_AVAILABLE=false

check_input_files() {

    INPUT_ME_AVAILABLE=false
    INPUT_METEO_AVAILABLE=false

    if [[ -s "$INPUT_FILE_ME" ]] &&
       jq -e . "$INPUT_FILE_ME" >/dev/null 2>&1; then
        INPUT_ME_AVAILABLE=true
    fi

    if [[ -s "$INPUT_FILE_METEO" ]] &&
       jq -e . "$INPUT_FILE_METEO" >/dev/null 2>&1; then
        INPUT_METEO_AVAILABLE=true
    fi
}

check_output_file() {

    OUTPUT_AVAILABLE=false

    [[ -s "$OUTPUT_FILE" ]] || return 1

    if jq -e \
        --arg run "$RUN_HHMM" \
        --arg run_date "$RUN_DATE_ISO" \
        '
        .run == $run and
        .run_date == $run_date and
        (.me.found == true) and
        (.me.data.DateIni | type == "string" and length > 0) and
        (.me.data.DateEnd | type == "string" and length > 0)
        ' "$OUTPUT_FILE" >/dev/null 2>&1; then

        OUTPUT_AVAILABLE=true
        return 0
    fi

    return 1
}

# ----------------------------------------------------------------------------------------------------------------------
# Save info file atomically

save_info() {

    local tmp_file

    tmp_file="$(mktemp "${INFOFILE}.tmp.XXXXXX")" || return 1

    if ! cat > "$tmp_file" <<EOF
name_run=$NAME_RUN
time_start=$TIME_START
time_end=$TIME_END
time_run=$TIME_RUN
time_check=$TIME_CHECK
exit_code=$EXIT_CODE
status=$RUN_STATUS
force_run=$FORCE
input_folder=$INPUT_FOLDER
output_folder=$OUTPUT_FOLDER
input_file_me=$INPUT_FILE_ME
input_file_meteo=$INPUT_FILE_METEO
output_file=$OUTPUT_FILE
input_me_available=$INPUT_ME_AVAILABLE
input_meteo_available=$INPUT_METEO_AVAILABLE
output_available=$OUTPUT_AVAILABLE
config_file=$CONFIG_FILE
script_file=$SCRIPT_FILE
log_file=$LOGFILE
lock_file=$LOCKFILE
wrapper_lock_file=$WRAPPER_LOCKFILE
EOF
    then
        rm -f "$tmp_file"
        return 1
    fi

    if ! mv -f "$tmp_file" "$INFOFILE"; then
        rm -f "$tmp_file"
        return 1
    fi
}

# ----------------------------------------------------------------------------------------------------------------------
# Initialize execution

TIME_START="$(date '+%Y-%m-%d %H:%M:%S')"
TIME_END=""
EXIT_CODE=1
RUN_STATUS="running"

echo "--------------------------------------------------------------------------------"
echo "[SEARCH MODEL RUN] Checking model run"
echo "[SEARCH MODEL RUN] Model run       : $TIME_RUN"
echo "[SEARCH MODEL RUN] Check time      : $TIME_CHECK"
echo "[SEARCH MODEL RUN] Force           : $FORCE"
echo "[SEARCH MODEL RUN] Input folder    : $INPUT_FOLDER"
echo "[SEARCH MODEL RUN] Output file     : $OUTPUT_FILE"
echo "[SEARCH MODEL RUN] Log file        : $LOGFILE"
echo "[SEARCH MODEL RUN] Info file       : $INFOFILE"
echo "--------------------------------------------------------------------------------"

# ----------------------------------------------------------------------------------------------------------------------
# Check existing output before execution

check_output_file || true

if [[ "$OUTPUT_AVAILABLE" == true && "$FORCE" == false ]]; then

    RUN_STATUS="done"
    EXIT_CODE=0
    TIME_END="$(date '+%Y-%m-%d %H:%M:%S')"

    check_input_files
    save_info

    echo "--------------------------------------------------------------------------------"
    echo "[SEARCH MODEL RUN] Execution already completed. Skipping."
    echo "[OK OUTPUT] $OUTPUT_FILE"
    echo "[SEARCH MODEL RUN] Status    : $RUN_STATUS"
    echo "[SEARCH MODEL RUN] Info file : $INFOFILE"
    echo "[SEARCH MODEL RUN] Exit code : $EXIT_CODE"
    echo "--------------------------------------------------------------------------------"

    exit 0
fi

# ----------------------------------------------------------------------------------------------------------------------
# Validate input files

check_input_files

echo "[SEARCH MODEL RUN] Input ME        : $INPUT_ME_AVAILABLE"
echo "[SEARCH MODEL RUN] Input Meteo     : $INPUT_METEO_AVAILABLE"
echo "[SEARCH MODEL RUN] Output valid    : $OUTPUT_AVAILABLE"

if [[ "$INPUT_ME_AVAILABLE" != true ||
      "$INPUT_METEO_AVAILABLE" != true ]]; then

    EXIT_CODE=2
    RUN_STATUS="forecast_run_not_available"
    TIME_END="$(date '+%Y-%m-%d %H:%M:%S')"

    save_info

    echo "--------------------------------------------------------------------------------"
    echo "[SEARCH MODEL RUN] Required inputs unavailable."
    echo "[SEARCH MODEL RUN] ME file    : $INPUT_FILE_ME"
    echo "[SEARCH MODEL RUN] Meteo file : $INPUT_FILE_METEO"
    echo "[SEARCH MODEL RUN] Exit code  : $EXIT_CODE"
    echo "--------------------------------------------------------------------------------"

    exit "$EXIT_CODE"
fi

# ----------------------------------------------------------------------------------------------------------------------
# Build command

CMD=(
    "$SCRIPT_FILE"
    --mode now
    --config "$CONFIG_FILE"
    --lockfile "$LOCKFILE"
)

if [[ -n "$RUN_TIME" ]]; then
    CMD+=(--run-time "$RUN_TIME")
fi

# ----------------------------------------------------------------------------------------------------------------------
# Save running status

save_info

# ----------------------------------------------------------------------------------------------------------------------
# Print execution configuration

echo "--------------------------------------------------------------------------------"
echo "[SEARCH MODEL RUN] Starting run selection"
echo "[SEARCH MODEL RUN] Start time      : $TIME_START"
echo "[SEARCH MODEL RUN] Time run        : $TIME_RUN"
echo "[SEARCH MODEL RUN] Check time      : $TIME_CHECK"
echo "[SEARCH MODEL RUN] Force           : $FORCE"
echo "[SEARCH MODEL RUN] Input folder    : $INPUT_FOLDER"
echo "[SEARCH MODEL RUN] Output file     : $OUTPUT_FILE"
echo "[SEARCH MODEL RUN] ME available    : $INPUT_ME_AVAILABLE"
echo "[SEARCH MODEL RUN] Meteo available : $INPUT_METEO_AVAILABLE"
echo "[SEARCH MODEL RUN] Log            : $LOGFILE"
echo "[SEARCH MODEL RUN] Info           : $INFOFILE"
echo "[SEARCH MODEL RUN] Wrapper lock   : $WRAPPER_LOCKFILE"
echo "[SEARCH MODEL RUN] Child lock     : $LOCKFILE"

printf '[SEARCH MODEL RUN] Command        : '
printf '%q ' "${CMD[@]}"
echo

echo "--------------------------------------------------------------------------------"

# ----------------------------------------------------------------------------------------------------------------------
# Execute child script

if "${CMD[@]}"; then
    EXIT_CODE=0
else
    EXIT_CODE=$?
fi

# ----------------------------------------------------------------------------------------------------------------------
# Validate output after execution

check_output_file || true

if (( EXIT_CODE == 0 )); then

    if [[ "$OUTPUT_AVAILABLE" == true ]]; then
        RUN_STATUS="done"
    else
        EXIT_CODE=7
        RUN_STATUS="no_output_found"
    fi

else
    RUN_STATUS="failed"
fi

TIME_END="$(date '+%Y-%m-%d %H:%M:%S')"

# ----------------------------------------------------------------------------------------------------------------------
# Save final info

save_info

# ----------------------------------------------------------------------------------------------------------------------
# Summary

echo "--------------------------------------------------------------------------------"
echo "[SEARCH MODEL RUN] Execution summary"
echo "[SEARCH MODEL RUN] Name run        : $NAME_RUN"
echo "[SEARCH MODEL RUN] Time run        : $TIME_RUN"
echo "[SEARCH MODEL RUN] Check time      : $TIME_CHECK"
echo "[SEARCH MODEL RUN] Start time      : $TIME_START"
echo "[SEARCH MODEL RUN] End time        : $TIME_END"
echo "[SEARCH MODEL RUN] Status          : $RUN_STATUS"
echo "[SEARCH MODEL RUN] Input ME        : $INPUT_ME_AVAILABLE"
echo "[SEARCH MODEL RUN] Input Meteo     : $INPUT_METEO_AVAILABLE"
echo "[SEARCH MODEL RUN] Output valid    : $OUTPUT_AVAILABLE"
echo "[SEARCH MODEL RUN] Output file     : $OUTPUT_FILE"
echo "[SEARCH MODEL RUN] Info file       : $INFOFILE"
echo "[SEARCH MODEL RUN] Log file        : $LOGFILE"
echo "[SEARCH MODEL RUN] Exit code       : $EXIT_CODE"
echo "--------------------------------------------------------------------------------"

exit "$EXIT_CODE"

