
#!/usr/bin/env bash
set -Eeuo pipefail

# -----------------------------------------------------------------------------------------
# SLIPER - WRAP DATA PROCESSING RAIN - FRC REALTIME
# Version: 1.3.3
# -----------------------------------------------------------------------------------------

ROOT="/home/admin/soilslips-exec/data"
SCRIPT_FILE="$ROOT/sliper_app_data_processing_rain_frc_realtime.sh"

NAME_RUN="wrap_data_rain_frc"

LOG_FOLDER="/home/admin/log/runner_data"
WRAP_FOLDER="/home/admin/info/runner_data"
LOCK_FOLDER="/home/admin/lock/runner_data"

INPUT_SOURCE="/home/admin/soilslips-ws/data_dynamic/source/data/rain/frc/perc80"
INPUT_SUBFOLDER_FORMAT="%Y/%m/%d/%H%M"
INPUT_FILENAME_FORMAT="Rain_%Y%m%d%H00.tif"

INPUT_DAYS_BEFORE=2
INPUT_DAYS_AFTER=2

OUTPUT_ROOT="/home/admin/soilslips-ws/data_dynamic/destination/data/rain/frc"
OUTPUT_SUBFOLDER_FORMAT="%Y/%m/%d/%H00"

RUN_TIME=""
FORCE=false

# -----------------------------------------------------------------------------------------
# Arguments
# -----------------------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --run-time)
            RUN_TIME="${2:?Missing run time}"
            shift 2
            ;;
        --input-days-before)
            INPUT_DAYS_BEFORE="${2:?Missing days before}"
            shift 2
            ;;
        --input-days-after)
            INPUT_DAYS_AFTER="${2:?Missing days after}"
            shift 2
            ;;
        -f|--force)
            FORCE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--run-time HH:MM] [--input-days-before N] [--input-days-after N] [-f|--force]"
            exit 0
            ;;
        *)
            echo "ERROR: Unknown argument: $1"
            exit 1
            ;;
    esac
done

for N in "$INPUT_DAYS_BEFORE" "$INPUT_DAYS_AFTER"; do
    [[ "$N" =~ ^[0-9]+$ ]] || {
        echo "ERROR: Input days must be non-negative integers"
        exit 1
    }
done

if [[ -n "$RUN_TIME" ]]; then
    [[ "$RUN_TIME" =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]] || {
        echo "ERROR: Invalid --run-time: $RUN_TIME"
        exit 1
    }

    TIME_RUN="$(date '+%Y-%m-%d') $RUN_TIME:00"
else
    TIME_RUN="$(date '+%Y-%m-%d %H:%M:%S')"
fi

TIME_SCRIPT="$(date -d "$TIME_RUN" '+%Y-%m-%d %H:%M')"
TIME_RUN_TAG="$(date -d "$TIME_RUN" '+%Y%m%d%H%M')"
TIME_MACHINE_TAG="$(date '+%H%M%S')"

mkdir -p "$LOG_FOLDER" "$WRAP_FOLDER" "$LOCK_FOLDER"

LOGFILE="$LOG_FOLDER/${NAME_RUN}_${TIME_RUN_TAG}_${TIME_MACHINE_TAG}.log"
WRAPFILE="$WRAP_FOLDER/${NAME_RUN}_${TIME_RUN_TAG}.info"
LOCKFILE="$LOCK_FOLDER/${NAME_RUN}_${TIME_RUN_TAG}.lock"

INPUT_DATE="$(date -d "$TIME_RUN" '+%Y-%m-%d')"
INPUT_TIME_START="$(date -d "$INPUT_DATE - $INPUT_DAYS_BEFORE days" '+%Y-%m-%d 00:00:00')"
INPUT_TIME_END="$(date -d "$INPUT_DATE + $INPUT_DAYS_AFTER days" '+%Y-%m-%d 00:00:00')"

INPUT_SUBFOLDER="$(date -d "$TIME_RUN" "+$INPUT_SUBFOLDER_FORMAT")"
INPUT_FOLDER="${INPUT_SOURCE%/}/$INPUT_SUBFOLDER"

OUTPUT_SUBFOLDER="$(date -d "$TIME_RUN" "+$OUTPUT_SUBFOLDER_FORMAT")"
OUTPUT_FOLDER="${OUTPUT_ROOT%/}/$OUTPUT_SUBFOLDER"

[[ -x "$SCRIPT_FILE" ]] || {
    echo "ERROR: Application not executable: $SCRIPT_FILE"
    exit 1
}

# -----------------------------------------------------------------------------------------
# Wrapper lock
# -----------------------------------------------------------------------------------------

if ! command -v flock >/dev/null 2>&1; then
    echo "ERROR: flock unavailable"
    exit 1
fi

exec 200>"$LOCKFILE"

if ! flock -n 200; then
    echo "WARNING: Another wrapper execution is running"
    echo "Lock: $LOCKFILE"
    exit 9
fi

# -----------------------------------------------------------------------------------------
# Previous execution
# -----------------------------------------------------------------------------------------

if [[ -f "$WRAPFILE" ]]; then

    PREVIOUS_EXIT_CODE="$(sed -n 's/^exit_code=//p' "$WRAPFILE" | tail -n 1)"

    echo "--------------------------------------------------------------------------------"
    echo "INFO: Checking previous execution ..."
    echo "INFO: Wrap file: $WRAPFILE"
    echo "INFO: Previous exit: ${PREVIOUS_EXIT_CODE:-unknown}"

    if [[ "$PREVIOUS_EXIT_CODE" == "0" ]]; then

        if [[ -d "$OUTPUT_FOLDER" ]] &&
           [[ -n "$(find "$OUTPUT_FOLDER" -type f -print -quit)" ]]; then

            if [[ "$FORCE" == false ]]; then
                echo "INFO: Previous execution and output verified. Skipping."
                exit 0
            fi
        fi
    fi

    echo "INFO: Running processing again."
fi

# -----------------------------------------------------------------------------------------
# Command
# -----------------------------------------------------------------------------------------

CMD=(
    "$SCRIPT_FILE"
    --mode now
    --date "$TIME_SCRIPT"
)

echo "--------------------------------------------------------------------------------"
echo "Run configuration"
echo "Name run    : $NAME_RUN"
echo "Time run    : $TIME_RUN"
echo "Time script : $TIME_SCRIPT"
echo "Force run   : $FORCE"
echo "Log file    : $LOGFILE"
echo "Wrap file   : $WRAPFILE"
echo "Lock file   : $LOCKFILE"
echo "Script      : $SCRIPT_FILE"
echo "Input dir   : $INPUT_FOLDER"
echo "Input from  : $INPUT_TIME_START"
echo "Input to    : $INPUT_TIME_END"
echo "Output dir  : $OUTPUT_FOLDER"
echo "--------------------------------------------------------------------------------"

TIME_START="$(date '+%Y-%m-%d %H:%M:%S')"

# -----------------------------------------------------------------------------------------
# Initialize .info
# -----------------------------------------------------------------------------------------

cat > "$WRAPFILE" <<EOF
name_run=$NAME_RUN
time_start=$TIME_START
time_end=
time_run=$TIME_RUN
exit_code=1
status=running
EOF

# -----------------------------------------------------------------------------------------
# Execution and input validation
# -----------------------------------------------------------------------------------------

INPUT_FILES_FOUND=0
INPUT_FILES_EXPECTED=0
INPUT_FILES_MISSING=0
EXIT_CODE=1

# All output is visible in terminal and written once to log.
exec > >(tee -a "$LOGFILE") 2>&1

echo "--------------------------------------------------------------------------------"
echo "[$TIME_START] Starting script ..."
echo "--------------------------------------------------------------------------------"

INPUT_START_EPOCH="$(date -u -d "$INPUT_TIME_START UTC" '+%s')"
INPUT_END_EPOCH="$(date -u -d "$INPUT_TIME_END UTC" '+%s')"

echo "Input file validation"
echo "Input folder : $INPUT_FOLDER"
echo "Time window  : $INPUT_TIME_START -> $INPUT_TIME_END (inclusive)"
echo "File pattern : $INPUT_FILENAME_FORMAT"
echo "--------------------------------------------------------------------------------"

for ((INPUT_EPOCH=INPUT_START_EPOCH;
      INPUT_EPOCH<=INPUT_END_EPOCH;
      INPUT_EPOCH+=3600)); do

    INPUT_FILES_EXPECTED=$((INPUT_FILES_EXPECTED + 1))

    INPUT_FILENAME="$(date -u -d "@$INPUT_EPOCH" "+$INPUT_FILENAME_FORMAT")"
    INPUT_FILE="$INPUT_FOLDER/$INPUT_FILENAME"

    if [[ -f "$INPUT_FILE" ]]; then
        INPUT_FILES_FOUND=$((INPUT_FILES_FOUND + 1))
        echo "[OK]      $INPUT_FILENAME"
    else
        INPUT_FILES_MISSING=$((INPUT_FILES_MISSING + 1))
        echo "[MISSING] $INPUT_FILENAME"
    fi
done

echo "--------------------------------------------------------------------------------"
echo "Input check summary: expected=$INPUT_FILES_EXPECTED available=$INPUT_FILES_FOUND missing=$INPUT_FILES_MISSING"
echo "--------------------------------------------------------------------------------"

# -----------------------------------------------------------------------------------------
# Run application
# -----------------------------------------------------------------------------------------

if (( INPUT_FILES_FOUND == 0 )); then
    EXIT_CODE=6
    echo "ERROR: No rainfall input available"

elif (( INPUT_FILES_MISSING > 0 )); then
    EXIT_CODE=3
    echo "ERROR: Some rainfall input files are missing"

else
    echo "INFO: Launching application"
    echo "INFO: Wrapper lock descriptor 200 will be closed in child"

    # Wrapper lock stays in parent Bash.
    # Child application receives no wrapper lock descriptor.
    if "${CMD[@]}" 200>&-; then
        EXIT_CODE=0
    else
        EXIT_CODE=$?
        echo "ERROR: Application exited with code $EXIT_CODE"
    fi
fi

# -----------------------------------------------------------------------------------------
# Validate output
# -----------------------------------------------------------------------------------------

if (( EXIT_CODE == 0 )); then

    if [[ ! -d "$OUTPUT_FOLDER" ]]; then
        echo "ERROR: Output folder missing: $OUTPUT_FOLDER"
        EXIT_CODE=7

    elif [[ -z "$(find "$OUTPUT_FOLDER" -type f -print -quit)" ]]; then
        echo "ERROR: Output folder is empty: $OUTPUT_FOLDER"
        EXIT_CODE=7

    else
        echo "INFO: Output files found in $OUTPUT_FOLDER"
    fi
fi

# -----------------------------------------------------------------------------------------
# Status
# -----------------------------------------------------------------------------------------

case "$EXIT_CODE" in
    0) RUN_STATUS="done" ;;
    3) RUN_STATUS="some_input_are_not_available" ;;
    6) RUN_STATUS="no_input_found" ;;
    7) RUN_STATUS="no_output_found" ;;
    9) RUN_STATUS="execution_locked" ;;
    *) RUN_STATUS="failed" ;;
esac

TIME_END="$(date '+%Y-%m-%d %H:%M:%S')"

WRAP_TMP="$(mktemp "${WRAPFILE}.tmp.XXXXXX")"

cat > "$WRAP_TMP" <<EOF
name_run=$NAME_RUN
time_start=$TIME_START
time_end=$TIME_END
time_run=$TIME_RUN
exit_code=$EXIT_CODE
status=$RUN_STATUS
input_folder=$INPUT_FOLDER
input_time_start=$INPUT_TIME_START
input_time_end=$INPUT_TIME_END
input_files_expected=$INPUT_FILES_EXPECTED
input_files_found=$INPUT_FILES_FOUND
input_files_missing=$INPUT_FILES_MISSING
EOF

mv -f "$WRAP_TMP" "$WRAPFILE"

echo "--------------------------------------------------------------------------------"
echo "Run completed"
echo "Time run   : $TIME_RUN"
echo "Time start : $TIME_START"
echo "Time end   : $TIME_END"
echo "Status     : $RUN_STATUS"
echo "Exit code  : $EXIT_CODE"
echo "Input files: $INPUT_FILES_FOUND / $INPUT_FILES_EXPECTED"
echo "Log file   : $LOGFILE"
echo "Wrap file  : $WRAPFILE"
echo "--------------------------------------------------------------------------------"

exit "$EXIT_CODE"

