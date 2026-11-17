#!/bin/bash

# -------------------------------------------------------------------------------------
# Configuration
ROOT="/home/admin/soilslips-system/manager_data"

CONFIG_FOLDER="$ROOT"
CONFIG_FILENAME="sliper_tools_sync_data_rain_frc.json"

SCRIPT_FOLDER="$ROOT"
SCRIPT_FILENAME="sliper_tools_sync_data.sh"

NAME_RUN="sync_data_rain_frc"

LOG_FOLDER="/home/admin/log/manager_data"
WRAP_FOLDER="/home/admin/info/manager_data"
LOCK_FOLDER="/home/admin/lock/manager_data"

# Single rainfall percentile source/destination (no domains)
INPUT_ROOT="/mnt/RainPercentileModels/risultati/MergeScenarioRegridded/Perc80"
OUTPUT_ROOT="/home/admin/soilslips-ws/data_dynamic/source/data/rain/frc/perc80"
FILE_PATTERN='Rain_????????????.tif'
INPUT_DAYS_BEFORE=0
INPUT_DAYS_AFTER=2
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Parse arguments
RUN_TIME=""
FORCE=false

while [[ $# -gt 0 ]]; do

    case "$1" in

        --run-time)
            if [[ -z "${2:-}" ]]; then
                echo "ERROR: --run-time requires a value, for example 04:00"
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
            echo "Usage: $0 [--run-time HH:MM] [-f|--force]"
            echo
            echo "Options:"
            echo "  --run-time HH:MM   Set execution time"
            echo "  -f, --force        Force execution even if already completed"
            echo
            echo "Examples:"
            echo "  $0"
            echo "  $0 --run-time 04:00"
            echo "  $0 --run-time 08:00"
            echo "  $0 --run-time 04:00 -f"
            exit 0
            ;;

        *)
            echo "ERROR: Unknown option: $1"
            echo "Usage: $0 [--run-time HH:MM] [-f|--force]"
            exit 1
            ;;

    esac

done
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Define function to ensure folders
ensure_folder() {

    local folder="$1"
    local label="$2"

    # Create folder if needed
    if [[ ! -d "$folder" ]]; then

        echo "Creating $label folder: $folder"

        if ! mkdir -p "$folder"; then
            echo "ERROR: Unable to create $label folder: $folder"
            exit 1
        fi

    fi

    # Check write permission
    if [[ ! -w "$folder" ]]; then
        echo "ERROR: $label folder is not writable: $folder"
        exit 1
    fi
}
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Ensure folders
ensure_folder "$LOG_FOLDER" "log"
ensure_folder "$WRAP_FOLDER" "info"
ensure_folder "$LOCK_FOLDER" "lock"
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Define run time
TIME_NOW=$(date '+%Y-%m-%d %H:%M:%S')

if [[ -z "$RUN_TIME" ]]; then

    # No run time provided: use current time
    TIME_RUN="$TIME_NOW"

else

    # Validate HH:MM
    if ! [[ "$RUN_TIME" =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]]; then
        echo "ERROR: Invalid run time '$RUN_TIME'"
        echo "Expected format: HH:MM"
        echo "Example: 04:00"
        exit 1
    fi

    # Use today's date with requested time
    TIME_RUN="$(date '+%Y-%m-%d') ${RUN_TIME}:00"

fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Create time tag for filenames
#
# Example:
# 2026-09-10 04:00:00
# ->
# 202609100400

if ! TIME_RUN_TAG=$(date -d "$TIME_RUN" '+%Y%m%d%H%M'); then
    echo "ERROR: Unable to create TIME_RUN_TAG from '$TIME_RUN'"
    exit 1
fi
# -------------------------------------------------------------------------------------

# Resolve rainfall source/destination for the selected run (no domains)
RUN_SUBFOLDER=$(date -d "$TIME_RUN" '+%Y/%m/%d/%H%M') || exit 1
INPUT_FOLDER="$INPUT_ROOT/$RUN_SUBFOLDER"
OUTPUT_FOLDER="$OUTPUT_ROOT/$RUN_SUBFOLDER"
# Both model-check reports are required for the exact selected forecast cycle.
MODEL_CHECK_FILE="$INPUT_FOLDER/MeteoModelCheck_${TIME_RUN_TAG}.json"
MODEL_ME_FILE="$INPUT_FOLDER/ME_${TIME_RUN_TAG}.json"
MODEL_CHECK_AVAILABLE=false
MODEL_ME_AVAILABLE=false
RUN_DATE=$(date -d "$TIME_RUN" '+%Y-%m-%d') || exit 1
WINDOW_START="$RUN_DATE 00:00:00"
WINDOW_END=$(date -d "$RUN_DATE + $INPUT_DAYS_AFTER days" '+%Y-%m-%d 00:00:00') || exit 1
WINDOW_START_EPOCH=$(date -d "$WINDOW_START" +%s) || exit 1
WINDOW_END_EPOCH=$(date -d "$WINDOW_END" +%s) || exit 1
EXPECTED_FILES=$(( (WINDOW_END_EPOCH - WINDOW_START_EPOCH) / 3600 + 1 ))
INPUT_FILES_FOUND=0
INPUT_FILES_MISSING=0
INPUT_FILES_INVALID=0
OUTPUT_FILES_FOUND=0
OUTPUT_FILES_MISSING=0

# Check each expected hourly rainfall file, without basin/domain subfolders.
# Compare source and destination sizes; the synchronizer's selection window
# must be configured separately to prevent extra files from being copied.
validate_data() {
    local verbose="${1:-false}"
    local epoch stamp name source target source_size target_size
    INPUT_FILES_FOUND=0
    INPUT_FILES_MISSING=0
    INPUT_FILES_INVALID=0
    OUTPUT_FILES_FOUND=0
    OUTPUT_FILES_MISSING=0

    [[ "$verbose" == true ]] && {
        echo "[RAIN CHECK] Source      : $INPUT_FOLDER"
        echo "[RAIN CHECK] Destination : $OUTPUT_FOLDER"
        echo "[RAIN CHECK] Window      : $WINDOW_START -> $WINDOW_END (inclusive)"
        echo "[RAIN CHECK] Expected    : $EXPECTED_FILES hourly files (no basins)"
    }

    for ((epoch=WINDOW_START_EPOCH; epoch<=WINDOW_END_EPOCH; epoch+=3600)); do
        stamp=$(date -d "@$epoch" '+%Y%m%d%H00') || return 1
        name="Rain_${stamp}.tif"
        source="$INPUT_FOLDER/$name"
        target="$OUTPUT_FOLDER/$name"

        if [[ ! -f "$source" ]]; then
            INPUT_FILES_MISSING=$((INPUT_FILES_MISSING + 1))
            [[ "$verbose" == true ]] && echo "[MISSING INPUT] $source"
            continue
        fi
        if [[ ! -s "$source" ]]; then
            INPUT_FILES_INVALID=$((INPUT_FILES_INVALID + 1))
            [[ "$verbose" == true ]] && echo "[INVALID INPUT] $source (empty)"
            continue
        fi
        INPUT_FILES_FOUND=$((INPUT_FILES_FOUND + 1))
        source_size=$(stat -c '%s' "$source") || return 1
        if [[ -s "$target" ]]; then
            target_size=$(stat -c '%s' "$target") || return 1
        else
            target_size=0
        fi
        if [[ "$source_size" -eq "$target_size" ]]; then
            OUTPUT_FILES_FOUND=$((OUTPUT_FILES_FOUND + 1))
            [[ "$verbose" == true ]] && echo "[OK OUTPUT] $name size=$target_size"
        else
            OUTPUT_FILES_MISSING=$((OUTPUT_FILES_MISSING + 1))
            [[ "$verbose" == true ]] && echo "[MISSING/MISMATCH OUTPUT] $name source_bytes=$source_size destination_bytes=$target_size"
        fi
    done

    [[ "$verbose" == true ]] && {
        echo "[RAIN SUMMARY] expected=$EXPECTED_FILES input_found=$INPUT_FILES_FOUND input_missing=$INPUT_FILES_MISSING input_invalid=$INPUT_FILES_INVALID output_ok=$OUTPUT_FILES_FOUND output_missing=$OUTPUT_FILES_MISSING"
    }
}
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Define files
LOG_FILENAME="${NAME_RUN}_${TIME_RUN_TAG}.log"
WRAP_FILENAME="${NAME_RUN}_${TIME_RUN_TAG}.info"
LOCK_FILENAME="${NAME_RUN}_${TIME_RUN_TAG}.lock"

LOGFILE="$LOG_FOLDER/$LOG_FILENAME"
WRAPFILE="$WRAP_FOLDER/$WRAP_FILENAME"
LOCKFILE="$LOCK_FOLDER/$LOCK_FILENAME"

# Separate wrapper lock from synchronization script lock
WRAPPER_LOCKFILE="$LOCK_FOLDER/${NAME_RUN}_${TIME_RUN_TAG}.wrapper.lock"

SCRIPT_FILE="$SCRIPT_FOLDER/$SCRIPT_FILENAME"
CONFIG_FILE="$CONFIG_FOLDER/$CONFIG_FILENAME"
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Check script availability
if [[ ! -f "$SCRIPT_FILE" ]]; then
    echo "ERROR: Script not found: $SCRIPT_FILE"
    exit 1
fi

if [[ ! -x "$SCRIPT_FILE" ]]; then
    echo "ERROR: Script is not executable: $SCRIPT_FILE"
    exit 1
fi

# Check configuration availability
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "ERROR: Configuration not found: $CONFIG_FILE"
    exit 1
fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Acquire execution lock

if ! command -v flock >/dev/null 2>&1; then
    echo "ERROR: flock command is not available."
    exit 1
fi

# Open wrapper lock file on descriptor 200
if ! exec 200>"$WRAPPER_LOCKFILE"; then
    echo "ERROR: Unable to open wrapper lock file: $WRAPPER_LOCKFILE"
    exit 1
fi

# Non-blocking lock
if ! flock -n 200; then

    echo "--------------------------------------------------------------------------------"
    echo "WARNING: Another execution is already running."
    echo " "
    echo "Name run          : $NAME_RUN"
    echo "Time run          : $TIME_RUN"
    echo "Wrapper lock file : $WRAPPER_LOCKFILE"
    echo "--------------------------------------------------------------------------------"

    exit 1

fi

# Lock is automatically released when the process exits
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Check previous execution status

if [[ -f "$WRAPFILE" ]]; then

    # Read previous exit code
    PREVIOUS_EXIT_CODE=$(
        sed -n 's/^exit_code=//p' "$WRAPFILE" | tail -n 1
    )

    # Validate previous exit code
    if [[ "$PREVIOUS_EXIT_CODE" =~ ^[0-9]+$ ]]; then

        if [[ "$PREVIOUS_EXIT_CODE" -eq 0 ]]; then

            validate_data false
            if [[ "$FORCE" == false && "$INPUT_FILES_FOUND" -eq "$EXPECTED_FILES" && "$INPUT_FILES_MISSING" -eq 0 && "$INPUT_FILES_INVALID" -eq 0 && "$OUTPUT_FILES_FOUND" -eq "$EXPECTED_FILES" && "$OUTPUT_FILES_MISSING" -eq 0 ]]; then

                echo "--------------------------------------------------------------------------------"
                echo "Execution already completed successfully. Skipping."
                echo " "
                echo "Name run   : $NAME_RUN"
                echo "Time run   : $TIME_RUN"
                echo "Wrap file  : $WRAPFILE"
                echo "Exit code  : $PREVIOUS_EXIT_CODE"
                echo "Input     : $INPUT_FILES_FOUND/$EXPECTED_FILES valid"
                echo "Output    : $OUTPUT_FILES_FOUND/$EXPECTED_FILES matching"
                echo "Source    : $INPUT_FOLDER"
                echo "Target    : $OUTPUT_FOLDER"
                echo " "
                echo "Use -f or --force to execute again."
                echo "--------------------------------------------------------------------------------"

                exit 0

            else

                echo "WARNING: Previous execution completed successfully."
                echo "WARNING: Force enabled or input/output validation is incomplete. Running again."

            fi

        else

            echo "WARNING: Previous execution failed (exit code: $PREVIOUS_EXIT_CODE)."
            echo "WARNING: Running again."

        fi

    else

        echo "WARNING: Previous wrap file has no valid exit_code."
        echo "WARNING: Running again."

    fi

else

    echo "INFO: No previous execution found. Running."

fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Build command
FIX_HOUR=$(date -d "$TIME_RUN" "+%H:%M") || exit 1
CMD=(
    "$SCRIPT_FILE"
    --mode now
    --fix "$FIX_HOUR"
    --config "$CONFIG_FILE"
    --filename-date-regex 'Rain_(?P<ts>[0-9]{12})\.tif'
    --verbose
    --debug-gate
    --lockfile "$LOCKFILE"
)
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Print run configuration
echo "--------------------------------------------------------------------------------"
echo "Run configuration"
echo " "
echo "Name run          : $NAME_RUN"
echo "Time run          : $TIME_RUN"
echo "Force run         : $FORCE"
echo "Input folder      : $INPUT_FOLDER"
echo "Input days before : $INPUT_DAYS_BEFORE"
echo "Input days after  : $INPUT_DAYS_AFTER"
echo "Forecast window   : $WINDOW_START -> $WINDOW_END (inclusive)"
echo "Expected files    : $EXPECTED_FILES (single rainfall dataset; no basins)"
echo "Output folder     : $OUTPUT_FOLDER"
echo "Model check file  : $MODEL_CHECK_FILE"
echo "Model ME file     : $MODEL_ME_FILE"
echo "Log file          : $LOGFILE"
echo "Wrap file         : $WRAPFILE"
echo "Lock file         : $LOCKFILE"
echo "Wrapper lock file : $WRAPPER_LOCKFILE"
echo "Config            : $CONFIG_FILE"
echo "Script            : $SCRIPT_FILE"
echo " "
echo "About to run:"
printf '%q ' "${CMD[@]}"
echo
echo "--------------------------------------------------------------------------------"
echo " "
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Save start time
TIME_START=$(date '+%Y-%m-%d %H:%M:%S')

# Invalidate previous success before starting a new execution
if ! cat > "$WRAPFILE" <<EOF
name_run=$NAME_RUN
time_start=$TIME_START
time_end=
time_run=$TIME_RUN
exit_code=1
status=running
force_run=$FORCE
run_subfolder=$RUN_SUBFOLDER
window_start=$WINDOW_START
window_end=$WINDOW_END
input_days_before=$INPUT_DAYS_BEFORE
input_days_after=$INPUT_DAYS_AFTER
input_folder=$INPUT_FOLDER
output_folder=$OUTPUT_FOLDER
model_check_file=$MODEL_CHECK_FILE
model_me_file=$MODEL_ME_FILE
model_check_available=$MODEL_CHECK_AVAILABLE
model_me_available=$MODEL_ME_AVAILABLE
input_files_expected=$EXPECTED_FILES
output_files_expected=$EXPECTED_FILES
EOF
then
    echo "ERROR: Unable to initialize wrap file: $WRAPFILE"
    exit 1
fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Execute command

# Write the log only when execution is required
if ! : > "$LOGFILE"; then
    echo "ERROR: Unable to initialize log file: $LOGFILE"
    exit 1
fi

exec 3>&1
{
    echo "--------------------------------------------------------------------------------"
    echo "[$TIME_START] Starting script ..."
    echo "--------------------------------------------------------------------------------"
    echo "Name run  : $NAME_RUN"
    echo "Time run  : $TIME_RUN"
    echo "Force run : $FORCE"
    echo "Input     : $INPUT_FOLDER"
    echo "Output    : $OUTPUT_FOLDER"
    echo "Window    : $WINDOW_START -> $WINDOW_END (inclusive)"
    echo "Expected  : $EXPECTED_FILES hourly rainfall files"
    echo
    echo "[CHECK] Checking model reports for the exact forecast cycle ..." | tee /dev/fd/3
    if [[ -s "$MODEL_CHECK_FILE" ]]; then
        MODEL_CHECK_AVAILABLE=true
        echo "[OK MODEL CHECK] $MODEL_CHECK_FILE" | tee /dev/fd/3
    else
        echo "[MISSING MODEL CHECK] $MODEL_CHECK_FILE" | tee /dev/fd/3
    fi
    if [[ -s "$MODEL_ME_FILE" ]]; then
        MODEL_ME_AVAILABLE=true
        echo "[OK MODEL ME] $MODEL_ME_FILE" | tee /dev/fd/3
    else
        echo "[MISSING MODEL ME] $MODEL_ME_FILE" | tee /dev/fd/3
    fi
    echo "[START] $(date '+%Y-%m-%d %H:%M:%S') Checking rainfall source" | tee /dev/fd/3
    validate_data true
    echo "[INPUT] expected=$EXPECTED_FILES valid=$INPUT_FILES_FOUND missing=$INPUT_FILES_MISSING invalid=$INPUT_FILES_INVALID" | tee /dev/fd/3
    if [[ "$MODEL_CHECK_AVAILABLE" != true || "$MODEL_ME_AVAILABLE" != true ]]; then
        EXIT_CODE=2
        echo "ERROR: Model check reports missing or empty for TIME_RUN=$TIME_RUN; synchronization cancelled (exit 2)." | tee /dev/fd/3
    elif [[ ! -d "$INPUT_FOLDER" || "$INPUT_FILES_FOUND" -eq 0 ]]; then
        EXIT_CODE=6
        echo "ERROR: No valid rainfall input files available." | tee /dev/fd/3
    elif [[ "$INPUT_FILES_MISSING" -gt 0 || "$INPUT_FILES_INVALID" -gt 0 ]]; then
        EXIT_CODE=3
        echo "ERROR: Some rainfall input files are invalid or empty." | tee /dev/fd/3
    else
        echo "[TRANSFER] Starting synchronization ..." | tee /dev/fd/3
        echo "[TRANSFER] Input: $INPUT_FOLDER" | tee /dev/fd/3
        echo "[TRANSFER] Output: $OUTPUT_FOLDER" | tee /dev/fd/3
        if "${CMD[@]}"; then
            EXIT_CODE=0
        else
            EXIT_CODE=$?
        fi
        echo "[TRANSFER] Synchronization exit code: $EXIT_CODE" | tee /dev/fd/3
        if [[ "$EXIT_CODE" -eq 0 ]]; then
            echo "[VERIFY] Checking copied rainfall files ..." | tee /dev/fd/3
            validate_data true
            echo "[VERIFY] expected=$EXPECTED_FILES input=$INPUT_FILES_FOUND input_missing=$INPUT_FILES_MISSING matching=$OUTPUT_FILES_FOUND output_missing=$OUTPUT_FILES_MISSING invalid=$INPUT_FILES_INVALID" | tee /dev/fd/3
            if [[ "$OUTPUT_FILES_FOUND" -eq 0 ]]; then
                EXIT_CODE=7
            elif [[ "$OUTPUT_FILES_MISSING" -gt 0 || "$OUTPUT_FILES_FOUND" -ne "$EXPECTED_FILES" ]]; then
                EXIT_CODE=8
            fi
        fi
    fi

    TIME_END=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[SUMMARY] Run time       : $TIME_RUN" | tee /dev/fd/3
    echo "[SUMMARY] Model check    : $MODEL_CHECK_AVAILABLE ($MODEL_CHECK_FILE)" | tee /dev/fd/3
    echo "[SUMMARY] Model ME       : $MODEL_ME_AVAILABLE ($MODEL_ME_FILE)" | tee /dev/fd/3
    echo "[SUMMARY] Input files    : $INPUT_FILES_FOUND/$EXPECTED_FILES (missing=$INPUT_FILES_MISSING, invalid=$INPUT_FILES_INVALID)" | tee /dev/fd/3
    echo "[SUMMARY] Output files   : $OUTPUT_FILES_FOUND/$EXPECTED_FILES (missing/mismatch=$OUTPUT_FILES_MISSING)" | tee /dev/fd/3
    echo "[SUMMARY] Source folder  : $INPUT_FOLDER" | tee /dev/fd/3
    echo "[SUMMARY] Target folder  : $OUTPUT_FOLDER" | tee /dev/fd/3
    echo "[END] $TIME_END exit_code=$EXIT_CODE" | tee /dev/fd/3

    echo
    echo "--------------------------------------------------------------------------------"

    if [[ "$EXIT_CODE" -eq 0 ]]; then
        echo "[$TIME_END] ... DONE!"
    else
        echo "[$TIME_END] ... FAILED!"
    fi

    echo "Exit code: $EXIT_CODE"
    echo "--------------------------------------------------------------------------------"
    echo

} >> "$LOGFILE" 2>&1
exec 3>&-
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Save wrap information

if [[ "$EXIT_CODE" -eq 0 ]]; then
    RUN_STATUS="done"
elif [[ "$EXIT_CODE" -eq 2 ]]; then
    RUN_STATUS="forecast_run_not_available"
elif [[ "$EXIT_CODE" -eq 3 ]]; then
    RUN_STATUS="some_input_are_not_available"
elif [[ "$EXIT_CODE" -eq 6 ]]; then
    RUN_STATUS="no_input_found"
elif [[ "$EXIT_CODE" -eq 7 ]]; then
    RUN_STATUS="no_output_found"
elif [[ "$EXIT_CODE" -eq 8 ]]; then
    RUN_STATUS="some_output_are_not_available"
else
    RUN_STATUS="failed"
fi

# Write to temporary file, then atomically replace the .info file
WRAP_TMP=$(mktemp "${WRAPFILE}.tmp.XXXXXX") || {
    echo "ERROR: Unable to create temporary wrap file"
    exit 1
}

if ! cat > "$WRAP_TMP" <<EOF
name_run=$NAME_RUN
time_start=$TIME_START
time_end=$TIME_END
time_run=$TIME_RUN
exit_code=$EXIT_CODE
status=$RUN_STATUS
force_run=$FORCE
run_subfolder=$RUN_SUBFOLDER
window_start=$WINDOW_START
window_end=$WINDOW_END
input_days_before=$INPUT_DAYS_BEFORE
input_days_after=$INPUT_DAYS_AFTER
input_folder=$INPUT_FOLDER
output_folder=$OUTPUT_FOLDER
model_check_file=$MODEL_CHECK_FILE
model_me_file=$MODEL_ME_FILE
model_check_available=$MODEL_CHECK_AVAILABLE
model_me_available=$MODEL_ME_AVAILABLE
input_files_expected=$EXPECTED_FILES
input_files_missing=$INPUT_FILES_MISSING
input_files_found=$INPUT_FILES_FOUND
input_files_invalid=$INPUT_FILES_INVALID
output_files_expected=$EXPECTED_FILES
output_files_found=$OUTPUT_FILES_FOUND
output_files_missing=$OUTPUT_FILES_MISSING
EOF
then
    rm -f "$WRAP_TMP"
    echo "ERROR: Unable to write wrap information"
    exit 1
fi

if ! mv -f "$WRAP_TMP" "$WRAPFILE"; then
    rm -f "$WRAP_TMP"
    echo "ERROR: Unable to save wrap file: $WRAPFILE"
    exit 1
fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Print final status
echo "--------------------------------------------------------------------------------"
echo "Run completed"
echo " "
echo "Name run   : $NAME_RUN"
echo "Time run   : $TIME_RUN"
echo "Time start : $TIME_START"
echo "Time end   : $TIME_END"
echo "Status     : $RUN_STATUS"
echo "Forecast window : $WINDOW_START -> $WINDOW_END (inclusive)"
echo "Source folder   : $INPUT_FOLDER"
echo "Model check     : $MODEL_CHECK_AVAILABLE"
echo "Model ME        : $MODEL_ME_AVAILABLE"
echo "Target folder   : $OUTPUT_FOLDER"
echo "Input files     : $INPUT_FILES_FOUND/$EXPECTED_FILES (missing: $INPUT_FILES_MISSING, invalid: $INPUT_FILES_INVALID)"
echo "Output files    : $OUTPUT_FILES_FOUND/$EXPECTED_FILES (missing/mismatch: $OUTPUT_FILES_MISSING)"
echo "Exit code  : $EXIT_CODE"
echo "Log file   : $LOGFILE"
echo "Wrap file  : $WRAPFILE"
echo "Lock file  : $LOCKFILE"
echo "--------------------------------------------------------------------------------"
echo " "

# Exit using command exit code
exit "$EXIT_CODE"
# -------------------------------------------------------------------------------------
