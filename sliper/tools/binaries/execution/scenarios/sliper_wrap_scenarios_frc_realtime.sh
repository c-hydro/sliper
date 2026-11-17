#!/bin/bash

# -------------------------------------------------------------------------------------
# Configuration
ROOT="/home/admin/soilslips-exec/scenarios"

SCRIPT_FOLDER="$ROOT"
SCRIPT_FILENAME="sliper_app_scenarios_frc_realtime.sh"

NAME_RUN="wrap_scenarios_frc"

LOG_FOLDER="/home/admin/log/runner_scenarios"
WRAP_FOLDER="/home/admin/info/runner_scenarios"
LOCK_FOLDER="/home/admin/lock/runner_scenarios"

# Required input datasets (RAIN and soil moisture)
INPUT_ROOT_RAIN="/home/admin/soilslips-ws/data_dynamic/destination/indicators/rain/frc/"
INPUT_ROOT_SM="/home/admin/soilslips-ws/data_dynamic/destination/indicators/soil_moisture/frc/"
INPUT_SUBFOLDER_FORMAT_RAIN="%Y/%m/%d/%H00"
INPUT_SUBFOLDER_FORMAT_SM="%Y/%m/%d/%H00"

# Scenarios output directory
OUTPUT_ROOT="/home/admin/soilslips-ws/data_dynamic/destination/scenarios/frc/"
OUTPUT_SUBFOLDER_FORMAT="%Y/%m/%d/%H00"
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
# Require a fixed, explicitly configured output root
if [[ "$OUTPUT_ROOT" == "/SET/ACTUAL/SCENARIOS/FRC/OUTPUT/PATH" || -z "$OUTPUT_ROOT" ]]; then
    echo "ERROR: Configure OUTPUT_ROOT in this script before running."
    exit 1
fi

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
# Define time passed to the scenarios script
if ! TIME_SCRIPT=$(date -d "$TIME_RUN" '+%Y-%m-%d %H:%M'); then
    echo "ERROR: Unable to create TIME_SCRIPT from '$TIME_RUN'"
    exit 1
fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Create time tag for filenames
if ! TIME_RUN_TAG=$(date -d "$TIME_RUN" '+%Y%m%d%H%M'); then
    echo "ERROR: Unable to create TIME_RUN_TAG from '$TIME_RUN'"
    exit 1
fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Define output folder using selected execution time
OUTPUT_SUBFOLDER=$(date -d "$TIME_RUN" +"$OUTPUT_SUBFOLDER_FORMAT")
OUTPUT_FOLDER="$OUTPUT_ROOT/$OUTPUT_SUBFOLDER"
if ! INPUT_SUBFOLDER_RAIN=$(date -d "$TIME_RUN" +"$INPUT_SUBFOLDER_FORMAT_RAIN"); then
    echo "ERROR: Unable to resolve RAIN input directory from TIME_RUN: $TIME_RUN"
    exit 1
fi
if ! INPUT_SUBFOLDER_SM=$(date -d "$TIME_RUN" +"$INPUT_SUBFOLDER_FORMAT_SM"); then
    echo "ERROR: Unable to resolve SM input directory from TIME_RUN: $TIME_RUN"
    exit 1
fi
INPUT_FOLDER_RAIN="${INPUT_ROOT_RAIN%/}/$INPUT_SUBFOLDER_RAIN"
INPUT_FOLDER_SM="${INPUT_ROOT_SM%/}/$INPUT_SUBFOLDER_SM"

# Define files
LOG_FILENAME="${NAME_RUN}_${TIME_RUN_TAG}.log"
WRAP_FILENAME="${NAME_RUN}_${TIME_RUN_TAG}.info"
LOCK_FILENAME="${NAME_RUN}_${TIME_RUN_TAG}.lock"

LOGFILE="$LOG_FOLDER/$LOG_FILENAME"
WRAPFILE="$WRAP_FOLDER/$WRAP_FILENAME"
LOCKFILE="$LOCK_FOLDER/$LOCK_FILENAME"

SCRIPT_FILE="$SCRIPT_FOLDER/$SCRIPT_FILENAME"
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
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Acquire execution lock

if ! command -v flock >/dev/null 2>&1; then
    echo "ERROR: flock command is not available."
    exit 1
fi

# Open lock file on descriptor 200
if ! exec 200>"$LOCKFILE"; then
    echo "ERROR: Unable to open lock file: $LOCKFILE"
    exit 1
fi

# Non-blocking lock
if ! flock -n 200; then

    echo "--------------------------------------------------------------------------------"
    echo "WARNING: Another execution is already running."
    echo " "
    echo "Name run  : $NAME_RUN"
    echo "Time run  : $TIME_RUN"
    echo "Lock file : $LOCKFILE"
    echo "--------------------------------------------------------------------------------"

    exit 1

fi

# Lock is automatically released when the process exits
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Check previous execution status

echo "--------------------------------------------------------------------------------"
echo "Checking previous execution ..."
echo "Name run      : $NAME_RUN"
echo "Time run      : $TIME_RUN"
echo "Info file     : $WRAPFILE"
echo "Output folder : $OUTPUT_FOLDER"

if [[ -f "$WRAPFILE" ]]; then
    PREVIOUS_EXIT_CODE=$(sed -n 's/^exit_code=//p' "$WRAPFILE" | tail -n 1)
    echo "Previous exit code : ${PREVIOUS_EXIT_CODE:-unknown}"

    if [[ "$PREVIOUS_EXIT_CODE" == "0" ]]; then
        echo "INFO: Previous execution completed successfully."
        echo "INFO: Checking previously generated output files ..."

        if [[ ! -d "$OUTPUT_FOLDER" ]]; then
            echo "WARNING: Output directory does not exist. Running again."
        elif [[ -z "$(find "$OUTPUT_FOLDER" -type f -print -quit)" ]]; then
            echo "WARNING: Output directory is empty. Running again."
        elif [[ "$FORCE" == true ]]; then
            echo "INFO: Output files found, but --force was requested. Running again."
        else
            echo "INFO: Output files found. Previous execution verified."
            echo "INFO: Skipping execution. Use --force to rerun."
            echo "--------------------------------------------------------------------------------"
            exit 0
        fi
    elif [[ "$PREVIOUS_EXIT_CODE" =~ ^[0-9]+$ ]]; then
        echo "WARNING: Previous execution failed; running again."
    else
        echo "WARNING: Previous exit code is missing or invalid; running again."
    fi
else
    echo "INFO: No previous execution found; running."
fi

echo "--------------------------------------------------------------------------------"
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Build command
CMD=(
    "$SCRIPT_FILE"
    --mode now
    --date "$TIME_SCRIPT"
)
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Print run configuration
echo "--------------------------------------------------------------------------------"
echo "Run configuration"
echo " "
echo "Name run    : $NAME_RUN"
echo "Time run    : $TIME_RUN"
echo "Time script : $TIME_SCRIPT"
echo "Force run   : $FORCE"
echo "Log file    : $LOGFILE"
echo "Wrap file   : $WRAPFILE"
echo "Lock file   : $LOCKFILE"
echo "Script      : $SCRIPT_FILE"
echo "Output dir  : $OUTPUT_FOLDER"
echo "Rain input  : $INPUT_FOLDER_RAIN"
echo "SM input    : $INPUT_FOLDER_SM"
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

{
    echo "--------------------------------------------------------------------------------"
    echo "[$TIME_START] Starting script ..."
    echo "--------------------------------------------------------------------------------"
    echo "Name run    : $NAME_RUN"
    echo "Time run    : $TIME_RUN"
    echo "Time script : $TIME_SCRIPT"
    echo "Force run   : $FORCE"
    echo

    # Check both required inputs before starting the underlying Python workflow.
    # A missing folder or a folder without any regular files counts as unavailable.
    rain_available=false
    sm_available=false

    if [[ -d "$INPUT_FOLDER_RAIN" ]] && [[ -n "$(find "$INPUT_FOLDER_RAIN" -type f -print -quit)" ]]; then
        rain_available=true
    fi
    if [[ -d "$INPUT_FOLDER_SM" ]] && [[ -n "$(find "$INPUT_FOLDER_SM" -type f -print -quit)" ]]; then
        sm_available=true
    fi

    if [[ "$rain_available" == false && "$sm_available" == false ]]; then
        echo "ERROR: Both RAIN and soil moisture inputs are missing or empty."
        echo "RAIN: $INPUT_FOLDER_RAIN"
        echo "SM:   $INPUT_FOLDER_SM"
        EXIT_CODE=6
    elif [[ "$rain_available" == false ]]; then
        echo "ERROR: RAIN input is missing or empty: $INPUT_FOLDER_RAIN"
        EXIT_CODE=4
    elif [[ "$sm_available" == false ]]; then
        echo "ERROR: Soil moisture input is missing or empty: $INPUT_FOLDER_SM"
        EXIT_CODE=5
    else
        echo "INFO: Both RAIN and soil moisture inputs are available."
        if "${CMD[@]}"; then
            EXIT_CODE=0
        else
            EXIT_CODE=$?
        fi
    fi

    # Validate output only after a successful execution.
    if [[ "$EXIT_CODE" -eq 0 ]]; then
        if [[ ! -d "$OUTPUT_FOLDER" ]]; then
            echo "ERROR: Output folder does not exist: $OUTPUT_FOLDER"
            EXIT_CODE=7
        elif [[ -z "$(find "$OUTPUT_FOLDER" -type f -print -quit)" ]]; then
            echo "ERROR: No output files found in: $OUTPUT_FOLDER"
            EXIT_CODE=7
        else
            echo "INFO: Output data available: $OUTPUT_FOLDER"
        fi
    fi

    TIME_END=$(date '+%Y-%m-%d %H:%M:%S')

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
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Save wrap information

case "$EXIT_CODE" in
    0) RUN_STATUS="done" ;;
    4) RUN_STATUS="no_rain_input_found" ;;
    5) RUN_STATUS="no_soil_moisture_input_found" ;;
    6) RUN_STATUS="no_input_found" ;;
    7) RUN_STATUS="no_output_found" ;;
    *) RUN_STATUS="failed" ;;
esac

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
echo "Exit code  : $EXIT_CODE"
echo "Log file   : $LOGFILE"
echo "Wrap file  : $WRAPFILE"
echo "Lock file  : $LOCKFILE"
echo "--------------------------------------------------------------------------------"
echo " "

# Exit using command exit code
exit "$EXIT_CODE"
# -------------------------------------------------------------------------------------
