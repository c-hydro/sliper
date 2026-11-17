#!/bin/bash

# -------------------------------------------------------------------------------------
# Configuration
ROOT="/home/admin/soilslips-exec/indicators"

SCRIPT_FOLDER="$ROOT"
SCRIPT_FILENAME="sliper_app_indicators_rain_obs_realtime.sh"

NAME_RUN="wrap_indicators_rain_obs"

LOG_FOLDER="/home/admin/log/runner_indicators"
WRAP_FOLDER="/home/admin/info/runner_indicators"
LOCK_FOLDER="/home/admin/lock/runner_indicators"
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Parse arguments
RUN_TIME=""

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

        -h|--help)
            echo "Usage: $0 [--run-time HH:MM]"
            echo
            echo "Examples:"
            echo "  $0"
            echo "  $0 --run-time 04:00"
            echo "  $0 --run-time 08:00"
            exit 0
            ;;

        *)
            echo "ERROR: Unknown option: $1"
            echo "Usage: $0 [--run-time HH:MM]"
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
# Define time passed to the indicators script
TIME_SCRIPT=$(date -d "$TIME_RUN" '+%Y-%m-%d %H:%M')

if [[ -z "$TIME_SCRIPT" ]]; then
    echo "ERROR: Unable to create TIME_SCRIPT from '$TIME_RUN'"
    exit 1
fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Create time tag for filenames
TIME_RUN_TAG=$(date -d "$TIME_RUN" '+%Y%m%d%H%M')

if [[ -z "$TIME_RUN_TAG" ]]; then
    echo "ERROR: Unable to create TIME_RUN_TAG from '$TIME_RUN'"
    exit 1
fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Define files
LOG_FILENAME="${NAME_RUN}_${TIME_RUN_TAG}.log"
WRAP_FILENAME="${NAME_RUN}_${TIME_RUN_TAG}.info"
LOCK_FILENAME="${NAME_RUN}_${TIME_RUN_TAG}.lock"

LOGFILE="$LOG_FOLDER/$LOG_FILENAME"
WRAPFILE="$WRAP_FOLDER/$WRAP_FILENAME"
LOCKFILE="$LOCK_FOLDER/$LOCK_FILENAME"

SCRIPT_FILE="$SCRIPT_FOLDER/$SCRIPT_FILENAME"

# Remove previous log
rm -f "$LOGFILE"
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
echo "Log file    : $LOGFILE"
echo "Wrap file   : $WRAPFILE"
echo "Lock file   : $LOCKFILE"
echo "Script      : $SCRIPT_FILE"
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

# Execute command
{
    echo "--------------------------------------------------------------------------------"
    echo "[$TIME_START] Starting script ..."
    echo "--------------------------------------------------------------------------------"
    echo "Name run    : $NAME_RUN"
    echo "Time run    : $TIME_RUN"
    echo "Time script : $TIME_SCRIPT"
    echo

    "${CMD[@]}"
    EXIT_CODE=$?

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
cat > "$WRAPFILE" <<EOF
name_run=$NAME_RUN
time_start=$TIME_START
time_end=$TIME_END
time_run=$TIME_RUN
exit_code=$EXIT_CODE
EOF

# Check wrap file
if [[ ! -f "$WRAPFILE" ]]; then
    echo "ERROR: Wrap file was not created: $WRAPFILE"
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
echo "Exit code  : $EXIT_CODE"
echo "Log file   : $LOGFILE"
echo "Wrap file  : $WRAPFILE"
echo "Lock file  : $LOCKFILE"
echo "--------------------------------------------------------------------------------"
echo " "

# Exit using command exit code
exit "$EXIT_CODE"
# -------------------------------------------------------------------------------------
