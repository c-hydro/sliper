#!/usr/bin/env bash
set -Eeuo pipefail

# -------------------------------------------------------------------------------------
# Configuration
ROOT="/home/admin/soilslips-system/manager_data"

SCRIPT_FOLDER="$ROOT"
SCRIPT_FILENAME="sliper_tools_sync_data.sh"

CONFIG_FOLDER="$ROOT"
CONFIG_FILENAME="sliper_tools_sync_data_rain_obs4frc.json"

NAME_RUN="sync_data_rain_obs4frc"

LOG_FOLDER="/home/admin/log/manager_data"
WRAP_FOLDER="/home/admin/info/manager_data"
LOCK_FOLDER="/home/admin/lock/manager_data"
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Parse arguments
RUN_TIME=""
FORCE=false

while [[ $# -gt 0 ]]; do

    case "$1" in

        --run-time|--run-hour)
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
            echo "Usage: $0 [--run-hour HH:MM] [-f|--force]"
            echo
            echo "Options:"
            echo "  --run-hour HH:MM   Select forecast run and last allowed observation timestamp"
            echo "  -f, --force        Force execution even if already completed"
            echo
            echo "Examples:"
            echo "  $0"
            echo "  $0 --run-hour 04:00"
            echo "  $0 --run-hour 08:00"
            echo "  $0 --run-hour 04:00 -f"
            exit 0
            ;;

        *)
            echo "ERROR: Unknown option: $1"
            echo "Usage: $0 [--run-hour HH:MM] [-f|--force]"
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

    if [[ ! -d "$folder" ]]; then

        echo "Creating $label folder: $folder"

        if ! mkdir -p "$folder"; then
            echo "ERROR: Unable to create $label folder: $folder"
            exit 1
        fi

    fi

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
# 2026-10-22 04:00:00
# ->
# 202610220400

if ! TIME_RUN_TAG=$(date -d "$TIME_RUN" '+%Y%m%d%H%M'); then
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

    PREVIOUS_EXIT_CODE=$(
        sed -n 's/^exit_code=//p' "$WRAPFILE" | tail -n 1
    )

    if [[ "$PREVIOUS_EXIT_CODE" =~ ^[0-9]+$ ]]; then

        if [[ "$PREVIOUS_EXIT_CODE" -eq 0 ]]; then

            if [[ "$FORCE" == false ]]; then

                echo "--------------------------------------------------------------------------------"
                echo "Execution already completed successfully. Skipping."
                echo " "
                echo "Name run  : $NAME_RUN"
                echo "Time run  : $TIME_RUN"
                echo "Wrap file : $WRAPFILE"
                echo "Exit code : $PREVIOUS_EXIT_CODE"
                echo " "
                echo "Use -f or --force to execute again."
                echo "--------------------------------------------------------------------------------"

                exit 0

            else

                echo "WARNING: Previous execution completed successfully."
                echo "WARNING: Force option enabled. Running again."

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
    --end-at-run-hour
    --config "$CONFIG_FILE"
    --script "$ROOT/sliper_tools_sync_data.py"
    --freq-minutes 60
    --n-days 2
    --start-hour "00:00"
    --verbose
    --debug-gate
    --end-at-date-obs
    --scan-source-days
    --filename-date-regex 'Rain_(?P<YYYY>\d{4})(?P<MM>\d{2})(?P<DD>\d{2})(?P<HH>\d{2})(?P<mm>\d{2})\.tif$'
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
echo "Run hour          : $(date -d "$TIME_RUN" +%H:%M)"
echo "Copy cutoff       : $TIME_RUN (inclusive)"
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

# Initialize wrap information
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
# Initialize log file
if ! : > "$LOGFILE"; then
    echo "ERROR: Unable to initialize log file: $LOGFILE"
    exit 1
fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Execute command
#
# Temporarily disable errexit so a failed synchronization command
# can be recorded in the .info file.

set +e

{
    echo "--------------------------------------------------------------------------------"
    echo "[$TIME_START] Starting script ..."
    echo "--------------------------------------------------------------------------------"
    echo "Name run  : $NAME_RUN"
    echo "Time run  : $TIME_RUN"
    echo "Force run : $FORCE"
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

set -e
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Save wrap information
if [[ "$EXIT_CODE" -eq 0 ]]; then
    RUN_STATUS="done"
else
    RUN_STATUS="failed"
fi

# Write temporary wrap file
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

# Atomically replace wrap file
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

# Exit using synchronization command exit code
exit "$EXIT_CODE"
# -------------------------------------------------------------------------------------
