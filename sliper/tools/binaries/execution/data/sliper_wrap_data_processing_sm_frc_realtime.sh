#!/bin/bash

# -------------------------------------------------------------------------------------
# Configuration
ROOT="/home/admin/soilslips-exec/data/"

SCRIPT_FOLDER="$ROOT"
SCRIPT_FILENAME="sliper_app_data_processing_sm_frc_realtime.sh"

NAME_RUN="wrap_data_sm_frc"

LOG_FOLDER="/home/admin/log/runner_data"
WRAP_FOLDER="/home/admin/info/runner_data"
LOCK_FOLDER="/home/admin/lock/runner_data"

OUTPUT_ROOT="/home/admin/soilslips-ws/data_dynamic/destination/data/soil_moisture/frc"
OUTPUT_SUBFOLDER_FORMAT="%Y/%m/%d/%H00"

# Input datasets: check every hourly forecast file for each configured domain.
INPUT_SOURCE="/home/admin/soilslips-ws/data_dynamic/source/data/soil_moisture/frc"
INPUT_SUBFOLDER_FORMAT="%Y/%m/%d/%H%M"
INPUT_FILENAME_FORMAT="hmc.output-grid.%Y%m%d%H00.nc.gz"
INPUT_DAYS_BEFORE=2
INPUT_DAYS_AFTER=2
INPUT_DOMAINS=(
    "AvetoTrebbiaDomain"
    "BormidaMDomain"
    "BormidaSDomain"
    "CentaDomain"
    "CentroPonenteDomain"
    "EntellaDomain"
    "ErroDomain"
    "FinaleseDomain"
    "ImperieseDomain"
    "LevanteGenoveseDomain"
    "MagraDomain"
    "OrbaSturaDomain"
    "PonenteDomain"
    "PonenteGenoveseDomain"
    "RoiaDomain"
    "SavoneseDomain"
    "ScriviaDomain"
    "TanaroDomain"
)
INPUT_EXPECTED_DOMAINS=${#INPUT_DOMAINS[@]}
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

        --input-days-before|--input-days-after)
            if [[ ! "${2:-}" =~ ^[0-9]+$ ]]; then
                echo "ERROR: $1 requires a nonnegative integer"
                exit 1
            fi
            if [[ "$1" == "--input-days-before" ]]; then
                INPUT_DAYS_BEFORE="$2"
            else
                INPUT_DAYS_AFTER="$2"
            fi
            shift 2
            ;;

        -f|--force)
            FORCE=true
            shift
            ;;

        -h|--help)
            echo "Usage: $0 [--run-time HH:MM] [--input-days-before N] [--input-days-after N] [-f|--force]"
            echo
            echo "Options:"
            echo "  --run-time HH:MM   Set execution time"
            echo "  --input-days-before N  Days before run date (default: 2)"
            echo "  --input-days-after N   Days after run date (default: 2)"
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
            echo "Usage: $0 [--run-time HH:MM] [--input-days-before N] [--input-days-after N] [-f|--force]"
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
# Define time passed to the processing script
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
INPUT_SUBFOLDER=$(date -d "$TIME_RUN" +"$INPUT_SUBFOLDER_FORMAT")
INPUT_DATE=$(date -d "$TIME_RUN" '+%Y-%m-%d')
INPUT_TIME_START=$(date -d "$INPUT_DATE $INPUT_DAYS_BEFORE days ago" '+%Y-%m-%d %H:%M:%S')
INPUT_TIME_END=$(date -d "$INPUT_DATE $INPUT_DAYS_AFTER days" '+%Y-%m-%d %H:%M:%S')
INPUT_FIRST_FILENAME=$(date -d "$INPUT_TIME_START" +"$INPUT_FILENAME_FORMAT")
INPUT_LAST_FILENAME=$(date -d "$INPUT_TIME_END" +"$INPUT_FILENAME_FORMAT")
INPUT_DOMAINS_FOUND=0
INPUT_START_EPOCH=$(date -u -d "$INPUT_TIME_START UTC" +%s)
INPUT_END_EPOCH=$(date -u -d "$INPUT_TIME_END UTC" +%s)
INPUT_HOURS_EXPECTED=$(( (INPUT_END_EPOCH - INPUT_START_EPOCH) / 3600 + 1 ))
INPUT_FILES_EXPECTED=$((INPUT_EXPECTED_DOMAINS * INPUT_HOURS_EXPECTED))
INPUT_FILES_FOUND=0
INPUT_FILES_MISSING=$INPUT_FILES_EXPECTED

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

    if [[ "$PREVIOUS_EXIT_CODE" =~ ^[0-9]+$ ]] && [[ "$PREVIOUS_EXIT_CODE" -eq 0 ]]; then
        echo "INFO: Previous execution completed successfully."
        echo "INFO: Checking previously generated output files ..."

        if [[ ! -d "$OUTPUT_FOLDER" ]]; then
            echo "WARNING: Output directory does not exist."
            echo "WARNING: Previous success cannot be confirmed; running Python again."
        elif [[ -z "$(find "$OUTPUT_FOLDER" -type f -print -quit)" ]]; then
            echo "WARNING: Output directory is empty (no regular files found)."
            echo "WARNING: Previous success cannot be confirmed; running Python again."
        elif [[ "$FORCE" == true ]]; then
            echo "INFO: Output files found, but --force was requested. Running Python again."
        else
            echo "INFO: Output files found. Previous execution verified."
            echo "INFO: Skipping Python execution. Use --force to rerun."
            echo "--------------------------------------------------------------------------------"
            exit 0
        fi
    elif [[ "$PREVIOUS_EXIT_CODE" =~ ^[0-9]+$ ]]; then
        echo "WARNING: Previous execution failed; running Python again."
    else
        echo "WARNING: Previous exit code is missing or invalid; running Python again."
    fi
else
    echo "INFO: No previous execution found; running Python."
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
echo "Input root  : $INPUT_SOURCE"
echo "Input date  : $INPUT_SUBFOLDER"
echo "Input range : $INPUT_TIME_START -> $INPUT_TIME_END (inclusive)"
echo "Domains     : $INPUT_EXPECTED_DOMAINS (expected)"
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

# Invalidate previous success before starting a new execution.
# This also protects against a stale successful .info file if the
# wrapper is interrupted during a forced rerun.
if ! cat > "$WRAPFILE" <<EOF
name_run=$NAME_RUN
time_start=$TIME_START
time_end=
time_run=$TIME_RUN
exit_code=1
status=running
input_root=$INPUT_SOURCE
input_subfolder=$INPUT_SUBFOLDER
input_time_start=$INPUT_TIME_START
input_time_end=$INPUT_TIME_END
input_domains_expected=$INPUT_EXPECTED_DOMAINS
input_domains_found=0
input_hours_expected=$INPUT_HOURS_EXPECTED
input_files_expected=$INPUT_FILES_EXPECTED
input_files_found=0
input_files_missing=$INPUT_FILES_MISSING
EOF
then
    echo "ERROR: Unable to initialize wrap file: $WRAPFILE"
    exit 1
fi

# -------------------------------------------------------------------------------------
# Execute command

# Write the log only when execution is required
if ! : > "$LOGFILE"; then
    echo "ERROR: Unable to initialize log file: $LOGFILE"
    exit 1
fi

# Preserve stdout for input validation messages while logging the execution.
exec 3>&1

{
    echo "--------------------------------------------------------------------------------"
    echo "[$TIME_START] Starting script ..."
    echo "--------------------------------------------------------------------------------"
    echo "Name run    : $NAME_RUN"
    echo "Time run    : $TIME_RUN"
    echo "Time script : $TIME_SCRIPT"
    echo "Force run   : $FORCE"
    echo

    # Validate every hourly forecast dataset, including the last timestep.
    # Each domain stores all its hourly files in one run-time folder.
    echo "--------------------------------------------------------------------------------" | tee /dev/fd/3
    echo "Input datasets validation" | tee /dev/fd/3
    echo "Root folder : $INPUT_SOURCE" | tee /dev/fd/3
    echo "Run folder  : $INPUT_SUBFOLDER" | tee /dev/fd/3
    echo "First file  : $INPUT_FIRST_FILENAME" | tee /dev/fd/3
    echo "Last file   : $INPUT_LAST_FILENAME (inclusive)" | tee /dev/fd/3
    echo "Expected    : $INPUT_EXPECTED_DOMAINS domains, $INPUT_HOURS_EXPECTED hours per domain, $INPUT_FILES_EXPECTED files" | tee /dev/fd/3
    echo "--------------------------------------------------------------------------------" | tee /dev/fd/3

    INPUT_DOMAINS_FOUND=0
    INPUT_FILES_FOUND=0
    INPUT_FILES_MISSING=0

    for DOMAIN_NAME in "${INPUT_DOMAINS[@]}"; do
        DOMAIN_RUN_FOLDER="$INPUT_SOURCE/$DOMAIN_NAME/$INPUT_SUBFOLDER"
        echo "Domain: $DOMAIN_NAME | Folder: $DOMAIN_RUN_FOLDER" | tee /dev/fd/3

        if [[ -d "$DOMAIN_RUN_FOLDER" ]]; then
            INPUT_DOMAINS_FOUND=$((INPUT_DOMAINS_FOUND + 1))
        else
            echo "  [MISSING FOLDER] $DOMAIN_RUN_FOLDER" | tee /dev/fd/3
        fi

        DOMAIN_FILES_FOUND=0
        DOMAIN_FILES_MISSING=0
        for ((INPUT_EPOCH=INPUT_START_EPOCH; INPUT_EPOCH<=INPUT_END_EPOCH; INPUT_EPOCH+=3600)); do
            INPUT_FILENAME=$(date -u -d "@$INPUT_EPOCH" +"$INPUT_FILENAME_FORMAT")
            if [[ -f "$DOMAIN_RUN_FOLDER/$INPUT_FILENAME" ]]; then
                echo "  [OK]      $INPUT_FILENAME" | tee /dev/fd/3
                INPUT_FILES_FOUND=$((INPUT_FILES_FOUND + 1))
                DOMAIN_FILES_FOUND=$((DOMAIN_FILES_FOUND + 1))
            else
                echo "  [MISSING] $INPUT_FILENAME" | tee /dev/fd/3
                INPUT_FILES_MISSING=$((INPUT_FILES_MISSING + 1))
                DOMAIN_FILES_MISSING=$((DOMAIN_FILES_MISSING + 1))
            fi
        done
        echo "Domain summary: $DOMAIN_NAME found=$DOMAIN_FILES_FOUND missing=$DOMAIN_FILES_MISSING" | tee /dev/fd/3
    done

    echo "Input summary: domains=$INPUT_DOMAINS_FOUND/$INPUT_EXPECTED_DOMAINS files_found=$INPUT_FILES_FOUND files_missing=$INPUT_FILES_MISSING" | tee /dev/fd/3
    echo "--------------------------------------------------------------------------------" | tee /dev/fd/3

    if (( INPUT_FILES_FOUND == 0 )); then
        EXIT_CODE=6
        echo "ERROR: No input datasets found (exit code 6)." | tee /dev/fd/3
    elif (( INPUT_DOMAINS_FOUND != INPUT_EXPECTED_DOMAINS || INPUT_FILES_MISSING > 0 )); then
        EXIT_CODE=3
        echo "ERROR: Some input datasets are not available (exit code 3)." | tee /dev/fd/3
    else
        "${CMD[@]}"
        EXIT_CODE=$?
    fi

	# Check output only if processing completed successfully
	if [[ "$EXIT_CODE" -eq 0 ]]; then

		echo "Checking output folder: $OUTPUT_FOLDER"

		if [[ ! -d "$OUTPUT_FOLDER" ]]; then

		    echo "ERROR: Output folder does not exist: $OUTPUT_FOLDER"
		    EXIT_CODE=7

		elif [[ -z "$(find "$OUTPUT_FOLDER" -type f -print -quit)" ]]; then

		    echo "ERROR: No data created in: $OUTPUT_FOLDER"
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

if [[ "$EXIT_CODE" -eq 0 ]]; then
    RUN_STATUS="done"
elif [[ "$EXIT_CODE" -eq 3 ]]; then
    RUN_STATUS="some_input_are_not_available"
elif [[ "$EXIT_CODE" -eq 6 ]]; then
    RUN_STATUS="no_input_found"
elif [[ "$EXIT_CODE" -eq 7 ]]; then
    RUN_STATUS="no_output_found"
else
    RUN_STATUS="failed"
fi

# Write to a temporary file, then atomically replace the .info file
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
input_root=$INPUT_SOURCE
input_subfolder=$INPUT_SUBFOLDER
input_time_start=$INPUT_TIME_START
input_time_end=$INPUT_TIME_END
input_domains_expected=$INPUT_EXPECTED_DOMAINS
input_domains_found=$INPUT_DOMAINS_FOUND
input_hours_expected=$INPUT_HOURS_EXPECTED
input_files_expected=$INPUT_FILES_EXPECTED
input_files_found=$INPUT_FILES_FOUND
input_files_missing=$INPUT_FILES_MISSING
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
