#!/bin/bash

# -------------------------------------------------------------------------------------
# Configuration
ROOT="/home/admin/soilslips-system/manager_data"

CONFIG_FOLDER="$ROOT"
CONFIG_FILENAME="sliper_tools_sync_data_sm_frc.json"

SCRIPT_FOLDER="$ROOT"
SCRIPT_FILENAME="sliper_tools_sync_data.sh"

NAME_RUN="sync_data_sm_frc"

LOG_FOLDER="/home/admin/log/manager_data"
WRAP_FOLDER="/home/admin/info/manager_data"
LOCK_FOLDER="/home/admin/lock/manager_data"

# Input source: Continuum gridded forecast archive for each domain.
INPUT_SOURCE="/mnt/ModelloContinuum/archive"
INPUT_MODEL_FOLDER="model_percentile_realtime80"
METEO_CHECK_ROOT="/mnt/RainPercentileModels/risultati/MergeScenarioRegridded/Perc80"
INFO_RUN_ROOT="/home/admin/soilslips-ws/data_dynamic/source/run"
INPUT_SUBFOLDER_FORMAT="%Y/%m/%d/gridded"

INPUT_DAYS_BEFORE=0
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

# Output destination: input source of wrap_data_sm_frc.
OUTPUT_ROOT="/home/admin/soilslips-ws/data_dynamic/source/data/soil_moisture/frc"
OUTPUT_SUBFOLDER_FORMAT="%Y/%m/%d/%H%M"
OUTPUT_FILENAME_FORMAT="hmc.output-grid.%Y%m%d%H00.nc.gz"
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
            echo "  --input-days-before N  Days before run date (default: 0)"
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
# Create time tag for filenames
if ! TIME_RUN_TAG=$(date -d "$TIME_RUN" '+%Y%m%d%H%M'); then
    echo "ERROR: Unable to create TIME_RUN_TAG from '$TIME_RUN'"
    exit 1
fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Define input and output folders from selected execution time
INPUT_DATE=$(date -d "$TIME_RUN" '+%Y-%m-%d') || exit 1
INPUT_SUBFOLDER=$(date -d "$TIME_RUN" "+$INPUT_SUBFOLDER_FORMAT") || exit 1
OUTPUT_SUBFOLDER=$(date -d "$TIME_RUN" "+$OUTPUT_SUBFOLDER_FORMAT") || exit 1
INPUT_TIME_START=$(date -d "$INPUT_DATE - $INPUT_DAYS_BEFORE days" '+%Y-%m-%d 00:00:00') || exit 1
INPUT_TIME_END=$(date -d "$INPUT_DATE + $INPUT_DAYS_AFTER days" '+%Y-%m-%d 00:00:00') || exit 1
INPUT_START_EPOCH=$(date -u -d "$INPUT_TIME_START UTC" '+%s') || exit 1
INPUT_END_EPOCH=$(date -u -d "$INPUT_TIME_END UTC" '+%s') || exit 1
INPUT_HOURS_EXPECTED=$(( (INPUT_END_EPOCH - INPUT_START_EPOCH) / 3600 + 1 ))
INPUT_START_TAG=$(date -d "$INPUT_TIME_START" +%Y%m%d%H%M) || exit 1
INPUT_END_TAG=$(date -d "$INPUT_TIME_END" +%Y%m%d%H%M) || exit 1
OUTPUT_FILES_EXPECTED=$((INPUT_EXPECTED_DOMAINS * INPUT_HOURS_EXPECTED))
INPUT_DOMAINS_FOUND=0
INPUT_DOMAINS_MISSING=$INPUT_EXPECTED_DOMAINS
OUTPUT_FILES_FOUND=0
OUTPUT_FILES_MISSING=0

# Validate every expected hourly file for every domain (inclusive window).
INPUT_FILES_EXPECTED=$((INPUT_EXPECTED_DOMAINS * INPUT_HOURS_EXPECTED))
INPUT_FILES_FOUND=0
INPUT_FILES_MISSING=0
INPUT_FILES_INVALID=0
validate_data() {
    local verbose="${1:-false}"
    local domain src dst epoch stamp name source target found missing invalid
    INPUT_DOMAINS_FOUND=0
    INPUT_DOMAINS_MISSING=0
    INPUT_FILES_FOUND=0
    INPUT_FILES_MISSING=0
    INPUT_FILES_INVALID=0
    OUTPUT_FILES_EXPECTED=$INPUT_FILES_EXPECTED
    OUTPUT_FILES_FOUND=0
    OUTPUT_FILES_MISSING=0

    for domain in "${INPUT_DOMAINS[@]}"; do
        src="$INPUT_SOURCE/$domain/$INPUT_MODEL_FOLDER/$INPUT_SUBFOLDER"
        dst="$OUTPUT_ROOT/$domain/$OUTPUT_SUBFOLDER"
        found=0; missing=0; invalid=0
        for ((epoch=INPUT_START_EPOCH; epoch<=INPUT_END_EPOCH; epoch+=3600)); do
            stamp=$(date -u -d "@$epoch" '+%Y%m%d%H00') || return 1
            name="hmc.output-grid.${stamp}.nc.gz"
            source="$src/$name"
            target="$dst/$name"
            if [[ ! -f "$source" ]]; then
                INPUT_FILES_MISSING=$((INPUT_FILES_MISSING + 1))
                missing=$((missing + 1))
                [[ "$verbose" == true ]] && echo "[MISSING INPUT] $source"
            elif [[ ! -s "$source" ]]; then
                INPUT_FILES_INVALID=$((INPUT_FILES_INVALID + 1))
                invalid=$((invalid + 1))
                [[ "$verbose" == true ]] && echo "[INVALID INPUT] $source (empty)"
            else
                INPUT_FILES_FOUND=$((INPUT_FILES_FOUND + 1))
                found=$((found + 1))
            fi
            if [[ -s "$target" ]]; then
                OUTPUT_FILES_FOUND=$((OUTPUT_FILES_FOUND + 1))
            else
                OUTPUT_FILES_MISSING=$((OUTPUT_FILES_MISSING + 1))
                [[ "$verbose" == true ]] && echo "[MISSING OUTPUT] $target"
            fi
        done
        if (( found == INPUT_HOURS_EXPECTED )); then
            INPUT_DOMAINS_FOUND=$((INPUT_DOMAINS_FOUND + 1))
        else
            INPUT_DOMAINS_MISSING=$((INPUT_DOMAINS_MISSING + 1))
        fi
        [[ "$verbose" == true ]] && echo "[DOMAIN] $domain input=$found/$INPUT_HOURS_EXPECTED missing=$missing invalid=$invalid"
    done
    if [[ "$verbose" == true ]]; then
        echo "[INPUT] domains=$INPUT_DOMAINS_FOUND/$INPUT_EXPECTED_DOMAINS files=$INPUT_FILES_FOUND/$INPUT_FILES_EXPECTED missing=$INPUT_FILES_MISSING invalid=$INPUT_FILES_INVALID"
        echo "[OUTPUT] files=$OUTPUT_FILES_FOUND/$OUTPUT_FILES_EXPECTED missing=$OUTPUT_FILES_MISSING"
    fi
}
# Check the meteorological model marker for the selected run, before copying.
# Filesystem birth time is not portable; modification time is used as the
# reliable timestamp for comparing the last writes of the marker and forcing.
METEO_RUN_SUBFOLDER=$(date -d "$TIME_RUN" '+%Y/%m/%d/%H%M') || exit 1
METEO_CHECK_FILE="$METEO_CHECK_ROOT/$METEO_RUN_SUBFOLDER/MeteoModelCheck_${TIME_RUN_TAG}.json"
METEO_CHECK_AVAILABLE=false
METEO_CHECK_MTIME=""
INFO_RUN_FOLDER="$INFO_RUN_ROOT/$(date -d "$TIME_RUN" '+%Y/%m/%d')"
INFO_RUN_DATE=$(date -d "$TIME_RUN" "+%Y%m%d") || exit 1
INFO_RUN_HHMM=$(date -d "$TIME_RUN" "+%H%M") || exit 1
INFO_RUN_FILE="$INFO_RUN_FOLDER/info_run_${INFO_RUN_DATE}_${INFO_RUN_HHMM}.json"
INFO_RUN_AVAILABLE=false

check_info_run() {
    INFO_RUN_AVAILABLE=false
    if [[ ! -s "$INFO_RUN_FILE" ]]; then
        echo "[INFO RUN] ERROR: Missing or empty summary: $INFO_RUN_FILE"
        return 2
    fi
    if ! command -v jq >/dev/null 2>&1; then
        echo "[INFO RUN] ERROR: jq is required for JSON validation"
        return 2
    fi
    local expected_run expected_date
    expected_run=$(date -d "$TIME_RUN" '+%H%M') || return 2
    expected_date=$(date -d "$TIME_RUN" '+%Y-%m-%d') || return 2
    if ! jq -e --arg run "$expected_run" --arg day "$expected_date" '
        .run == $run and .run_date == $day and
        .me.found == true and
        (.me.data.DateIni | type == "string" and length > 0) and
        (.me.data.DateEnd | type == "string" and length > 0)
    ' "$INFO_RUN_FILE" >/dev/null 2>&1; then
        echo "[INFO RUN] ERROR: Invalid summary or wrong run: $INFO_RUN_FILE"
        return 2
    fi
    INFO_RUN_AVAILABLE=true
    echo "[INFO RUN] Available and valid: $INFO_RUN_FILE"
    return 0
}

FORCING_OLDER_COUNT=0
FORCING_COMPARED_COUNT=0
FORCING_OLDER_EXAMPLES=""

check_meteo_model() {
    METEO_CHECK_AVAILABLE=false
    METEO_CHECK_MTIME=""
    if [[ ! -s "$METEO_CHECK_FILE" ]]; then
        echo "[MODEL CHECK] ERROR: Missing or empty meteorological model marker: $METEO_CHECK_FILE"
        return 2
    fi
    METEO_CHECK_AVAILABLE=true
    METEO_CHECK_MTIME=$(stat -c %Y "$METEO_CHECK_FILE") || return 2
    echo "[MODEL CHECK] Available: $METEO_CHECK_FILE"
    echo "[MODEL CHECK] Last modified: $(date -d "@$METEO_CHECK_MTIME" '+%Y-%m-%d %H:%M:%S %Z')"
}

check_forcing_age() {
    local domain src epoch stamp source file_mtime
    FORCING_OLDER_COUNT=0
    FORCING_COMPARED_COUNT=0
    FORCING_OLDER_EXAMPLES=""
    for domain in "${INPUT_DOMAINS[@]}"; do
        src="$INPUT_SOURCE/$domain/$INPUT_MODEL_FOLDER/$INPUT_SUBFOLDER"
        for ((epoch=INPUT_START_EPOCH; epoch<=INPUT_END_EPOCH; epoch+=3600)); do
            stamp=$(date -u -d "@$epoch" '+%Y%m%d%H00') || return 1
            source="$src/hmc.output-grid.${stamp}.nc.gz"
            [[ -s "$source" ]] || continue
            file_mtime=$(stat -c %Y "$source") || return 1
            FORCING_COMPARED_COUNT=$((FORCING_COMPARED_COUNT + 1))
            if (( file_mtime < METEO_CHECK_MTIME )); then
                FORCING_OLDER_COUNT=$((FORCING_OLDER_COUNT + 1))
                echo "[FORCING WARNING] Older than MeteoModelCheck: $source (modified $(date -d "@$file_mtime" '+%F %T %Z'))"
                if [[ -z "$FORCING_OLDER_EXAMPLES" ]]; then
                    FORCING_OLDER_EXAMPLES="$source"
                fi
            fi
        done
    done
    echo "[FORCING CHECK] Compared=$FORCING_COMPARED_COUNT older_than_meteo=$FORCING_OLDER_COUNT"
    if (( FORCING_OLDER_COUNT > 0 )); then
        echo "[FORCING WARNING] $FORCING_OLDER_COUNT forcing files were last modified before the meteorological model marker. Continuing (warning only)."
    fi
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

# Separate wrapper lock from the synchronization script lock
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

            validate_data false || exit 1
            if [[ "$FORCE" == false && -s "$INFO_RUN_FILE" && -s "$METEO_CHECK_FILE" && "$INPUT_FILES_FOUND" -eq "$INPUT_FILES_EXPECTED" && "$INPUT_FILES_MISSING" -eq 0 && "$INPUT_FILES_INVALID" -eq 0 && "$OUTPUT_FILES_MISSING" -eq 0 ]]; then

                echo "--------------------------------------------------------------------------------"
                # Do not reuse a prior success if the marker is newer than forcing.
                if ! check_info_run || ! check_meteo_model; then
                    echo "WARNING: Required run metadata invalid; re-running checks."
                    FORCING_OLDER_COUNT=1
                else
                    check_forcing_age
                fi
                if (( FORCING_OLDER_COUNT > 0 )); then
                    echo "WARNING: Existing forcing predates the meteorological marker; re-running synchronization."
                else
                echo "Execution already completed successfully. Skipping."
                echo " "
                echo "Name run   : $NAME_RUN"
                echo "Time run   : $TIME_RUN"
                echo "Wrap file  : $WRAPFILE"
                echo "Exit code  : $PREVIOUS_EXIT_CODE"
                echo " "
                echo "Use -f or --force to execute again."
                echo "--------------------------------------------------------------------------------"

                exit 0
                fi

            else

                echo "WARNING: Previous execution completed successfully."
                echo "WARNING: Force enabled or expected source/output data incomplete. Running again."

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
CMD=(
    "$SCRIPT_FILE"
    --mode now
    --fix "$(date -d "$TIME_RUN" +%H:%M)"
    --config "$CONFIG_FILE"
    --filename-date-regex 'hmc\.output-grid\.(?P<ts>[0-9]{12})\.nc\.gz'
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
echo "Log file          : $LOGFILE"
echo "Wrap file         : $WRAPFILE"
echo "Lock file         : $LOCKFILE"
echo "Wrapper lock file : $WRAPPER_LOCKFILE"
echo "Config            : $CONFIG_FILE"
echo "Script            : $SCRIPT_FILE"
echo "Input root        : $INPUT_SOURCE"
echo "Input subfolder   : $INPUT_SUBFOLDER"
echo "Output root       : $OUTPUT_ROOT"
echo "Output subfolder  : $OUTPUT_SUBFOLDER"
echo "Source file check : matching forecast files within the validation time window"
echo "Validation window : $INPUT_TIME_START -> $INPUT_TIME_END (inclusive)"
echo "Expected domains  : $INPUT_EXPECTED_DOMAINS"
echo "Expected files    : $INPUT_FILES_EXPECTED ($INPUT_HOURS_EXPECTED per domain)"
echo "Meteo check file  : $METEO_CHECK_FILE"
echo "Info run file     : $INFO_RUN_FILE"
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
input_root=$INPUT_SOURCE
input_subfolder=$INPUT_SUBFOLDER
output_root=$OUTPUT_ROOT
output_subfolder=$OUTPUT_SUBFOLDER
input_domains_expected=$INPUT_EXPECTED_DOMAINS
input_files_expected=$INPUT_FILES_EXPECTED
input_files_found=$INPUT_FILES_FOUND
input_files_missing=$INPUT_FILES_MISSING
input_files_invalid=$INPUT_FILES_INVALID
output_files_expected=$OUTPUT_FILES_EXPECTED
info_run_file=$INFO_RUN_FILE
info_run_available=$INFO_RUN_AVAILABLE
meteo_check_file=$METEO_CHECK_FILE
meteo_check_available=$METEO_CHECK_AVAILABLE
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
    echo "Name run    : $NAME_RUN"
    echo "Time run    : $TIME_RUN"
    echo "Force run   : $FORCE"
    echo

    # Check source domains before synchronization.
    echo "--------------------------------------------------------------------------------" | tee /dev/fd/3
    echo "[TRANSFER] Before synchronization: checking source and destination ..." | tee /dev/fd/3
    echo "--------------------------------------------------------------------------------" | tee /dev/fd/3
    if check_info_run; then
        INFO_CHECK_RC=0
    else
        INFO_CHECK_RC=2
    fi
    if check_meteo_model; then
        check_forcing_age
        MODEL_CHECK_RC=0
    else
        MODEL_CHECK_RC=2
    fi
    validate_data true
    if (( INFO_CHECK_RC != 0 || MODEL_CHECK_RC != 0 )); then
        EXIT_CODE=2
        echo "ERROR: Run summary or meteorological model marker unavailable/invalid; synchronization cancelled." | tee /dev/fd/3
    elif (( INPUT_FILES_MISSING > 0 || INPUT_FILES_INVALID > 0 || INPUT_FILES_FOUND != INPUT_FILES_EXPECTED )); then
        EXIT_CODE=6
        echo "ERROR: Incomplete input forecast: found=$INPUT_FILES_FOUND/$INPUT_FILES_EXPECTED missing=$INPUT_FILES_MISSING invalid=$INPUT_FILES_INVALID. Synchronization cancelled." | tee /dev/fd/3
    else
        echo "[TRANSFER] Starting synchronization ..." | tee /dev/fd/3
        echo "[TRANSFER] Source      : $INPUT_SOURCE" | tee /dev/fd/3
        echo "[TRANSFER] Destination : $OUTPUT_ROOT" | tee /dev/fd/3
        if "${CMD[@]}"; then
            EXIT_CODE=0
        else
            EXIT_CODE=$?
        fi
        echo "[TRANSFER] Synchronization exit code: $EXIT_CODE" | tee /dev/fd/3

        # Validate destination files after successful synchronization.
        if [[ "$EXIT_CODE" -eq 0 ]]; then
            echo "[TRANSFER] After synchronization: checking transferred files ..." | tee /dev/fd/3
            validate_data true
            if [[ "$OUTPUT_FILES_FOUND" -eq 0 ]]; then
                EXIT_CODE=7
            elif [[ "$OUTPUT_FILES_MISSING" -gt 0 ]]; then
                EXIT_CODE=8
            fi
        fi
    fi

    echo "[SUMMARY] Info run: $INFO_RUN_AVAILABLE ($INFO_RUN_FILE)" | tee /dev/fd/3
    echo "[TRANSFER] Final check: input=$INPUT_FILES_FOUND/$INPUT_FILES_EXPECTED missing=$INPUT_FILES_MISSING invalid=$INPUT_FILES_INVALID; output=$OUTPUT_FILES_FOUND/$OUTPUT_FILES_EXPECTED missing=$OUTPUT_FILES_MISSING" | tee /dev/fd/3
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
elif [[ "$EXIT_CODE" -eq 2 ]]; then
	RUN_STATUS="forecast_run_not_available"
elif [[ "$EXIT_CODE" -eq 3 ]]; then
    RUN_STATUS="some_input_are_not_available"
elif [[ "$EXIT_CODE" -eq 6 ]]; then
    RUN_STATUS="input_files_incomplete"
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
input_root=$INPUT_SOURCE
input_subfolder=$INPUT_SUBFOLDER
input_domains_expected=$INPUT_EXPECTED_DOMAINS
input_domains_found=$INPUT_DOMAINS_FOUND
input_domains_missing=$INPUT_DOMAINS_MISSING
input_files_expected=$INPUT_FILES_EXPECTED
input_files_found=$INPUT_FILES_FOUND
input_files_missing=$INPUT_FILES_MISSING
input_files_invalid=$INPUT_FILES_INVALID
output_root=$OUTPUT_ROOT
output_subfolder=$OUTPUT_SUBFOLDER
output_time_start=$INPUT_TIME_START
output_time_end=$INPUT_TIME_END
output_validation=matching_source_files
output_files_expected=$OUTPUT_FILES_EXPECTED
output_files_found=$OUTPUT_FILES_FOUND
output_files_missing=$OUTPUT_FILES_MISSING
info_run_file=$INFO_RUN_FILE
info_run_available=$INFO_RUN_AVAILABLE
meteo_check_file=$METEO_CHECK_FILE
meteo_check_available=$METEO_CHECK_AVAILABLE
meteo_check_modified_epoch=$METEO_CHECK_MTIME
forcing_files_compared=$FORCING_COMPARED_COUNT
forcing_files_older_than_meteo=$FORCING_OLDER_COUNT
forcing_older_example=$FORCING_OLDER_EXAMPLES
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
echo "Input files: $INPUT_FILES_FOUND/$INPUT_FILES_EXPECTED (missing=$INPUT_FILES_MISSING invalid=$INPUT_FILES_INVALID)"
echo "Output files: $OUTPUT_FILES_FOUND/$OUTPUT_FILES_EXPECTED (missing=$OUTPUT_FILES_MISSING)"
echo "Meteo marker : $METEO_CHECK_FILE (available=$METEO_CHECK_AVAILABLE)"
echo "Info run     : $INFO_RUN_FILE (available=$INFO_RUN_AVAILABLE)"
echo "Forcing age  : $FORCING_OLDER_COUNT/$FORCING_COMPARED_COUNT older than meteo marker (warning only)"
echo "Exit code  : $EXIT_CODE"
echo "Log file   : $LOGFILE"
echo "Wrap file  : $WRAPFILE"
echo "Lock file  : $LOCKFILE"
echo "--------------------------------------------------------------------------------"
echo " "

# Exit using command exit code
exit "$EXIT_CODE"
# -------------------------------------------------------------------------------------
