#!/usr/bin/env bash
set -Eeuo pipefail

# -------------------------------------------------------------------------------------
# Configuration
# Copy observed soil moisture into the selected forecast run (18 domains).
# Window: run date - 2 days at 00:00 through run date at HH:MM, inclusive.
ROOT="/home/admin/soilslips-system/manager_data"
NAME_RUN="sync_data_sm_obs4frc"
LOG_FOLDER="/home/admin/log/manager_data"
WRAP_FOLDER="/home/admin/info/manager_data"
LOCK_FOLDER="/home/admin/lock/manager_data"
INPUT_ROOT="/mnt/ModelloContinuum/archive"
OUTPUT_ROOT="/home/admin/soilslips-ws/data_dynamic/source/data/soil_moisture/frc"
INPUT_MODEL_FOLDER="weather_stations_realtime"
INPUT_DAYS_BEFORE=2

# -------------------------------------------------------------------------------------
# Parse arguments
RUN_TIME=""
FORCE=false

DOMAINS=(
  AvetoTrebbiaDomain BormidaMDomain BormidaSDomain CentaDomain
  CentroPonenteDomain EntellaDomain ErroDomain FinaleseDomain
  ImperieseDomain LevanteGenoveseDomain MagraDomain OrbaSturaDomain
  PonenteDomain PonenteGenoveseDomain RoiaDomain SavoneseDomain
  ScriviaDomain TanaroDomain
)

# -------------------------------------------------------------------------------------

while (($#)); do
  case "$1" in
    --run-time)
      [[ $# -ge 2 ]] || { echo 'ERROR: --run-time requires HH:MM'; exit 1; }
      RUN_TIME="$2"; shift 2 ;;
    --input-days-before)
      [[ $# -ge 2 && "$2" =~ ^[0-9]+$ ]] || { echo 'ERROR: --input-days-before requires a nonnegative integer'; exit 1; }
      INPUT_DAYS_BEFORE="$2"; shift 2 ;;
    -f|--force) FORCE=true; shift ;;
    -h|--help)
      echo "Usage: $0 [--run-time HH:MM] [--input-days-before N] [-f|--force]"
      echo 'Copies hourly observation files into each domain forecast-run folder, never past the run hour.'
      exit 0 ;;
    *) echo "ERROR: Unknown argument: $1"; exit 1 ;;
  esac
done

# -------------------------------------------------------------------------------------
# Define run time
if [[ -n "$RUN_TIME" ]]; then
  [[ "$RUN_TIME" =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]] || { echo "ERROR: Invalid run time: $RUN_TIME"; exit 1; }
  TIME_RUN="$(date '+%Y-%m-%d') ${RUN_TIME}:00"
else
  TIME_RUN="$(date '+%Y-%m-%d %H:00:00')"
fi
# -------------------------------------------------------------------------------------
# Define time tag and observation window
TIME_RUN_TAG=$(date -d "$TIME_RUN" '+%Y%m%d%H%M')
RUN_DATE=$(date -d "$TIME_RUN" '+%Y-%m-%d')
RUN_SUBFOLDER=$(date -d "$TIME_RUN" '+%Y/%m/%d/%H%M')
START_DATE=$(date -d "$RUN_DATE - $INPUT_DAYS_BEFORE days" '+%Y-%m-%d')
START_EPOCH=$(date -d "$START_DATE 00:00:00" '+%s')
END_EPOCH=$(date -d "$TIME_RUN" '+%s')
# Hourly files only; HH:MM may not be an exact hour, in which case the latest full hour is included.
END_EPOCH=$((END_EPOCH - END_EPOCH % 3600))
EXPECTED_PER_DOMAIN=$(((END_EPOCH - START_EPOCH) / 3600 + 1))
EXPECTED_TOTAL=$((EXPECTED_PER_DOMAIN * ${#DOMAINS[@]}))

# -------------------------------------------------------------------------------------
# Define log, info and lock files
mkdir -p "$LOG_FOLDER" "$WRAP_FOLDER" "$LOCK_FOLDER"
LOGFILE="$LOG_FOLDER/${NAME_RUN}_${TIME_RUN_TAG}.log"
WRAPFILE="$WRAP_FOLDER/${NAME_RUN}_${TIME_RUN_TAG}.info"
LOCKFILE="$LOCK_FOLDER/${NAME_RUN}_${TIME_RUN_TAG}.lock"
WRAPPER_LOCKFILE="$LOCK_FOLDER/${NAME_RUN}_${TIME_RUN_TAG}.wrapper.lock"

# -------------------------------------------------------------------------------------
# Acquire non-blocking locks (never bypass with --force)
exec 200>"$WRAPPER_LOCKFILE"
if ! flock -n 200; then
  echo "ERROR: Active wrapper lock: $WRAPPER_LOCKFILE (force cannot bypass it)"
  exit 1
fi
# Retain the conventional synchronization lock to prevent overlapping jobs using the same slot.
exec 201>"$LOCKFILE"
if ! flock -n 201; then
  echo "ERROR: Active synchronization lock: $LOCKFILE (force cannot bypass it)"
  exit 1
fi

# -------------------------------------------------------------------------------------
# Initialize validation counters
INPUT_FILES_FOUND=0
INPUT_FILES_MISSING=0
INPUT_FILES_INVALID=0
OUTPUT_FILES_FOUND=0
OUTPUT_FILES_MISSING=0
COPIED_FILES=0
COPY_ERRORS=0

# -------------------------------------------------------------------------------------
# Check source and destination files
# Never use a pipeline here: counters must persist.
check_files() {
  local verbose="$1" domain epoch stamp day folder name source target size_src size_dst
  INPUT_FILES_FOUND=0; INPUT_FILES_MISSING=0; INPUT_FILES_INVALID=0
  OUTPUT_FILES_FOUND=0; OUTPUT_FILES_MISSING=0
  for domain in "${DOMAINS[@]}"; do
    local domain_in=0 domain_missing=0 domain_out=0
    for ((epoch=START_EPOCH; epoch<=END_EPOCH; epoch+=3600)); do
      stamp=$(date -d "@$epoch" '+%Y%m%d%H%M')
      day=$(date -d "@$epoch" '+%Y/%m/%d')
      name="hmc.output-grid.${stamp}.nc.gz"
      source="$INPUT_ROOT/$domain/$INPUT_MODEL_FOLDER/$day/gridded/$name"
      target="$OUTPUT_ROOT/$domain/$RUN_SUBFOLDER/$name"
      if [[ ! -f "$source" ]]; then
        ((INPUT_FILES_MISSING+=1)); ((domain_missing+=1))
        [[ "$verbose" == true ]] && echo "[MISSING INPUT] $source"
        continue
      fi
      if [[ ! -s "$source" ]]; then
        ((INPUT_FILES_INVALID+=1))
        [[ "$verbose" == true ]] && echo "[INVALID INPUT] $source (empty)"
        continue
      fi
      ((INPUT_FILES_FOUND+=1)); ((domain_in+=1))
      size_src=$(stat -c '%s' "$source")
      if [[ -s "$target" ]]; then
        size_dst=$(stat -c '%s' "$target")
      else
        size_dst=0
      fi
      if [[ "$size_src" == "$size_dst" && "$size_dst" -gt 0 ]]; then
        ((OUTPUT_FILES_FOUND+=1)); ((domain_out+=1))
      else
        ((OUTPUT_FILES_MISSING+=1))
        [[ "$verbose" == true ]] && echo "[MISSING/MISMATCH OUTPUT] $target (source=$size_src bytes, destination=$size_dst bytes)"
      fi
    done
    [[ "$verbose" == true ]] && echo "[DOMAIN CHECK] $domain expected=$EXPECTED_PER_DOMAIN input=$domain_in missing_input=$domain_missing matching_output=$domain_out"
  done
  [[ "$verbose" == true ]] && echo "[SUMMARY] expected=$EXPECTED_TOTAL input_found=$INPUT_FILES_FOUND input_missing=$INPUT_FILES_MISSING input_invalid=$INPUT_FILES_INVALID output_ok=$OUTPUT_FILES_FOUND output_missing=$OUTPUT_FILES_MISSING"
}

# -------------------------------------------------------------------------------------
# Write atomic .info status
write_info() {
  local status="$1" code="$2" finished="$3" tmp
  tmp=$(mktemp "${WRAPFILE}.tmp.XXXXXX") || return 1
  cat > "$tmp" <<INFO
name_run=$NAME_RUN
time_start=$TIME_START
time_end=$finished
time_run=$TIME_RUN
exit_code=$code
status=$status
input_root=$INPUT_ROOT
output_root=$OUTPUT_ROOT
output_subfolder=$RUN_SUBFOLDER
input_time_start=$START_DATE 00:00:00
input_time_end=$(date -d "@$END_EPOCH" '+%Y-%m-%d %H:%M:%S')
input_files_expected=$EXPECTED_TOTAL
input_files_found=$INPUT_FILES_FOUND
input_files_missing=$INPUT_FILES_MISSING
input_files_invalid=$INPUT_FILES_INVALID
output_files_found=$OUTPUT_FILES_FOUND
output_files_missing=$OUTPUT_FILES_MISSING
copied_files=$COPIED_FILES
copy_errors=$COPY_ERRORS
INFO
  mv -f "$tmp" "$WRAPFILE"
}

# -------------------------------------------------------------------------------------
# Print run configuration
printf '%s\n' '--------------------------------------------------------------------------------'
echo "Name run         : $NAME_RUN"
echo "Time run         : $TIME_RUN"
echo "Input window     : $START_DATE 00:00:00 -> $(date -d "@$END_EPOCH" '+%Y-%m-%d %H:%M:%S') (inclusive)"
echo "Forecast folder  : $RUN_SUBFOLDER"
echo "Expected         : $EXPECTED_PER_DOMAIN files/domain x ${#DOMAINS[@]} domains = $EXPECTED_TOTAL"
echo "Force            : $FORCE"
echo "Log file         : $LOGFILE"
echo "Info file        : $WRAPFILE"
echo '--------------------------------------------------------------------------------'

# -------------------------------------------------------------------------------------
# Check previous execution status
# Skip only when the stored exit_code is 0 and --force was not requested.
# A failed, missing or malformed .info always triggers a new execution.
if [[ -f "$WRAPFILE" ]]; then
  PREVIOUS_EXIT_CODE=$(sed -n 's/^exit_code=//p' "$WRAPFILE" | tail -n 1)

  if [[ "$PREVIOUS_EXIT_CODE" =~ ^[0-9]+$ ]]; then
    if [[ "$PREVIOUS_EXIT_CODE" -eq 0 ]]; then
      if [[ "$FORCE" == false ]]; then
        echo '[SKIP] Previous execution completed successfully (exit_code=0).'
        echo "[SKIP] Info file: $WRAPFILE"
        echo '[SKIP] Use --force to execute again.'
        exit 0
      fi
      echo '[REPEAT] Previous execution succeeded, but --force was specified.'
    else
      echo "[REPEAT] Previous execution failed (exit_code=$PREVIOUS_EXIT_CODE)."
    fi
  else
    echo '[REPEAT] Previous .info has no valid exit_code.'
  fi
else
  echo '[RUN] No previous .info file found.'
fi

# -------------------------------------------------------------------------------------
# Initialize execution status and log
TIME_START=$(date '+%Y-%m-%d %H:%M:%S')
write_info running 1 ''
: > "$LOGFILE"
# Log to both console and file; no pipeline around the processing loop.
exec 3>&1
exec > >(tee -a "$LOGFILE" >&3) 2>&1

# -------------------------------------------------------------------------------------
# Execute transfer and verify files
echo '[CHECK] Before copying:'
check_files true
echo '[TRANSFER] Copying observed files into forecast-run folders ...'
for domain in "${DOMAINS[@]}"; do
  domain_copied=0
  for ((epoch=START_EPOCH; epoch<=END_EPOCH; epoch+=3600)); do
    stamp=$(date -d "@$epoch" '+%Y%m%d%H%M')
    day=$(date -d "@$epoch" '+%Y/%m/%d')
    name="hmc.output-grid.${stamp}.nc.gz"
    source="$INPUT_ROOT/$domain/$INPUT_MODEL_FOLDER/$day/gridded/$name"
    target="$OUTPUT_ROOT/$domain/$RUN_SUBFOLDER/$name"
    [[ -s "$source" ]] || continue
    if [[ -s "$target" && $(stat -c '%s' "$source") == $(stat -c '%s' "$target") ]]; then
      continue
    fi
    if ! mkdir -p "$(dirname "$target")"; then
      echo "[COPY ERROR] Cannot create folder: $(dirname "$target")"
      ((COPY_ERRORS+=1)); continue
    fi
    # Atomic file replacement prevents consumers from seeing partial output.
    tmp="${target}.tmp.$$"
    if cp -p -- "$source" "$tmp" && mv -f -- "$tmp" "$target"; then
      ((COPIED_FILES+=1)); ((domain_copied+=1))
      echo "[COPIED] $source -> $target"
    else
      rm -f -- "$tmp"
      ((COPY_ERRORS+=1))
      echo "[COPY ERROR] $source -> $target"
    fi
  done
  echo "[DOMAIN COPY] $domain copied=$domain_copied"
done

echo '[CHECK] After copying:'
check_files true
# -------------------------------------------------------------------------------------
# Resolve final status and exit code
EXIT_CODE=0
RUN_STATUS=done
if (( INPUT_FILES_FOUND == 0 )); then EXIT_CODE=6; RUN_STATUS=no_input_found
elif (( INPUT_FILES_MISSING > 0 || INPUT_FILES_INVALID > 0 )); then EXIT_CODE=3; RUN_STATUS=some_input_are_not_available
elif (( OUTPUT_FILES_FOUND == 0 )); then EXIT_CODE=7; RUN_STATUS=no_output_found
elif (( OUTPUT_FILES_MISSING > 0 || COPY_ERRORS > 0 )); then EXIT_CODE=8; RUN_STATUS=some_output_are_not_available
fi
# -------------------------------------------------------------------------------------
# Save final execution status
TIME_END=$(date '+%Y-%m-%d %H:%M:%S')
write_info "$RUN_STATUS" "$EXIT_CODE" "$TIME_END"
echo "[FINAL] status=$RUN_STATUS exit_code=$EXIT_CODE copied=$COPIED_FILES expected=$EXPECTED_TOTAL output_ok=$OUTPUT_FILES_FOUND missing_input=$INPUT_FILES_MISSING missing_output=$OUTPUT_FILES_MISSING"
exit "$EXIT_CODE"
