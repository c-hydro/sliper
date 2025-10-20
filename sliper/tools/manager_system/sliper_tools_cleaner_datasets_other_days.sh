#!/bin/bash
set -euo pipefail

# --- DEFAULTS ---
CATCH_FILE=""
WHEN=""
CUSTOM_DATE=""
NDAYS=1
DRY_RUN=false
LOG_FILE=""

# --- PATH TEMPLATES ---
RAIN_TEMPLATE="/home/admin/soilslips-ws/storage/source/data/rain/obs/{YYYY}/{MM}/{DD}"
SOIL_TEMPLATE="/home/admin/soilslips-ws/storage/source/data/soil_moisture/obs/{catchment}/{YYYY}/{MM}/{DD}"

# --- LOG FUNCTION ---
log() {
  local msg="$1"
  local timestamp="[$(date '+%Y-%m-%d %H:%M:%S')]"
  echo "${timestamp} ${msg}"
  if [[ -n "${LOG_FILE:-}" ]]; then
    echo "${timestamp} ${msg}" >> "$LOG_FILE"
  fi
}

# --- USAGE ---
print_usage() {
  cat <<EOF
Usage:
  $0 --catch-file FILE [--when today|today-N | --date YYYYMMDD] [--n-days N] [--dry-run] [--log-file FILE]

Options:
  --catch-file FILE     File listing catchments (one per line; # comments allowed)
  --when today|today-N  Choose base date relative to today (e.g. today-2)
  --date YYYYMMDD       Custom base date (mutually exclusive with --when)
  --n-days N            Number of previous days to process (default: 1)
  --dry-run             Show deletions without actually removing files
  --log-file FILE       Save logs to a file (directory will be created if missing)

Examples:
  $0 --catch-file catchments.txt --when today
  $0 --catch-file catchments.txt --when today-2
  $0 --catch-file catchments.txt --date 20251015 --n-days 3
EOF
}

# --- ARGUMENT PARSING ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --catch-file) CATCH_FILE="${2:-}"; shift 2 ;;
    --when) WHEN="${2:-}"; shift 2 ;;
    --date) CUSTOM_DATE="${2:-}"; shift 2 ;;
    --n-days) NDAYS="${2:-1}"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --log-file) LOG_FILE="${2:-}"; shift 2 ;;
    -h|--help) print_usage; exit 0 ;;
    *) echo "Unknown argument: $1"; print_usage; exit 1 ;;
  esac
done

# --- VALIDATION ---
[[ -z "$CATCH_FILE" ]] && { echo "❌ --catch-file required"; exit 1; }
[[ ! -f "$CATCH_FILE" ]] && { echo "❌ Catchment file not found: $CATCH_FILE"; exit 1; }

if [[ -n "$WHEN" && -n "$CUSTOM_DATE" ]]; then
  echo "❌ Use only one of --when or --date"; exit 1
fi

if [[ -z "$WHEN" && -z "$CUSTOM_DATE" ]]; then
  echo "❌ Specify either --when today|today-N or --date YYYYMMDD"; exit 1
fi

if [[ -n "$CUSTOM_DATE" && ! "$CUSTOM_DATE" =~ ^[0-9]{8}$ ]]; then
  echo "❌ --date must be in YYYYMMDD format (e.g. 20251015)"; exit 1
fi

if ! [[ "$NDAYS" =~ ^[0-9]+$ && "$NDAYS" -ge 1 ]]; then
  echo "❌ --n-days must be a positive integer"; exit 1
fi

# --- ENSURE LOG DIRECTORY EXISTS ---
if [[ -n "$LOG_FILE" ]]; then
  LOG_DIR="$(dirname "$LOG_FILE")"
  if [[ ! -d "$LOG_DIR" ]]; then
    echo "[INFO] Creating log directory: $LOG_DIR"
    mkdir -p "$LOG_DIR" || { echo "❌ Failed to create log directory: $LOG_DIR"; exit 1; }
  fi
fi

# --- DETERMINE BASE DATE ---
if [[ -n "$WHEN" ]]; then
  if [[ "$WHEN" =~ ^today$ ]]; then
    BASE_DATE="$(date +%Y%m%d)"
  elif [[ "$WHEN" =~ ^today-([0-9]+)$ ]]; then
    OFFSET="${BASH_REMATCH[1]}"
    BASE_DATE="$(date -d "today -${OFFSET} day" +%Y%m%d)"
  else
    echo "❌ Invalid --when value: $WHEN (use today or today-N)"; exit 1
  fi
else
  BASE_DATE="$CUSTOM_DATE"
fi

# --- GENERATE DATE LIST ---
dates_to_process=()
for ((i=0; i<NDAYS; i++)); do
  d=$(date -d "${BASE_DATE} -${i} day" +%Y%m%d)
  dates_to_process+=("$d")
done

# --- LOG HEADER ---
log "=============================================================="
log "Run configuration:"
log "Catchment file: $CATCH_FILE"
log "Base date: $BASE_DATE"
log "When: ${WHEN:-N/A}"
log "Days to process: $NDAYS"
log "Dry run: $DRY_RUN"
[[ -n "$LOG_FILE" ]] && log "Log file: $LOG_FILE"
log "Dates: ${dates_to_process[*]}"
log "=============================================================="

# --- CLEAN FUNCTION ---
process_find() {
  local dir="$1"
  local tag="$2"

  if [[ -d "$dir" ]]; then
    log "Directory: $dir"
    while IFS= read -r -d '' file; do
      if [[ "$file" == *"$tag"* ]]; then
        log "KEEP: $file"
      else
        if $DRY_RUN; then
          log "WOULD REMOVE: $file"
        else
          log "REMOVE: $file"
          rm -v "$file" >> "${LOG_FILE:-/dev/null}" 2>&1
        fi
      fi
    done < <(find "$dir" -type f -print0)
  else
    log "MISSING: $dir"
  fi
}

# --- MAIN LOOP ---
for KEEP_TAG in "${dates_to_process[@]}"; do
  YEAR="${KEEP_TAG:0:4}"
  MONTH="${KEEP_TAG:4:2}"
  DAY="${KEEP_TAG:6:2}"

  RAIN_DIR="${RAIN_TEMPLATE//\{YYYY\}/$YEAR}"
  RAIN_DIR="${RAIN_DIR//\{MM\}/$MONTH}"
  RAIN_DIR="${RAIN_DIR//\{DD\}/$DAY}"

  log "=============================================================="
  log "Processing date: $KEEP_TAG ($YEAR/$MONTH/$DAY)"
  log "=============================================================="

  process_find "$RAIN_DIR" "$KEEP_TAG"

  while IFS= read -r catchment || [[ -n "$catchment" ]]; do
    [[ -z "$catchment" || "$catchment" =~ ^# ]] && continue

    SOIL_DIR="${SOIL_TEMPLATE//\{catchment\}/$catchment}"
    SOIL_DIR="${SOIL_DIR//\{YYYY\}/$YEAR}"
    SOIL_DIR="${SOIL_DIR//\{MM\}/$MONTH}"
    SOIL_DIR="${SOIL_DIR//\{DD\}/$DAY}"

    process_find "$SOIL_DIR" "$KEEP_TAG"
  done < "$CATCH_FILE"
done

log "Cleanup completed for ${#dates_to_process[@]} day(s)."
log "=============================================================="

