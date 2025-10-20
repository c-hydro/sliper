#!/usr/bin/env bash
set -euo pipefail

# -------------------------------------------------------------------------
# sliper_tools_recover_data.sh
#
# Copy hourly observation files (RAIN or SOIL MOISTURE) between date ranges.
#
# DESTINATION TEMPLATES (derived per timestamp):
#   - RAIN: /home/admin/soilslips-ws/storage/source/data/rain/obs/{Y}/{m}/{d}/
#   - SM  : /home/admin/soilslips-ws/storage/source/data/soil_moisture/obs/{catchment}/{Y}/{m}/{d}/
#
# SM SOURCE TEMPLATE (per catchment, per date):
#   /mnt/ModelloContinuum/archive/{catchment}/weather_stations_realtime/{Y}/{m}/{d}/gridded/
#   file: hmc.output-grid.YYYYMMDDHHMM.nc.gz
#
# RAIN SOURCE TEMPLATE:
#   /mnt/MeteoData/RainGRISO/Rain_YYYYMMDDHHMM.tif
#
# Minutes are preserved from --start (e.g., 16:30 → every hour at :30).
# -------------------------------------------------------------------------

RAIN_ROOT_DEFAULT="/mnt/MeteoData/RainGRISO"
SM_ROOT_DEFAULT="/mnt/ModelloContinuum/archive"
RAIN_DEST_BASE_DEFAULT="/home/admin/soilslips-ws/storage/source/data/rain/obs"
SM_DEST_BASE_DEFAULT="/home/admin/soilslips-ws/storage/source/data/soil_moisture/obs"

usage() {
  cat <<'EOF'
Usage:
  sliper_tools_recover_data.sh --dataset {rain|sm} \
    --start "YYYY-MM-DD HH:MM" --end "YYYY-MM-DD HH:MM" \
    [--rain-root PATH] [--sm-root PATH] \
    [--rain-dest-base PATH] [--sm-dest-base PATH] \
    [--catchment NAME | --catchments-file FILE] \
    [--log-file PATH] [--dry-run]

DESTINATION (auto from each timestamp):
  - Rain: RAIN_DEST_BASE/{Y}/{m}/{d}/
  - SM  : SM_DEST_BASE/{catchment}/{Y}/{m}/{d}/

SOURCE:
  - Rain: RAIN_ROOT/Rain_YYYYMMDDHHMM.tif
  - SM  : SM_ROOT/{catchment}/weather_stations_realtime/{Y}/{m}/{d}/gridded/hmc.output-grid.YYYYMMDDHHMM.nc.gz

Notes:
  - For SM use either --catchment or --catchments-file (one per line). '#' and blanks ignored.
  - --dry-run prints actions without copying; --log-file also appends to a file.

Examples:
  # Rain
  ./sliper_tools_recover_data.sh --dataset rain \
    --start "2025-10-01 00:00" --end "2025-10-05 23:00"

  # SM, many catchments
  ./sliper_tools_recover_data.sh --dataset sm \
    --start "2025-10-11 00:00" --end "2025-10-12 12:00" \
    --catchments-file /path/to/catchments.txt
EOF
}

die() { echo "ERROR: $*" >&2; exit 1; }
have_gnu_date() { date --version >/dev/null 2>&1; }

trim() {
  local s="$1"
  s="${s//$'\r'/}"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

log() {
  local msg="$1"
  echo "$msg"
  if [[ -n "${LOG_FILE:-}" ]]; then
    echo "$msg" >> "$LOG_FILE"
  fi
}

# ---- Args
DATASET="" START="" END=""
RAIN_ROOT="$RAIN_ROOT_DEFAULT" SM_ROOT="$SM_ROOT_DEFAULT"
RAIN_DEST_BASE="$RAIN_DEST_BASE_DEFAULT" SM_DEST_BASE="$SM_DEST_BASE_DEFAULT"
CATCHMENT="" CATCHMENTS_FILE="" DRY_RUN=0 LOG_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dataset) DATASET="${2:-}"; shift 2;;
    --start) START="${2:-}"; shift 2;;
    --end) END="${2:-}"; shift 2;;
    --rain-root) RAIN_ROOT="${2:-}"; shift 2;;
    --sm-root) SM_ROOT="${2:-}"; shift 2;;
    --rain-dest-base) RAIN_DEST_BASE="${2:-}"; shift 2;;
    --sm-dest-base) SM_DEST_BASE="${2:-}"; shift 2;;
    --catchment) CATCHMENT="${2:-}"; shift 2;;
    --catchments-file) CATCHMENTS_FILE="${2:-}"; shift 2;;
    --log-file) LOG_FILE="${2:-}"; shift 2;;
    --dry-run) DRY_RUN=1; shift;;
    -h|--help) usage; exit 0;;
    *) die "Unknown argument: $1";;
  esac
done

[[ -n "$DATASET" && -n "$START" && -n "$END" ]] || { usage; exit 1; }
[[ "$DATASET" == "rain" || "$DATASET" == "sm" ]] || die "--dataset must be 'rain' or 'sm'"
have_gnu_date || die "GNU date required."

to_epoch() { date -d "$1" +%s; }
START_EPOCH=$(to_epoch "$START")
END_EPOCH=$(to_epoch "$END")
(( END_EPOCH >= START_EPOCH )) || die "--end must be >= --start"

# Validate SM inputs
if [[ "$DATASET" == "sm" ]]; then
  if [[ -n "$CATCHMENT" && -n "$CATCHMENTS_FILE" ]]; then
    die "Use either --catchment or --catchments-file, not both."
  fi
  if [[ -z "$CATCHMENT" && -z "$CATCHMENTS_FILE" ]]; then
    die "For --dataset sm you must provide --catchment or --catchments-file."
  fi
fi

copy_file() {
  local src="$1" dest_dir="$2" tag="$3"  # tag = RAIN or SM:<catchment>
  local bn; bn=$(basename "$src")
  if [[ -f "$src" ]]; then
    mkdir -p "$dest_dir"
    if (( DRY_RUN )); then
      log "[DRY] $tag COPY  $src  ->  $dest_dir/$bn"
    else
      if cp -n "$src" "$dest_dir/$bn"; then
        log "[OK ] $tag COPY  $src  ->  $dest_dir/$bn"
      else
        log "[ERR] $tag COPY FAILED for $src"
      fi
    fi
  else
    log "[MISS] $tag       $src"
  fi
}

# Prepare catchment list
declare -a CATCHMENTS=()
if [[ "$DATASET" == "sm" ]]; then
  if [[ -n "$CATCHMENT" ]]; then
    CATCHMENTS+=("$CATCHMENT")
  else
    [[ -f "$CATCHMENTS_FILE" ]] || die "Catchments file not found: $CATCHMENTS_FILE"
    while IFS= read -r line || [[ -n "$line" ]]; do
      line="$(trim "$line")"
      [[ -z "$line" || "${line:0:1}" == "#" ]] && continue
      CATCHMENTS+=("$line")
    done < "$CATCHMENTS_FILE"
    (( ${#CATCHMENTS[@]} > 0 )) || die "No valid catchments in $CATCHMENTS_FILE"
  fi
fi

log "Dataset       : $DATASET"
log "From          : $START"
log "To            : $END"
log "RAIN root     : $RAIN_ROOT"
log "SM root       : $SM_ROOT"
log "RAIN dest base: $RAIN_DEST_BASE/{Y}/{m}/{d}"
log "SM dest base  : $SM_DEST_BASE/{catchment}/{Y}/{m}/{d}"
if [[ "$DATASET" == "sm" ]]; then
  log "Catchments    : ${CATCHMENTS[*]}"
fi
[[ -n "$LOG_FILE" ]] && log "Logging to    : $LOG_FILE"
(( DRY_RUN )) && log "[DRY RUN]"

CUR=$START_EPOCH
while (( CUR <= END_EPOCH )); do
  TS=$(date -d "@$CUR" +%Y%m%d%H%M)
  Y=$(date -d "@$CUR" +%Y)
  m=$(date -d "@$CUR" +%m)
  d=$(date -d "@$CUR" +%d)

  if [[ "$DATASET" == "rain" ]]; then
    SRC="$RAIN_ROOT/Rain_${TS}.tif"
    DEST_DIR="$RAIN_DEST_BASE/$Y/$m/$d"
    copy_file "$SRC" "$DEST_DIR" "RAIN"
  else
    for C in "${CATCHMENTS[@]}"; do
      SM_SRC_DIR="$SM_ROOT/$C/weather_stations_realtime/$Y/$m/$d/gridded"
      SRC="$SM_SRC_DIR/hmc.output-grid.${TS}.nc.gz"
      DEST_DIR="$SM_DEST_BASE/$C/$Y/$m/$d"     # <-- includes {catchment}
      copy_file "$SRC" "$DEST_DIR" "SM:$C"
    done
  fi

  CUR=$(( CUR + 3600 ))   # +1 hour, portable
done

log "Done."

