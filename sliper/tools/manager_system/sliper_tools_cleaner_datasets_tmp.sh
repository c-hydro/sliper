#!/usr/bin/env bash
# --------------------------------------------------------------------
# SoilSlips cleaner: keep only files with a target extension (default .gz)
# in a dated tree whose ROOT is defined with tags.
#
# Supported ROOT forms (choose ONE as your dataset root):
#   A) With catchment: ROOT=.../obs/{catchment_name}/{Y}/{m}/{d}
#   B) No catchment:   ROOT=.../obs/{Y}/{m}/{d}
#
# Modes:
#   --mode realtime  : from today back N days (inclusive)
#   --mode history   : from --start YYYY-MM-DD to --end YYYY-MM-DD (inclusive)
#
# Options:
#   --root/-r TAGGED_PATH   : dataset root with tags
#   --n-days/-n N           : realtime lookback (non-negative int)
#   --start/-s YYYY-MM-DD   : history start date
#   --end/-e   YYYY-MM-DD   : history end date
#   --ext/-x .EXT           : extension to keep (default .gz)
#   --dry-run/-d            : print actions, do not delete
#   --verbose/-v            : extra logs
#   --help/-h               : this help
#
# Basins (only if ROOT contains {catchment_name}):
#   - Option 1: hardcode BASINS=(Arno Tevere ...)
#   - Option 2: provide a file (one basin per line) at $BASIN_FILE
#
# Requirements: Linux with GNU date/find. TZ default Europe/Rome.
#
# --- CHANGELOG (2025-10-27)
# * Hardened arg parsing for --start/--end and other options.
# * Added explicit validation before calling collect_dates_history to avoid
#   "variabile non assegnata" on local 's'/'e' when args are missing.
# * Clearer error messages; consistent exits.
# --------------------------------------------------------------------

# --- Force bash execution (auto-relaunch if run under sh) ---
if [ -z "${BASH_VERSION:-}" ]; then
  echo "[INFO] Relaunching under bash..."
  exec bash "$0" "$@"
fi

set -euo pipefail

# ---- Logging & helpers ----
log()  { echo "[INFO] $*"; }
warn() { echo "[WARN] $*" >&2; }
err()  { echo "[ERROR] $*" >&2; }
die()  { err "$*"; exit 1; }

# ---- Defaults ----
: "${TZ:=Europe/Rome}"

MODE=""          # realtime | history
N_DAYS=""
START_DATE=""
END_DATE=""
DRY_RUN=false
VERBOSE=false
EXT=".gz"

# Set your dataset ROOT with tags (you can override with --root):
# Example with catchments:
ROOT="/home/admin/soilslips-ws/storage/data/source/soil_moisture/obs/{catchment_name}/{Y}/{m}/{d}"
# Example without catchments:
# ROOT="/home/admin/soilslips-ws/storage/data/source/soil_moisture/obs/{Y}/{m}/{d}"

# Basins (used only if ROOT contains {catchment_name})
# Option 1: uncomment and set your basins here:
# BASINS=(Arno Tevere Reno)
# Option 2: read from file if set and BASINS not defined:
BASIN_FILE="/home/admin/soilslips-system/manager_system/sliper_tools_cleaner_datasets_catchments.txt"
BASINS=()

usage() {
  sed -n '1,120p' "$0" | sed -n '1,60p' | sed 's/^# \{0,1\}//'
  cat <<'EOF'
Examples:
  # Per-basin root (A) – dry run for last 2 days
  bash sliper_tools_cleaner_datasets_tmp.sh --mode realtime --n-days 2 \
    --root /home/admin/soilslips-ws/storage/data/source/soil_moisture/obs/{catchment_name}/{Y}/{m}/{d} --dry-run

  # Single-path root (B) – history window
  bash sliper_tools_cleaner_datasets_tmp.sh -m history -s 2025-10-01 -e 2025-10-05 \
    -r /home/admin/soilslips-ws/storage/data/source/soil_moisture/obs/{Y}/{m}/{d}
EOF
}

# ---- Robust arg parsing (no GNU getopt) ----
need_val() {
  # Ensure next arg exists and isn't another flag
  [[ $# -ge 2 ]] || die "Missing value for $1"
  [[ "${2:-}" != "" && "${2:0:1}" != "-" ]] || die "Invalid value for $1: ${2:-<empty>}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode|-m)       need_val "$@"; MODE="$2"; shift 2;;
    --n-days|-n)     need_val "$@"; N_DAYS="$2"; shift 2;;
    --start|-s)      need_val "$@"; START_DATE="$2"; shift 2;;
    --end|-e)        need_val "$@"; END_DATE="$2"; shift 2;;
    --root|-r)       need_val "$@"; ROOT="$2"; shift 2;;
    --ext|-x)        need_val "$@"; EXT="$2"; shift 2;;
    --dry-run|-d)    DRY_RUN=true; shift;;
    --verbose|-v)    VERBOSE=true; shift;;
    --help|-h)       usage; exit 0;;
    -*)              die "Unknown option: $1";;
    *)               break;;
  esac
done

# ---- Validate mode/dates ----
[[ -z "$MODE" ]] && { usage; die "--mode is required"; }
case "$MODE" in
  realtime)
    [[ -n "$N_DAYS" && "$N_DAYS" =~ ^[0-9]+$ ]] || die "--n-days (non-negative int) required for realtime"
    ;;
  history)
    # Hardened checks (prevents unbound locals in collect_dates_history)
    [[ -n "${START_DATE:-}" && -n "${END_DATE:-}" ]] || die "--start and --end are required for history"
    date -d "$START_DATE" +%F >/dev/null 2>&1 || die "Invalid --start date: $START_DATE"
    date -d "$END_DATE"   +%F >/dev/null 2>&1 || die "Invalid --end date: $END_DATE"
    (( $(date -d "$START_DATE" +%s) <= $(date -d "$END_DATE" +%s) )) || die "--start must be <= --end"
    ;;
  *) die "Invalid --mode: $MODE";;
esac

# ---- Validate ROOT tags ----
case "$ROOT" in *"{Y}"* ) : ;; * ) die "--root must include {Y}";; esac
case "$ROOT" in *"{m}"* ) : ;; * ) die "--root must include {m}";; esac
case "$ROOT" in *"{d}"* ) : ;; * ) die "--root must include {d}";; esac
case "$EXT"  in .*) : ;; * ) die "--ext must start with a dot (e.g., .gz)";; esac

WITH_BASINS=false
case "$ROOT" in *"{catchment_name}"* ) WITH_BASINS=true ;; * ) WITH_BASINS=false ;; esac

# ---- Load basins if needed ----
if $WITH_BASINS; then
  if (( ${#BASINS[@]} == 0 )); then
    if [[ -f "$BASIN_FILE" ]]; then
      # read non-empty, non-comment lines
      mapfile -t BASINS < <(grep -v '^[[:space:]]*$' "$BASIN_FILE" | grep -v '^[[:space:]]*#' | sed 's/[[:space:]]\+$//')
    else
      die "Basin list not set and file missing: $BASIN_FILE"
    fi
  fi
  (( ${#BASINS[@]} > 0 )) || die "No basins provided"
fi

$VERBOSE && {
  log "TZ=$TZ"
  log "MODE=$MODE"
  log "ROOT=$ROOT"
  log "WITH_BASINS=$WITH_BASINS"
  $WITH_BASINS && log "BASINS=${BASINS[*]}"
  log "EXT=$EXT DRY_RUN=$DRY_RUN"
}

# ---- Date iteration ----
collect_dates_realtime() {
  local n="$1"
  for ((i=0;i<=n;i++)); do date -d "today - $i day" +%F; done
}
collect_dates_history() {
  local s="$1" e="$2" d="$1"
  while :; do
    echo "$d"
    [[ "$d" == "$e" ]] && break
    d=$(date -d "$d + 1 day" +%F)
  done
}

# Build date list with extra guard for history mode
mapfile -t DATES < <(
  if [[ "$MODE" == "realtime" ]]; then
    collect_dates_realtime "$N_DAYS"
  else
    # Ensure not empty (defensive even after earlier validation)
    [[ -n "${START_DATE:-}" && -n "${END_DATE:-}" ]] || die "--start and --end must be provided for history mode"
    collect_dates_history "$START_DATE" "$END_DATE"
  fi
)

# ---- Template expansion ----
expand_path() {
  local date_iso="$1" basin="${2:-}"
  local Y m d p
  Y=$(date -d "$date_iso" +%Y)
  m=$(date -d "$date_iso" +%m)
  d=$(date -d "$date_iso" +%d)
  p="$ROOT"
  p="${p//\{Y\}/$Y}"
  p="${p//\{m\}/$m}"
  p="${p//\{d\}/$d}"
  if $WITH_BASINS; then
    [[ -n "$basin" ]] || die "Internal: basin not provided"
    p="${p//\{catchment_name\}/$basin}"
  fi
  echo "$p"
}

# ---- Cleaning primitive ----
clean_dir_keep_ext() {
  local dir="$1" ext="$2"
  if [[ ! -d "$dir" ]]; then
    $VERBOSE && warn "Missing directory: $dir"
    return 0
  fi
  if $DRY_RUN; then
    mapfile -t files < <(find "$dir" -type f ! -name "*${ext}" -print | sort)
    if (( ${#files[@]} > 0 )); then
      echo "[DRY-RUN] Would delete ${#files[@]} file(s) in $dir:"
      printf '  %s\n' "${files[@]}"
    else
      $VERBOSE && log "No files to remove in $dir"
    fi
  else
    local count=0
    while IFS= read -r -d '' f; do
      ((count++))
      rm -f -- "$f"
      $VERBOSE && echo "Deleted: $f"
    done < <(find "$dir" -type f ! -name "*${ext}" -print0)
    if (( count > 0 )); then
      log "Deleted $count file(s) in $dir (kept *${ext})"
    else
      $VERBOSE && log "No files deleted in $dir"
    fi
  fi
}

# ---- Main loop ----
if ! $WITH_BASINS; then
  for di in "${DATES[@]}"; do
    target_dir="$(expand_path "$di")"
    clean_dir_keep_ext "$target_dir" "$EXT"
  done
else
  for basin in "${BASINS[@]}"; do
    for di in "${DATES[@]}"; do
      target_dir="$(expand_path "$di" "$basin")"
      clean_dir_keep_ext "$target_dir" "$EXT"
    done
  done
fi

log "Done."

