#!/usr/bin/env bash
set -euo pipefail

# ==========================================
# copy_newest_per_day_with_catchments.sh
# ==========================================
# For each source/destination pair (with optional {catchment}),
# copy only the newest HHMM folder for each day in the window.
# ==========================================

# -------------------------------
# Defaults
# -------------------------------
N_DAYS=4
RUN_DATE=""              # default = yesterday
DRY_RUN=0
VERBOSE=0
LOCKFILE="/home/admin/lock/manager_system/sliper_tools_mover_folders_2_storage.lock"
LIST_FILE=""
CATCHMENTS_ARG=""
CATCH_FILE=""
AUTO_CATCHMENTS=0

# -------------------------------
# Helpers
# -------------------------------
usage() {
  cat <<'EOF'
Usage:
  copy_newest_per_day_with_catchments.sh [options]

Options:
  --list FILE           File with "SRC_ROOT DEST_ROOT" pairs (supports {catchment})
  --n-days N            Number of days including RUN_DATE (default: 4)
  --run-date YYYY-MM-DD Reference date (default: yesterday)
  --catchments "A B"    Catchments (space-separated)
  --catch-file FILE     File listing catchments (one per line)
  --auto-catchments     Auto-discover catchments from filesystem
  --dry-run             Print what would be copied
  --verbose             Verbose log output
  --help                Show this help

Behavior:
  For each day from RUN_DATE .. RUN_DATE-(N_DAYS-1),
  copy the newest HHMM subfolder per day.
EOF
}

log()  { echo "[$(date +'%F %T')] $*"; }
vlog() { [[ $VERBOSE -eq 1 ]] && echo "  [DEBUG] $*"; }

is_gnu_date_ok() { date -d "$1" +%s >/dev/null 2>&1; }

# Find newest HHMM directory (by name)
find_newest_time_dir() {
  local day_dir="$1"
  mapfile -t CANDIDATES < <(find "$day_dir" -mindepth 1 -maxdepth 1 -type d -printf "%f\n" \
                            | grep -E '^([01][0-9]|2[0-3])[0-5][0-9]$' | sort -nr)
  [[ ${#CANDIDATES[@]} -gt 0 ]] && echo "${CANDIDATES[0]}" || true
}

# Expand a {catchment} placeholder in path
expand_path() {
  local template="$1" catch="$2"
  echo "${template//\{catchment\}/$catch}"
}

# Discover catchments from filesystem
discover_catchments() {
  local src_template="$1"
  local prefix="${src_template%%\{catchment\}*}"
  local suffix="${src_template#*\{catchment\}}"

  [[ "$src_template" == "$prefix" ]] && return 0  # no placeholder
  [[ ! -d "$prefix" ]] && return 0

  vlog "Discovering catchments in $prefix"
  local c
  while IFS= read -r c; do
    [[ -z "$c" ]] && continue
    local base="$prefix$c$suffix"
    [[ -d "$base" ]] && echo "$c"
  done < <(find "$prefix" -mindepth 1 -maxdepth 1 -type d -printf "%f\n" | sort)
}

# -------------------------------
# Parse arguments
# -------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --list)            LIST_FILE="$2"; shift 2 ;;
    --n-days)          N_DAYS="$2"; shift 2 ;;
    --run-date)        RUN_DATE="$2"; shift 2 ;;
    --catchments)      CATCHMENTS_ARG="$2"; shift 2 ;;
    --catch-file)      CATCH_FILE="$2"; shift 2 ;;
    --auto-catchments) AUTO_CATCHMENTS=1; shift ;;
    --dry-run)         DRY_RUN=1; shift ;;
    --verbose)         VERBOSE=1; shift ;;
    --help|-h)         usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

# -------------------------------
# Input validation
# -------------------------------
if [[ -z "$LIST_FILE" ]]; then
  echo "ERROR: Please provide --list FILE." >&2
  exit 2
fi
if [[ ! -f "$LIST_FILE" ]]; then
  echo "ERROR: List file not found: $LIST_FILE" >&2
  exit 2
fi

# Build catchment list
CATCHMENTS=()
if [[ -n "$CATCH_FILE" ]]; then
  if [[ ! -f "$CATCH_FILE" ]]; then
    echo "ERROR: Catchment file not found: $CATCH_FILE" >&2
    exit 2
  fi
  mapfile -t CATCHMENTS < <(grep -vE '^\s*#' "$CATCH_FILE" | grep .)
fi
if [[ -n "$CATCHMENTS_ARG" ]]; then
  # shellcheck disable=SC2206
  CATCHMENTS+=($CATCHMENTS_ARG)
fi

# -------------------------------
# Lock (prevent parallel runs)
# -------------------------------
mkdir -p "$(dirname "$LOCKFILE")"
exec 9>"$LOCKFILE"
if ! flock -n 9; then
  echo "Another instance is running (lock: $LOCKFILE). Exiting." >&2
  exit 3
fi

# -------------------------------
# Resolve RUN_DATE
# -------------------------------
if [[ -z "$RUN_DATE" ]]; then
  RUN_DATE=$(date -d "yesterday" +%F)
fi
if ! is_gnu_date_ok "$RUN_DATE"; then
  echo "ERROR: run date '$RUN_DATE' not parseable." >&2
  exit 4
fi

log "Start RUN_DATE=$RUN_DATE, N_DAYS=$N_DAYS"

# -------------------------------
# Read source/destination pairs
# -------------------------------
mapfile -t PAIRS < <(grep -vE '^\s*#' "$LIST_FILE" | grep .)
log "Loaded ${#PAIRS[@]} pair(s) from list."

# -------------------------------
# Main processing
# -------------------------------
for PAIR in "${PAIRS[@]}"; do
  read -r SRC_TEMPLATE DEST_TEMPLATE <<<"$PAIR"
  vlog "Processing pair: SRC=$SRC_TEMPLATE DEST=$DEST_TEMPLATE"

  USES_CATCH=0
  [[ "$SRC_TEMPLATE" == *"{catchment}"* || "$DEST_TEMPLATE" == *"{catchment}"* ]] && USES_CATCH=1

  EFFECTIVE_CATCHMENTS=()
  if [[ $USES_CATCH -eq 1 ]]; then
    vlog "Pair uses {catchment}"
    if [[ ${#CATCHMENTS[@]} -gt 0 ]]; then
      declare -A seen=()
      for c in "${CATCHMENTS[@]}"; do
        [[ -z "$c" ]] && continue
        if [[ -z "${seen[$c]:-}" ]]; then
          EFFECTIVE_CATCHMENTS+=("$c")
          seen["$c"]=1
        fi
      done
      unset seen
    fi
    if [[ $AUTO_CATCHMENTS -eq 1 ]]; then
      mapfile -t DISC < <(discover_catchments "$SRC_TEMPLATE" || true)
      if [[ ${#DISC[@]} -gt 0 ]]; then
        vlog "Auto-discovered catchments: ${DISC[*]}"
        declare -A seen2=()
        for c in "${EFFECTIVE_CATCHMENTS[@]}"; do seen2["$c"]=1; done
        for c in "${DISC[@]}"; do
          if [[ -z "${seen2[$c]:-}" ]]; then
            EFFECTIVE_CATCHMENTS+=("$c")
            seen2["$c"]=1
          fi
        done
        unset seen2
      fi
    fi
    if [[ ${#EFFECTIVE_CATCHMENTS[@]} -eq 0 ]]; then
      log "WARN: No catchments found for $PAIR — skipping."
      continue
    fi
  else
    EFFECTIVE_CATCHMENTS=("")
  fi

  # -------------------------------
  # For each catchment
  # -------------------------------
  for CATCH in "${EFFECTIVE_CATCHMENTS[@]}"; do
    SRC_ROOT=$(expand_path "$SRC_TEMPLATE" "$CATCH")
    DEST_ROOT=$(expand_path "$DEST_TEMPLATE" "$CATCH")

    vlog "Catchment=$CATCH SRC_ROOT=$SRC_ROOT DEST_ROOT=$DEST_ROOT"

    if [[ ! -d "$SRC_ROOT" ]]; then
      vlog "No source root: $SRC_ROOT — skipping."
      continue
    fi
    mkdir -p "$DEST_ROOT"

    # Loop from RUN_DATE down to (RUN_DATE - (N_DAYS - 1))
    for ((offset=0; offset<N_DAYS; offset++)); do
      DAY=$(date -d "$RUN_DATE - $offset day" +%F)
      Y=$(date -d "$DAY" +%Y)
      m=$(date -d "$DAY" +%m)
      d=$(date -d "$DAY" +%d)

      vlog "Checking day $DAY..."
      DAY_DIR="$SRC_ROOT/$Y/$m/$d"
      if [[ ! -d "$DAY_DIR" ]]; then
        vlog "[$CATCH] No directory for $DAY ($DAY_DIR)"
        continue
      fi

      NEWEST=$(find_newest_time_dir "$DAY_DIR" || true)
      if [[ -z "$NEWEST" ]]; then
        vlog "[$CATCH] No HHMM folders in $DAY_DIR"
        continue
      fi

      SRC_PATH="$DAY_DIR/$NEWEST"
      DEST_PATH="$DEST_ROOT/$Y/$m/$d/$NEWEST"

      if [[ -d "$DEST_PATH" ]]; then
        vlog "[$CATCH][$DAY] Destination exists — skipping."
        continue
      fi

      log "[$DAY][$CATCH] Copying $SRC_PATH -> $DEST_PATH"
      if [[ $DRY_RUN -eq 1 ]]; then
        echo "DRY-RUN: rsync -a \"$SRC_PATH/\" \"$DEST_PATH/\""
      else
        mkdir -p "$DEST_PATH"
        RSYNC_OPTS="-a"
        [[ $VERBOSE -eq 1 ]] && RSYNC_OPTS="$RSYNC_OPTS -v"
        rsync $RSYNC_OPTS "$SRC_PATH/" "$DEST_PATH/"
      fi
    done
  done
done

log "Done."

