#!/usr/bin/env bash
set -euo pipefail

# ===============================
# Defaults
# ===============================
N_DAYS=1
RUN_DATE=""              # default yesterday
DRY_RUN=0
VERBOSE=0
LOCKFILE="/home/admin/lock/manager_system/sliper_tools_mover_folders_2_storage.lock"
LIST_FILE=""
CATCHMENTS_ARG=""        # space-separated list
CATCH_FILE=""
AUTO_CATCHMENTS=0        # discover catchments from filesystem for pairs using {catchment}

# ===============================
# Helpers
# ===============================
usage() {
  cat <<'EOF'
Usage:
  copy_newest_per_day_with_catchments.sh [options]

Options:
  --list FILE           File with "SRC_ROOT DEST_ROOT" pairs, space-separated (one pair per line).
                        Pairs may include the literal token {catchment} as a path segment.
  --n-days N            Days back from RUN_DATE (default: 1).
  --run-date YYYY-MM-DD Reference date (default: yesterday).
  --catchments "A B"    Space-separated catchment names for pairs that use {catchment}.
  --catch-file FILE     File listing catchment names (one per line).
  --auto-catchments     Auto-discover catchments by scanning SRC_ROOT where {catchment} appears.
  --dry-run             Print actions only.
  --verbose             Verbose logs.
  --help                Show this help.

Behavior:
  For each day in the window (RUN_DATE-1 .. RUN_DATE-N), and for each pair:
    - If the pair has no {catchment}, operate directly on:
        SRC_ROOT/YYYY/MM/DD/HHMM  -> DEST_ROOT/YYYY/MM/DD/HHMM
    - If the pair uses {catchment}, expand it for each catchment and operate on:
        SRC_ROOT(…/{catchment}/…)/YYYY/MM/DD/HHMM
      Copy ONLY the newest time-stamped subfolder (by HHMM directory name) per day.
      Skip if destination already exists.

List file format examples:
  /home/.../rain/frc/models /home/.../selected_newest/rain/frc/models
  /home/.../soil_moisture/frc/{catchment} /home/.../selected_newest/soil_moisture/frc/{catchment}

EOF
}

log()  { echo "[$(date +'%F %T')] $*"; }
vlog() { [[ $VERBOSE -eq 1 ]] && log "$@"; }

is_gnu_date_ok() { date -d "$1" +%s >/dev/null 2>&1; }

# Find newest HHMM directory by name (not mtime)
find_newest_time_dir() {
  local day_dir="$1"
  mapfile -t CANDIDATES < <(find "$day_dir" -mindepth 1 -maxdepth 1 -type d -printf "%f\n" \
                            | grep -E '^[0-2][0-9][0-5][0-9]$' | sort -nr)
  [[ ${#CANDIDATES[@]} -gt 0 ]] && echo "${CANDIDATES[0]}" || true
}

# Expand a template path with {catchment}
expand_path() {
  local template="$1" catch="$2"
  echo "${template//\{catchment\}/$catch}"
}

# Discover catchments: list subdirectories at {catchment} level
discover_catchments() {
  local src_template="$1"
  # Split at "{catchment}" to get prefix (before) and suffix (after)
  local prefix="${src_template%%\{catchment\}*}"
  local suffix="${src_template#*\{catchment\}}"

  if [[ "$src_template" == "$prefix" ]]; then
    # No placeholder
    return 0
  fi
  [[ ! -d "$prefix" ]] && return 0

  # List immediate subdirs in prefix as candidate catchments; optionally verify that
  # prefix/<catch>/suffix exists.
  local c
  while IFS= read -r c; do
    [[ -z "$c" ]] && continue
    local base="$prefix$c$suffix"
    [[ -d "$base" ]] && echo "$c"
  done < <(find "$prefix" -mindepth 1 -maxdepth 1 -type d -printf "%f\n" | sort)
}

# ===============================
# Parse arguments
# ===============================
while [[ $# -gt 0 ]]; do
  case "$1" in
    --list)           LIST_FILE="$2"; shift 2 ;;
    --n-days)         N_DAYS="$2"; shift 2 ;;
    --run-date)       RUN_DATE="$2"; shift 2 ;;
    --catchments)     CATCHMENTS_ARG="$2"; shift 2 ;;
    --catch-file)     CATCH_FILE="$2"; shift 2 ;;
    --auto-catchments) AUTO_CATCHMENTS=1; shift ;;
    --dry-run)        DRY_RUN=1; shift ;;
    --verbose)        VERBOSE=1; shift ;;
    --help|-h)        usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

# Validate list file or fall back to embedded example (empty by default)
if [[ -z "$LIST_FILE" ]]; then
  echo "ERROR: Please provide --list FILE with SRC/DST pairs (supports {catchment})." >&2
  exit 2
fi
if [[ ! -f "$LIST_FILE" ]]; then
  echo "ERROR: List file not found: $LIST_FILE" >&2
  exit 2
fi

# Build catchment list (if provided)
CATCHMENTS=()
if [[ -n "$CATCH_FILE" ]]; then
  if [[ ! -f "$CATCH_FILE" ]]; then
    echo "ERROR: catchment file not found: $CATCH_FILE" >&2
    exit 2
  fi
  mapfile -t CATCHMENTS < <(grep -vE '^\s*#' "$CATCH_FILE" | grep .)
fi
if [[ -n "$CATCHMENTS_ARG" ]]; then
  # Append to CATCHMENTS array
  # shellcheck disable=SC2206
  CATCHMENTS+=($CATCHMENTS_ARG)
fi

# Lock to avoid concurrent runs
exec 9>"$LOCKFILE"
if ! flock -n 9; then
  echo "Another instance is running (lock: $LOCKFILE). Exiting." >&2
  exit 3
fi

# Resolve RUN_DATE
if [[ -z "$RUN_DATE" ]]; then
  RUN_DATE=$(date -d "yesterday" +%F)
fi
if ! is_gnu_date_ok "$RUN_DATE"; then
  echo "ERROR: run date '$RUN_DATE' not parseable by 'date -d'." >&2
  exit 4
fi

log "Start RUN_DATE=$RUN_DATE, N_DAYS=$N_DAYS"

# Read pairs
mapfile -t PAIRS < <(grep -vE '^\s*#' "$LIST_FILE" | grep .)

# Process
for PAIR in "${PAIRS[@]}"; do
  SRC_TEMPLATE=$(echo "$PAIR" | awk '{print $1}')
  DEST_TEMPLATE=$(echo "$PAIR" | awk '{print $2}')

  if [[ -z "$SRC_TEMPLATE" || -z "$DEST_TEMPLATE" ]]; then
    vlog "Skipping malformed line: $PAIR"
    continue
  fi

  USES_CATCH=0
  [[ "$SRC_TEMPLATE" == *"{catchment}"* || "$DEST_TEMPLATE" == *"{catchment}"* ]] && USES_CATCH=1

  # Build effective catchment list for this pair
  EFFECTIVE_CATCHMENTS=()
  if [[ $USES_CATCH -eq 1 ]]; then
    # Start with provided catchments
    if [[ ${#CATCHMENTS[@]} -gt 0 ]]; then
      # Deduplicate
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
    # Optionally auto-discover
    if [[ $AUTO_CATCHMENTS -eq 1 ]]; then
      mapfile -t DISC < <(discover_catchments "$SRC_TEMPLATE" || true)
      if [[ ${#DISC[@]} -gt 0 ]]; then
        # Merge
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
      log "WARN: No catchments resolved for pair with {catchment}: $PAIR — skipping."
      continue
    fi
  else
    # No catchment placeholder; use a single empty token
    EFFECTIVE_CATCHMENTS=("")
  fi

  # Now iterate catchments for this pair
  for CATCH in "${EFFECTIVE_CATCHMENTS[@]}"; do
    SRC_ROOT="$SRC_TEMPLATE"
    DEST_ROOT="$DEST_TEMPLATE"
    if [[ $USES_CATCH -eq 1 ]]; then
      SRC_ROOT=$(expand_path "$SRC_TEMPLATE" "$CATCH")
      DEST_ROOT=$(expand_path "$DEST_TEMPLATE" "$CATCH")
    fi

    if [[ ! -d "$SRC_ROOT" ]]; then
      vlog "No source root: $SRC_ROOT — skipping."
      continue
    fi
    mkdir -p "$DEST_ROOT"

    for ((offset=1; offset<=N_DAYS; offset++)); do
      DAY=$(date -d "$RUN_DATE - $offset day" +%F)
      Y=$(date -d "$DAY" +%Y)
      m=$(date -d "$DAY" +%m)
      d=$(date -d "$DAY" +%d)

      DAY_DIR="$SRC_ROOT/$Y/$m/$d"
      if [[ ! -d "$DAY_DIR" ]]; then
        vlog "[$CATCH] No dir for $DAY under $SRC_ROOT"
        continue
      fi

      NEWEST=$(find_newest_time_dir "$DAY_DIR" || true)
      if [[ -z "$NEWEST" ]]; then
        vlog "[$CATCH] No HHMM subfolders in $DAY_DIR"
        continue
      fi

      SRC_PATH="$DAY_DIR/$NEWEST"
      DEST_PATH="$DEST_ROOT/$Y/$m/$d/$NEWEST"

      if [[ -d "$DEST_PATH" ]]; then
        vlog "[$CATCH][$DAY] Exists: $DEST_PATH — skipping."
        continue
      fi

      log "[$DAY][$CATCH] $SRC_PATH -> $DEST_PATH"
      if [[ $DRY_RUN -eq 1 ]]; then
        echo "DRY-RUN: rsync -a \"$SRC_PATH/\" \"$DEST_PATH/\""
      else
        mkdir -p "$DEST_PATH"
        rsync -a "$SRC_PATH/" "$DEST_PATH/"
      fi
    done
  done
done

log "Done."

