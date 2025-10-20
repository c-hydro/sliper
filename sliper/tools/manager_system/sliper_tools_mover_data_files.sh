#!/usr/bin/env bash
set -euo pipefail

# =========================================================
# Copy files per day using rule file:
#   MODE SRC_TEMPLATE DEST_ROOT [GLOB]
# MODE:   all | latest
# SRC_TEMPLATE/DEST_ROOT may include literal {catchment}
# GLOB is optional (default "*")
# The script appends /YYYY/MM/DD to SRC and DEST.
#
# Examples in rules.txt:
#   all    /home/.../soil_moisture/obs/MagraDomain     /home/.../selected/soil_moisture/obs/MagraDomain     "*"
#   all    /home/.../soil_moisture/obs/{catchment}     /home/.../selected/soil_moisture/obs/{catchment}     "*.nc"
#   latest /home/.../run                                /home/.../selected/run                              "*.txt"
# =========================================================

RUN_DATE=""              # default: yesterday
N_DAYS=1                 # process RUN_DATE-1 .. RUN_DATE-N
RULES_FILE=""
CATCH_FILE=""
CATCHMENTS_ARG=""
AUTO_CATCHMENTS=0        # discover catchments at the {catchment} level
LATEST_BY="mtime"        # "mtime" or "name"
DRY_RUN=0
VERBOSE=0
LOCKFILE="/home/admin/lock/manager_system/sliper_tools_mover_files_2_storage.lock"

usage() {
  cat <<'EOF'
Usage:
  copy_daily_files_by_rule.sh --rules rules.txt [options]

Options:
  --rules FILE           Rules file (required). Format per line:
                           MODE SRC_TEMPLATE DEST_ROOT [GLOB]
                         MODE: all | latest
  --run-date YYYY-MM-DD  Reference date (default: yesterday). Script processes previous N_DAYS.
  --n-days N             Number of days back from RUN_DATE (default: 1).
  --catch-file FILE      File with catchments (one per line) for rules with {catchment}.
  --catchments "A B"     Space-separated catchment list for rules with {catchment}.
  --auto-catchments      Discover catchments by scanning filesystem at {catchment} level.
  --latest-by mtime|name How to choose "latest" file (default: mtime).
  --dry-run              Print actions only.
  --verbose              Verbose logs.
  --help                 Show help.

Behavior:
  For each rule and each day in the window:
    - Build SRC_DAY = SRC_TEMPLATE/YYYY/MM/DD (expand {catchment} if present)
    - MODE=all    -> copy all files matching GLOB (default "*") to DEST/YYYY/MM/DD/
    - MODE=latest -> copy only the single latest file (by mtime or by name) matching GLOB
  Dest paths mirror source day structure (including {catchment} if used in DEST_ROOT).
  Existing files are not overwritten by default (rsync -a).
EOF
}

log()  { echo "[$(date +'%F %T')] $*"; }
vlog() { [[ $VERBOSE -eq 1 ]] && log "$@"; }

expand_path() { echo "${1//\{catchment\}/$2}"; }

discover_catchments() {
  local src_template="$1"
  local prefix="${src_template%%\{catchment\}*}"
  local suffix="${src_template#*\{catchment\}}"
  [[ "$src_template" == "$prefix" ]] && return 0
  [[ -d "$prefix" ]] || return 0
  find "$prefix" -mindepth 1 -maxdepth 1 -type d -printf "%f\n" \
    | while read -r c; do
        [[ -d "$prefix/$c$suffix" ]] && echo "$c"
      done | sort
}

parse_rules_line() {
  # Reads a line and outputs: MODE|SRC_TEMPLATE|DEST_ROOT|GLOB
  # Supports 3 or 4 tokens; if only 3, GLOB="*"
  local line="$1"
  # shellcheck disable=SC2206
  local parts=($line)
  [[ ${#parts[@]} -lt 3 ]] && { echo ""; return; }
  local MODE="${parts[0]}"
  local SRC="${parts[1]}"
  local DST="${parts[2]}"
  local GLOB="*"
  [[ ${#parts[@]} -ge 4 ]] && GLOB="${parts[3]}"
  echo "${MODE}|${SRC}|${DST}|${GLOB}"
}

copy_all_files() {
  local src_dir="$1" dst_dir="$2" glob="$3"
  shopt -s nullglob
  local files=( "$src_dir"/$glob )
  shopt -u nullglob
  [[ ${#files[@]} -eq 0 ]] && return 1
  mkdir -p "$dst_dir"
  if [[ $DRY_RUN -eq 1 ]]; then
    for f in "${files[@]}"; do echo "DRY-RUN: rsync -a \"$f\" \"$dst_dir/\""; done
  else
    rsync -a "${files[@]}" "$dst_dir/"
  fi
}

choose_latest_file() {
  local src_dir="$1" glob="$2" by="$3"
  shopt -s nullglob
  local files=( "$src_dir"/$glob )
  shopt -u nullglob
  [[ ${#files[@]} -eq 0 ]] && return 1
  local latest=""
  if [[ "$by" == "name" ]]; then
    # lexicographically last
    IFS=$'\n' files=($(printf "%s\n" "${files[@]}" | sort))
    latest="${files[-1]}"
  else
    # by mtime (newest first)
    latest="$(ls -1t "${files[@]}" 2>/dev/null | head -n1 || true)"
  fi
  [[ -n "$latest" ]] || return 1
  printf "%s" "$latest"
}

copy_latest_file() {
  local src_dir="$1" dst_dir="$2" glob="$3" by="$4"
  local f
  f="$(choose_latest_file "$src_dir" "$glob" "$by")" || return 1
  mkdir -p "$dst_dir"
  if [[ $DRY_RUN -eq 1 ]]; then
    echo "DRY-RUN: rsync -a \"$f\" \"$dst_dir/\""
  else
    rsync -a "$f" "$dst_dir/"
  fi
}

# --------- args ---------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --rules) RULES_FILE="$2"; shift 2 ;;
    --run-date) RUN_DATE="$2"; shift 2 ;;
    --n-days) N_DAYS="$2"; shift 2 ;;
    --catch-file) CATCH_FILE="$2"; shift 2 ;;
    --catchments) CATCHMENTS_ARG="$2"; shift 2 ;;
    --auto-catchments) AUTO_CATCHMENTS=1; shift ;;
    --latest-by) LATEST_BY="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --verbose) VERBOSE=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

[[ -n "$RULES_FILE" && -f "$RULES_FILE" ]] || { echo "ERROR: --rules FILE is required."; exit 2; }

# Lock
exec 9>"$LOCKFILE"
if ! flock -n 9; then
  echo "Another instance is running (lock: $LOCKFILE). Exiting." >&2
  exit 3
fi

# Run date default
[[ -n "$RUN_DATE" ]] || RUN_DATE=$(date -d "yesterday" +%F) || { echo "ERROR: cannot compute yesterday"; exit 4; }

# Build global catchments list
CATCHMENTS=()
if [[ -n "$CATCH_FILE" ]]; then
  mapfile -t CATCHMENTS < <(grep -vE '^\s*#' "$CATCH_FILE" | grep . || true)
fi
if [[ -n "$CATCHMENTS_ARG" ]]; then
  # shellcheck disable=SC2206
  CATCHMENTS+=($CATCHMENTS_ARG)
fi

log "Start RUN_DATE=$RUN_DATE N_DAYS=$N_DAYS rules=$RULES_FILE latest-by=$LATEST_BY"

# Read rules
mapfile -t RULES < <(grep -vE '^\s*#' "$RULES_FILE" | grep . || true)

for rule_line in "${RULES[@]}"; do
  parsed="$(parse_rules_line "$rule_line")"
  [[ -z "$parsed" ]] && { vlog "Skip malformed: $rule_line"; continue; }
  MODE="${parsed%%|*}"; rest="${parsed#*|}"
  SRC_TEMPLATE="${rest%%|*}"; rest="${rest#*|}"
  DEST_TEMPLATE="${rest%%|*}"; GLOB="${rest#*|}"

  USES_CATCH=0
  [[ "$SRC_TEMPLATE" == *"{catchment}"* || "$DEST_TEMPLATE" == *"{catchment}"* ]] && USES_CATCH=1

  # Resolve catchments for this rule
  EFFECTIVE_C=()
  if [[ $USES_CATCH -eq 1 ]]; then
    # from provided lists
    if [[ ${#CATCHMENTS[@]} -gt 0 ]]; then
      declare -A seen=()
      for c in "${CATCHMENTS[@]}"; do
        [[ -z "$c" ]] && continue
        if [[ -z "${seen[$c]:-}" ]]; then EFFECTIVE_C+=("$c"); seen["$c"]=1; fi
      done
      unset seen
    fi
    # auto discover if requested
    if [[ $AUTO_CATCHMENTS -eq 1 ]]; then
      mapfile -t DISC < <(discover_catchments "$SRC_TEMPLATE" || true)
      if [[ ${#DISC[@]} -gt 0 ]]; then
        declare -A seen2=()
        for c in "${EFFECTIVE_C[@]}"; do seen2["$c"]=1; done
        for c in "${DISC[@]}"; do
          [[ -n "${seen2[$c]:-}" ]] && continue
          EFFECTIVE_C+=("$c"); seen2["$c"]=1
        done
        unset seen2
      fi
    fi
    if [[ ${#EFFECTIVE_C[@]} -eq 0 ]]; then
      log "WARN: No catchments resolved for rule: $rule_line"
      continue
    fi
  else
    EFFECTIVE_C=("")  # single pass without catchment
  fi

  for CATCH in "${EFFECTIVE_C[@]}"; do
    SRC_ROOT="$SRC_TEMPLATE"
    DEST_ROOT="$DEST_TEMPLATE"
    [[ $USES_CATCH -eq 1 ]] && {
      SRC_ROOT="$(expand_path "$SRC_TEMPLATE" "$CATCH")"
      DEST_ROOT="$(expand_path "$DEST_TEMPLATE" "$CATCH")"
    }

    for ((offset=1; offset<=N_DAYS; offset++)); do
      DAY=$(date -d "$RUN_DATE - $offset day" +%F)
      Y=$(date -d "$DAY" +%Y); m=$(date -d "$DAY" +%m); d=$(date -d "$DAY" +%d)

      SRC_DAY="$SRC_ROOT/$Y/$m/$d"
      DEST_DAY="$DEST_ROOT/$Y/$m/$d"

      if [[ ! -d "$SRC_DAY" ]]; then
        vlog "[${CATCH:-noct}][$DAY] No directory: $SRC_DAY"
        continue
      fi

      case "$MODE" in
        all)
          log "[$DAY][${CATCH:-noct}] Copy ALL $SRC_DAY -> $DEST_DAY (glob: $GLOB)"
          if ! copy_all_files "$SRC_DAY" "$DEST_DAY" "$GLOB"; then
            vlog "No files for $SRC_DAY/$GLOB"
          fi
          ;;
        latest)
          log "[$DAY][${CATCH:-noct}] Copy LATEST ($LATEST_BY) $SRC_DAY -> $DEST_DAY (glob: $GLOB)"
          if ! copy_latest_file "$SRC_DAY" "$DEST_DAY" "$GLOB" "$LATEST_BY"; then
            vlog "No files for $SRC_DAY/$GLOB"
          fi
          ;;
        *)
          log "WARN: Unknown MODE '$MODE' in rule: $rule_line"
          ;;
      esac
    done
  done
done

log "Done."

