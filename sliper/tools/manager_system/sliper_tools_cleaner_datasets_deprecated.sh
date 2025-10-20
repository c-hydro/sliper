#!/usr/bin/env bash
#
# SLIPER TOOLS - CLEANER DATASETS (deprecated paths) - REALTIME
# Version: 1.1.0
# Date: 2025-10-17
# Notes: safer deletions, whitespace-safe, dry-run, verbose logging.

#-------------------------------------------------------------------------------------
set -euo pipefail

# -------------------------------------------------------------------------------------
# set script option(s)
script_name="SLIPER TOOLS - CLEANER DATASETS DEPRECATED - REALTIME"
script_version="1.1.0"
script_date="2025/10/17"
script_file="sliper_tools_cleaner_datasets_deprecated.sh"

# set script flag(s)
DRY_RUN=false
VERBOSE=false

# set datasets configuration (names align by index across arrays)
group_datasets_name=(
  "WORKSPACE - ANCILLARY - DATA"
  "WORKSPACE - ANCILLARY - INDICATORS"
  "WORKSPACE - ANCILLARY - SCENARIOS"
  "WORKSPACE - ANCILLARY - PREDICTORS"
  "WORKSPACE - ANCILLARY - ANALYSIS"
  "WORKSPACE - DESTINATION - RAIN FORECAST"
  "WORKSPACE - DESTINATION - SOIL MOISTURE FORECAST"
)

group_folder_datasets=(
  "/home/admin/soilslips-ws/data_dynamic/ancillary/data/"
  "/home/admin/soilslips-ws/data_dynamic/ancillary/indicators/"
  "/home/admin/soilslips-ws/data_dynamic/ancillary/scenarios/"
  "/home/admin/soilslips-ws/data_dynamic/ancillary/predictors/"
  "/home/admin/soilslips-ws/data_dynamic/ancillary/analysis/"
  "/home/admin/soilslips-ws/data_dynamic/destination/data/rain/frc/"
  "/home/admin/soilslips-ws/data_dynamic/destination/data/soil_moisture/frc/"
)

group_file_datasets_clean=(
  true
  true
  true
  true
  true
  true
  true
)

group_file_datasets_elapsed_days=(
  2
  2
  4
  4
  4
  3
  3
)
#-------------------------------------------------------------------------------------


#-------------------------------------------------------------------------------------
# method to log
log() { printf '%s\n' "$*"; }
vlog() { "$VERBOSE" && printf '%s\n' "$*" || true; }

# method to print usage
usage() {
  cat <<EOF
Usage: $0 [--dry-run] [--verbose]

  --dry-run   Show what would be deleted, do not actually delete
  --verbose   Extra logging
EOF
}
#-------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------
# parse args
while [[ "${1:-}" =~ ^- ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true ;;
    --verbose|-v) VERBOSE=true ;;
    --help|-h) usage; exit 0 ;;
    *) log "Unknown option: $1"; usage; exit 2 ;;
  esac
  shift
done

# Time info (UTC midnight for logging only)
time_script_now="$(date -u +"%Y-%m-%d 00:00")"
#-------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------
# script header
log " ==================================================================================="
log " ==> $script_name (Version: $script_version Release_Date: $script_date)"
log " ==> START ..."
log " ===> EXECUTION at UTC $time_script_now ..."
"$DRY_RUN" && log " ===> DRY-RUN MODE ::: No files or folders will be deleted."
log " "

# sanity checks
len_name=${#group_datasets_name[@]}
len_fold=${#group_folder_datasets[@]}
len_clean=${#group_file_datasets_clean[@]}
len_days=${#group_file_datasets_elapsed_days[@]}

if [[ $len_name -ne $len_fold || $len_name -ne $len_clean || $len_name -ne $len_days ]]; then
  log " ===> ERROR: configuration arrays must be the same length."
  log " name: $len_name, folder: $len_fold, clean: $len_clean, days: $len_days"
  exit 3
fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# method to delete old files
delete_old_files() {
  local base="$1"
  local days="$2"

  # Find regular files older than N days; safe for spaces via NUL delimiting
  if "$DRY_RUN"; then
    find "$base" -type f -mtime +"$days" -print0 \
      | while IFS= read -r -d '' f; do
          log " ::: DRY-RUN: would delete file: $f"
        done
  else
    find "$base" -type f -mtime +"$days" -print0 \
      | while IFS= read -r -d '' f; do
          if [[ -f "$f" ]]; then
            vlog " ::: Delete file: $f"
            rm -f -- "$f"
          else
            vlog " ::: Skipped non-regular or missing file: $f"
          fi
        done
  fi
}

# method to prune empty folder(s), including parents that become empty after pruning children
prune_empty_dirs() {
  local base="$1"

  # sanity check
  if [ -z "$base" ] || [ ! -d "$base" ]; then
    log " !!! prune_empty_dirs: base path is not a directory: $base"
    return 1
  fi

  # We traverse with -depth so children are handled before parents.
  # Using -print0 ... -delete avoids the pipe race where the parent
  # might be evaluated before the child is actually removed.
  if "$DRY_RUN"; then
    # Just list what *would* be removed, in depth-first order
    find "$base" -depth -mindepth 1 -type d -empty -print0 \
      | while IFS= read -r -d '' d; do
          log " ::: DRY-RUN: would remove empty dir: $d"
        done
  else
    # Print then delete within the same find process (no race).
    # We read the printed paths to emit nice vlog lines.
    while IFS= read -r -d '' d; do
      vlog " ::: Remove empty dir: $d"
    done < <(find "$base" -depth -mindepth 1 -type d -empty -print0 -delete)
  fi
}
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# iterate over datasets
for (( datasets_id=0; datasets_id<len_name; datasets_id++ )); do
  datasets_name="${group_datasets_name[$datasets_id]}"
  folder_datasets="${group_folder_datasets[$datasets_id]}"
  file_datasets_clean="${group_file_datasets_clean[$datasets_id]}"
  file_datasets_elapsed_days="${group_file_datasets_elapsed_days[$datasets_id]}"
  
  # info datasets start
  log " ====> DATASETS TYPE ${datasets_name} ... "
  
  # checks
  if [[ -z "$folder_datasets" ]]; then
    log " =====> SKIPPED: empty folder path for '${datasets_name}'."
    continue
  fi
  
  if ! "$file_datasets_clean"; then
    log " =====> SKIPPED: cleaning not activated for '${datasets_name}'."
    continue
  fi

  if [[ ! -d "$folder_datasets" ]]; then
    log " =====> SKIPPED: folder not found: $folder_datasets"
    continue
  fi

  # delete files older than N days
  log " =====> Clean files older than ${file_datasets_elapsed_days} day(s) in: $folder_datasets"
  delete_old_files "$folder_datasets" "$file_datasets_elapsed_days"

  # prune empty directories (not the root)
  log " =====> Prune empty directories in: $folder_datasets"
  prune_empty_dirs "$folder_datasets"
  
  # info datasets end
  log " ====> DATASETS TYPE ${datasets_name} ... DONE"
  
done

log " "
log " ==> ... END"
log " ==> Bye, Bye"
log " ==================================================================================="
# -------------------------------------------------------------------------------------
