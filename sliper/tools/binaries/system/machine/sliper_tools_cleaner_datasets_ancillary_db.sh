#!/usr/bin/env bash

# ==============================================================================
# Delete files older than x minutes
# /home/admin/soilslips-ws/data_dynamic/ancillary
# ==============================================================================

set -euo pipefail

TARGET_DIR="/home/admin/soilslips-ws/db/ancillary"
AGE_MINUTES=10

if [[ ! -d "$TARGET_DIR" ]]; then
    echo "ERROR: Directory '$TARGET_DIR' does not exist."
    exit 1
fi

echo "========================================"
echo "Cleaning directory: $TARGET_DIR"
echo "Removing files older than ${AGE_MINUTES} minutes"
echo "Time: $(date)"
echo "========================================"

# Show files to be removed
find "$TARGET_DIR" -type f -mmin +${AGE_MINUTES} -print

# Delete them
find "$TARGET_DIR" -type f -mmin +${AGE_MINUTES} -delete

echo
echo "Cleanup completed."
echo "Remaining files: $(find "$TARGET_DIR" -type f | wc -l)"
