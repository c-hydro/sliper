#!/usr/bin/env bash
# Dumps the current user's crontab into a dated backup file.

# Directory where backups will be stored
BACKUP_DIR="/home/admin/crontab/"

# Create directory if not exists
mkdir -p "$BACKUP_DIR"

# Get current date in YYYYMMDD format
DATE_TAG=$(date +"%Y%m%d")

# Build backup filename
BACKUP_FILE="$BACKUP_DIR/crontab_${DATE_TAG}.bkp"

# Dump crontab
crontab -l > "$BACKUP_FILE"

# Optional: keep only last 30 backups
find "$BACKUP_DIR" -type f -name "crontab_*.bkp" -mtime +30 -delete

