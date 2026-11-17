
#!/usr/bin/env bash
set -euo pipefail

# =====================================================================
# SLIPER - DATABASE SYNCHRONIZATION
# =====================================================================
# Execute on: 10.24.193.158
#
# SOURCE:
#   /home/admin/soilslips-ws/db/
#
# DESTINATION:
#   admin@10.24.193.157:/home/admin/DB/
#
# MODE:
#   - Copy only missing files
#   - Never overwrite existing destination files
#   - Never delete destination files
#   - Resume interrupted transfers
#   - Prevent concurrent executions
#   - Log synchronization
# =====================================================================

# Configuration
SRC_DIR="/home/admin/soilslips-ws/storage/"

DST_USER="admin"
DST_HOST="10.24.193.157"
DST_DIR="/home/admin/STORAGE/"

SSH_KEY="${HOME}/.ssh/id_rsa"

LOG_DIR="/home/admin/log/manager_system/"
LOG_FILE="${LOG_DIR}/sliper_tools_sync_data_to_STORAGE.log"

# Create log directory
mkdir -p "${LOG_DIR}"

# Logging
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" \
        | tee -a "${LOG_FILE}"
}

# Check required commands
for cmd in ssh rsync flock; do
    if ! command -v "${cmd}" >/dev/null 2>&1; then
        log "ERROR: Command '${cmd}' not found."
        exit 1
    fi
done

# Prevent concurrent runs
exec 9>"${LOG_DIR}/sliper_tools_sync_data_db.lock"

if ! flock -n 9; then
    log "WARNING: Synchronization already running."
    exit 0
fi

log "=================================================="
log "SLIPER DATABASE SYNCHRONIZATION - START"
log "Source: ${SRC_DIR}"
log "Destination: ${DST_USER}@${DST_HOST}:${DST_DIR}"

# Check source directory
if [[ ! -d "${SRC_DIR}" ]]; then
    log "ERROR: Source directory not found."
    exit 1
fi

# Check SSH key
if [[ ! -f "${SSH_KEY}" ]]; then
    log "ERROR: SSH private key not found: ${SSH_KEY}"
    exit 1
fi

# SSH configuration
SSH_CMD="ssh \
    -i ${SSH_KEY} \
    -o IdentitiesOnly=yes \
    -o BatchMode=yes \
    -o StrictHostKeyChecking=yes \
    -o ConnectTimeout=15 \
    -o ServerAliveInterval=60 \
    -o ServerAliveCountMax=3"

# Check remote connection and destination
log "Checking remote SSH connection ..."

if ! ${SSH_CMD} "${DST_USER}@${DST_HOST}" \
    "mkdir -p '${DST_DIR}'"; then

    log "ERROR: SSH connection failed or destination unavailable."
    exit 1
fi

log "SSH connection OK."

# Start synchronization
log "Starting rsync ..."

if rsync \
    -avh \
    --ignore-existing \
    --partial-dir=.rsync-partial \
    --progress \
    --stats \
    --timeout=300 \
    -e "${SSH_CMD}" \
    "${SRC_DIR}" \
    "${DST_USER}@${DST_HOST}:${DST_DIR}" \
    >> "${LOG_FILE}" 2>&1; then

    log "Synchronization completed successfully."

else
    STATUS=$?
    log "ERROR: Synchronization failed (exit code: ${STATUS})."
    exit "${STATUS}"
fi

log "SLIPER DATABASE SYNCHRONIZATION - END"
log "=================================================="

