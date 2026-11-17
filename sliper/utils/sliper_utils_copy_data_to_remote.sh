#!/bin/bash

# Local source
SRC="/home/fabio/soil_moisture/"

# Remote destination
REMOTE_USER="silvestro"
REMOTE_HOST="130.251.104.66"
REMOTE_DIR="/SHARE/SM_CONTINUUM_ARPAL/"

# Sync local to remote
rsync -avhP \
    "${SRC}" \
    "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}"
