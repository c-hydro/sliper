#!/bin/bash

# === CONFIGURATION ===
PROJECT_ROOT="/home/admin/soilslips-system/organizer_data_sm/"
ACTIVATE_SCRIPT="/home/admin/library/env_organizer_data/sliper_activate_env_organizer_data.sh"
PYTHON_SCRIPT="$PROJECT_ROOT/sliper_tools_checker_data.py"
CONFIG_FILE="$PROJECT_ROOT/sliper_tools_checker_data_realtime_frc.json"

# === ACTIVATE ENVIRONMENT ===
if [ -f "$ACTIVATE_SCRIPT" ]; then
  source "$ACTIVATE_SCRIPT"
else
  echo "❌ Activation script not found: $ACTIVATE_SCRIPT"
  exit 1
fi

# === RUN PYTHON SCRIPT ===
if [ -f "$PYTHON_SCRIPT" ]; then
  python "$PYTHON_SCRIPT" -settings_file "$CONFIG_FILE"
else
  echo "❌ Python script not found: $PYTHON_SCRIPT"
  exit 1
fi

# === DEACTIVATE ENVIRONMENT ===
deactivate

