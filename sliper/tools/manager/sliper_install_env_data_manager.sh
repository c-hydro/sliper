#!/bin/bash

# === CONFIGURATION ===
PROJECT_ROOT="/home/admin/library/env_data_manager"
ENV_NAME="env_data_manager"
PYTHON_BIN="$(which python3)"  # Adjust if needed
REQUIREMENTS_FILE="$PROJECT_ROOT/requirements_env_data_manager.txt"
ACTIVATE_SCRIPT="$PROJECT_ROOT/sliper_activate_env_data_manager.sh"

# === CREATE PROJECT FOLDER ===
mkdir -p "$PROJECT_ROOT"
cd "$PROJECT_ROOT" || exit 1

# === CREATE VIRTUAL ENVIRONMENT ===
echo "🔧 Creating virtual environment: $ENV_NAME"
$PYTHON_BIN -m venv "$ENV_NAME"

# === CREATE ACTIVATION SCRIPT ===
echo "📝 Generating activation script: $ACTIVATE_SCRIPT"
cat > "$ACTIVATE_SCRIPT" <<EOF
#!/bin/bash
# Activate Python environment
source "$PROJECT_ROOT/$ENV_NAME/bin/activate"
EOF
chmod +x "$ACTIVATE_SCRIPT"

# === INSTALL DEPENDENCIES ===
if [ -f "$REQUIREMENTS_FILE" ]; then
  echo "📦 Installing dependencies from requirements.txt"
  source "$PROJECT_ROOT/$ENV_NAME/bin/activate"
  pip install --upgrade pip
  pip install -r "$REQUIREMENTS_FILE"
  deactivate
else
  echo "ℹ️ No requirements.txt found. Skipping dependency installation."
fi

echo "✅ Environment setup complete."
echo "👉 To activate it later, run: source $ACTIVATE_SCRIPT"

