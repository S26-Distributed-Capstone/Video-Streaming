#!/usr/bin/env bash
# run_chaos.sh — Run Chaos Killer Bot locally
# Reads config from project root .env file

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="${PROJECT_ROOT}/.env"

# Load .env if it exists
if [[ -f "$ENV_FILE" ]]; then
    echo "Loading configuration from: $ENV_FILE"
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "WARNING: .env not found at $ENV_FILE"
    echo "Using environment variables or defaults"
fi

# Check if Python is available
if ! command -v python &> /dev/null && ! command -v python3 &> /dev/null; then
    echo "ERROR: Python not found. Please install Python 3.8+"
    exit 1
fi

# Set Python command
PYTHON=$(command -v python3 || command -v python)

# Install dependencies
echo "Installing dependencies..."
pip install -q -r "$SCRIPT_DIR/requirements.txt"

# Run the bot
echo ""
echo "Starting Chaos Killer Bot..."
echo "Press Ctrl+C to stop"
echo ""

"$PYTHON" "$SCRIPT_DIR/killer_bot.py"
