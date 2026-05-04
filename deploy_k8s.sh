#!/usr/bin/env bash
# deploy_k8s.sh — Deploy the video streaming platform to Kubernetes via Helm.
# Reads secrets from .env (already gitignored), passes them to Helm.
#
# Usage:
#   ./deploy_k8s.sh              # helm install (first time)
#   ./deploy_k8s.sh upgrade      # helm upgrade (after changes)
#   ./deploy_k8s.sh uninstall    # helm uninstall
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
CHART_DIR="${SCRIPT_DIR}/k8s"
RELEASE_NAME="vs"

# ── Load .env ──────────────────────────────────────────────────
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: .env file not found at $ENV_FILE"
  echo "Copy .env.example to .env and fill in your secrets."
  exit 1
fi

source_env() {
  local key="$1"
  local val
  val=$(grep "^${key}=" "$ENV_FILE" | head -1 | cut -d'=' -f2-)
  if [[ -z "$val" ]]; then
    echo "ERROR: $key not found in .env"
    exit 1
  fi
  echo "$val"
}

PG_USER=$(source_env PG_USER)
PG_PASSWORD=$(source_env PG_PASSWORD)
RABBITMQ_USER=$(source_env RABBITMQ_USER)
RABBITMQ_PASS=$(source_env RABBITMQ_PASS)
RABBITMQ_ERLANG_COOKIE=$(source_env RABBITMQ_ERLANG_COOKIE)
MINIO_ACCESS_KEY=$(source_env MINIO_ACCESS_KEY)
MINIO_SECRET_KEY=$(source_env MINIO_SECRET_KEY)

# ── Helm secret overrides ─────────────────────────────────────
SECRETS=(
  --set "secrets.pgUser=${PG_USER}"
  --set "secrets.pgPassword=${PG_PASSWORD}"
  --set "secrets.rabbitmqUser=${RABBITMQ_USER}"
  --set "secrets.rabbitmqPass=${RABBITMQ_PASS}"
  --set "secrets.rabbitmqErlangCookie=${RABBITMQ_ERLANG_COOKIE}"
  --set "secrets.minioAccessKey=${MINIO_ACCESS_KEY}"
  --set "secrets.minioSecretKey=${MINIO_SECRET_KEY}"
)

# ── Execute ────────────────────────────────────────────────────
ACTION="${1:-install}"

case "$ACTION" in
  install)
    echo "Installing Helm release '${RELEASE_NAME}'..."
    helm install "$RELEASE_NAME" "$CHART_DIR" "${SECRETS[@]}" "${@:2}"
    echo ""
    echo "Done! Run 'minikube tunnel' in another terminal to access services."
    echo "  Upload:    http://localhost:8080"
    echo "  Status:    http://localhost:8081"
    echo "  Streaming: http://localhost:8083"
    ;;
  upgrade)
    echo "Upgrading Helm release '${RELEASE_NAME}'..."
    helm upgrade "$RELEASE_NAME" "$CHART_DIR" "${SECRETS[@]}" "${@:2}"
    ;;
  uninstall)
    echo "Uninstalling Helm release '${RELEASE_NAME}'..."
    helm uninstall "$RELEASE_NAME"
    ;;
  *)
    echo "Usage: $0 {install|upgrade|uninstall}"
    exit 1
    ;;
esac
