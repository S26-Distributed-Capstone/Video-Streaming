#!/usr/bin/env bash
# migrate-processing-spool-emptydir.sh
#
# One-time migration from per-replica local-path PVCs to ephemeral emptyDir
# spool storage for vs-processing. This removes PV node affinity so
# cordon-based autoscaling can reschedule processing pods onto any eligible
# app node.
#
# Usage:
#   ./scripts/migrate-processing-spool-emptydir.sh
#
# Optional environment overrides:
#   CONTROL_PLANE=sack@192.168.8.11
#   SSH_KEY=$HOME/.ssh/cluster_key
#   NAMESPACE=video-streaming
#   RELEASE_NAME=vs
#   REMOTE_K8S_DIR=/home/sack/videostreaming/k8s

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONTROL_PLANE="${CONTROL_PLANE:-sack@192.168.8.11}"
SSH_KEY="${SSH_KEY:-${HOME}/.ssh/cluster_key}"
NAMESPACE="${NAMESPACE:-video-streaming}"
RELEASE_NAME="${RELEASE_NAME:-vs}"
REMOTE_K8S_DIR="${REMOTE_K8S_DIR:-/home/sack/videostreaming/k8s}"
STATEFULSET="${RELEASE_NAME}-processing"
AUTOSCALER="${RELEASE_NAME}-autoscaler"
RENDERED_FILE="${ROOT_DIR}/k8s/rendered.yaml"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

remote_run() {
  ssh -i "$SSH_KEY" "$CONTROL_PLANE" "$1"
}

remote_kubectl() {
  remote_run "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n ${NAMESPACE} $*"
}

require_cmd helm
require_cmd ssh
require_cmd scp

echo "==> Rendering Helm chart with emptyDir processing spool"
SECRET_VALUES=""
if [[ -f "${ROOT_DIR}/k8s/values.secret.yaml" ]]; then
  SECRET_VALUES="-f ${ROOT_DIR}/k8s/values.secret.yaml"
fi
helm template "$RELEASE_NAME" "${ROOT_DIR}/k8s" ${SECRET_VALUES} --namespace "$NAMESPACE" \
  > "$RENDERED_FILE"

echo "==> Syncing k8s manifests to ${CONTROL_PLANE}:${REMOTE_K8S_DIR}"
remote_run "rm -rf '${REMOTE_K8S_DIR}'"
scp -i "$SSH_KEY" -r "${ROOT_DIR}/k8s" "${CONTROL_PLANE}:${REMOTE_K8S_DIR}"

echo "==> Capturing current replica counts"
PROCESSING_REPLICAS="$(remote_kubectl "get statefulset ${STATEFULSET} -o jsonpath='{.spec.replicas}'" 2>/dev/null || echo 0)"
AUTOSCALER_REPLICAS="$(remote_kubectl "get deployment ${AUTOSCALER} -o jsonpath='{.spec.replicas}'" 2>/dev/null || echo 0)"
PROCESSING_REPLICAS="${PROCESSING_REPLICAS:-0}"
AUTOSCALER_REPLICAS="${AUTOSCALER_REPLICAS:-0}"
echo "  ${STATEFULSET} replicas: ${PROCESSING_REPLICAS}"
echo "  ${AUTOSCALER} replicas: ${AUTOSCALER_REPLICAS}"

echo "==> Pausing autoscaler so it does not recreate processing pods mid-migration"
if [[ "$AUTOSCALER_REPLICAS" != "0" ]]; then
  remote_kubectl "scale deployment/${AUTOSCALER} --replicas=0"
  remote_kubectl "rollout status deployment/${AUTOSCALER} --timeout=2m" || true
fi

echo "==> Scaling ${STATEFULSET} to 0"
if [[ "$PROCESSING_REPLICAS" != "0" ]]; then
  remote_kubectl "scale statefulset/${STATEFULSET} --replicas=0"
  remote_kubectl "wait --for=delete pod -l app=${STATEFULSET} --timeout=5m" || true
fi

echo "==> Deleting immutable old StatefulSet and node-bound processing PVCs"
remote_kubectl "delete statefulset/${STATEFULSET} --ignore-not-found"
remote_kubectl "delete pvc -l app=${STATEFULSET} --ignore-not-found"

echo "==> Applying new manifests"
remote_kubectl "apply -k '${REMOTE_K8S_DIR}/'"

echo "==> Restoring ${STATEFULSET} replica count"
remote_kubectl "scale statefulset/${STATEFULSET} --replicas=${PROCESSING_REPLICAS}"
if [[ "$PROCESSING_REPLICAS" != "0" ]]; then
  remote_kubectl "rollout status statefulset/${STATEFULSET} --timeout=10m"
fi

echo "==> Restoring autoscaler replica count"
remote_kubectl "scale deployment/${AUTOSCALER} --replicas=${AUTOSCALER_REPLICAS}"
if [[ "$AUTOSCALER_REPLICAS" != "0" ]]; then
  remote_kubectl "rollout status deployment/${AUTOSCALER} --timeout=5m"
fi

echo "==> Verifying processing pods and remaining processing PVCs"
remote_kubectl "get pods -l app=${STATEFULSET} -o wide"
remote_kubectl "get pvc -l app=${STATEFULSET}" || true

echo
echo "Migration complete. ${STATEFULSET} now uses emptyDir for /app/processing-spool."
