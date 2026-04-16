#!/usr/bin/env bash
# deploy_chaos_to_k8s.sh — Deploy Chaos Killer Bot to Kubernetes as a CronJob
# Usage:
#   ./deploy_chaos_to_k8s.sh              # Deploy
#   ./deploy_chaos_to_k8s.sh delete       # Remove

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRONJOB_FILE="${SCRIPT_DIR}/helm/cronjob.yaml"

ACTION="${1:-deploy}"

case "$ACTION" in
  deploy)
    echo "Deploying Chaos Killer Bot to Kubernetes..."
    kubectl apply -f "$CRONJOB_FILE"
    echo ""
    echo "✓ Deployed! Chaos Killer Bot will run automatically."
    echo ""
    echo "Monitor logs:"
    echo "  kubectl logs -l app=chaos-killer-bot -f"
    echo ""
    echo "View chaos events in dev logs:"
    echo "  http://localhost:8081/dev-logs?format=json"
    echo ""
    echo "Disable temporarily:"
    echo "  kubectl set env cronjob/chaos-killer-bot CHAOS_ENABLED=false"
    ;;
  delete)
    echo "Removing Chaos Killer Bot from Kubernetes..."
    kubectl delete cronjob chaos-killer-bot || true
    kubectl delete serviceaccount chaos-killer-bot || true
    kubectl delete clusterrole chaos-killer-bot || true
    kubectl delete clusterrolebinding chaos-killer-bot || true
    echo "✓ Removed!"
    ;;
  *)
    echo "Usage: $0 {deploy|delete}"
    exit 1
    ;;
esac
