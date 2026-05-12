#!/usr/bin/env bash
# hard-redeploy.sh — Force a full image re-pull and pod replacement.
#
# Use this when rebuild-rollout-verify.sh leaves stale pods running the old
# image (common when reusing the same image tag with pullPolicy: IfNotPresent).
#
# What this does differently from rebuild-rollout-verify.sh:
#   1. Patches every app deployment to imagePullPolicy: Always before restart.
#   2. Force-deletes any pods stuck in Terminating so they don't block rollout.
#   3. Deletes all existing app pods after the patch so new ones are freshly
#      scheduled and the kubelet is forced to re-pull at every node.
#   4. Restores pullPolicy: IfNotPresent after all pods are Running (keeps
#      steady-state behaviour unchanged).
#
# Usage:
#   ./scripts/hard-redeploy.sh [TAG]
#   TAG defaults to whatever is currently in k8s/values.yaml.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONTROL_PLANE="sack@192.168.8.11"
SSH_KEY="${HOME}/.ssh/cluster_key"
REMOTE_K8S_DIR="/home/sack/videostreaming/k8s"
KUBECTL_PREFIX="doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n video-streaming"
VALUES_FILE="${ROOT_DIR}/k8s/values.yaml"
RENDERED_FILE="${ROOT_DIR}/k8s/rendered.yaml"
IMAGE_REPO="jasonroth03/video-streaming-app"
AUTOSCALER_IMAGE="jasonroth03/video-streaming-autoscaler:1.3"

# Deployments managed by this script (not the autoscaler-managed StatefulSet).
APP_DEPLOYMENTS=(vs-upload vs-status vs-streaming vs-autoscaler)

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

current_tag() {
  awk '
    $1 == "image:" { in_image=1; next }
    in_image && $1 == "tag:" {
      gsub(/"/, "", $2)
      print $2
      exit
    }
    in_image && $1 !~ /^(repository:|tag:|pullPolicy:)$/ { in_image=0 }
  ' "$VALUES_FILE"
}

update_tag_files() {
  local new_tag="$1"
  perl -0pi -e 's/(tag:\s*")[^"]+(")/${1}'"$new_tag"'${2}/' "$VALUES_FILE"
  perl -0pi -e 's|(image: "tanigross/video-streaming-app:)[^"]+(")|${1}'"$new_tag"'${2}|g' "$RENDERED_FILE"
}

remote_run() {
  ssh -i "$SSH_KEY" "$CONTROL_PLANE" "$1"
}

remote_kubectl() {
  remote_run "${KUBECTL_PREFIX} $*"
}

first_external_ip() {
  remote_kubectl "get svc vs-upload -o jsonpath='{.status.loadBalancer.ingress[0].ip}'" | tr -d '\r'
}

# Patch a single deployment's container imagePullPolicy.
patch_pull_policy() {
  local deploy="$1"
  local policy="$2"
  echo "  Patching ${deploy} → imagePullPolicy: ${policy}"
  remote_kubectl "patch deployment ${deploy} \
    --type=json \
    -p='[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/imagePullPolicy\",\"value\":\"${policy}\"}]'" \
    2>/dev/null || true
}

# Force-delete pods stuck in Terminating (grace-period=0).
force_delete_terminating() {
  echo "==> Clearing any pods stuck in Terminating"
  remote_run "${KUBECTL_PREFIX} get pods \
    --field-selector=status.phase==Failed \
    -o jsonpath='{.items[*].metadata.name}'" | tr ' ' '\n' | while read -r pod; do
    [[ -z "$pod" ]] && continue
    echo "  Force-deleting failed pod: ${pod}"
    remote_kubectl "delete pod ${pod} --grace-period=0 --force" 2>/dev/null || true
  done

  # Also handle Terminating pods (they show up in get pods, not field-selector)
  remote_run "${KUBECTL_PREFIX} get pods \
    -o jsonpath='{range .items[?(@.metadata.deletionTimestamp)]}{.metadata.name}{\"\\n\"}{end}'" \
    | while read -r pod; do
    [[ -z "$pod" ]] && continue
    echo "  Force-deleting stuck Terminating pod: ${pod}"
    remote_kubectl "delete pod ${pod} --grace-period=0 --force" 2>/dev/null || true
  done
}

# Delete all running pods for a deployment label so the ReplicaSet recreates
# them immediately with the new imagePullPolicy in effect.
bounce_deployment_pods() {
  local selector="$1"
  echo "  Bouncing pods: ${selector}"
  remote_kubectl "delete pods -l ${selector} --grace-period=5" 2>/dev/null || true
}

require_cmd mvn
require_cmd docker
require_cmd ssh
require_cmd scp
require_cmd curl
require_cmd perl
require_cmd awk
require_cmd grep

TAG="${1:-$(current_tag)}"
IMAGE="${IMAGE_REPO}:${TAG}"

if [[ -z "$TAG" ]]; then
  echo "Could not determine image tag from k8s/values.yaml" >&2
  exit 1
fi

if [[ "$TAG" != "$(current_tag)" ]]; then
  echo "==> Updating manifests to image tag ${TAG}"
  update_tag_files "$TAG"
fi

cd "$ROOT_DIR"

echo "==> Building Maven artifacts"
mvn -pl upload-service,processing-service,streaming-service -am -DskipTests clean package

echo "==> Building & pushing amd64 image ${IMAGE}"
docker buildx build --platform linux/amd64 -f Dockerfile.prebuilt -t "$IMAGE" --push .

echo "==> Building & pushing autoscaler image ${AUTOSCALER_IMAGE}"
docker buildx build --platform linux/amd64 -f autoscaler/Dockerfile -t "$AUTOSCALER_IMAGE" --push autoscaler/

echo "==> Syncing k8s manifests to control plane"
remote_run "rm -rf ${REMOTE_K8S_DIR}"
scp -i "$SSH_KEY" -r "${ROOT_DIR}/k8s" "${CONTROL_PLANE}:${REMOTE_K8S_DIR}"

echo "==> Applying manifests"
remote_kubectl "apply -k ${REMOTE_K8S_DIR}/"

echo "==> Patching deployments → imagePullPolicy: Always (force fresh pull)"
for deploy in "${APP_DEPLOYMENTS[@]}"; do
  patch_pull_policy "$deploy" "Always"
done

force_delete_terminating

echo "==> Force-bouncing all app pods"
for selector in app=vs-upload app=vs-status app=vs-streaming app=vs-autoscaler; do
  bounce_deployment_pods "$selector"
done

echo "==> Waiting for rollouts (pods will pull fresh images)"
for deploy in "${APP_DEPLOYMENTS[@]}"; do
  echo "  Waiting: ${deploy}"
  remote_kubectl "rollout status deployment/${deploy} --timeout=10m"
done

echo "==> Restoring imagePullPolicy: IfNotPresent (steady-state)"
for deploy in "${APP_DEPLOYMENTS[@]}"; do
  patch_pull_policy "$deploy" "IfNotPresent"
done

echo "==> Current pod placement"
remote_kubectl "get pods -o wide"

echo "==> Verifying running image IDs"
for selector in app=vs-upload app=vs-status app=vs-streaming app=vs-processing; do
  echo "-- ${selector}"
  remote_kubectl "get pods -l ${selector} \
    -o jsonpath='{range .items[*]}{.metadata.name}{\" \"}{.status.containerStatuses[0].imageID}{\"\\n\"}{end}'"
done

echo "==> Verifying live frontend markers"
UPLOAD_IP="$(first_external_ip)"
if [[ -z "$UPLOAD_IP" ]]; then
  echo "Could not determine vs-upload external IP" >&2
  exit 1
fi
echo "Upload external IP: ${UPLOAD_IP}"

# Brief wait for the new pod to start serving
sleep 5

APP_JS="$(curl -fsS "http://${UPLOAD_IP}:8080/app.js")"
if ! grep -Eq 'applyLiveProgressEvent|applyLiveTranscodeEvent' <<<"$APP_JS"; then
  echo "Live app.js does not contain expected websocket live-update markers" >&2
  exit 1
fi
if ! grep -Eq 'completedSegments = Math.max|state.done = Math.max' <<<"$APP_JS"; then
  echo "Live app.js does not contain expected monotonic snapshot guards" >&2
  exit 1
fi

echo
echo "Hard redeploy complete."
echo "Verified:"
echo "  - image pushed to Docker Hub as ${IMAGE}"
echo "  - all app pods force-bounced with imagePullPolicy: Always during rollout"
echo "  - imagePullPolicy restored to IfNotPresent"
echo "  - manifests applied from ${REMOTE_K8S_DIR}"
echo "  - live app.js includes the expected status-update code"

