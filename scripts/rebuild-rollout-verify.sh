#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONTROL_PLANE="sack@192.168.8.11"
SSH_KEY="${HOME}/.ssh/cluster_key"
REMOTE_K8S_DIR="/home/sack/videostreaming/k8s"
KUBECTL_PREFIX="doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n video-streaming"
VALUES_FILE="${ROOT_DIR}/k8s/values.yaml"
RENDERED_FILE="${ROOT_DIR}/k8s/rendered.yaml"
IMAGE_REPO="tanigross/video-streaming-app"
AUTOSCALER_IMAGE="tanigross/video-streaming-autoscaler:1.4"

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

echo "==> Restarting app workloads"
remote_kubectl "rollout restart deployment/vs-upload deployment/vs-status deployment/vs-streaming deployment/vs-autoscaler"
# Note: vs-processing StatefulSet is managed by the autoscaler — do not force-restart it.

echo "==> Waiting for rollouts"
remote_kubectl "rollout status deployment/vs-upload --timeout=10m"
remote_kubectl "rollout status deployment/vs-status --timeout=10m"
remote_kubectl "rollout status deployment/vs-streaming --timeout=10m"
remote_kubectl "rollout status deployment/vs-autoscaler --timeout=10m"
# StatefulSet rollout is autoscaler-driven — skipped here.

echo "==> Current pod placement"
remote_kubectl "get pods -o wide"

echo "==> Verifying running image IDs"
for selector in app=vs-upload app=vs-status app=vs-streaming app=vs-processing; do
  echo "-- ${selector}"
  remote_kubectl "get pods -l ${selector} -o jsonpath='{range .items[*]}{.metadata.name}{\" \"}{.status.containerStatuses[0].imageID}{\"\\n\"}{end}'"
done

echo "==> Verifying live frontend markers"
UPLOAD_IP="$(first_external_ip)"
if [[ -z "$UPLOAD_IP" ]]; then
  echo "Could not determine vs-upload external IP" >&2
  exit 1
fi
echo "Upload external IP: ${UPLOAD_IP}"
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
echo "Rollout complete."
echo "Verified:"
echo "  - image pushed to Docker Hub as ${IMAGE}"
echo "  - manifests applied from ${REMOTE_K8S_DIR}"
echo "  - app workloads restarted"
echo "  - live app.js includes the new status-update code"
