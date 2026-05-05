#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONTROL_PLANE="sack@192.168.8.11"
REMOTE_K8S_DIR="/home/sack/videostreaming/k8s"
KUBECTL_PREFIX="doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl"
VALUES_FILE="${ROOT_DIR}/k8s/values.yaml"
RENDERED_FILE="${ROOT_DIR}/k8s/rendered.yaml"
DIST_TAR="${ROOT_DIR}/dist/images-amd64.tar"
IMAGE_REPO="tanigross/video-streaming-app"
BASE_IMAGES=(
  "rabbitmq:3-management"
  "quay.io/minio/minio:latest"
  "postgres:16-alpine"
  "busybox:1.36"
)

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
  ssh "$CONTROL_PLANE" "$1"
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

echo "==> Building amd64 image ${IMAGE}"
docker buildx build --platform linux/amd64 -t "$IMAGE" --load .

echo "==> Verifying local image architecture"
LOCAL_ARCH="$(docker image inspect "$IMAGE" --format '{{.Architecture}}/{{.Os}}')"
echo "Local image architecture: ${LOCAL_ARCH}"
if [[ "$LOCAL_ARCH" != "amd64/linux" ]]; then
  echo "Expected amd64/linux but got ${LOCAL_ARCH}" >&2
  exit 1
fi

mkdir -p "${ROOT_DIR}/dist"

echo "==> Saving image bundle to ${DIST_TAR}"
docker save -o "$DIST_TAR" "$IMAGE" "${BASE_IMAGES[@]}"

echo "==> Distributing image bundle to cluster nodes"
"${ROOT_DIR}/scripts/distribute-images.sh"

echo "==> Syncing k8s manifests to control plane"
remote_run "rm -rf ${REMOTE_K8S_DIR}"
scp -r "${ROOT_DIR}/k8s" "${CONTROL_PLANE}:${REMOTE_K8S_DIR}"

echo "==> Applying manifests"
remote_kubectl "apply -k ${REMOTE_K8S_DIR}/"

echo "==> Restarting app workloads"
remote_kubectl "rollout restart deployment/vs-upload deployment/vs-status deployment/vs-streaming"
remote_kubectl "rollout restart statefulset/vs-processing"

echo "==> Waiting for rollouts"
remote_kubectl "rollout status deployment/vs-upload --timeout=10m"
remote_kubectl "rollout status deployment/vs-status --timeout=10m"
remote_kubectl "rollout status deployment/vs-streaming --timeout=10m"
remote_kubectl "rollout status statefulset/vs-processing --timeout=20m"

echo "==> Current pod placement"
remote_kubectl "get pods -o wide"

echo "==> Verifying running image IDs"
LOCAL_IMAGE_ID="$(docker image inspect "$IMAGE" --format '{{.Id}}' | sed 's|^sha256:||')"
echo "Local image ID: ${LOCAL_IMAGE_ID}"
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
echo "  - local image built as amd64"
echo "  - manifests applied from ${REMOTE_K8S_DIR}"
echo "  - app workloads restarted"
echo "  - live app.js includes the new status-update code"
