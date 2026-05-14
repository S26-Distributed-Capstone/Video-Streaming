#!/usr/bin/env bash
# hard-redeploy-no-autoscaling.sh — Hard redeploy without node-wide image caching.
#
# This is the faster variant. It does NOT import the app image into every k3s
# node. It leaves the autoscaler running, applies manifests, and forces the
# currently running app workloads to pull the freshly pushed image.
#
# If the autoscaler later creates processing pods on nodes without the image
# cached, those pods may still pull from Docker Hub in waves. Use
# hard-redeploy.sh when autoscaler scale-up performance matters.
#
# Usage:
#   ./scripts/hard-redeploy-no-autoscaling.sh [TAG]
#   TAG defaults to whatever is currently in k8s/values.yaml.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONTROL_PLANE="sack@192.168.8.11"
SSH_KEY="${HOME}/.ssh/cluster_key"
REMOTE_K8S_DIR="/home/sack/videostreaming/k8s"
KUBECTL_PREFIX="doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n video-streaming"
VALUES_FILE="${ROOT_DIR}/k8s/values.yaml"
RENDERED_FILE="${ROOT_DIR}/k8s/rendered.yaml"
IMAGE_REPO="tanigross/video-streaming-app"

APP_DEPLOYMENTS=(vs-upload vs-status vs-streaming)
PROCESSING_STATEFULSET="vs-processing"

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

patch_deployment_pull_policy() {
  local deploy="$1"
  local policy="$2"
  echo "  Patching deployment/${deploy} -> imagePullPolicy: ${policy}"
  remote_kubectl "patch deployment ${deploy} \
    --type=json \
    -p='[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/imagePullPolicy\",\"value\":\"${policy}\"}]'" \
    2>/dev/null || true
}

patch_processing_pull_policy() {
  local policy="$1"
  echo "  Patching statefulset/${PROCESSING_STATEFULSET} -> imagePullPolicy: ${policy}"
  remote_kubectl "patch statefulset ${PROCESSING_STATEFULSET} \
    --type=json \
    -p='[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/imagePullPolicy\",\"value\":\"${policy}\"}]'" \
    2>/dev/null || true
}

force_delete_terminating() {
  echo "==> Clearing any pods stuck in Terminating"
  remote_run "${KUBECTL_PREFIX} get pods \
    -o jsonpath='{range .items[?(@.metadata.deletionTimestamp)]}{.metadata.name}{\"\\n\"}{end}'" \
    | while read -r pod; do
    [[ -z "$pod" ]] && continue
    echo "  Force-deleting stuck Terminating pod: ${pod}"
    remote_kubectl "delete pod ${pod} --grace-period=0 --force" 2>/dev/null || true
  done
}

require_cmd mvn
require_cmd docker
require_cmd ssh
require_cmd scp
require_cmd curl
require_cmd perl
require_cmd awk
require_cmd grep
require_cmd helm

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

echo "==> Re-rendering Helm templates"
SECRET_VALUES=""
if [[ -f "${ROOT_DIR}/k8s/values.secret.yaml" ]]; then
  SECRET_VALUES="-f ${ROOT_DIR}/k8s/values.secret.yaml"
fi
helm template vs "${ROOT_DIR}/k8s" ${SECRET_VALUES} --namespace video-streaming \
  > "${RENDERED_FILE}"

echo "==> Syncing k8s manifests to control plane"
remote_run "rm -rf ${REMOTE_K8S_DIR}"
scp -i "$SSH_KEY" -r "${ROOT_DIR}/k8s" "${CONTROL_PLANE}:${REMOTE_K8S_DIR}"

echo "==> Applying manifests"
remote_kubectl "apply -k ${REMOTE_K8S_DIR}/"

echo "==> Patching workloads -> imagePullPolicy: Always"
for deploy in "${APP_DEPLOYMENTS[@]}"; do
  patch_deployment_pull_policy "$deploy" "Always"
done
patch_processing_pull_policy "Always"

force_delete_terminating

echo "==> Restarting workloads"
for deploy in "${APP_DEPLOYMENTS[@]}"; do
  remote_kubectl "rollout restart deployment/${deploy}"
done
remote_kubectl "rollout restart statefulset/${PROCESSING_STATEFULSET}"

echo "==> Waiting for rollouts"
for deploy in "${APP_DEPLOYMENTS[@]}"; do
  echo "  Waiting: deployment/${deploy}"
  remote_kubectl "rollout status deployment/${deploy} --timeout=10m"
done
echo "  Waiting: statefulset/${PROCESSING_STATEFULSET}"
remote_kubectl "rollout status statefulset/${PROCESSING_STATEFULSET} --timeout=15m"

echo "==> Restoring imagePullPolicy: IfNotPresent"
for deploy in "${APP_DEPLOYMENTS[@]}"; do
  patch_deployment_pull_policy "$deploy" "IfNotPresent"
done
patch_processing_pull_policy "IfNotPresent"

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
echo "No-autoscaling hard redeploy complete."
echo "Verified:"
echo "  - image pushed to Docker Hub as ${IMAGE}"
echo "  - autoscaler was left running"
echo "  - upload/status/streaming/processing restarted with imagePullPolicy: Always"
echo "  - imagePullPolicy restored to IfNotPresent"
echo "  - no node-wide image import was performed"
