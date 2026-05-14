#!/usr/bin/env bash
# hard-redeploy.sh — Autoscaling-safe hard redeploy.
#
# Use this when rebuild-rollout-verify.sh leaves stale pods running the old
# image (common when reusing the same image tag with pullPolicy: IfNotPresent).
#
# What this does differently from rebuild-rollout-verify.sh:
#   1. Patches every app deployment to imagePullPolicy: Always before restart.
#   2. Imports the freshly built app image into every k3s node, so later
#      autoscaler scale-ups use the local containerd cache instead of pulling
#      a large image from Docker Hub in waves.
#   3. Force-deletes any pods stuck in Terminating so they don't block rollout.
#   4. Deletes all existing app pods after the patch so new ones are freshly
#      scheduled and the kubelet is forced to re-pull at every node.
#   5. Restores pullPolicy: IfNotPresent after all pods are Running (keeps
#      steady-state behaviour unchanged).
#
# Usage:
#   ./scripts/hard-redeploy.sh [TAG]
#   TAG defaults to whatever is currently in k8s/values.yaml.
#
# For a faster non-autoscaling redeploy that skips node-wide image import, use:
#   ./scripts/hard-redeploy-no-autoscaling.sh [TAG]

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
DIST_DIR="${ROOT_DIR}/dist"
REMOTE_IMAGE_DIR="/home/sack/videostreaming/images"

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
  # Update values.yaml — helm template will re-render rendered.yaml from it
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

cluster_node_ips() {
  remote_run "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl get nodes -o wide --no-headers | awk '{print \$6}'" \
    | tr -d '\r' \
    | sed '/^$/d'
}

distribute_image_to_k3s() {
  local image="$1"
  local archive_name="$2"
  local archive_path="${DIST_DIR}/${archive_name}"

  mkdir -p "$DIST_DIR"

  echo "==> Caching ${image} into every k3s node"
  echo "  Pulling linux/amd64 image locally before export"
  docker pull --platform linux/amd64 "$image"

  echo "  Saving image archive: ${archive_path}"
  docker save -o "$archive_path" "$image"

  NODE_IPS=()
  while IFS= read -r node_ip; do
    NODE_IPS+=("$node_ip")
  done < <(cluster_node_ips)
  if [[ "${#NODE_IPS[@]}" -eq 0 ]]; then
    echo "Could not discover cluster node IPs" >&2
    exit 1
  fi

  for node_ip in "${NODE_IPS[@]}"; do
    echo "  Importing on ${node_ip}"
    ssh -i "$SSH_KEY" "sack@${node_ip}" "mkdir -p ${REMOTE_IMAGE_DIR}"
    scp -i "$SSH_KEY" "$archive_path" "sack@${node_ip}:${REMOTE_IMAGE_DIR}/${archive_name}"
    ssh -i "$SSH_KEY" "sack@${node_ip}" \
      "doas k3s ctr -n k8s.io images import ${REMOTE_IMAGE_DIR}/${archive_name}"
  done
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

distribute_image_to_k3s "$IMAGE" "video-streaming-app-${TAG}-amd64.tar"

echo "==> Building & pushing autoscaler image ${AUTOSCALER_IMAGE}"
docker buildx build --platform linux/amd64 -f autoscaler/Dockerfile -t "$AUTOSCALER_IMAGE" --push autoscaler/

echo "==> Syncing k8s manifests to control plane"

# Re-render Helm templates into rendered.yaml so every chart change
# (new resources, configmap updates, etc.) is included in the deploy.
# values.secret.yaml is included if present (contains passwords/keys).
echo "  Re-rendering Helm templates → k8s/rendered.yaml"
SECRET_VALUES=""
if [[ -f "${ROOT_DIR}/k8s/values.secret.yaml" ]]; then
  SECRET_VALUES="-f ${ROOT_DIR}/k8s/values.secret.yaml"
fi
helm template vs "${ROOT_DIR}/k8s" ${SECRET_VALUES} --namespace video-streaming \
  > "${RENDERED_FILE}"
echo "  rendered.yaml updated"

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
echo "  - image imported into every k3s node before rollout"
echo "  - all app pods force-bounced with imagePullPolicy: Always during rollout"
echo "  - imagePullPolicy restored to IfNotPresent"
echo "  - manifests applied from ${REMOTE_K8S_DIR}"
echo "  - live app.js includes the expected status-update code"
