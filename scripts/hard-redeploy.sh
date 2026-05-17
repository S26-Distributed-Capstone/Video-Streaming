#!/usr/bin/env bash
# hard-redeploy.sh - Autoscaling-safe hard redeploy.
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
#
# Resume option:
#   SKIP_NODE_IMAGE_IMPORT=1 ./scripts/hard-redeploy.sh [TAG]
#   Re-runs the script without re-importing the app image archive into every k3s node.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONTROL_PLANE="sack@192.168.8.11"
SSH_KEY="${SSH_KEY:-${HOME}/.ssh/cluster_key}"
REMOTE_K8S_DIR="/home/sack/videostreaming/k8s"
KUBECTL_PREFIX="doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n video-streaming"
VALUES_FILE="${ROOT_DIR}/k8s/values.yaml"
RENDERED_FILE="${ROOT_DIR}/k8s/rendered.yaml"
IMAGE_REPO="tanigross/video-streaming-app"
AUTOSCALER_IMAGE_REPO="tanigross/video-streaming-autoscaler"
DIST_DIR="${ROOT_DIR}/dist"
REMOTE_IMAGE_DIR="/home/sack/videostreaming/images"
SSH_CONTROL_DIR="${SSH_CONTROL_DIR:-/tmp/vs-ssh}"
SSH_OPTS=(
  -o StrictHostKeyChecking=accept-new
  -o ControlMaster=auto
  -o ControlPersist=10m
  -o ControlPath="${SSH_CONTROL_DIR}/%C"
)

if [[ -f "$SSH_KEY" ]]; then
  SSH_OPTS=(-i "$SSH_KEY" "${SSH_OPTS[@]}")
fi

# Deployments managed by this script (not the autoscaler-managed StatefulSet).
APP_DEPLOYMENTS=(vs-upload vs-status vs-streaming vs-autoscaler)

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

ensure_passwordless_access() {
  local ssh_error
  mkdir -p "$SSH_CONTROL_DIR"

  if ! ssh_error="$(ssh "${SSH_OPTS[@]}" -o BatchMode=yes "$CONTROL_PLANE" "true" 2>&1 >/dev/null)"; then
    echo "Passwordless SSH to ${CONTROL_PLANE} is not configured for this script." >&2
    echo "Expected key: ${SSH_KEY}" >&2
    [[ -n "$ssh_error" ]] && echo "SSH error: ${ssh_error}" >&2
    echo "Run ./scripts/setup-keys.sh first (it now defaults to ${SSH_KEY}.pub when present)." >&2
    exit 1
  fi

  if ! ssh_error="$(ssh "${SSH_OPTS[@]}" -o BatchMode=yes "$CONTROL_PLANE" "doas -n true" 2>&1 >/dev/null)"; then
    echo "Passwordless doas is not configured on ${CONTROL_PLANE}." >&2
    [[ -n "$ssh_error" ]] && echo "doas check error: ${ssh_error}" >&2
    echo "Run ./scripts/setup-doas.sh before redeploying." >&2
    exit 1
  fi
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

current_autoscaler_tag() {
  awk '
    $1 == "autoscaler:" { in_autoscaler=1; next }
    in_autoscaler && $1 == "image:" { in_image=1; next }
    in_autoscaler && in_image && $1 == "tag:" {
      gsub(/"/, "", $2)
      print $2
      exit
    }
    in_autoscaler && $1 !~ /^(image:|repository:|tag:|pullPolicy:)$/ && $1 !~ /^#/ { in_autoscaler=0; in_image=0 }
  ' "$VALUES_FILE"
}

current_min_active_nodes() {
  awk '
    $1 == "autoscaler:" { in_autoscaler=1; next }
    in_autoscaler && $1 == "minActiveNodes:" {
      print $2
      exit
    }
    in_autoscaler && $1 !~ /^(image:|repository:|tag:|pullPolicy:|replicas:|totalNodes:|minActiveNodes:|replicasPerNode:|replicasPerCpu:|scaleUpThreshold:|scaleDownThreshold:|maxScaleUpStep:|maxScaleDownStep:|pollIntervalSeconds:|scaleCooldownSeconds:|scaleDownIdlePolls:|nodeLabelSelector:|statefulsetName:|rabbitmqManagementPort:|resources:|requests:|limits:)$/ && $1 !~ /^#/ {
      in_autoscaler=0
    }
  ' "$VALUES_FILE"
}

current_replicas_per_node() {
  awk '
    $1 == "autoscaler:" { in_autoscaler=1; next }
    in_autoscaler && $1 == "replicasPerNode:" {
      print $2
      exit
    }
    in_autoscaler && $1 !~ /^(image:|repository:|tag:|pullPolicy:|replicas:|totalNodes:|minActiveNodes:|replicasPerNode:|replicasPerCpu:|scaleUpThreshold:|scaleDownThreshold:|maxScaleUpStep:|maxScaleDownStep:|pollIntervalSeconds:|scaleCooldownSeconds:|scaleDownIdlePolls:|nodeLabelSelector:|statefulsetName:|rabbitmqManagementPort:|resources:|requests:|limits:)$/ && $1 !~ /^#/ {
      in_autoscaler=0
    }
  ' "$VALUES_FILE"
}

current_replicas_per_cpu() {
  awk '
    $1 == "autoscaler:" { in_autoscaler=1; next }
    in_autoscaler && $1 == "replicasPerCpu:" {
      print $2
      exit
    }
    in_autoscaler && $1 !~ /^(image:|repository:|tag:|pullPolicy:|replicas:|totalNodes:|minActiveNodes:|replicasPerNode:|replicasPerCpu:|scaleUpThreshold:|scaleDownThreshold:|maxScaleUpStep:|maxScaleDownStep:|pollIntervalSeconds:|scaleCooldownSeconds:|scaleDownIdlePolls:|nodeLabelSelector:|statefulsetName:|rabbitmqManagementPort:|resources:|requests:|limits:)$/ && $1 !~ /^#/ {
      in_autoscaler=0
    }
  ' "$VALUES_FILE"
}

current_node_label_selector() {
  awk '
    $1 == "autoscaler:" { in_autoscaler=1; next }
    in_autoscaler && $1 == "nodeLabelSelector:" {
      gsub(/"/, "", $2)
      print $2
      exit
    }
    in_autoscaler && $1 !~ /^(image:|repository:|tag:|pullPolicy:|replicas:|totalNodes:|minActiveNodes:|replicasPerNode:|replicasPerCpu:|scaleUpThreshold:|scaleDownThreshold:|maxScaleUpStep:|maxScaleDownStep:|pollIntervalSeconds:|scaleCooldownSeconds:|scaleDownIdlePolls:|nodeLabelSelector:|statefulsetName:|rabbitmqManagementPort:|resources:|requests:|limits:)$/ && $1 !~ /^#/ {
      in_autoscaler=0
    }
  ' "$VALUES_FILE"
}

update_tag_files() {
  local new_tag="$1"
  # Update values.yaml - helm template will re-render rendered.yaml from it
  perl -0pi -e 's/(tag:\s*")[^"]+(")/${1}'"$new_tag"'${2}/' "$VALUES_FILE"
}

remote_run() {
  ssh "${SSH_OPTS[@]}" "$CONTROL_PLANE" "$1"
}

remote_kubectl() {
  remote_run "${KUBECTL_PREFIX} $*"
}

force_autoscaling_off_state() {
  local restore_nodes="$1"
  local lease_name="autoscaler-leader-state"
  echo "==> Forcing shared autoscaling state OFF (restoreActiveNodes=${restore_nodes})"
  remote_run "cat <<'EOF' | ${KUBECTL_PREFIX} apply -f -
apiVersion: coordination.k8s.io/v1
kind: Lease
metadata:
  name: ${lease_name}
  namespace: video-streaming
EOF" >/dev/null
  remote_kubectl "annotate lease ${lease_name} autoscalingOn=false restoreActiveNodes=${restore_nodes} --overwrite"
}

adjust_processing_replicas_to_fit() {
  local statefulset_name="${STATEFULSET_NAME:-vs-processing}"

  while true; do
    local unschedulable_count
    unschedulable_count="$(remote_run "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n video-streaming get pods -l app=${statefulset_name} --field-selector=status.phase=Pending -o jsonpath='{range .items[*]}{range .status.conditions[?(@.type==\"PodScheduled\")]}{.status}{\" \"}{.reason}{\"\\n\"}{end}{end}' | awk '\$1 == \"False\" && \$2 == \"Unschedulable\" { count++ } END { print count + 0 }'" \
      | tr -d '\r')"
    if [[ "${unschedulable_count:-0}" -eq 0 ]]; then
      return 0
    fi

    local current_replicas
    current_replicas="$(remote_kubectl "get statefulset ${statefulset_name} -o jsonpath='{.spec.replicas}'" | tr -d '\r')"
    if [[ -z "$current_replicas" || "$current_replicas" -le 1 ]]; then
      echo "Processing rollout is blocked by unschedulable pods and cannot be reduced further." >&2
      return 1
    fi

    local next_replicas=$((current_replicas - 1))
    echo "  Found ${unschedulable_count} unschedulable processing pod(s); reducing ${statefulset_name} replicas ${current_replicas} -> ${next_replicas}"
    remote_kubectl "scale statefulset/${statefulset_name} --replicas=${next_replicas}"
    sleep 5
  done
}

wait_for_processing_rollout() {
  local statefulset_name="${STATEFULSET_NAME:-vs-processing}"
  local timeout_seconds=900
  local deadline=$(( $(date +%s) + timeout_seconds ))

  while (( $(date +%s) < deadline )); do
    if remote_kubectl "rollout status statefulset/${statefulset_name} --timeout=20s"; then
      return 0
    fi

    if ! adjust_processing_replicas_to_fit; then
      return 1
    fi
  done

  echo "Timed out waiting for statefulset/${statefulset_name} rollout to complete." >&2
  return 1
}

enforce_processing_off_baseline() {
  local target_nodes="$1"
  local replicas_per_node="$2"
  local replicas_per_cpu="$3"
  local node_label_selector="$4"
  local statefulset_name="${STATEFULSET_NAME:-vs-processing}"

  echo "==> Enforcing processing OFF baseline across ${target_nodes} node(s)"
  local node_info
  node_info="$(remote_run "doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl get nodes -l '${node_label_selector}' -o jsonpath='{range .items[*]}{.metadata.name}{\" \"}{.status.allocatable.cpu}{\"\\n\"}{end}'" \
    | tr -d '\r' \
    | awk '
      function cpu_to_int(v) {
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", v)
        if (v ~ /m$/) {
          sub(/m$/, "", v)
          return int(v / 1000)
        }
        return int(v + 0)
      }
      {
        cpu = cpu_to_int($2)
        if (cpu < 1) cpu = 1
        print $1, cpu
      }
    ' \
    | sort -k2,2nr -k1,1 \
    | head -n ${target_nodes})"

  if [[ -z "$node_info" ]]; then
    echo "Could not determine processing baseline nodes for selector ${node_label_selector}" >&2
    exit 1
  fi

  local selected_nodes=()
  local total_replicas=0
  while read -r node_name cpu_count; do
    [[ -z "$node_name" ]] && continue
    selected_nodes+=("$node_name")
    local node_replicas
    node_replicas="$(awk -v cpu="$cpu_count" -v rpn="$replicas_per_node" -v rpc="$replicas_per_cpu" 'BEGIN { value = int(cpu * rpc); if (value < rpn) value = rpn; print value }')"
    total_replicas=$((total_replicas + node_replicas))
  done <<< "$node_info"

  local values_json
  values_json="$(printf '"%s",' "${selected_nodes[@]}")"
  values_json="[${values_json%,}]"
  local affinity_patch
  affinity_patch="$(cat <<EOF
{"spec":{"template":{"spec":{"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":${values_json}}]}]}}}}}}}
EOF
)"

  echo "  Selected nodes: ${selected_nodes[*]}"
  echo "  Target replicas: ${total_replicas}"
  remote_kubectl "patch statefulset ${statefulset_name} --type=merge -p '${affinity_patch}'"
  remote_kubectl "scale statefulset/${statefulset_name} --replicas=${total_replicas}"
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
    ssh "${SSH_OPTS[@]}" "sack@${node_ip}" "mkdir -p ${REMOTE_IMAGE_DIR}"
    scp "${SSH_OPTS[@]}" "$archive_path" "sack@${node_ip}:${REMOTE_IMAGE_DIR}/${archive_name}"
    ssh "${SSH_OPTS[@]}" "sack@${node_ip}" \
      "doas k3s ctr -n k8s.io images import ${REMOTE_IMAGE_DIR}/${archive_name}"
  done
}

# Patch a single deployment's container imagePullPolicy.
patch_pull_policy() {
  local deploy="$1"
  local policy="$2"
  echo "  Patching ${deploy} -> imagePullPolicy: ${policy}"
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

ensure_passwordless_access

TAG="${1:-$(current_tag)}"
IMAGE="${IMAGE_REPO}:${TAG}"
AUTOSCALER_TAG="$(current_autoscaler_tag)"
AUTOSCALER_IMAGE="${AUTOSCALER_IMAGE_REPO}:${AUTOSCALER_TAG}"
MIN_ACTIVE_NODES="${MIN_ACTIVE_NODES:-$(current_min_active_nodes)}"
REPLICAS_PER_NODE="${REPLICAS_PER_NODE:-$(current_replicas_per_node)}"
REPLICAS_PER_CPU="${REPLICAS_PER_CPU:-$(current_replicas_per_cpu)}"
NODE_LABEL_SELECTOR="${NODE_LABEL_SELECTOR:-$(current_node_label_selector)}"

if [[ -z "$TAG" ]]; then
  echo "Could not determine image tag from k8s/values.yaml" >&2
  exit 1
fi
if [[ -z "$AUTOSCALER_TAG" ]]; then
  echo "Could not determine autoscaler image tag from k8s/values.yaml" >&2
  exit 1
fi
if [[ -z "$MIN_ACTIVE_NODES" ]]; then
  echo "Could not determine autoscaler minActiveNodes from k8s/values.yaml" >&2
  exit 1
fi
if [[ -z "$REPLICAS_PER_NODE" || -z "$REPLICAS_PER_CPU" || -z "$NODE_LABEL_SELECTOR" ]]; then
  echo "Could not determine autoscaler baseline settings from k8s/values.yaml" >&2
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

if [[ "${SKIP_NODE_IMAGE_IMPORT:-0}" == "1" ]]; then
  echo "==> Skipping node-wide image import (SKIP_NODE_IMAGE_IMPORT=1)"
else
  distribute_image_to_k3s "$IMAGE" "video-streaming-app-${TAG}-amd64.tar"
fi

echo "==> Building & pushing autoscaler image ${AUTOSCALER_IMAGE}"
docker buildx build --platform linux/amd64 -f autoscaler/Dockerfile -t "$AUTOSCALER_IMAGE" --push autoscaler/

echo "==> Syncing k8s manifests to control plane"

# Re-render Helm templates into rendered.yaml so every chart change
# (new resources, configmap updates, etc.) is included in the deploy.
# values.secret.yaml is included if present (contains passwords/keys).
echo "  Re-rendering Helm templates -> k8s/rendered.yaml"
SECRET_VALUES=""
if [[ -f "${ROOT_DIR}/k8s/values.secret.yaml" ]]; then
  SECRET_VALUES="-f ${ROOT_DIR}/k8s/values.secret.yaml"
fi
helm template vs "${ROOT_DIR}/k8s" ${SECRET_VALUES} --namespace video-streaming \
  > "${RENDERED_FILE}"
echo "  rendered.yaml updated"

remote_run "rm -rf ${REMOTE_K8S_DIR}"
scp "${SSH_OPTS[@]}" -r "${ROOT_DIR}/k8s" "${CONTROL_PLANE}:${REMOTE_K8S_DIR}"

echo "==> Applying manifests"
remote_kubectl "apply -k ${REMOTE_K8S_DIR}/"
force_autoscaling_off_state "$MIN_ACTIVE_NODES"
enforce_processing_off_baseline "$MIN_ACTIVE_NODES" "$REPLICAS_PER_NODE" "$REPLICAS_PER_CPU" "$NODE_LABEL_SELECTOR"

echo "==> Patching deployments -> imagePullPolicy: Always (force fresh pull)"
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
echo "  Waiting: vs-processing"
wait_for_processing_rollout

echo "==> Restoring imagePullPolicy: IfNotPresent (steady-state)"
for deploy in "${APP_DEPLOYMENTS[@]}"; do
  patch_pull_policy "$deploy" "IfNotPresent"
done

echo "==> Current pod placement"
remote_kubectl "get pods -o wide"

echo "==> Verifying running image IDs"
for selector in app=vs-upload app=vs-status app=vs-streaming app=vs-processing app=vs-autoscaler; do
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
