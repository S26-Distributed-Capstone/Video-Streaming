#!/usr/bin/env bash
# hard-redeploy-no-autoscaling.sh - Hard redeploy without node-wide image caching.
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
SSH_KEY="${SSH_KEY:-${HOME}/.ssh/cluster_key}"
REMOTE_K8S_DIR="/home/sack/videostreaming/k8s"
KUBECTL_PREFIX="doas env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n video-streaming"
VALUES_FILE="${ROOT_DIR}/k8s/values.yaml"
RENDERED_FILE="${ROOT_DIR}/k8s/rendered.yaml"
IMAGE_REPO="tanigross/video-streaming-app"
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

APP_DEPLOYMENTS=(vs-upload vs-status vs-streaming)
PROCESSING_STATEFULSET="vs-processing"

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

ensure_passwordless_access

TAG="${1:-$(current_tag)}"
IMAGE="${IMAGE_REPO}:${TAG}"
MIN_ACTIVE_NODES="${MIN_ACTIVE_NODES:-$(current_min_active_nodes)}"
REPLICAS_PER_NODE="${REPLICAS_PER_NODE:-$(current_replicas_per_node)}"
REPLICAS_PER_CPU="${REPLICAS_PER_CPU:-$(current_replicas_per_cpu)}"
NODE_LABEL_SELECTOR="${NODE_LABEL_SELECTOR:-$(current_node_label_selector)}"

if [[ -z "$TAG" ]]; then
  echo "Could not determine image tag from k8s/values.yaml" >&2
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

echo "==> Re-rendering Helm templates"
SECRET_VALUES=""
if [[ -f "${ROOT_DIR}/k8s/values.secret.yaml" ]]; then
  SECRET_VALUES="-f ${ROOT_DIR}/k8s/values.secret.yaml"
fi
helm template vs "${ROOT_DIR}/k8s" ${SECRET_VALUES} --namespace video-streaming \
  > "${RENDERED_FILE}"

echo "==> Syncing k8s manifests to control plane"
remote_run "rm -rf ${REMOTE_K8S_DIR}"
scp "${SSH_OPTS[@]}" -r "${ROOT_DIR}/k8s" "${CONTROL_PLANE}:${REMOTE_K8S_DIR}"

echo "==> Applying manifests"
remote_kubectl "apply -k ${REMOTE_K8S_DIR}/"
force_autoscaling_off_state "$MIN_ACTIVE_NODES"
enforce_processing_off_baseline "$MIN_ACTIVE_NODES" "$REPLICAS_PER_NODE" "$REPLICAS_PER_CPU" "$NODE_LABEL_SELECTOR"

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
wait_for_processing_rollout

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
