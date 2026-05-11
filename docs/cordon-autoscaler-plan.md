# Cordon/Uncordon Autoscaler & Node Status Dashboard

## Overview

Replace the stub autoscaler implementation (only `.pyc` artifacts exist; source `.py` files are absent) with a new **cordon/uncordon-based autoscaler** that monitors the `processing.tasks.queue` depth via the RabbitMQ Management API and activates/deactivates k3s worker nodes by uncordoning/cordoning them. Scaling is combined with proportional StatefulSet replica management so new processing pods are also scheduled or drained.

A cluster-wide `NodeStatusEvent` is published to RabbitMQ (`upload.events` exchange, routing key `upload.cluster.node_status`) so the status-service can broadcast it to all WebSocket clients. The frontend gains a **12-node icon grid** and a **queue-depth progress bar** on the processing page.

---

## Part 1 — Autoscaler (Python)

### 1.1 Design: Combined Cordon + StatefulSet Replica Scaling

Cordoning alone prevents new pods from scheduling on a node, but does **not** evict existing pods. Relying solely on cordon would leave stranded workers on "deactivated" nodes. The chosen approach combines:

| Action          | k8s operation                                   | StatefulSet replicas       |
|-----------------|-------------------------------------------------|----------------------------|
| Activate node   | `uncordon node`                                 | `+= REPLICAS_PER_NODE`     |
| Deactivate node | `evict pods on node` → `cordon node`            | `-= REPLICAS_PER_NODE`     |

**Configuration constants (all overridable via env):**

| Env var                    | Default | Purpose                                          |
|----------------------------|---------|--------------------------------------------------|
| `TOTAL_NODES`              | `12`    | Total worker node count in the cluster           |
| `MIN_ACTIVE_NODES`         | `2`     | Nodes always kept uncordoned                     |
| `REPLICAS_PER_NODE`        | `6`     | Processing pods per active node (72 / 12)        |
| `SCALE_UP_THRESHOLD`       | `20`    | Queue depth that triggers uncordoning one node   |
| `SCALE_DOWN_THRESHOLD`     | `5`     | Queue depth below which one node is cordoned     |
| `POLL_INTERVAL_SECONDS`    | `15`    | How often the autoscaler checks queue depth      |
| `SCALE_COOLDOWN_SECONDS`   | `60`    | Minimum gap between successive scale events      |
| `NODE_LABEL_SELECTOR`      | `workload-role=app` | Label that identifies worker nodes  |
| `STATEFULSET_NAME`         | `<release>-processing` | Target StatefulSet to patch       |
| `KUBE_NAMESPACE`           | `default` | Kubernetes namespace                           |
| `RABBITMQ_MANAGEMENT_PORT` | `15672` | RabbitMQ Management HTTP API port               |

**Startup behavior:** On first leader election, cordon all nodes except `MIN_ACTIVE_NODES` and set the StatefulSet replicas to `MIN_ACTIVE_NODES × REPLICAS_PER_NODE`. This operation is idempotent.

---

### 1.2 New Python Source Files — `autoscaler/`

The `autoscaler/` directory only contains compiled `.pyc` files. All source files must be created fresh. The autoscaler runs as a **2-replica Deployment** in Kubernetes, but only the leader replica (elected via a k8s `Lease`) acts.

#### `autoscaler/config.py`
Reads all configuration from environment variables with hardcoded defaults. Uses `os.environ.get()`. Validates required fields and provides a `Config` dataclass.

#### `autoscaler/metrics_collector.py`
Polls the RabbitMQ Management HTTP API:
```
GET http://{RABBITMQ_HOST}:{RABBITMQ_MANAGEMENT_PORT}/api/queues/%2F/{queue_name}
```
Uses `requests` with Basic Auth (`RABBITMQ_USER`/`RABBITMQ_PASS`). Returns `messages_ready + messages_unacknowledged` as the effective queue depth. Implements exponential backoff retry on connection failure.

#### `autoscaler/topology_manager.py`
Uses the `kubernetes` Python client. Provides:
- `list_worker_nodes()` → list of node objects filtered by `NODE_LABEL_SELECTOR`
- `get_node_states()` → dict mapping node name → `{"cordoned": bool}`
- `cordon_node(name)` → patches `spec.unschedulable = True`
- `uncordon_node(name)` → patches `spec.unschedulable = False`
- `evict_pods_on_node(name, namespace, app_label)` → creates `Eviction` objects for all non-DaemonSet pods matching app label. Polls with 120s timeout waiting for pod termination.
- `get_active_node_count()` → count of worker nodes where `spec.unschedulable` is False or None

#### `autoscaler/scaling_executor.py`
Uses the `kubernetes` Python client (`AppsV1Api`). Provides:
- `get_statefulset_replicas(namespace, name)` → current replica count
- `set_statefulset_replicas(namespace, name, count)` → patches StatefulSet replicas
- `scale_up_one_node(topology_mgr, config)` → picks first cordoned worker node; calls `uncordon_node()` then increases replicas
- `scale_down_one_node(topology_mgr, config)` → picks last active worker node; calls `evict_pods_on_node()`, `cordon_node()`, then decreases replicas

#### `autoscaler/decision_engine.py`
Pure logic (no I/O). Returns `"scale_up"`, `"scale_down"`, or `"noop"`:
- `queue_depth >= SCALE_UP_THRESHOLD` and `active_nodes < TOTAL_NODES` → `"scale_up"`
- `queue_depth <= SCALE_DOWN_THRESHOLD` and `active_nodes > MIN_ACTIVE_NODES` → `"scale_down"`
- Otherwise → `"noop"`

Cooldown enforcement happens in the main loop.

#### `autoscaler/state_store.py`
In-memory state (sufficient since leader election guarantees single active replica):
- `last_scale_time: float` — epoch seconds of last scale action
- `is_cooldown_active(cooldown_seconds) → bool`
- `record_scale_event()`
- `last_published_node_states: dict` — debounces node status publishes (only publish when state actually changes)

#### `autoscaler/leader_election.py`
Uses `kubernetes` Python client `CoordinationV1Api` with a `Lease` object. Lease name: `autoscaler-leader`. Acquires lease by patching `holderIdentity` (pod hostname) with `leaseDurationSeconds=30`. Renews every 10s in a background thread. On renewal failure, sets `is_leader = False` so the main loop pauses.

#### `autoscaler/publisher.py` *(new)*
Uses `pika` to connect to RabbitMQ and publish `NodeStatusEvent` JSON to the `upload.events` exchange with routing key `upload.cluster.node_status`.

**`NodeStatusEvent` schema:**
```json
{
  "jobId": "__cluster__",
  "taskId": "node_status",
  "type": "node_status",
  "nodes": [
    { "name": "worker-1", "state": "active" },
    { "name": "worker-2", "state": "cordoned" }
  ],
  "queueDepth": 42,
  "activeCount": 3,
  "totalCount": 12
}
```
`state` is either `"active"` (uncordoned) or `"cordoned"`.

Publishes on startup (as leader) and after every cordon/uncordon operation. Debounced by `state_store` to skip publish if node states are unchanged from the last publish.

#### `autoscaler/autoscaler.py` *(main entrypoint)*
Main polling loop:
1. Load config
2. Connect to Kubernetes via `kubernetes.config.load_incluster_config()` (falls back to `load_kube_config()` for local dev)
3. Create all service objects
4. On becoming leader: enforce initial state (cordon all nodes except `MIN_ACTIVE_NODES`, set StatefulSet replicas accordingly), publish initial `NodeStatusEvent`
5. Poll loop every `POLL_INTERVAL_SECONDS`:
   - Skip if not leader
   - Gather `queue_depth` and `active_nodes`
   - Get `decision` from `DecisionEngine`
   - If not noop and cooldown not active: execute scale, record event, publish `NodeStatusEvent`
   - If node states changed externally: publish updated `NodeStatusEvent`

#### `autoscaler/requirements.txt`
```
kubernetes>=29.0.0
pika>=1.3.0
requests>=2.31.0
```

#### `autoscaler/Dockerfile`
```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py .
CMD ["python", "-u", "autoscaler.py"]
```

---

### 1.3 Kubernetes RBAC — New Helm Template

#### `k8s/templates/autoscaler-rbac.yaml`
Creates `ServiceAccount`, `ClusterRole`, and `ClusterRoleBinding` for the autoscaler with these permissions:
- `nodes`: `get`, `list`, `watch`, `patch` (for cordon/uncordon)
- `pods`: `get`, `list`, `watch`, `delete` (for eviction checks)
- `pods/eviction`: `create` (for graceful eviction)
- `coordination.k8s.io/leases`: `get`, `create`, `update`, `patch` (for leader election)
- `apps/statefulsets`: `get`, `patch`, `update` (for replica management)

#### `k8s/templates/autoscaler.yaml`
New `Deployment` for the autoscaler:
- `replicas: 2` — two replicas, only the leader acts
- Image: new Python autoscaler image (separate from the Java app image)
- `serviceAccountName: <release>-autoscaler`
- `nodeSelector: workload-role: infra` (runs on control-plane nodes)
- Health/ready probes on port `8084`
- Resources: requests `cpu: 100m / memory: 128Mi`; limits `cpu: 500m / memory: 256Mi`
- Env vars from the main ConfigMap + new autoscaler-specific values

#### `k8s/values.yaml` additions
```yaml
autoscaler:
  image:
    repository: tanigross/video-streaming-autoscaler
    tag: "1.0"
  replicas: 2
  totalNodes: 12
  minActiveNodes: 2
  replicasPerNode: 6
  scaleUpThreshold: 20
  scaleDownThreshold: 5
  pollIntervalSeconds: 15
  scaleCooldownSeconds: 60
  nodeLabelSelector: "workload-role=app"
  statefulsetName: ""   # defaults to <release>-processing
  rabbitmqManagementPort: "15672"
  resources:
    requests: { cpu: "100m", memory: "128Mi" }
    limits: { cpu: "500m", memory: "256Mi" }
```

---

## Part 2 — Event Pipeline: Autoscaler → Status-Service → Browser

### 2.1 New Shared Java Event: `NodeStatusEvent.java`

**Location:** `shared/src/main/java/com/distributed26/videostreaming/shared/upload/events/NodeStatusEvent.java`

Extends `JobEvent` with fields:
- `String type = "node_status"` (non-final, Jackson serializes it)
- `List<NodeInfo> nodes` — inner record: `String name`, `String state` (`"active"` or `"cordoned"`)
- `int queueDepth`
- `int activeCount`
- `int totalCount`

Uses `jobId = "__cluster__"` as a sentinel. The status-service broadcasts events with this jobId to **all** connected WebSocket clients, not just job-specific subscribers.

### 2.2 `RabbitMQStatusEventCodec.java` — add decode branch

Add a branch for `type == "node_status"` before the generic `taskId` fallback in `toEvent()`. Parses the `nodes` array from JSON and constructs a `NodeStatusEvent`. Also adds a `describeEventType()` branch returning `"node_status"` for `NodeStatusEvent` instances.

### 2.3 `RabbitMQStatusEventBus.java` — add cluster event binding

In `declareConsumerQueue()`, after the existing `queueBind(statusBinding)`, add:
```java
channel.queueBind(queueName, exchange, "upload.cluster.*");
```

The existing replica-local exclusive queue (per status-service and upload-service replica) also receives `upload.cluster.node_status` messages. No new queue needed.

### 2.4 `UploadStatusWebSocket.java` — broadcast cluster events

All WebSocket connections (regardless of which `jobId` they are monitoring) should receive `NodeStatusEvent` updates:
- Add a `Map<WsContext, JobEventListener> clusterListenersByContext` field
- In `bindJob()`: subscribe the connection to `"__cluster__"` events (in addition to the job-specific subscription)
- Also subscribe on `onConnect` (before any job is bound) so node status is visible immediately
- In `cleanupContext()`: unsubscribe from `"__cluster__"` as well

### 2.5 RabbitMQ Routing Key Summary

| Event           | Producer       | Routing Key                     | Consumer Queue Binding  |
|-----------------|----------------|---------------------------------|-------------------------|
| Progress/meta   | upload-service | `upload.status.<jobId>`         | `upload.status.*`       |
| Failure         | node-watcher   | `upload.failure`                | `upload.failure`        |
| **Node status** | **autoscaler** | **`upload.cluster.node_status`**| **`upload.cluster.*`**  |

---

## Part 3 — Frontend Dashboard

### 3.1 `frontend/index.html` — Cluster Dashboard Card

Add a 4th `<article class="stage-card" id="clusterDashboard">` inside `.stage-grid`, containing:
- A queue depth progress bar with label showing the raw message count
- A 12-cell node icon grid (one icon per worker node)
- A legend: green = active, red = cordoned

The cluster dashboard is **always visible** (no `.hidden` class) — node status is cluster-wide, not per-job.

### 3.2 `frontend/app.js` — Node Status Handler

New state variables:
- `clusterNodeStates = []` — array of `{name, state}` from the latest `NodeStatusEvent`
- `clusterQueueDepth = 0`
- `QUEUE_DEPTH_MAX = 100` — cap for the progress bar display

New functions:
- `applyNodeStatusEvent(payload)` — validates type, updates state, calls renderers
- `renderNodeGrid()` — clears and rebuilds the 12-icon grid with `active`/`cordoned` CSS class
- `renderQueueDepth()` — sets the bar width and label

WebSocket message handler: add a branch for `payload.type === "node_status"` before the generic `taskId` check. Calls `applyNodeStatusEvent(payload)` and returns.

### 3.3 `frontend/app.css` — Node Grid Styles

New rules for `.node-grid`, `.node-icon`, `.node-icon.active`, `.node-icon.cordoned`, `.node-grid-legend`, `.node-dot`. Uses server/rack emoji or Unicode glyphs for the node icons. Uses existing CSS custom properties (`--accent`, `--accent-3`, `--muted`, etc.) for colors.

---

## Part 4 — Implementation Order

1. **`shared/` — Java event** (`NodeStatusEvent.java`) + codec update (`RabbitMQStatusEventCodec.java`) + bus binding (`RabbitMQStatusEventBus.java`)
2. **`upload-service/` — WebSocket broadcast** (`UploadStatusWebSocket.java`): subscribe all connections to `"__cluster__"` events
3. **Build shared + upload-service JARs:** `mvn -pl upload-service,shared -am -DskipTests install`
4. **`autoscaler/` — Python source files**: write all `.py` files; write `Dockerfile`; write `requirements.txt`; build and push autoscaler Docker image
5. **`k8s/templates/` — RBAC + Deployment**: add `autoscaler-rbac.yaml` and `autoscaler.yaml`; update `k8s/values.yaml` with `autoscaler.*` keys; add `RABBITMQ_MANAGEMENT_PORT` to ConfigMap
6. **`frontend/`** — add cluster dashboard HTML to `index.html`; add handler and DOM refs to `app.js`; add CSS to `app.css`
7. **Helm upgrade:** `helm upgrade vs ./k8s -f k8s/values.secret.yaml`; verify autoscaler logs; confirm node status appears in browser

---

## Part 5 — Key Considerations

### 5.1 Drain Behavior & Graceful Pod Eviction

`evict_pods_on_node()` must respect `terminationGracePeriodSeconds` (default 30s). Set a 120s eviction timeout. Log a warning if pods don't terminate — don't block the main autoscaler loop. Use the Kubernetes `Eviction` API (`v1.create_namespaced_pod_eviction()`), same semantics as `kubectl drain --ignore-daemonsets`.

### 5.2 Initial StatefulSet Replicas

The current `values.yaml` has `processingService.replicas: 72`. This conflicts with the autoscaler's startup target of `MIN_ACTIVE_NODES × REPLICAS_PER_NODE = 12`. **Recommended:** set `values.yaml processingService.replicas: 12` (matching `MIN_ACTIVE_NODES=2 × 6`) so the autoscaler owns replica management from the start. Alternatively, the autoscaler's startup cordon sequence will drain nodes down from 72 — noisier and slower.

### 5.3 RabbitMQ Management API Access

The autoscaler needs the RabbitMQ Management HTTP API at port 15672. Verify the `rabbitmq` Service in `k8s/templates/rabbitmq.yaml` exposes port 15672 as a ClusterIP service port. If not, add it. The autoscaler connects to `http://<release>-rabbitmq:15672`.

### 5.4 Autoscaler Image Build

The autoscaler needs its own Docker image (Python, not the Java JAR image). Add `autoscaler/Dockerfile` and update `deploy_k8s.sh` to build and push the autoscaler image before `helm upgrade`. Tag: `tanigross/video-streaming-autoscaler:1.0`.

### 5.5 Node Status Dashboard — Always Visible

The cluster dashboard shows cluster-wide state, not per-job state. It should be visible at all times (even before a video is uploaded). Unlike `#processingBlock` and `#transcodeBlock`, it does not get the `hidden` class — it renders immediately with empty/loading state and populates as soon as the first `NodeStatusEvent` arrives over WebSocket.

### 5.6 WebSocket Connection for Global Events

Currently, the WebSocket connection is only established when a job is being tracked. To receive `NodeStatusEvent` messages at any time (e.g. on the Stream tab), the frontend should establish a WebSocket connection on page load (without a jobId) so global cluster events flow immediately. The existing `connectWebSocket()` and `bindJob()` flow handles this correctly since the status-service subscribes each connection to `"__cluster__"` on connect.

