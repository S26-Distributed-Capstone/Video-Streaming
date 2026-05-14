# Kubernetes Deployment Guide

Deploy the video streaming platform to Kubernetes using Helm.

---

## Prerequisites

Install the following tools:

```bash
brew install minikube kubectl helm
```

Verify:

```bash
minikube version
kubectl version --client
helm version
```

## 1. Start a Local Cluster

```bash
minikube start --cpus=4 --memory=8192 --driver=docker
```

Enable the metrics addon (needed if you want autoscaling later):

```bash
minikube addons enable metrics-server
```

## 2. Build the App Image

Point Docker to Minikube's daemon so the image is available inside the cluster without a registry:

```bash
eval $(minikube docker-env)
```

Build the Java modules and Docker image:

```bash
mvn -pl upload-service,processing-service,streaming-service -am -DskipTests install
docker build -t video-streaming-app:latest .
```

## 3. Configure Secrets

The `.env` file holds all credentials. It is gitignored and never committed.

Copy the example and fill in your values:

```bash
cp .env.example .env
```

Required keys in `.env`:

```
PG_USER=admin
PG_PASSWORD=<your-password>
RABBITMQ_USER=guest
RABBITMQ_PASS=<your-password>
RABBITMQ_ERLANG_COOKIE=<random-string>
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=<your-password>
```

The deploy script reads these automatically. No secrets are stored in the Helm chart or committed to git.

## 4. Deploy

```bash
./deploy_k8s.sh
```

This runs `helm install` and injects your `.env` secrets via `--set` flags.

> **Note:** After deploying, it may take up to 30 seconds before the frontend is accessible. The 3-node RabbitMQ cluster needs to form first. The chart uses init containers to wait for dependencies, and the Java RabbitMQ clients also retry broker initialization during startup, which reduces early `CrashLoopBackOff` failures while the broker is still warming up.

> **Processing workload note:** `processing-service` is deployed as a `StatefulSet` on this branch, not a `Deployment`. Each replica gets a stable pod identity, but its `processing-spool` is ephemeral `emptyDir` storage so cordon-based autoscaling can reschedule workers onto any eligible app node without persistent-volume node affinity.

## 5. Start the Network Tunnel

In a **separate terminal**, run:

```bash
minikube tunnel
```

This exposes LoadBalancer services on localhost. It will ask for your macOS password. Keep it running.

## 6. Access the App

| Service   | URL                       |
|-----------|---------------------------|
| Upload    | http://localhost:8080      |
| Status    | http://localhost:8081      |
| Streaming | http://localhost:8083      |
| MinIO     | http://localhost:9000      |
| RabbitMQ  | http://localhost:15672     |

RabbitMQ management login uses the `RABBITMQ_USER`/`RABBITMQ_PASS` from your `.env`.

## Common Operations

### Upgrade after code or config changes

```bash
# Rebuild if Java code changed
eval $(minikube docker-env)
mvn -pl upload-service,processing-service,streaming-service -am -DskipTests install
docker build -t video-streaming-app:latest .

# Apply changes
./deploy_k8s.sh upgrade

# Restart pods to pick up new image/config
kubectl rollout restart deployment vs-upload vs-status vs-streaming
kubectl rollout restart statefulset vs-processing
```

### Separate infra and app nodes

The chart now defaults to:

- `postgres`, `rabbitmq`, and `minio` on nodes labeled `workload-role=infra`
- `upload`, `status`, and `streaming` on nodes labeled `workload-role=infra`
- `processing` on nodes labeled `workload-role=app`

This is controlled in [`k8s/values.yaml`](/Users/tani/Desktop/YUGithub/Capstone Project/Video-Streaming/k8s/values.yaml) under `scheduling`.

Label your nodes once before deploying:

```bash
kubectl label node cp1.cluster.local workload-role=infra --overwrite
kubectl label node cp2.cluster.local workload-role=infra --overwrite
kubectl label node cp3.cluster.local workload-role=infra --overwrite

```

Scheduling is controlled in [`k8s/values.yaml`](/Users/tani/Desktop/YUGithub/Capstone Project/Video-Streaming/k8s/values.yaml) under `scheduling`:

```yaml
scheduling:
  infra:
    nodeSelector:
      workload-role: infra
  app:
    {}
  processing:
    nodeSelector:
      workload-role: app
```

The infra workloads also include tolerations for the usual control-plane taints:
`node-role.kubernetes.io/control-plane:NoSchedule` and `node-role.kubernetes.io/master:NoSchedule`.

### Scale a service

```bash
# Scale processing workers to 5
kubectl scale statefulset vs-processing --replicas=5

# Or change it permanently in values.yaml and upgrade
./deploy_k8s.sh upgrade
```

### Tune processing CPU usage

The processing service already has an internal transcode worker pool. Do not
default to "one worker per pod". Instead, size each pod by CPU and let the app
run multiple single-threaded FFmpeg jobs in parallel.

Recommended baseline:

- Keep `THREADS_PER_WORKER=1`
- Leave `WORKER_POOL_SIZE` blank so the app auto-sizes from visible CPU
- Set each processing pod's CPU limit to roughly the amount of machine CPU you
  want that pod to use
- Start with `1` processing pod per machine, then benchmark before packing more

Suggested starting points:

| Pod CPU limit | Suggested WORKER_POOL_SIZE |
|---------------|----------------------------|
| 2 vCPU        | 1                          |
| 4 vCPU        | 3                          |
| 8 vCPU        | 6 or 7                     |
| 16 vCPU       | 12 to 14                   |

If `WORKER_POOL_SIZE` is left blank, the processing service defaults to about
`3/4` of the CPUs visible to the container. On modern Kubernetes/JDK setups,
that usually tracks the pod's CPU limit well enough to use as the default.

For mixed-size machines, the usual pattern is:

1. Label nodes by hardware class
2. Run separate processing workloads per class, or pin specific processing pods
   to specific nodes
3. Give larger-node pods larger CPU limits instead of creating many tiny
   one-worker pods

The current default is a single `vs-processing` StatefulSet pinned by
`scheduling.processing` to nodes labeled `workload-role=app`. The template
still supports `processingPools` if you decide to bring back per-node-class
processing splits later.

### View logs

```bash
kubectl logs -l app=vs-upload -f          # follow upload-service logs
kubectl logs -l app=vs-processing -f      # follow processing-service logs
kubectl logs -l app=vs-streaming -f       # follow streaming-service logs
kubectl logs -l app=vs-status -f          # follow status-service logs
kubectl logs vs-rabbitmq-0 -f             # follow a specific RabbitMQ node
```

### Check pod status

```bash
kubectl get pods                          # all pods
kubectl get pods -o wide                  # with IPs and node info
kubectl describe pod <pod-name>           # detailed info and events
```

### Open the Kubernetes dashboard

```bash
minikube dashboard
```

### Verify the RabbitMQ cluster

```bash
kubectl exec vs-rabbitmq-0 -- rabbitmqctl cluster_status
```

### Tear down

```bash
./deploy_k8s.sh uninstall

# To also delete persistent data (databases, queues, object storage):
kubectl delete pvc --all
```

### Full reset (start over)

```bash
./deploy_k8s.sh uninstall
kubectl delete pvc --all
minikube stop
minikube delete
```

## Chart Structure

``` 
k8s/
├── Chart.yaml                   # Chart metadata
├── kustomization.yaml           # Plain Kustomize entrypoint
├── rendered.yaml                # Static manifest bundle for kubectl apply -k
├── values.yaml                  # All configurable values (no secrets)
└── templates/
    ├── configmap.yaml           # App environment variables
    ├── secrets.yaml             # Credentials (populated from .env at deploy time)
    ├── postgres-init.yaml       # DB schema loaded on first boot
    ├── postgres.yaml            # StatefulSet + Service
    ├── rabbitmq.yaml            # 3-node clustered StatefulSet + Services + RBAC
    ├── minio.yaml               # StatefulSet + Service
    ├── upload-service.yaml      # Deployment + LoadBalancer Service (port 8080)
    ├── status-service.yaml      # Deployment + LoadBalancer Service (port 8081)
    ├── processing-service.yaml  # StatefulSet + ephemeral processing spool + ClusterIP Service (port 8082)
    └── streaming-service.yaml   # Deployment + LoadBalancer Service (port 8083)
```

## What Gets Created

Running `./deploy_k8s.sh` creates the following in your cluster:

| Resource | Name | Purpose |
|----------|------|---------|
| StatefulSet | vs-postgres (1 replica) | Postgres database with schema auto-init |
| StatefulSet | vs-rabbitmq (3 replicas) | Clustered RabbitMQ message broker |
| StatefulSet | vs-minio (1 replica) | S3-compatible object storage |
| Deployment | vs-upload (from `uploadService.replicas`) | Accepts video uploads, segments into chunks |
| Deployment | vs-status (from `statusService.replicas`) | WebSocket status updates to the browser |
| StatefulSet | vs-processing (from `processingService.replicas`) | FFmpeg transcoding workers with stable identities and ephemeral local spool storage |
| Deployment | vs-streaming (from `streamingService.replicas`) | Serves HLS manifests and presigned URLs |
| emptyDir | `processing-spool` volumes on `vs-processing` pods | Per-pod ephemeral spool storage capped by `processingService.spoolStorageSize` |
| ConfigMap | vs-config | Shared app configuration |
| Secret | vs-secrets | Database, RabbitMQ, and MinIO credentials |

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Pods stuck in `Pending` | `kubectl describe pod <name>` — check for cordons, taints, node selectors, or resource pressure |
| `CrashLoopBackOff` | `kubectl logs <pod> --previous` — check the crash reason |
| Pods running but `0/1 Ready` | `kubectl describe pod <name>` — readiness probe failing |
| `ERR_CONNECTION_REFUSED` in browser | Make sure `minikube tunnel` is running |
| Services show `EXTERNAL-IP: <pending>` | `minikube tunnel` is not running |
| RabbitMQ nodes not clustering | `kubectl delete pvc -l app=vs-rabbitmq` and redeploy |

## Related Documents

- [docs/installation.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/installation.md)
- [docs/api.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/api.md)
- [docs/architecture.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/architecture.md)
- [docs/challenges.md](https://github.com/S26-Distributed-Capstone/Video-Streaming/blob/main/docs/challenges.md)
