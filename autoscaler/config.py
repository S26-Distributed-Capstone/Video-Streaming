"""
config.py — Autoscaler configuration loaded from environment variables.
"""
import os
from dataclasses import dataclass


def _get(key: str, default: str) -> str:
    return os.environ.get(key, default).strip()


def _get_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, str(default)))
    except (ValueError, TypeError):
        return default


@dataclass
class Config:
    # Kubernetes
    kube_namespace: str
    statefulset_name: str
    node_label_selector: str
    lease_name: str

    # Scaling logic
    total_nodes: int
    min_active_nodes: int
    replicas_per_node: int    # minimum / floor replicas for any node
    replicas_per_cpu: float   # desired replicas per CPU core; actual = max(replicas_per_node, floor(cpus * replicas_per_cpu))
    scale_up_threshold: int
    scale_down_threshold: int
    max_scale_up_step: int    # max nodes to activate in a single poll cycle
    max_scale_down_step: int  # max nodes to deactivate in a single poll cycle (keep conservative)
    poll_interval_seconds: int
    scale_cooldown_seconds: int
    scale_down_idle_polls: int

    # RabbitMQ connection
    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_management_port: int
    rabbitmq_user: str
    rabbitmq_pass: str
    rabbitmq_vhost: str
    rabbitmq_exchange: str
    rabbitmq_task_queue: str

    # HTTP health server
    health_port: int


def load() -> Config:
    release_name = _get("HELM_RELEASE_NAME", "vs")
    default_ss = f"{release_name}-processing"

    cfg = Config(
        kube_namespace=_get("KUBE_NAMESPACE", "default"),
        statefulset_name=_get("STATEFULSET_NAME", default_ss) or default_ss,
        node_label_selector=_get("NODE_LABEL_SELECTOR", "workload-role=app"),
        lease_name=_get("LEASE_NAME", "autoscaler-leader"),

        total_nodes=_get_int("TOTAL_NODES", 11),
        min_active_nodes=_get_int("MIN_ACTIVE_NODES", 2),
        replicas_per_node=_get_int("REPLICAS_PER_NODE", 1),
        replicas_per_cpu=float(os.environ.get("REPLICAS_PER_CPU", "1.0")),
        scale_up_threshold=_get_int("SCALE_UP_THRESHOLD", 20),
        scale_down_threshold=_get_int("SCALE_DOWN_THRESHOLD", 5),
        max_scale_up_step=_get_int("MAX_SCALE_UP_STEP", 3),
        max_scale_down_step=_get_int("MAX_SCALE_DOWN_STEP", 1),
        poll_interval_seconds=_get_int("POLL_INTERVAL_SECONDS", 10),
        scale_cooldown_seconds=_get_int("SCALE_COOLDOWN_SECONDS", 10),
        scale_down_idle_polls=_get_int("SCALE_DOWN_IDLE_POLLS", 6),

        rabbitmq_host=_get("RABBITMQ_HOST", "localhost"),
        rabbitmq_port=_get_int("RABBITMQ_PORT", 5672),
        rabbitmq_management_port=_get_int("RABBITMQ_MANAGEMENT_PORT", 15672),
        rabbitmq_user=_get("RABBITMQ_USER", "guest"),
        rabbitmq_pass=_get("RABBITMQ_PASS", "guest"),
        rabbitmq_vhost=_get("RABBITMQ_VHOST", "/"),
        rabbitmq_exchange=_get("RABBITMQ_EXCHANGE", "upload.events"),
        rabbitmq_task_queue=_get("RABBITMQ_TASK_QUEUE", "processing.tasks.queue"),

        health_port=_get_int("AUTOSCALER_HEALTH_PORT", 8084),
    )

    # Sanity checks
    if cfg.min_active_nodes < 1:
        raise ValueError("MIN_ACTIVE_NODES must be >= 1")
    if cfg.min_active_nodes > cfg.total_nodes:
        raise ValueError("MIN_ACTIVE_NODES must be <= TOTAL_NODES")
    if cfg.scale_down_threshold >= cfg.scale_up_threshold:
        raise ValueError("SCALE_DOWN_THRESHOLD must be < SCALE_UP_THRESHOLD")

    return cfg
