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
    replicas_per_node: int
    scale_up_threshold: int
    scale_down_threshold: int
    poll_interval_seconds: int
    scale_cooldown_seconds: int

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
        replicas_per_node=_get_int("REPLICAS_PER_NODE", 6),
        scale_up_threshold=_get_int("SCALE_UP_THRESHOLD", 20),
        scale_down_threshold=_get_int("SCALE_DOWN_THRESHOLD", 5),
        poll_interval_seconds=_get_int("POLL_INTERVAL_SECONDS", 10),
        scale_cooldown_seconds=_get_int("SCALE_COOLDOWN_SECONDS", 10),

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

