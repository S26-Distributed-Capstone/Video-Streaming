"""
metrics_collector.py — Polls the RabbitMQ Management HTTP API for queue depth.
"""
import logging
import time
import requests
from config import Config

log = logging.getLogger(__name__)


class MetricsCollector:
    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._base_url = (
            f"http://{cfg.rabbitmq_host}:{cfg.rabbitmq_management_port}/api"
        )
        self._auth = (cfg.rabbitmq_user, cfg.rabbitmq_pass)

    def get_queue_depth(self, retries: int = 3) -> int:
        """
        Returns messages_ready + messages_unacknowledged for the transcode task queue.
        Retries with exponential backoff on transient failures.
        """
        vhost_enc = requests.utils.quote(self._cfg.rabbitmq_vhost, safe="")
        queue_enc = requests.utils.quote(self._cfg.rabbitmq_task_queue, safe="")
        url = f"{self._base_url}/queues/{vhost_enc}/{queue_enc}"

        delay = 1.0
        for attempt in range(retries):
            try:
                resp = requests.get(url, auth=self._auth, timeout=5)
                resp.raise_for_status()
                data = resp.json()
                ready = data.get("messages_ready", 0)
                unacked = data.get("messages_unacknowledged", 0)
                depth = ready + unacked
                log.debug("Queue depth: ready=%d unacked=%d total=%d", ready, unacked, depth)
                return depth
            except Exception as exc:
                if attempt < retries - 1:
                    log.warning(
                        "Failed to fetch queue depth (attempt %d/%d): %s — retrying in %.1fs",
                        attempt + 1, retries, exc, delay,
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, 10.0)
                else:
                    log.error("Failed to fetch queue depth after %d attempts: %s", retries, exc)
                    return 0
        return 0

