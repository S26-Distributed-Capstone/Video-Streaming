"""
metrics_collector.py — Polls the RabbitMQ Management HTTP API for queue depth.
"""
import logging
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import psycopg
import requests
from config import Config

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProcessingBacklog:
    open_upload_tasks: int = 0
    active_claims: int = 0

    def has_active_work(self) -> bool:
        return self.open_upload_tasks > 0 or self.active_claims > 0


class MetricsCollector:
    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._base_url = (
            f"http://{cfg.rabbitmq_host}:{cfg.rabbitmq_management_port}/api"
        )
        self._auth = (cfg.rabbitmq_user, cfg.rabbitmq_pass)
        self._pg_conninfo = self._build_pg_conninfo()
        self._warned_pg_unavailable = False

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

    def get_processing_backlog(self) -> ProcessingBacklog:
        """
        Returns counts of non-RabbitMQ processing work that should block
        scale-down: open local spool upload tasks and active transcode/upload
        claims.
        """
        if not self._pg_conninfo:
            return ProcessingBacklog()
        try:
            with psycopg.connect(self._pg_conninfo) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM processing_upload_task")
                    open_upload_tasks = int(cur.fetchone()[0])
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM processing_task_claim
                        WHERE updated_at >= NOW() - (%s * INTERVAL '1 millisecond')
                        """,
                        (max(0, self._cfg.processing_task_claim_stale_millis),),
                    )
                    active_claims = int(cur.fetchone()[0])
            self._warned_pg_unavailable = False
            return ProcessingBacklog(
                open_upload_tasks=open_upload_tasks,
                active_claims=active_claims,
            )
        except Exception as exc:
            if not self._warned_pg_unavailable:
                log.warning("Failed to query processing backlog from Postgres: %s", exc)
                self._warned_pg_unavailable = True
            return ProcessingBacklog()

    def _build_pg_conninfo(self) -> str:
        jdbc_url = (self._cfg.pg_url or "").strip()
        if not jdbc_url or not self._cfg.pg_user:
            return ""

        if jdbc_url.startswith("jdbc:"):
            jdbc_url = jdbc_url[len("jdbc:"):]
        parsed = urlparse(jdbc_url)
        if parsed.scheme not in {"postgresql", "postgres"} or not parsed.hostname:
            log.warning("Unsupported PG_URL for autoscaler backlog checks: %s", self._cfg.pg_url)
            return ""

        parts = [
            f"host={parsed.hostname}",
            f"port={parsed.port or 5432}",
            f"dbname={parsed.path.lstrip('/')}",
            f"user={self._cfg.pg_user}",
        ]
        if self._cfg.pg_password:
            parts.append(f"password={self._cfg.pg_password}")
        return " ".join(parts)
