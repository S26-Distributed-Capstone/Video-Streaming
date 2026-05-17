"""
upload_task_recovery.py — Recovers local-spool upload tasks stranded on unhealthy pods/nodes.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, Optional
from urllib.parse import urlparse

import pika
import psycopg
from kubernetes import client

from config import Config

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RecoverableUploadTask:
    id: int
    video_id: str
    spool_owner: str
    profile: str
    segment_number: int
    chunk_key: str
    output_ts_offset_seconds: float
    state: str


class UploadTaskRecovery:
    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._core = client.CoreV1Api()
        self._pg_conninfo = self._build_pg_conninfo()
        self._warned_pg_unavailable = False
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel = None

    def recover(self) -> int:
        if not self._pg_conninfo:
            return 0

        pod_health = self._load_processing_pod_health()
        tasks = self._load_candidate_tasks()
        if not tasks:
            return 0

        recovered = 0
        for task in tasks:
            health = pod_health.get(task.spool_owner)
            if health is not None and health["healthy"]:
                continue
            reason = "pod_missing"
            if health is not None:
                reason = health["reason"]
            try:
                self._publish_transcode_task(task)
                self._delete_upload_task_and_requeue_state(task)
                recovered += 1
                log.warning(
                    "Recovered stranded upload task videoId=%s profile=%s segment=%d spool_owner=%s reason=%s",
                    task.video_id,
                    task.profile,
                    task.segment_number,
                    task.spool_owner,
                    reason,
                )
            except Exception as exc:
                log.warning(
                    "Failed to recover stranded upload task videoId=%s profile=%s segment=%d spool_owner=%s: %s",
                    task.video_id,
                    task.profile,
                    task.segment_number,
                    task.spool_owner,
                    exc,
                )
        return recovered

    def close(self) -> None:
        try:
            if self._connection and not self._connection.is_closed:
                self._connection.close()
        except Exception:
            pass

    def _load_processing_pod_health(self) -> Dict[str, dict]:
        pods = self._core.list_namespaced_pod(
            namespace=self._cfg.kube_namespace,
            label_selector=f"app={self._cfg.statefulset_name}",
        ).items
        nodes = {
            n.metadata.name: self._is_node_ready(n)
            for n in self._core.list_node().items
        }
        health: Dict[str, dict] = {}
        for pod in pods:
            pod_name = pod.metadata.name
            node_name = pod.spec.node_name if pod.spec else None
            pod_ready = self._is_pod_ready(pod)
            node_ready = bool(node_name and nodes.get(node_name, False))
            healthy = (
                (pod.status.phase or "") == "Running"
                and pod_ready
                and node_ready
            )
            reason = "healthy" if healthy else self._derive_unhealthy_reason(pod, node_ready)
            health[pod_name] = {
                "healthy": healthy,
                "reason": reason,
                "node_name": node_name,
            }
        return health

    def _load_candidate_tasks(self) -> list[RecoverableUploadTask]:
        sql = """
            SELECT id,
                   video_id::text,
                   spool_owner,
                   profile,
                   segment_number,
                   chunk_key,
                   output_ts_offset_seconds,
                   state
            FROM processing_upload_task
            WHERE (
                    state = 'PENDING'
                AND updated_at < NOW() - (%s * INTERVAL '1 millisecond')
            ) OR (
                    state = 'UPLOADING'
                AND updated_at < NOW() - (%s * INTERVAL '1 millisecond')
            )
            ORDER BY updated_at ASC, id ASC
        """
        try:
            with psycopg.connect(self._pg_conninfo) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql,
                        (
                            max(1_000, self._cfg.upload_task_recovery_stale_millis),
                            max(
                                max(1_000, self._cfg.upload_task_recovery_stale_millis),
                                max(1_000, self._cfg.processing_task_claim_stale_millis),
                            ),
                        ),
                    )
                    rows = cur.fetchall()
            self._warned_pg_unavailable = False
            return [
                RecoverableUploadTask(
                    id=row[0],
                    video_id=row[1],
                    spool_owner=row[2],
                    profile=row[3],
                    segment_number=row[4],
                    chunk_key=row[5],
                    output_ts_offset_seconds=float(row[6] or -1.0),
                    state=row[7],
                )
                for row in rows
            ]
        except Exception as exc:
            if not self._warned_pg_unavailable:
                log.warning("Failed to query stranded upload tasks from Postgres: %s", exc)
                self._warned_pg_unavailable = True
            return []

    def _publish_transcode_task(self, task: RecoverableUploadTask) -> None:
        self._ensure_rabbitmq_connected()
        payload = {
            "jobId": task.video_id,
            "taskId": f"transcode:{task.profile}:{task.segment_number}",
            "type": "transcode_task",
            "chunkKey": task.chunk_key,
            "profile": task.profile,
            "segmentNumber": task.segment_number,
            "outputTsOffsetSeconds": task.output_ts_offset_seconds,
        }
        self._channel.basic_publish(
            exchange=self._cfg.rabbitmq_exchange,
            routing_key=self._cfg.rabbitmq_task_binding,
            body=json.dumps(payload).encode("utf-8"),
            properties=pika.BasicProperties(content_type="application/json"),
        )

    def _delete_upload_task_and_requeue_state(self, task: RecoverableUploadTask) -> None:
        sql_delete_task = "DELETE FROM processing_upload_task WHERE id = %s"
        sql_delete_claim = """
            DELETE FROM processing_task_claim
            WHERE video_id = %s AND profile = %s AND segment_number = %s
        """
        sql_upsert_status = """
            INSERT INTO transcoded_segment_status (video_id, profile, segment_number, state)
            VALUES (%s, %s, %s, 'QUEUED')
            ON CONFLICT (video_id, profile, segment_number) DO UPDATE
            SET state = CASE
                WHEN transcoded_segment_status.state = 'DONE' THEN transcoded_segment_status.state
                ELSE 'QUEUED'
            END,
            updated_at = NOW()
        """
        with psycopg.connect(self._pg_conninfo) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql_upsert_status,
                    (task.video_id, task.profile, task.segment_number),
                )
                cur.execute(
                    sql_delete_claim,
                    (task.video_id, task.profile, task.segment_number),
                )
                cur.execute(sql_delete_task, (task.id,))
            conn.commit()

    def _ensure_rabbitmq_connected(self) -> None:
        if self._connection is not None and not self._connection.is_closed:
            return
        credentials = pika.PlainCredentials(self._cfg.rabbitmq_user, self._cfg.rabbitmq_pass)
        params = pika.ConnectionParameters(
            host=self._cfg.rabbitmq_host,
            port=self._cfg.rabbitmq_port,
            virtual_host=self._cfg.rabbitmq_vhost,
            credentials=credentials,
            heartbeat=60,
            connection_attempts=5,
            retry_delay=2,
        )
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=self._cfg.rabbitmq_exchange,
            exchange_type="topic",
            durable=True,
        )

    @staticmethod
    def _is_node_ready(node) -> bool:
        for condition in (node.status.conditions or []):
            if condition.type == "Ready":
                return condition.status == "True"
        return False

    @staticmethod
    def _is_pod_ready(pod) -> bool:
        for condition in (pod.status.conditions or []):
            if condition.type == "Ready":
                return condition.status == "True"
        return False

    @staticmethod
    def _derive_unhealthy_reason(pod, node_ready: bool) -> str:
        if not node_ready:
            return "node_not_ready"
        phase = (pod.status.phase or "").lower()
        if phase and phase != "running":
            return f"pod_phase_{phase}"
        return "pod_not_ready"

    def _build_pg_conninfo(self) -> str:
        jdbc_url = (self._cfg.pg_url or "").strip()
        if not jdbc_url or not self._cfg.pg_user:
            return ""

        if jdbc_url.startswith("jdbc:"):
            jdbc_url = jdbc_url[len("jdbc:"):]
        parsed = urlparse(jdbc_url)
        if parsed.scheme not in {"postgresql", "postgres"} or not parsed.hostname:
            log.warning("Unsupported PG_URL for upload task recovery: %s", self._cfg.pg_url)
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
