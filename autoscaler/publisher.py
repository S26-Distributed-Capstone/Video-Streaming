"""
publisher.py — Publishes NodeStatusEvent to RabbitMQ for the status-service to forward
to all browser WebSocket clients.
"""
import json
import logging
import time
from typing import Dict, List, Optional

import pika
import pika.exceptions
from config import Config

log = logging.getLogger(__name__)

_NODE_STATUS_ROUTING_KEY = "upload.cluster.node_status"
_RECONNECT_DELAY_SECONDS = 5


class Publisher:
    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel = None

    def _connect(self) -> None:
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
        log.info("Publisher connected to RabbitMQ at %s:%d", self._cfg.rabbitmq_host, self._cfg.rabbitmq_port)

    def _ensure_connected(self) -> None:
        if self._connection is None or self._connection.is_closed:
            self._connect()

    def publish_node_status(
        self,
        node_states: Dict[str, dict],
        queue_depth: int,
    ) -> None:
        """
        Publishes a NodeStatusEvent to the exchange.
        node_states: {node_name: {"cordoned": bool}}
        """
        nodes = []
        active_count = 0
        for name, state in node_states.items():
            cordoned = state.get("cordoned", True)
            s = "cordoned" if cordoned else "active"
            if not cordoned:
                active_count += 1
            nodes.append({
                "name": name,
                "state": s,
                "cpuCount": state.get("cpu_count", 0),
            })

        payload = {
            "jobId": "__cluster__",
            "taskId": "node_status",
            "type": "node_status",
            "nodes": nodes,
            "queueDepth": queue_depth,
            "activeCount": active_count,
            "totalCount": len(nodes),
        }

        body = json.dumps(payload).encode("utf-8")
        retries = 3
        for attempt in range(retries):
            try:
                self._ensure_connected()
                self._channel.basic_publish(
                    exchange=self._cfg.rabbitmq_exchange,
                    routing_key=_NODE_STATUS_ROUTING_KEY,
                    body=body,
                    properties=pika.BasicProperties(content_type="application/json"),
                )
                log.info(
                    "Published NodeStatusEvent: active=%d/%d queueDepth=%d",
                    active_count, len(nodes), queue_depth,
                )
                return
            except (pika.exceptions.AMQPError, Exception) as exc:
                log.warning(
                    "Failed to publish NodeStatusEvent (attempt %d/%d): %s",
                    attempt + 1, retries, exc,
                )
                self._connection = None
                if attempt < retries - 1:
                    time.sleep(_RECONNECT_DELAY_SECONDS)

        log.error("Gave up publishing NodeStatusEvent after %d attempts", retries)

    def close(self) -> None:
        try:
            if self._connection and not self._connection.is_closed:
                self._connection.close()
        except Exception:
            pass

