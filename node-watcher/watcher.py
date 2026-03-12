import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

import docker
import pika
import psycopg2

from datetime import datetime, timedelta


def parse_jdbc_url(jdbc_url: str):
    if not jdbc_url.startswith("jdbc:postgresql://"):
        raise ValueError(f"Unsupported PG_URL: {jdbc_url}")
    rest = jdbc_url[len("jdbc:postgresql://"):]
    host_port, _, dbname = rest.partition("/")
    host, _, port = host_port.partition(":")
    return host, int(port) if port else 5432, dbname


def get_env(name, default=None):
    val = os.getenv(name)
    return val if val is not None and val != "" else default


def connect_db():
    jdbc_url = get_env("PG_URL")
    if not jdbc_url:
        raise RuntimeError("PG_URL is not set")
    user = get_env("PG_USER")
    if not user:
        raise RuntimeError("PG_USER is not set")
    password = get_env("PG_PASSWORD", "")
    host, port, dbname = parse_jdbc_url(jdbc_url)
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)


def connect_rabbitmq():
    host = get_env("RABBITMQ_HOST", "localhost")
    port = int(get_env("RABBITMQ_PORT", "5672"))
    user = get_env("RABBITMQ_USER", "guest")
    password = get_env("RABBITMQ_PASS", "guest")
    vhost = get_env("RABBITMQ_VHOST", "/")
    exchange = get_env("RABBITMQ_EXCHANGE", "upload.events")

    credentials = pika.PlainCredentials(user, password)
    params = pika.ConnectionParameters(host=host, port=port, virtual_host=vhost, credentials=credentials)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
    return conn, ch, exchange


def normalize_container_id(container_id):
    if not container_id:
        return None
    return container_id[:12]


def find_upload_video_ids(conn, container_id):
    short_id = normalize_container_id(container_id)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT video_id FROM video_upload WHERE status = 'PROCESSING' AND (container_id = %s OR container_id LIKE %s)",
            (container_id, f"{short_id}%"),
        )
        return [row[0] for row in cur.fetchall()]


def find_processing_video_ids(conn, container_id):
    short_id = normalize_container_id(container_id)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT video_id
            FROM processing_task_claim
            WHERE claimed_by = %s OR claimed_by = %s
            """,
            (container_id, short_id),
        )
        return [row[0] for row in cur.fetchall()]


def is_video_processing(conn, video_id):
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM video_upload WHERE video_id = %s", (video_id,))
        row = cur.fetchone()
        return row is not None and row[0] == "PROCESSING"


def mark_failed(conn, video_id):
    with conn.cursor() as cur:
        cur.execute("UPDATE video_upload SET status = 'FAILED' WHERE video_id = %s AND status = 'PROCESSING'", (video_id,))
    conn.commit()


def publish_failed(ch, exchange, video_id, reason, machine_id, container_id):
    payload = {
        "jobId": str(video_id),
        "type": "failed",
        "reason": reason,
        "machineId": machine_id,
        "containerId": container_id,
    }
    routing_key = f"upload.status.{video_id}"
    ch.basic_publish(exchange=exchange, routing_key=routing_key, body=json.dumps(payload).encode("utf-8"))


def matches_watched_container(event, name_prefix, label_filter):
    attrs = event.get("Actor", {}).get("Attributes", {})
    name = attrs.get("name", "")
    if label_filter:
        key, _, expected = label_filter.partition("=")
        if attrs.get(key) == expected:
            return True
    if name_prefix:
        prefixes = [p.strip() for p in name_prefix.split(",") if p.strip()]
        for prefix in prefixes:
            if name.startswith(prefix) or prefix in name:
                return True
    return False


def get_container_name(event):
    actor = event.get("Actor", {})
    attrs = actor.get("Attributes", {})
    return attrs.get("name")


def get_container_id(event):
    if event.get("id"):
        return event.get("id")
    actor = event.get("Actor", {})
    if actor.get("ID"):
        return actor.get("ID")
    attrs = actor.get("Attributes", {})
    for key in ("container", "container_id", "id"):
        if attrs.get(key):
            return attrs.get(key)
    return None


def derive_service_name(name_or_prefix: str):
    if not name_or_prefix:
        return None
    parts = [part.strip() for part in name_or_prefix.split(",") if part.strip()]
    for part in parts:
        normalized = part.lower().replace("_", "-")
        if "processing-service" in normalized:
            return "processing"
        if "upload-service" in normalized:
            return "upload"
        for suffix in ("-service", "-container"):
            if normalized.endswith(suffix):
                normalized = normalized[: -len(suffix)]
                break
        if normalized:
            return normalized
    return None


def resolve_failure_reason(configured_reason: str, name_prefix: str):
    if configured_reason and configured_reason not in {"container_died", "service_container_died"}:
        return configured_reason
    service_name = derive_service_name(name_prefix)
    if service_name:
        return f"{service_name}_container_died"
    return configured_reason or "container_died"


def health_url_for_service(service_name: str):
    if service_name == "processing":
        return get_env("PROCESSING_SERVICE_HEALTH_URL", "http://processing-service:8082/health")
    return get_env("UPLOAD_SERVICE_HEALTH_URL", "http://upload-service:8080/health")


def service_healthy(service_name: str, timeout_seconds: float):
    url = health_url_for_service(service_name)
    try:
        with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, TimeoutError, ValueError):
        return False


class PendingFailureStore:
    def __init__(self):
        self._lock = threading.Lock()
        self._items = {}

    def upsert(self, video_id, payload):
        with self._lock:
            existing = self._items.get(video_id)
            if existing is None or payload["deadline"] < existing["deadline"]:
                self._items[video_id] = payload

    def due_items(self, now):
        due = []
        with self._lock:
            for video_id, payload in list(self._items.items()):
                if payload["deadline"] <= now:
                    due.append((video_id, payload))
                    self._items.pop(video_id, None)
        return due

    def empty(self):
        with self._lock:
            return not self._items


def process_pending_failures(store, stop_event):
    poll_interval_seconds = float(get_env("FAILURE_POLL_INTERVAL_SECONDS", "1"))
    health_timeout_seconds = float(get_env("SERVICE_HEALTH_TIMEOUT_SECONDS", "2"))
    update_db = get_env("UPDATE_DB_STATUS", "true").lower() == "true"
    machine_id = get_env("MACHINE_ID")
    while not stop_event.is_set():
        now = datetime.utcnow()
        due_failures = store.due_items(now)
        for video_id, payload in due_failures:
            service_name = payload["service_name"]
            if service_healthy(service_name, health_timeout_seconds):
                print(
                    f"node-watcher: recovery succeeded service={service_name} video_id={video_id} "
                    f"container_id={payload['container_id']}",
                    flush=True,
                )
                continue

            db_conn = None
            rabbit_conn = None
            channel = None
            try:
                db_conn = connect_db()
                if not is_video_processing(db_conn, video_id):
                    print(
                        f"node-watcher: skip terminal failure video_id={video_id} because status is no longer PROCESSING",
                        flush=True,
                    )
                    continue
                rabbit_conn, channel, exchange = connect_rabbitmq()
                if update_db:
                    mark_failed(db_conn, video_id)
                    print(f"node-watcher: marked FAILED video_id={video_id}", flush=True)
                print(f"node-watcher: publishing failed for video_id={video_id}", flush=True)
                publish_failed(channel, exchange, video_id, payload["reason"], machine_id, payload["container_id"])
            except Exception as exc:
                print(f"node-watcher: pending failure processing error video_id={video_id}: {exc}", file=sys.stderr, flush=True)
            finally:
                try:
                    if channel is not None and channel.is_open:
                        channel.close()
                except Exception:
                    pass
                try:
                    if rabbit_conn is not None and rabbit_conn.is_open:
                        rabbit_conn.close()
                except Exception:
                    pass
                try:
                    if db_conn is not None:
                        db_conn.close()
                except Exception:
                    pass
        stop_event.wait(poll_interval_seconds)


def main():
    print("SETTING UP THE NODE WATCHER", flush=True)
    name_prefix = get_env("WATCH_CONTAINER_NAME_PREFIX", "upload-service,processing-service")
    label_filter = get_env("WATCH_CONTAINER_LABEL")
    reason = resolve_failure_reason(get_env("FAILURE_REASON", "container_died"), name_prefix)
    debug_events = get_env("DEBUG_EVENTS", "false").lower() == "true"
    grace_seconds = int(get_env("FAILURE_GRACE_SECONDS", "10"))
    print(
        f"node-watcher: config name_prefix={name_prefix} label_filter={label_filter} "
        f"failure_reason={reason} grace_seconds={grace_seconds}",
        flush=True,
    )

    db_conn = None
    try:
        db_conn = connect_db()
    except Exception as exc:
        print(f"DB connection failed: {exc}", file=sys.stderr)
        raise

    client = docker.DockerClient(base_url="unix://var/run/docker.sock")
    print("node-watcher: connected to docker socket", flush=True)
    filters = {"type": "container", "event": ["die", "stop"]}

    dedupe_window_seconds = int(get_env("DEDUP_WINDOW_SECONDS", "10"))
    seen_containers = {}
    pending_failures = PendingFailureStore()
    stop_event = threading.Event()
    worker = threading.Thread(target=process_pending_failures, args=(pending_failures, stop_event), daemon=True)
    worker.start()

    try:
        for event in client.events(decode=True, filters=filters):
            if debug_events:
                name = event.get("Actor", {}).get("Attributes", {}).get("name")
                cid = get_container_id(event)
                print(
                    f"node-watcher: event type={event.get('Type')} action={event.get('Action')} "
                    f"name={name} id={cid}",
                    flush=True,
                )
            if not matches_watched_container(event, name_prefix, label_filter):
                if debug_events:
                    print("node-watcher: event did not match configured container filter", flush=True)
                continue
            container_name = get_container_name(event)
            container_id = get_container_id(event)
            if not container_id:
                print(f"node-watcher: event missing container id keys={list(event.keys())}", flush=True)
                continue
            now = datetime.utcnow()
            if seen_containers:
                cutoff = now - timedelta(seconds=dedupe_window_seconds)
                stale = [cid for cid, ts in seen_containers.items() if ts < cutoff]
                for cid in stale:
                    seen_containers.pop(cid, None)
            last_seen = seen_containers.get(container_id)
            if last_seen and now - last_seen < timedelta(seconds=dedupe_window_seconds):
                if debug_events:
                    print(
                        f"node-watcher: dedupe skip container_id={container_id} action={event.get('Action')}",
                        flush=True,
                    )
                continue
            seen_containers[container_id] = now
            print(f"node-watcher: container down id={container_id}", flush=True)
            event_reason = resolve_failure_reason(get_env("FAILURE_REASON", "container_died"), container_name or name_prefix)
            service_name = derive_service_name(container_name or name_prefix)
            if service_name == "processing":
                video_ids = find_processing_video_ids(db_conn, container_id)
            else:
                video_ids = find_upload_video_ids(db_conn, container_id)
            if not video_ids:
                print(
                    f"node-watcher: no active {service_name or 'watched'} jobs found for container_id={container_id}",
                    flush=True,
                )
                continue
            deadline = now + timedelta(seconds=grace_seconds)
            for video_id in video_ids:
                pending_failures.upsert(
                    str(video_id),
                    {
                        "service_name": service_name or "upload",
                        "container_id": container_id,
                        "reason": event_reason,
                        "deadline": deadline,
                    },
                )
                print(
                    f"node-watcher: queued terminal failure check video_id={video_id} "
                    f"service={service_name} deadline={deadline.isoformat()}",
                    flush=True,
                )
    except Exception as exc:
        print(f"Watcher error: {exc}", file=sys.stderr)
        time.sleep(2)
        raise
    finally:
        stop_event.set()
        worker.join(timeout=2)
        try:
            client.close()
        except Exception:
            pass
        try:
            if db_conn is not None:
                db_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
