import json
import os
import sys
import time

import docker
import pika
import psycopg2
from docker import errors as docker_errors
from pika import exceptions as pika_exceptions
from psycopg2 import InterfaceError, OperationalError

from datetime import datetime, timedelta

def parse_jdbc_url(jdbc_url: str):
    # Expected: jdbc:postgresql://host:port/db
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


def close_quietly(resource):
    if resource is None:
        return
    try:
        resource.close()
    except Exception:
        pass


def db_connection_healthy(conn):
    return conn is not None and getattr(conn, "closed", 1) == 0


def rabbit_connection_healthy(conn, channel):
    return (
        conn is not None
        and getattr(conn, "is_open", False)
        and channel is not None
        and getattr(channel, "is_open", False)
    )


def ensure_db_connection(conn):
    if db_connection_healthy(conn):
        return conn
    close_quietly(conn)
    print("node-watcher: reconnecting to postgres", flush=True)
    return connect_db()


def ensure_rabbitmq_connection(conn, channel):
    if rabbit_connection_healthy(conn, channel):
        exchange = get_env("RABBITMQ_EXCHANGE", "upload.events")
        return conn, channel, exchange
    close_quietly(channel)
    close_quietly(conn)
    print("node-watcher: reconnecting to rabbitmq", flush=True)
    return connect_rabbitmq()


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


def mark_failed(conn, video_id):
    with conn.cursor() as cur:
        cur.execute("UPDATE video_upload SET status = 'FAILED' WHERE video_id = %s", (video_id,))
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


def lookup_video_ids(db_conn, service_name, container_id):
    if service_name == "processing":
        return find_processing_video_ids(db_conn, container_id)
    return find_upload_video_ids(db_conn, container_id)


def handle_container_event(db_conn, rabbit_conn, channel, exchange, container_id, container_name, name_prefix, machine_id, update_db):
    event_reason = resolve_failure_reason(get_env("FAILURE_REASON", "container_died"), container_name or name_prefix)
    service_name = derive_service_name(container_name or name_prefix)

    try:
        db_conn = ensure_db_connection(db_conn)
        video_ids = lookup_video_ids(db_conn, service_name, container_id)
    except (OperationalError, InterfaceError) as exc:
        print(f"node-watcher: postgres operation failed: {exc}", flush=True)
        db_conn = ensure_db_connection(None)
        video_ids = lookup_video_ids(db_conn, service_name, container_id)

    if not video_ids:
        print(
            f"node-watcher: no active {service_name or 'watched'} jobs found for container_id={container_id}",
            flush=True,
        )
        return db_conn, rabbit_conn, channel, exchange

    for video_id in video_ids:
        print(f"node-watcher: publishing failed for video_id={video_id}", flush=True)
        try:
            rabbit_conn, channel, exchange = ensure_rabbitmq_connection(rabbit_conn, channel)
            publish_failed(channel, exchange, video_id, event_reason, machine_id, container_id)
        except pika_exceptions.AMQPError as exc:
            print(f"node-watcher: rabbitmq publish failed: {exc}", flush=True)
            rabbit_conn, channel, exchange = ensure_rabbitmq_connection(None, None)
            publish_failed(channel, exchange, video_id, event_reason, machine_id, container_id)

        if update_db:
            try:
                db_conn = ensure_db_connection(db_conn)
                mark_failed(db_conn, video_id)
            except (OperationalError, InterfaceError) as exc:
                print(f"node-watcher: postgres update failed: {exc}", flush=True)
                db_conn = ensure_db_connection(None)
                mark_failed(db_conn, video_id)
            print(f"node-watcher: marked FAILED video_id={video_id}", flush=True)

    return db_conn, rabbit_conn, channel, exchange


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


def main():
    print("SETTING UP THE NODE WATCHER", flush=True)
    name_prefix = get_env("WATCH_CONTAINER_NAME_PREFIX", "upload-service,processing-service")
    label_filter = get_env("WATCH_CONTAINER_LABEL")
    reason = resolve_failure_reason(get_env("FAILURE_REASON", "container_died"), name_prefix)
    update_db = get_env("UPDATE_DB_STATUS", "true").lower() == "true"
    machine_id = get_env("MACHINE_ID")
    debug_events = get_env("DEBUG_EVENTS", "false").lower() == "true"
    print(
        f"node-watcher: config name_prefix={name_prefix} label_filter={label_filter} "
        f"failure_reason={reason} update_db={update_db} machine_id={machine_id}",
        flush=True,
    )

    filters = {"type": "container", "event": ["die", "stop"]}

    dedupe_window_seconds = int(get_env("DEDUP_WINDOW_SECONDS", "10"))
    seen_containers = {}

    db_conn = None
    rabbit_conn = None
    channel = None
    exchange = get_env("RABBITMQ_EXCHANGE", "upload.events")
    client = None

    while True:
        try:
            db_conn = ensure_db_connection(db_conn)
            rabbit_conn, channel, exchange = ensure_rabbitmq_connection(rabbit_conn, channel)
            if client is None:
                client = docker.DockerClient(base_url="unix://var/run/docker.sock")
                print("node-watcher: connected to docker socket", flush=True)

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
                db_conn, rabbit_conn, channel, exchange = handle_container_event(
                    db_conn,
                    rabbit_conn,
                    channel,
                    exchange,
                    container_id,
                    container_name,
                    name_prefix,
                    machine_id,
                    update_db,
                )
        except (docker_errors.DockerException, pika_exceptions.AMQPError, OperationalError, InterfaceError) as exc:
            print(f"Watcher connection error: {exc}", file=sys.stderr, flush=True)
        except Exception as exc:
            print(f"Watcher error: {exc}", file=sys.stderr, flush=True)
        finally:
            close_quietly(channel)
            close_quietly(rabbit_conn)
            close_quietly(db_conn)
            close_quietly(client)
            db_conn = None
            rabbit_conn = None
            channel = None
            client = None
            time.sleep(2)


if __name__ == "__main__":
    main()
