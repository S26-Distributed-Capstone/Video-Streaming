import json
import os
import sys
import time

import docker
import pika
import psycopg2

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


def find_processing_video_ids(conn, container_id):
    # Match full or prefix (HOSTNAME often uses short ID)
    like_prefix = container_id[:12] + "%"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT video_id FROM video_upload WHERE status = 'PROCESSING' AND (container_id = %s OR container_id LIKE %s)",
            (container_id, like_prefix),
        )
        return [row[0] for row in cur.fetchall()]


def find_processing_video_ids_by_machine(conn, machine_id):
    if not machine_id:
        return []
    with conn.cursor() as cur:
        cur.execute(
            "SELECT video_id FROM video_upload WHERE status = 'PROCESSING' AND machine_id = %s AND (container_id IS NULL OR container_id = '')",
            (machine_id,),
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


def is_upload_container(event, name_prefix, label_filter):
    attrs = event.get("Actor", {}).get("Attributes", {})
    name = attrs.get("name", "")
    if label_filter:
        key, _, expected = label_filter.partition("=")
        if attrs.get(key) == expected:
            return True
    if name_prefix and name.startswith(name_prefix):
        return True
    return False


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


def main():
    print("SETTING UP THE NODE WATCHER", flush=True)
    name_prefix = get_env("WATCH_CONTAINER_NAME_PREFIX", "upload-service")
    label_filter = get_env("WATCH_CONTAINER_LABEL")
    reason = get_env("FAILURE_REASON", "container_died")
    update_db = get_env("UPDATE_DB_STATUS", "true").lower() == "true"
    machine_id = get_env("MACHINE_ID")
    debug_events = get_env("DEBUG_EVENTS", "false").lower() == "true"
    print(
        f"node-watcher: config name_prefix={name_prefix} label_filter={label_filter} "
        f"update_db={update_db} machine_id={machine_id}",
        flush=True,
    )

    try:
        db_conn = connect_db()
    except Exception as exc:
        print(f"DB connection failed: {exc}", file=sys.stderr)
        raise

    try:
        rabbit_conn, channel, exchange = connect_rabbitmq()
    except Exception as exc:
        print(f"RabbitMQ connection failed: {exc}", file=sys.stderr)
        raise

    client = docker.DockerClient(base_url="unix://var/run/docker.sock")
    print("node-watcher: connected to docker socket", flush=True)
    filters = {"type": "container", "event": ["die", "stop"]}

    dedupe_window_seconds = int(get_env("DEDUP_WINDOW_SECONDS", "10"))
    seen_containers = {}

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
            if not is_upload_container(event, name_prefix, label_filter):
                if debug_events:
                    print("node-watcher: event did not match upload filter", flush=True)
                continue
            container_id = get_container_id(event)
            if not container_id:
                print(f"node-watcher: event missing container id keys={list(event.keys())}", flush=True)
                continue
            now = datetime.utcnow()
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
            video_ids = find_processing_video_ids(db_conn, container_id)
            if not video_ids:
                fallback_ids = find_processing_video_ids_by_machine(db_conn, machine_id)
                if fallback_ids:
                    print(
                        f"node-watcher: no container_id match; falling back to machine_id={machine_id} ids={fallback_ids}",
                        flush=True,
                    )
                video_ids = fallback_ids
            if not video_ids:
                print(
                    f"node-watcher: no processing uploads found for container_id={container_id}",
                    flush=True,
                )
            for video_id in video_ids:
                print(f"node-watcher: publishing failed for video_id={video_id}", flush=True)
                publish_failed(channel, exchange, video_id, reason, machine_id, container_id)
                if update_db:
                    mark_failed(db_conn, video_id)
                    print(f"node-watcher: marked FAILED video_id={video_id}", flush=True)
    except Exception as exc:
        print(f"Watcher error: {exc}", file=sys.stderr)
        time.sleep(2)
        raise
    finally:
        try:
            rabbit_conn.close()
        except Exception:
            pass
        try:
            db_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
