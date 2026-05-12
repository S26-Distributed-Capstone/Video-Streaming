#!/usr/bin/env python3
"""
mock_autoscaler.py — Simulates the autoscaler publishing NodeStatusEvent messages
to a locally running Docker Compose stack. Use this to test the frontend cluster
dashboard without needing a real Kubernetes cluster.

Usage:
  # Default: connect to localhost RabbitMQ, simulate 12 nodes, animate scaling
  python3 scripts/mock_autoscaler.py

  # Custom RabbitMQ host/port
  RABBITMQ_HOST=192.168.1.10 RABBITMQ_USER=admin RABBITMQ_PASS=admin123 \
    python3 scripts/mock_autoscaler.py

  # Point at the k3s cluster's RabbitMQ directly (needs port-forward or LoadBalancer)
  RABBITMQ_HOST=<lb-ip> python3 scripts/mock_autoscaler.py

What it does:
  - On startup: publishes initial state (2 active, 10 cordoned)
  - Every 5 s: bumps fake queue depth, occasionally activates / deactivates a node
  - Prints each event it sends so you can see what the browser should be receiving
"""

import json
import math
import os
import random
import signal
import sys
import time

try:
    import pika
except ImportError:
    print("ERROR: pika is not installed. Run: pip install pika")
    sys.exit(1)

# ── Config ──────────────────────────────────────────────────────────────────
RABBITMQ_HOST     = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT     = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER     = os.environ.get("RABBITMQ_USER", os.environ.get("RABBITMQ_DEFAULT_USER", "guest"))
RABBITMQ_PASS     = os.environ.get("RABBITMQ_PASS", os.environ.get("RABBITMQ_DEFAULT_PASS", "guest"))
RABBITMQ_VHOST    = os.environ.get("RABBITMQ_VHOST", "/")
RABBITMQ_EXCHANGE = os.environ.get("RABBITMQ_EXCHANGE", "upload.events")
ROUTING_KEY       = "upload.cluster.node_status"

TOTAL_NODES       = int(os.environ.get("TOTAL_NODES", "12"))
MIN_ACTIVE        = int(os.environ.get("MIN_ACTIVE_NODES", "2"))
INTERVAL_SECONDS  = float(os.environ.get("MOCK_INTERVAL", "5"))

# ── Node names ──────────────────────────────────────────────────────────────
NODE_NAMES = [f"worker-{i+1:02d}" for i in range(TOTAL_NODES)]

# ── State ────────────────────────────────────────────────────────────────────
# Start with MIN_ACTIVE nodes active, rest cordoned
node_states = {}
for i, name in enumerate(NODE_NAMES):
    node_states[name] = "active" if i < MIN_ACTIVE else "cordoned"

tick = 0
_stop = False


def _handle_signal(sig, _):
    global _stop
    print(f"\nReceived signal {sig} — stopping.")
    _stop = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def simulated_queue_depth(t: int) -> int:
    """
    Produces a sine-wave queue depth that peaks around 80 and dips near 0,
    roughly cycling every 60 ticks (~5 minutes at 5s interval).
    """
    raw = (math.sin(t / 10.0) + 1.0) / 2.0  # 0..1
    return int(raw * 80)


def maybe_scale(queue_depth: int) -> None:
    """Simulate autoscaler scale decisions based on queue depth."""
    global node_states
    active = [n for n, s in node_states.items() if s == "active"]
    cordoned = [n for n, s in node_states.items() if s == "cordoned"]

    if queue_depth >= 20 and cordoned:
        node = cordoned[0]
        node_states[node] = "active"
        print(f"  [scale-up]  uncordoned {node} (queue={queue_depth})")
    elif queue_depth <= 5 and len(active) > MIN_ACTIVE:
        node = active[-1]
        node_states[node] = "cordoned"
        print(f"  [scale-down] cordoned {node} (queue={queue_depth})")


def build_payload(queue_depth: int) -> dict:
    nodes = [{"name": n, "state": s} for n, s in node_states.items()]
    active_count = sum(1 for s in node_states.values() if s == "active")
    return {
        "jobId": "__cluster__",
        "taskId": "node_status",
        "type": "node_status",
        "nodes": nodes,
        "queueDepth": queue_depth,
        "activeCount": active_count,
        "totalCount": TOTAL_NODES,
    }


def main():
    global tick, _stop

    print(f"Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            virtual_host=RABBITMQ_VHOST,
            credentials=credentials,
            heartbeat=60,
            connection_attempts=5,
            retry_delay=2,
        )
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        ch.exchange_declare(exchange=RABBITMQ_EXCHANGE, exchange_type="topic", durable=True)
        print(f"Connected. Publishing to exchange '{RABBITMQ_EXCHANGE}' key '{ROUTING_KEY}' every {INTERVAL_SECONDS}s")
        print(f"Open http://localhost:8080 to see the cluster dashboard update.\n")
        print("Press Ctrl+C to stop.\n")
    except Exception as exc:
        print(f"ERROR: Could not connect to RabbitMQ: {exc}")
        print("Make sure Docker Compose is running: docker compose up -d")
        sys.exit(1)

    while not _stop:
        queue_depth = simulated_queue_depth(tick)
        maybe_scale(queue_depth)
        payload = build_payload(queue_depth)

        active_count = payload["activeCount"]
        active_names = [n["name"] for n in payload["nodes"] if n["state"] == "active"]
        print(f"[tick={tick:4d}] queue={queue_depth:3d}  active={active_count}/{TOTAL_NODES}  nodes={active_names}")

        body = json.dumps(payload).encode("utf-8")
        try:
            ch.basic_publish(
                exchange=RABBITMQ_EXCHANGE,
                routing_key=ROUTING_KEY,
                body=body,
                properties=pika.BasicProperties(content_type="application/json"),
            )
        except Exception as exc:
            print(f"  [warn] publish failed: {exc} — reconnecting...")
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn = pika.BlockingConnection(params)
                ch = conn.channel()
                ch.exchange_declare(exchange=RABBITMQ_EXCHANGE, exchange_type="topic", durable=True)
            except Exception as exc2:
                print(f"  [error] reconnect failed: {exc2}")

        tick += 1
        time.sleep(INTERVAL_SECONDS)

    try:
        conn.close()
    except Exception:
        pass
    print("Mock autoscaler stopped.")


if __name__ == "__main__":
    main()

