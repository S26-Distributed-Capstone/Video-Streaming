"""
autoscaler.py — Main entrypoint.

Polling loop:
  1. Acquire Kubernetes Lease (blocks until we become leader).
  2. Enforce initial cluster state (cordon excess nodes, set StatefulSet replicas).
  3. Every POLL_INTERVAL_SECONDS:
       - Read queue depth from RabbitMQ Management API.
       - Count active (uncordoned) worker nodes.
       - Decide whether to scale up or down.
       - Execute scale action if cooldown allows.
       - Publish NodeStatusEvent if topology changed.
  4. If leadership is lost, step back to election loop.
"""

import json
import logging
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from kubernetes import config as kube_config

import config as cfg_module
from config import Config
from decision_engine import DecisionEngine
from leader_election import LeaderElection
from metrics_collector import MetricsCollector
from publisher import Publisher
from scaling_executor import ScalingExecutor
from state_store import StateStore
from topology_manager import TopologyManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("autoscaler")

_shutdown = threading.Event()

# ── Pause flag — when set, scaling decisions are skipped ────────────────────
_paused = threading.Event()


def _setup_signals() -> None:
    def _handler(sig, _frame):
        log.info("Received signal %s — shutting down", sig)
        _shutdown.set()
    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def _start_health_server(port: int, topology: "TopologyManager") -> None:
    """HTTP server on port 8084 — health check + pause/resume + manual cordon/uncordon."""

    class Handler(BaseHTTPRequestHandler):
        def _send_json(self, status: int, body: dict) -> None:
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(data)

        def do_OPTIONS(self):
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

        def do_GET(self):
            if self.path in ("/health", "/ready", "/"):
                self.send_response(200)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(b"OK")
            elif self.path == "/status":
                self._send_json(200, {"paused": _paused.is_set()})
            else:
                self._send_json(404, {"error": "not found"})

        def do_POST(self):
            if self.path == "/pause":
                _paused.set()
                log.info("Autoscaler paused via API")
                self._send_json(200, {"paused": True})
            elif self.path == "/resume":
                _paused.clear()
                log.info("Autoscaler resumed via API")
                self._send_json(200, {"paused": False})
            elif self.path.startswith("/cordon/"):
                node_name = self.path[len("/cordon/"):]
                if not node_name:
                    self._send_json(400, {"error": "missing node name"})
                    return
                try:
                    topology.cordon_node(node_name)
                    log.info("Manually cordoned node %s via API", node_name)
                    self._send_json(200, {"node": node_name, "cordoned": True})
                except Exception as exc:
                    log.error("Failed to cordon node %s: %s", node_name, exc)
                    self._send_json(500, {"error": str(exc)})
            elif self.path.startswith("/uncordon/"):
                node_name = self.path[len("/uncordon/"):]
                if not node_name:
                    self._send_json(400, {"error": "missing node name"})
                    return
                try:
                    topology.uncordon_node(node_name)
                    log.info("Manually uncordoned node %s via API", node_name)
                    self._send_json(200, {"node": node_name, "cordoned": False})
                except Exception as exc:
                    log.error("Failed to uncordon node %s: %s", node_name, exc)
                    self._send_json(500, {"error": str(exc)})
            else:
                self._send_json(404, {"error": "not found"})

        def log_message(self, *args):
            pass  # suppress HTTP access logs

    server = HTTPServer(("", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="health-server")
    thread.start()
    log.info("Health server listening on :%d", port)


def _load_kube() -> None:
    try:
        kube_config.load_incluster_config()
        log.info("Loaded in-cluster Kubernetes config")
    except kube_config.ConfigException:
        kube_config.load_kube_config()
        log.info("Loaded local kubeconfig (dev mode)")


def run_poll_loop(
    cfg: Config,
    topology: TopologyManager,
    executor: ScalingExecutor,
    metrics: MetricsCollector,
    engine: DecisionEngine,
    store: StateStore,
    pub: Publisher,
    election: LeaderElection,
) -> None:
    """Main polling loop — runs only while we are leader."""
    log.info("Poll loop started (interval=%ds)", cfg.poll_interval_seconds)

    while not _shutdown.is_set():
        if not election.is_leader:
            log.info("Lost leadership — exiting poll loop")
            store.reset_bootstrap()
            return

        # ── Bootstrap once per leadership term ─────────────────────────────
        if not store.has_bootstrapped():
            log.info("Bootstrapping initial cluster state...")
            try:
                executor.enforce_initial_state(topology)
                store.mark_bootstrapped()
            except Exception as exc:
                log.error("Bootstrap failed: %s — will retry next poll", exc)
                time.sleep(cfg.poll_interval_seconds)
                continue

        # ── Collect metrics ─────────────────────────────────────────────────
        try:
            queue_depth = metrics.get_queue_depth()
            node_states = topology.get_node_states()
            active_nodes = sum(1 for s in node_states.values() if not s["cordoned"])
        except Exception as exc:
            log.error("Metrics collection failed: %s", exc)
            time.sleep(cfg.poll_interval_seconds)
            continue

        idle_polls = store.record_queue_depth(queue_depth, cfg.scale_down_threshold)
        log.info(
            "queue_depth=%d active_nodes=%d/%d idle_polls=%d/%d",
            queue_depth, active_nodes, cfg.total_nodes, idle_polls, cfg.scale_down_idle_polls,
        )

        # ── Scaling decision ────────────────────────────────────────────────
        if _paused.is_set():
            log.info("Autoscaler is paused — skipping scaling decision")
        else:
            decision, steps = engine.decide(queue_depth, active_nodes)

            if decision == "scale_down" and idle_polls < cfg.scale_down_idle_polls:
                log.info(
                    "Decision: scale_down ×%d deferred until queue is idle for %d consecutive poll(s)",
                    steps, cfg.scale_down_idle_polls,
                )
            elif decision != "noop" and not store.is_cooldown_active(cfg.scale_cooldown_seconds):
                try:
                    if decision == "scale_up":
                        log.info(
                            "Decision: scale_up ×%d (queue_depth=%d >= threshold=%d)",
                            steps, queue_depth, cfg.scale_up_threshold,
                        )
                        executor.scale_up_nodes(topology, steps)
                    else:
                        log.info(
                            "Decision: scale_down ×%d (queue_depth=%d <= threshold=%d)",
                            steps, queue_depth, cfg.scale_down_threshold,
                        )
                        executor.scale_down_nodes(topology, steps)

                    store.record_scale_event()
                    node_states = topology.get_node_states()  # refresh after change
                except Exception as exc:
                    log.error("Scale action failed: %s", exc)
            elif decision != "noop":
                remaining = cfg.scale_cooldown_seconds - (
                    int(time.monotonic()) -
                    int(getattr(store, "_last_scale_time", 0))
                )
                log.info("Decision: %s ×%d but cooldown active (~%ds remaining)", decision, steps, max(0, remaining))

        # ── Publish topology every poll ─────────────────────────────────────
        # Always publish (not just on change) so newly connected browsers
        # receive current state within one poll interval of connecting.
        try:
            pub.publish_node_status(node_states, queue_depth)
            store.record_publish(node_states)
        except Exception as exc:
            log.warning("Failed to publish node status: %s", exc)

        _shutdown.wait(cfg.poll_interval_seconds)


def main() -> None:
    _setup_signals()
    log.info("Autoscaler starting...")

    cfg = cfg_module.load()
    log.info("Config: %s", cfg)

    _load_kube()

    topology = TopologyManager(cfg)
    _start_health_server(cfg.health_port, topology)
    executor = ScalingExecutor(cfg)
    metrics  = MetricsCollector(cfg)
    engine   = DecisionEngine(cfg)
    store    = StateStore()
    pub      = Publisher(cfg)
    election = LeaderElection(cfg.kube_namespace, cfg.lease_name)

    while not _shutdown.is_set():
        log.info("Entering leader election...")
        election.run_election()

        if _shutdown.is_set():
            break

        try:
            run_poll_loop(cfg, topology, executor, metrics, engine, store, pub, election)
        except Exception as exc:
            log.error("Unhandled error in poll loop: %s — restarting", exc, exc_info=True)
            store.reset_bootstrap()
            time.sleep(5)

    log.info("Autoscaler shutting down")
    election.stop()
    pub.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
