"""
autoscaler.py — Main entrypoint.

Polling loop:
  1. Acquire Kubernetes Lease (blocks until we become leader).
  2. Start with autoscaling OFF; pin processing pods to the fixed MIN_ACTIVE_NODES baseline without cordoning nodes.
  3. Every POLL_INTERVAL_SECONDS:
       - Read queue depth from RabbitMQ Management API.
       - Count active (uncordoned) worker nodes.
       - If autoscaling is ON, decide whether to scale up or down.
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

from autoscaling_state import AutoscalingStateStore
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
_paused.set()


def _apply_startup_pause() -> None:
    _paused.set()
    log.info("Autoscaler starting OFF — bootstrap, replica reconciliation, and scaling decisions are disabled until turned on")


def _resolve_resume_active_nodes(cfg: Config, store: StateStore, autoscaling_state: AutoscalingStateStore) -> int:
    restore_count = autoscaling_state.get_restore_active_nodes() or store.get_restore_active_nodes() or cfg.min_active_nodes
    return max(cfg.min_active_nodes, min(restore_count, cfg.total_nodes))


def _setup_signals() -> None:
    def _handler(sig, _frame):
        log.info("Received signal %s — shutting down", sig)
        _shutdown.set()
    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def _start_health_server(
    cfg: Config,
    topology: "TopologyManager",
    executor: "ScalingExecutor",
    store: StateStore,
    autoscaling_state: AutoscalingStateStore,
) -> None:
    """HTTP server on port 8084 — health check + autoscaling on/off + manual cordon/uncordon."""

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
                autoscaling_on = autoscaling_state.get_autoscaling_on()
                self._send_json(200, {"paused": not autoscaling_on, "autoscalingOn": autoscaling_on})
            else:
                self._send_json(404, {"error": "not found"})

        def do_POST(self):
            if self.path == "/pause":
                try:
                    node_states = topology.get_node_states()
                    active_nodes = sum(1 for s in node_states.values() if not s["cordoned"])
                    store.record_autoscaling_off_active_nodes(active_nodes)
                    autoscaling_state.set_autoscaling_on(False, active_nodes)
                except Exception as exc:
                    log.warning("Failed to persist autoscaling OFF state: %s", exc)
                    self._send_json(500, {"error": str(exc), "paused": False, "autoscalingOn": True})
                    return
                _paused.set()
                log.info("Autoscaling turned OFF via API")
                self._send_json(200, {"paused": True, "autoscalingOn": False})
            elif self.path == "/resume":
                try:
                    restore_count = _resolve_resume_active_nodes(cfg, store, autoscaling_state)
                    autoscaling_state.set_autoscaling_on(True, restore_count)
                    log.info("Autoscaling ON requested via API with target_active_nodes=%d", restore_count)
                    self._send_json(200, {
                        "paused": False,
                        "autoscalingOn": True,
                        "targetActiveNodes": restore_count,
                    })
                except Exception as exc:
                    log.error("Failed to turn autoscaling on: %s", exc)
                    self._send_json(500, {"error": str(exc), "paused": True, "autoscalingOn": False})
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

    server = HTTPServer(("", cfg.health_port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="health-server")
    thread.start()
    log.info("Health server listening on :%d", cfg.health_port)


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
    autoscaling_state: AutoscalingStateStore,
) -> None:
    """Main polling loop — runs only while we are leader."""
    log.info("Poll loop started (interval=%ds)", cfg.poll_interval_seconds)

    while not _shutdown.is_set():
        if not election.is_leader:
            log.info("Lost leadership — exiting poll loop")
            store.reset_bootstrap()
            return

        try:
            desired_autoscaling_on = autoscaling_state.get_autoscaling_on()
            if desired_autoscaling_on and _paused.is_set():
                restore_count = _resolve_resume_active_nodes(cfg, store, autoscaling_state)
                log.info("Autoscaling shared state is ON — enabling leader with target_active_nodes=%d", restore_count)
                executor.clear_processing_node_constraint()
                executor.enforce_active_node_count(topology, restore_count)
                store.mark_bootstrapped()
                _paused.clear()
            elif not desired_autoscaling_on and not _paused.is_set():
                log.info("Autoscaling shared state is OFF — disabling leader and enforcing no-cordon baseline")
                executor.enforce_processing_baseline_without_cordoning(topology, cfg.min_active_nodes)
                store.mark_bootstrapped()
                _paused.set()
        except Exception as exc:
            log.error("Failed to apply shared autoscaling state: %s", exc)
            time.sleep(cfg.poll_interval_seconds)
            continue

        # ── Bootstrap once per leadership term ─────────────────────────────
        if not store.has_bootstrapped():
            if _paused.is_set():
                log.info(
                    "Autoscaling is OFF before bootstrap — enforcing fixed %d-node processing baseline without cordoning nodes",
                    cfg.min_active_nodes,
                )
                try:
                    executor.enforce_processing_baseline_without_cordoning(topology, cfg.min_active_nodes)
                    store.mark_bootstrapped()
                except Exception as exc:
                    log.error("Autoscaling-off baseline bootstrap failed: %s — will retry next poll", exc)
                    time.sleep(cfg.poll_interval_seconds)
                    continue
            else:
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

        if not _paused.is_set():
            log.debug("Reconciling StatefulSet replicas to active topology")
            try:
                executor.reconcile_replicas_to_active_topology(topology)
            except Exception as exc:
                log.error("Replica reconciliation failed: %s", exc)
                time.sleep(cfg.poll_interval_seconds)
                continue

        idle_polls = store.record_queue_depth(queue_depth, cfg.scale_down_threshold)
        log.info(
            "queue_depth=%d active_nodes=%d/%d idle_polls=%d/%d",
            queue_depth, active_nodes, cfg.total_nodes, idle_polls, cfg.scale_down_idle_polls,
        )

        # ── Scaling decision ────────────────────────────────────────────────
        if _paused.is_set():
            log.info("Autoscaling is OFF — skipping scaling decision")
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
    _apply_startup_pause()

    _load_kube()

    topology = TopologyManager(cfg)
    executor = ScalingExecutor(cfg)
    metrics  = MetricsCollector(cfg)
    engine   = DecisionEngine(cfg)
    store    = StateStore()
    autoscaling_state = AutoscalingStateStore(cfg.kube_namespace, cfg.lease_name)
    _start_health_server(cfg, topology, executor, store, autoscaling_state)
    pub      = Publisher(cfg)
    election = LeaderElection(cfg.kube_namespace, cfg.lease_name)

    while not _shutdown.is_set():
        log.info("Entering leader election...")
        election.run_election()

        if _shutdown.is_set():
            break

        try:
            run_poll_loop(cfg, topology, executor, metrics, engine, store, pub, election, autoscaling_state)
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
