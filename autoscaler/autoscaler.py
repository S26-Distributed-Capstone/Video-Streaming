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


def _setup_signals() -> None:
    def _handler(sig, _frame):
        log.info("Received signal %s — shutting down", sig)
        _shutdown.set()
    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def _start_health_server(port: int) -> None:
    """Simple HTTP health endpoint on port 8084."""
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
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

        log.info("queue_depth=%d active_nodes=%d/%d", queue_depth, active_nodes, cfg.total_nodes)

        # ── Scaling decision ────────────────────────────────────────────────
        decision = engine.decide(queue_depth, active_nodes)

        if decision != "noop" and not store.is_cooldown_active(cfg.scale_cooldown_seconds):
            try:
                if decision == "scale_up":
                    log.info("Decision: scale_up (queue_depth=%d >= threshold=%d)",
                             queue_depth, cfg.scale_up_threshold)
                    executor.scale_up_one_node(topology)
                else:
                    log.info("Decision: scale_down (queue_depth=%d <= threshold=%d)",
                             queue_depth, cfg.scale_down_threshold)
                    executor.scale_down_one_node(topology)

                store.record_scale_event()
                node_states = topology.get_node_states()  # refresh after change
            except Exception as exc:
                log.error("Scale action failed: %s", exc)
        elif decision != "noop":
            remaining = cfg.scale_cooldown_seconds - (
                int(time.monotonic()) -
                int(getattr(store, "_last_scale_time", 0))
            )
            log.info("Decision: %s but cooldown active (~%ds remaining)", decision, max(0, remaining))

        # ── Publish topology if changed ─────────────────────────────────────
        if store.node_states_changed(node_states):
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
    _start_health_server(cfg.health_port)

    topology = TopologyManager(cfg)
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

