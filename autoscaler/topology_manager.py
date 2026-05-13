"""
topology_manager.py — Kubernetes node cordon/uncordon and pod eviction.
"""
import logging
import math
import time
from typing import Dict, List, Optional

from kubernetes import client
from config import Config

log = logging.getLogger(__name__)

_EVICT_TIMEOUT_SECONDS = 120
_EVICT_POLL_INTERVAL   = 3


class TopologyManager:
    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._core = client.CoreV1Api()
        # Parse label selector: "key=value" format
        parts = cfg.node_label_selector.split("=", 1)
        self._label_key = parts[0]
        self._label_value = parts[1] if len(parts) > 1 else ""

    # ── Node listing ────────────────────────────────────────────────────────

    def list_worker_nodes(self) -> List[client.V1Node]:
        """Return all nodes matching the worker label selector."""
        resp = self._core.list_node(
            label_selector=self._cfg.node_label_selector
        )
        return resp.items

    @staticmethod
    def _parse_cpu(cpu_str: str) -> int:
        """
        Parse a Kubernetes CPU quantity string to an integer core count.
        Handles millicores ("4000m" → 4) and plain integers/floats ("4" → 4).
        Always returns at least 1.
        """
        if not cpu_str:
            return 1
        cpu_str = cpu_str.strip()
        if cpu_str.endswith("m"):
            return max(1, int(cpu_str[:-1]) // 1000)
        try:
            return max(1, int(float(cpu_str)))
        except (ValueError, TypeError):
            return 1

    def get_node_states(self) -> Dict[str, dict]:
        """
        Returns {node_name: {"cordoned": bool, "cpu_count": int}} for all worker
        nodes, sorted by name for deterministic ordering.
        cpu_count is derived from node.status.allocatable["cpu"].
        """
        nodes = self.list_worker_nodes()
        states = {}
        for n in nodes:
            name = n.metadata.name
            cordoned = bool(n.spec.unschedulable)
            allocatable = (n.status.allocatable or {}) if n.status else {}
            cpu_str = allocatable.get("cpu", "1")
            cpu_count = self._parse_cpu(cpu_str)
            states[name] = {"cordoned": cordoned, "cpu_count": cpu_count}
        return dict(sorted(states.items()))

    def replicas_for_node(self, cpu_count: int) -> int:
        """
        Compute the desired processing-pod replica count for a node with the
        given CPU core count.
        Formula: max(replicas_per_node, floor(cpu_count * replicas_per_cpu))
        replicas_per_node acts as the minimum floor.
        """
        return max(
            self._cfg.replicas_per_node,
            math.floor(cpu_count * self._cfg.replicas_per_cpu),
        )

    def total_replicas_for_active_nodes(self) -> int:
        """Sum replicas_for_node across all currently uncordoned worker nodes."""
        states = self.get_node_states()
        return sum(
            self.replicas_for_node(s["cpu_count"])
            for s in states.values()
            if not s["cordoned"]
        )

    def get_active_node_count(self) -> int:
        """Count of worker nodes that are NOT cordoned."""
        states = self.get_node_states()
        return sum(1 for s in states.values() if not s["cordoned"])

    def get_cordoned_node_count(self) -> int:
        states = self.get_node_states()
        return sum(1 for s in states.values() if s["cordoned"])

    def pick_node_to_activate(self) -> Optional[str]:
        """
        Return the cordoned worker node with the most CPUs, so we gain the most
        capacity per scale-up step.  Falls back to alphabetical order on a tie.
        """
        states = self.get_node_states()
        candidates = [(n, s["cpu_count"]) for n, s in states.items() if s["cordoned"]]
        if not candidates:
            return None
        # Sort descending by cpu_count, then ascending by name for tie-breaking
        candidates.sort(key=lambda x: (-x[1], x[0]))
        return candidates[0][0]

    def pick_node_to_deactivate(self) -> Optional[str]:
        """
        Return the active worker node with the fewest CPUs, so we lose the least
        capacity per scale-down step.  Falls back to reverse alphabetical on a tie.
        """
        states = self.get_node_states()
        candidates = [(n, s["cpu_count"]) for n, s in states.items() if not s["cordoned"]]
        if not candidates:
            return None
        # Sort ascending by cpu_count, then descending by name for tie-breaking
        candidates.sort(key=lambda x: (x[1], [-ord(c) for c in x[0]]))
        return candidates[0][0]

    # ── Cordon / Uncordon ────────────────────────────────────────────────────

    def cordon_node(self, name: str) -> None:
        body = {"spec": {"unschedulable": True}}
        self._core.patch_node(name, body)
        log.info("Cordoned node: %s", name)

    def uncordon_node(self, name: str) -> None:
        body = {"spec": {"unschedulable": False}}
        self._core.patch_node(name, body)
        log.info("Uncordoned node: %s", name)

    # ── Pod eviction ─────────────────────────────────────────────────────────

    def evict_pods_on_node(self, node_name: str, namespace: str, app_label_value: str) -> None:
        """
        Evict all non-DaemonSet pods on the given node that match the app label.
        Waits up to EVICT_TIMEOUT_SECONDS for pods to terminate.
        """
        label_selector = f"app={app_label_value}"
        pods = self._core.list_namespaced_pod(
            namespace=namespace,
            field_selector=f"spec.nodeName={node_name}",
            label_selector=label_selector,
        ).items

        if not pods:
            log.info("No matching pods on node %s to evict", node_name)
            return

        pod_names = []
        for pod in pods:
            # Skip DaemonSet-owned pods
            owners = pod.metadata.owner_references or []
            if any(o.kind == "DaemonSet" for o in owners):
                continue
            pod_name = pod.metadata.name
            pod_names.append(pod_name)
            eviction = client.V1Eviction(
                metadata=client.V1ObjectMeta(name=pod_name, namespace=namespace),
            )
            try:
                self._core.create_namespaced_pod_eviction(
                    name=pod_name, namespace=namespace, body=eviction
                )
                log.info("Evicted pod %s from node %s", pod_name, node_name)
            except client.exceptions.ApiException as exc:
                if exc.status == 404:
                    log.debug("Pod %s already gone during eviction", pod_name)
                else:
                    log.warning("Eviction request failed for pod %s: %s", pod_name, exc)

        if not pod_names:
            log.info("No evictable pods on node %s", node_name)
            return

        # Wait for pods to terminate
        deadline = time.monotonic() + _EVICT_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            remaining = []
            for pod_name in pod_names:
                try:
                    self._core.read_namespaced_pod(name=pod_name, namespace=namespace)
                    remaining.append(pod_name)
                except client.exceptions.ApiException as exc:
                    if exc.status == 404:
                        pass  # gone — good
                    else:
                        remaining.append(pod_name)
            if not remaining:
                log.info("All evicted pods on node %s have terminated", node_name)
                return
            log.debug("%d pod(s) still terminating on node %s", len(remaining), node_name)
            time.sleep(_EVICT_POLL_INTERVAL)

        log.warning(
            "Timeout waiting for pod eviction on node %s — proceeding with cordon anyway",
            node_name,
        )

