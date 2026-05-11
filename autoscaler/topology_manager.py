"""
topology_manager.py — Kubernetes node cordon/uncordon and pod eviction.
"""
import logging
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

    def get_node_states(self) -> Dict[str, dict]:
        """
        Returns {node_name: {"cordoned": bool}} for all worker nodes,
        sorted by name for deterministic ordering.
        """
        nodes = self.list_worker_nodes()
        states = {}
        for n in nodes:
            name = n.metadata.name
            cordoned = bool(n.spec.unschedulable)
            states[name] = {"cordoned": cordoned}
        return dict(sorted(states.items()))

    def get_active_node_count(self) -> int:
        """Count of worker nodes that are NOT cordoned."""
        states = self.get_node_states()
        return sum(1 for s in states.values() if not s["cordoned"])

    def get_cordoned_node_count(self) -> int:
        states = self.get_node_states()
        return sum(1 for s in states.values() if s["cordoned"])

    def pick_node_to_activate(self) -> Optional[str]:
        """Return the name of the first cordoned worker node, or None."""
        states = self.get_node_states()
        for name, state in states.items():
            if state["cordoned"]:
                return name
        return None

    def pick_node_to_deactivate(self) -> Optional[str]:
        """Return the name of the last active worker node (reverse order), or None."""
        states = self.get_node_states()
        active = [n for n, s in states.items() if not s["cordoned"]]
        return active[-1] if active else None

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

