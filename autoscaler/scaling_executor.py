"""
scaling_executor.py — Orchestrates cordon/uncordon + StatefulSet replica patching.
"""
import logging
from typing import Optional

from kubernetes import client
from config import Config
from topology_manager import TopologyManager

log = logging.getLogger(__name__)


class ScalingExecutor:
    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._apps = client.AppsV1Api()

    # ── StatefulSet replica management ──────────────────────────────────────

    def get_statefulset_replicas(self) -> int:
        ss = self._apps.read_namespaced_stateful_set(
            name=self._cfg.statefulset_name,
            namespace=self._cfg.kube_namespace,
        )
        return ss.spec.replicas or 0

    def set_statefulset_replicas(self, count: int) -> None:
        count = max(0, count)
        body = {"spec": {"replicas": count}}
        self._apps.patch_namespaced_stateful_set(
            name=self._cfg.statefulset_name,
            namespace=self._cfg.kube_namespace,
            body=body,
        )
        log.info("StatefulSet %s replicas set to %d", self._cfg.statefulset_name, count)

    # ── Scale actions ────────────────────────────────────────────────────────

    def scale_up_one_node(self, topology: TopologyManager) -> Optional[str]:
        """
        Uncordon one cordoned worker node and increase StatefulSet replicas.
        Returns the node name that was activated, or None if no action taken.
        """
        node_name = topology.pick_node_to_activate()
        if node_name is None:
            log.warning("scale_up requested but no cordoned nodes available")
            return None

        topology.uncordon_node(node_name)
        current = self.get_statefulset_replicas()
        self.set_statefulset_replicas(current + self._cfg.replicas_per_node)
        log.info("Scaled UP: activated node=%s replicas %d→%d",
                 node_name, current, current + self._cfg.replicas_per_node)
        return node_name

    def scale_down_one_node(self, topology: TopologyManager) -> Optional[str]:
        """
        Evict processing pods from one active worker node, cordon it, then
        reduce StatefulSet replicas.
        Returns the node name that was deactivated, or None if no action taken.
        """
        node_name = topology.pick_node_to_deactivate()
        if node_name is None:
            log.warning("scale_down requested but no active nodes to deactivate")
            return None

        app_label = self._cfg.statefulset_name
        topology.evict_pods_on_node(node_name, self._cfg.kube_namespace, app_label)
        topology.cordon_node(node_name)

        current = self.get_statefulset_replicas()
        target = max(
            self._cfg.min_active_nodes * self._cfg.replicas_per_node,
            current - self._cfg.replicas_per_node,
        )
        self.set_statefulset_replicas(target)
        log.info("Scaled DOWN: deactivated node=%s replicas %d→%d",
                 node_name, current, target)
        return node_name

    def enforce_initial_state(self, topology: TopologyManager) -> None:
        """
        On startup, cordon all nodes except MIN_ACTIVE_NODES and set replicas
        to MIN_ACTIVE_NODES × REPLICAS_PER_NODE. Idempotent.
        """
        states = topology.get_node_states()
        active_nodes = [n for n, s in states.items() if not s["cordoned"]]
        cordoned_nodes = [n for n, s in states.items() if s["cordoned"]]

        target_active = self._cfg.min_active_nodes
        excess = len(active_nodes) - target_active

        if excess > 0:
            # Cordon the excess active nodes (last N in sorted order)
            to_cordon = active_nodes[target_active:]
            for node_name in to_cordon:
                app_label = self._cfg.statefulset_name
                topology.evict_pods_on_node(node_name, self._cfg.kube_namespace, app_label)
                topology.cordon_node(node_name)
        elif excess < 0:
            # Uncordon enough to reach target
            to_uncordon = cordoned_nodes[:(-excess)]
            for node_name in to_uncordon:
                topology.uncordon_node(node_name)

        target_replicas = target_active * self._cfg.replicas_per_node
        self.set_statefulset_replicas(target_replicas)
        log.info(
            "Initial state enforced: active_nodes=%d target_active=%d replicas=%d",
            len(active_nodes), target_active, target_replicas,
        )

