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
        Uncordon one cordoned worker node and increase StatefulSet replicas by
        that node's CPU-derived replica count.
        Returns the node name that was activated, or None if no action taken.
        """
        node_name = topology.pick_node_to_activate()
        if node_name is None:
            log.warning("scale_up requested but no cordoned nodes available")
            return None

        # Fetch CPU count before uncordoning (state still has the node as cordoned)
        states = topology.get_node_states()
        cpu_count = states.get(node_name, {}).get("cpu_count", 1)
        node_replicas = topology.replicas_for_node(cpu_count)

        topology.uncordon_node(node_name)
        current = self.get_statefulset_replicas()
        self.set_statefulset_replicas(current + node_replicas)
        log.info(
            "Scaled UP: activated node=%s cpus=%d node_replicas=%d replicas %d→%d",
            node_name, cpu_count, node_replicas, current, current + node_replicas,
        )
        return node_name

    def scale_down_one_node(self, topology: TopologyManager) -> Optional[str]:
        """
        Cordon one active worker node first (so evicted pods reschedule elsewhere),
        then evict processing pods from it, then reduce StatefulSet replicas by
        that node's CPU-derived replica count.
        Returns the node name that was deactivated, or None if no action taken.
        """
        node_name = topology.pick_node_to_deactivate()
        if node_name is None:
            log.warning("scale_down requested but no active nodes to deactivate")
            return None

        # Fetch CPU count BEFORE cordoning so we know what to subtract
        states = topology.get_node_states()
        cpu_count = states.get(node_name, {}).get("cpu_count", 1)
        node_replicas = topology.replicas_for_node(cpu_count)

        # Floor = sum of replicas for the remaining active (non-cordoned) nodes
        current = self.get_statefulset_replicas()
        target = max(0, current - node_replicas)
        self.set_statefulset_replicas(target)

        # Cordon after lowering the StatefulSet replica target. If we evict
        # first, the controller may create replacement pods before the scale-down
        # patch lands, which creates avoidable churn and batchy processing.
        topology.cordon_node(node_name)

        app_label = self._cfg.statefulset_name
        topology.evict_pods_on_node(node_name, self._cfg.kube_namespace, app_label)
        log.info(
            "Scaled DOWN: deactivated node=%s cpus=%d node_replicas=%d replicas %d→%d",
            node_name, cpu_count, node_replicas, current, target,
        )
        return node_name

    def scale_up_nodes(self, topology: TopologyManager, count: int) -> list:
        """
        Activate up to `count` cordoned nodes in one go.
        Returns a list of node names that were activated.
        """
        activated = []
        for _ in range(count):
            name = self.scale_up_one_node(topology)
            if name is None:
                break
            activated.append(name)
        return activated

    def scale_down_nodes(self, topology: TopologyManager, count: int) -> list:
        """
        Deactivate up to `count` active nodes in one go.
        Returns a list of node names that were deactivated.
        """
        deactivated = []
        for _ in range(count):
            name = self.scale_down_one_node(topology)
            if name is None:
                break
            deactivated.append(name)
        return deactivated

    def enforce_initial_state(self, topology: TopologyManager) -> None:
        """
        On startup, cordon all nodes except MIN_ACTIVE_NODES and set replicas to
        the sum of CPU-derived replica counts across the active nodes. Idempotent.
        """
        states = topology.get_node_states()
        active_nodes = [n for n, s in states.items() if not s["cordoned"]]
        cordoned_nodes = [n for n, s in states.items() if s["cordoned"]]

        target_active = self._cfg.min_active_nodes
        excess = len(active_nodes) - target_active

        if excess > 0:
            # Cordon excess nodes FIRST so evicted pods don't reschedule back onto them,
            # then evict their pods.
            to_cordon = active_nodes[target_active:]
            for node_name in to_cordon:
                topology.cordon_node(node_name)
            for node_name in to_cordon:
                app_label = self._cfg.statefulset_name
                topology.evict_pods_on_node(node_name, self._cfg.kube_namespace, app_label)
        elif excess < 0:
            # Uncordon enough to reach target
            to_uncordon = cordoned_nodes[:(-excess)]
            for node_name in to_uncordon:
                topology.uncordon_node(node_name)

        # Compute target replicas as the sum across the nodes that will be active
        target_replicas = topology.total_replicas_for_active_nodes()
        self.set_statefulset_replicas(target_replicas)
        log.info(
            "Initial state enforced: active_nodes=%d target_active=%d replicas=%d",
            len(active_nodes), target_active, target_replicas,
        )
