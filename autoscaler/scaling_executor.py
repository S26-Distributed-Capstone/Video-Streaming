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

    def constrain_processing_to_nodes(self, node_names: list[str]) -> None:
        body = {
            "spec": {
                "template": {
                    "spec": {
                        "affinity": {
                            "nodeAffinity": {
                                "requiredDuringSchedulingIgnoredDuringExecution": {
                                    "nodeSelectorTerms": [
                                        {
                                            "matchExpressions": [
                                                {
                                                    "key": "kubernetes.io/hostname",
                                                    "operator": "In",
                                                    "values": node_names,
                                                }
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        }
        self._apps.patch_namespaced_stateful_set(
            name=self._cfg.statefulset_name,
            namespace=self._cfg.kube_namespace,
            body=body,
        )
        log.info("StatefulSet %s constrained to processing nodes: %s", self._cfg.statefulset_name, node_names)

    def clear_processing_node_constraint(self) -> None:
        body = {
            "spec": {
                "template": {
                    "spec": {
                        "affinity": {
                            "nodeAffinity": None,
                        }
                    }
                }
            }
        }
        self._apps.patch_namespaced_stateful_set(
            name=self._cfg.statefulset_name,
            namespace=self._cfg.kube_namespace,
            body=body,
        )
        log.info("StatefulSet %s processing-node constraint cleared", self._cfg.statefulset_name)

    def reconcile_replicas_to_active_topology(self, topology: TopologyManager) -> bool:
        """
        Ensure the StatefulSet replica target exactly matches the full processing
        capacity of the currently uncordoned worker nodes.

        This keeps replica count aligned even when nodes are manually cordoned or
        uncordoned outside the normal queue-driven scale-up/scale-down path.
        Returns True when a reconciliation patch was applied.
        """
        current = self.get_statefulset_replicas()
        target = topology.total_replicas_for_active_nodes()
        if current == target:
            return False
        self.set_statefulset_replicas(target)
        log.info(
            "Reconciled StatefulSet %s replicas to active topology: %d→%d",
            self._cfg.statefulset_name,
            current,
            target,
        )
        return True

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

    def enforce_active_node_count(self, topology: TopologyManager, target_active: int) -> None:
        """
        Cordon/uncordon workers until exactly target_active nodes are schedulable,
        then set processing replicas to those active nodes' CPU-derived capacity.
        """
        states = topology.get_node_states()
        target_active = max(0, min(target_active, len(states)))
        ranked_nodes = sorted(
            states.items(),
            key=lambda item: (-item[1]["cpu_count"], item[0]),
        )
        target_nodes = {name for name, _ in ranked_nodes[:target_active]}

        to_cordon = [name for name, state in states.items() if name not in target_nodes and not state["cordoned"]]
        to_uncordon = [name for name, state in states.items() if name in target_nodes and state["cordoned"]]

        for node_name in to_cordon:
            topology.cordon_node(node_name)
        for node_name in to_cordon:
            app_label = self._cfg.statefulset_name
            topology.evict_pods_on_node(node_name, self._cfg.kube_namespace, app_label)
        for node_name in to_uncordon:
            topology.uncordon_node(node_name)

        target_replicas = sum(
            topology.replicas_for_node(states[name]["cpu_count"])
            for name in target_nodes
        )
        self.set_statefulset_replicas(target_replicas)
        log.info(
            "Active node count enforced: target_active=%d target_nodes=%s replicas=%d",
            target_active, sorted(target_nodes), target_replicas,
        )

    def enforce_processing_baseline_without_cordoning(self, topology: TopologyManager, target_active: int) -> None:
        """
        Autoscaling OFF baseline: do not cordon or uncordon any Kubernetes node.
        Instead, pin only the processing StatefulSet to target_active worker nodes
        and set replicas to those nodes' full 1-CPU-pod capacity.
        """
        states = topology.get_node_states()
        target_active = max(0, min(target_active, len(states)))
        ranked_nodes = sorted(
            states.items(),
            key=lambda item: (-item[1]["cpu_count"], item[0]),
        )
        target_nodes = [name for name, _ in ranked_nodes[:target_active]]
        target_replicas = sum(
            topology.replicas_for_node(states[name]["cpu_count"])
            for name in target_nodes
        )

        if target_nodes:
            self.constrain_processing_to_nodes(target_nodes)
        self.set_statefulset_replicas(target_replicas)
        log.info(
            "Autoscaling-off processing baseline enforced without cordoning: nodes=%s replicas=%d",
            target_nodes, target_replicas,
        )

    def enforce_initial_state(self, topology: TopologyManager) -> None:
        """
        On startup, cordon all nodes except MIN_ACTIVE_NODES and set replicas to
        the sum of CPU-derived replica counts across the active nodes. Idempotent.
        """
        self.clear_processing_node_constraint()
        self.enforce_active_node_count(topology, self._cfg.min_active_nodes)
        log.info("Initial state enforced: target_active=%d", self._cfg.min_active_nodes)

