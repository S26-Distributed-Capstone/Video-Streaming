"""
autoscaling_state.py — Shared autoscaling on/off state for all autoscaler replicas.

The upload page can hit any autoscaler replica through the Service. Only one
replica is the leader and runs the polling loop, so on/off state cannot live only
in process memory. Store the desired mode in a small Kubernetes Lease annotation
that every replica can read/write.
"""
import logging
from typing import Optional

from kubernetes import client
from kubernetes.client.exceptions import ApiException

log = logging.getLogger(__name__)

_AUTOSCALING_ON = "autoscalingOn"
_RESTORE_ACTIVE_NODES = "restoreActiveNodes"


class AutoscalingStateStore:
    def __init__(self, namespace: str, lease_name: str):
        self._namespace = namespace
        self._lease_name = f"{lease_name}-state"
        self._coord = client.CoordinationV1Api()

    def get_autoscaling_on(self) -> bool:
        annotations = self._read_annotations()
        return str(annotations.get(_AUTOSCALING_ON, "false")).lower() == "true"

    def get_restore_active_nodes(self) -> Optional[int]:
        annotations = self._read_annotations()
        raw = annotations.get(_RESTORE_ACTIVE_NODES)
        if raw is None or str(raw).strip() == "":
            return None
        try:
            value = int(str(raw).strip())
            return value if value > 0 else None
        except ValueError:
            return None

    def set_autoscaling_on(self, enabled: bool, restore_active_nodes: Optional[int] = None) -> None:
        annotations = self._read_annotations()
        annotations[_AUTOSCALING_ON] = "true" if enabled else "false"
        if restore_active_nodes is not None and restore_active_nodes > 0:
            annotations[_RESTORE_ACTIVE_NODES] = str(restore_active_nodes)
        self._patch_annotations(annotations)
        log.info(
            "Shared autoscaling state updated: autoscalingOn=%s restoreActiveNodes=%s",
            annotations.get(_AUTOSCALING_ON),
            annotations.get(_RESTORE_ACTIVE_NODES, ""),
        )

    def _read_annotations(self) -> dict:
        try:
            lease = self._coord.read_namespaced_lease(self._lease_name, self._namespace)
            return dict((lease.metadata.annotations or {}) if lease.metadata else {})
        except ApiException as exc:
            if exc.status != 404:
                raise
            self._create_default_lease()
            return {_AUTOSCALING_ON: "false"}

    def _create_default_lease(self) -> None:
        body = client.V1Lease(
            metadata=client.V1ObjectMeta(
                name=self._lease_name,
                namespace=self._namespace,
                annotations={_AUTOSCALING_ON: "false"},
            ),
            spec=client.V1LeaseSpec(holder_identity="autoscaler-shared-state"),
        )
        try:
            self._coord.create_namespaced_lease(self._namespace, body)
            log.info("Created shared autoscaling state lease %s", self._lease_name)
        except ApiException as exc:
            if exc.status != 409:
                raise

    def _patch_annotations(self, annotations: dict) -> None:
        try:
            self._coord.patch_namespaced_lease(
                name=self._lease_name,
                namespace=self._namespace,
                body={"metadata": {"annotations": annotations}},
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
            self._create_default_lease()
            self._coord.patch_namespaced_lease(
                name=self._lease_name,
                namespace=self._namespace,
                body={"metadata": {"annotations": annotations}},
            )

