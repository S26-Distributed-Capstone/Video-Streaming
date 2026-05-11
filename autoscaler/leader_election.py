"""
leader_election.py — Kubernetes Lease-based leader election.

Only the pod that holds the Lease is the active leader.
All other replicas block in their polling loop and do nothing.
"""
import logging
import os
import socket
import threading
import time
from datetime import datetime, timezone

from kubernetes import client
from kubernetes.client.exceptions import ApiException

log = logging.getLogger(__name__)

_LEASE_DURATION_SECONDS = 30
_RENEW_INTERVAL_SECONDS = 10
_ACQUIRE_RETRY_SECONDS  = 5


class LeaderElection:
    def __init__(self, namespace: str, lease_name: str):
        self._namespace = namespace
        self._lease_name = lease_name
        self._identity = os.environ.get("HOSTNAME", socket.gethostname())
        self._coord = client.CoordinationV1Api()
        self._is_leader = False
        self._lock = threading.Lock()
        self._renew_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    @property
    def is_leader(self) -> bool:
        with self._lock:
            return self._is_leader

    def _now_str(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _try_acquire(self) -> bool:
        """Attempt to create or take over the Lease. Returns True if we became leader."""
        lease_body = client.V1Lease(
            metadata=client.V1ObjectMeta(name=self._lease_name, namespace=self._namespace),
            spec=client.V1LeaseSpec(
                holder_identity=self._identity,
                lease_duration_seconds=_LEASE_DURATION_SECONDS,
                acquire_time=datetime.now(timezone.utc),
                renew_time=datetime.now(timezone.utc),
            ),
        )
        try:
            # Try to create a fresh lease
            self._coord.create_namespaced_lease(self._namespace, lease_body)
            log.info("Leader election: acquired lease as %s (created)", self._identity)
            return True
        except ApiException as exc:
            if exc.status != 409:  # Conflict — lease already exists
                raise

        # Lease exists — check if it has expired
        try:
            existing = self._coord.read_namespaced_lease(self._lease_name, self._namespace)
            renew_time = existing.spec.renew_time
            duration = existing.spec.lease_duration_seconds or _LEASE_DURATION_SECONDS
            if renew_time is None:
                expired = True
            else:
                elapsed = (datetime.now(timezone.utc) - renew_time).total_seconds()
                expired = elapsed > duration

            if existing.spec.holder_identity == self._identity:
                # We already hold it — renew
                self._renew_lease()
                return True

            if expired:
                log.info(
                    "Lease held by %s has expired — taking over",
                    existing.spec.holder_identity,
                )
                existing.spec.holder_identity = self._identity
                existing.spec.renew_time = datetime.now(timezone.utc)
                existing.spec.acquire_time = datetime.now(timezone.utc)
                self._coord.replace_namespaced_lease(
                    self._lease_name, self._namespace, existing
                )
                log.info("Leader election: acquired lease as %s (takeover)", self._identity)
                return True
        except ApiException:
            pass

        return False

    def _renew_lease(self) -> None:
        try:
            existing = self._coord.read_namespaced_lease(self._lease_name, self._namespace)
            if existing.spec.holder_identity != self._identity:
                log.warning("Lost lease to %s — stepping down", existing.spec.holder_identity)
                with self._lock:
                    self._is_leader = False
                return
            existing.spec.renew_time = datetime.now(timezone.utc)
            self._coord.replace_namespaced_lease(self._lease_name, self._namespace, existing)
            log.debug("Lease renewed by %s", self._identity)
        except Exception as exc:
            log.warning("Failed to renew lease: %s — stepping down", exc)
            with self._lock:
                self._is_leader = False

    def _renew_loop(self) -> None:
        while not self._stop_event.wait(_RENEW_INTERVAL_SECONDS):
            with self._lock:
                if not self._is_leader:
                    break
            self._renew_lease()

    def run_election(self) -> None:
        """
        Blocking until we acquire the lease.
        Starts the background renew thread once leadership is confirmed.
        """
        log.info("Leader election: candidate identity=%s lease=%s", self._identity, self._lease_name)
        while True:
            try:
                if self._try_acquire():
                    with self._lock:
                        self._is_leader = True
                    self._stop_event.clear()
                    self._renew_thread = threading.Thread(
                        target=self._renew_loop, daemon=True, name="lease-renew"
                    )
                    self._renew_thread.start()
                    log.info("Leadership acquired by %s", self._identity)
                    return
                else:
                    log.debug("Did not acquire lease — retrying in %ds", _ACQUIRE_RETRY_SECONDS)
            except Exception as exc:
                log.warning("Error during lease acquisition: %s", exc)
            time.sleep(_ACQUIRE_RETRY_SECONDS)

    def stop(self) -> None:
        self._stop_event.set()
        with self._lock:
            self._is_leader = False

