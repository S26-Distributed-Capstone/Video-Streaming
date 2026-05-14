"""
state_store.py — In-memory autoscaler state. Thread-safe for single-leader use.
"""
import time
from typing import Dict, Optional


class StateStore:
    def __init__(self):
        self._last_scale_time: float = 0.0
        self._last_published_node_states: Optional[Dict] = None
        self._became_leader_bootstrapped: bool = False
        self._idle_poll_count: int = 0

    # ── Cooldown ─────────────────────────────────────────────────────────────

    def is_cooldown_active(self, cooldown_seconds: int) -> bool:
        return (time.monotonic() - self._last_scale_time) < cooldown_seconds

    def record_scale_event(self) -> None:
        self._last_scale_time = time.monotonic()

    # ── Idle tracking ───────────────────────────────────────────────────────

    def record_queue_depth(self, queue_depth: int, idle_threshold: int) -> int:
        if queue_depth <= idle_threshold:
            self._idle_poll_count += 1
        else:
            self._idle_poll_count = 0
        return self._idle_poll_count

    # ── Publish deduplication ────────────────────────────────────────────────

    def node_states_changed(self, current_states: Dict) -> bool:
        """Returns True if current_states differs from the last published states."""
        return current_states != self._last_published_node_states

    def record_publish(self, states: Dict) -> None:
        self._last_published_node_states = dict(states)

    # ── Leader bootstrap ─────────────────────────────────────────────────────

    def has_bootstrapped(self) -> bool:
        return self._became_leader_bootstrapped

    def mark_bootstrapped(self) -> None:
        self._became_leader_bootstrapped = True

    def reset_bootstrap(self) -> None:
        """Call when leadership is lost so next election triggers a fresh bootstrap."""
        self._became_leader_bootstrapped = False
        self._last_published_node_states = None
        self._idle_poll_count = 0
