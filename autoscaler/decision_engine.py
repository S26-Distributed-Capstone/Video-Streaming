"""
decision_engine.py — Pure scaling logic; no I/O.
"""
import math
from typing import Tuple

from config import Config


class DecisionEngine:
    def __init__(self, cfg: Config):
        self._cfg = cfg

    def decide(self, queue_depth: int, active_nodes: int) -> Tuple[str, int]:
        """
        Returns (action, steps) where action is 'scale_up', 'scale_down', or 'noop'
        and steps is the number of nodes to activate/deactivate.

        Scale-up steps are proportional to load:
          steps = min(max_scale_up_step, ceil(queue_depth / scale_up_threshold))
        This means a queue at 2× threshold spins up 2 nodes, 3× spins up 3, etc.

        Scale-down is conservative (capped at max_scale_down_step, default 1) to
        avoid thrashing while pods drain.

        Cooldown enforcement is handled by the caller.
        """
        if queue_depth >= self._cfg.scale_up_threshold and active_nodes < self._cfg.total_nodes:
            # How many scale-up thresholds worth of work is queued?
            raw_steps = math.ceil(queue_depth / self._cfg.scale_up_threshold)
            # Cap at max_scale_up_step and available cordoned nodes
            available = self._cfg.total_nodes - active_nodes
            steps = min(self._cfg.max_scale_up_step, raw_steps, available)
            return ("scale_up", max(1, steps))

        if queue_depth <= self._cfg.scale_down_threshold and active_nodes > self._cfg.min_active_nodes:
            # Conservative: only remove up to max_scale_down_step nodes at once
            available = active_nodes - self._cfg.min_active_nodes
            steps = min(self._cfg.max_scale_down_step, available)
            return ("scale_down", max(1, steps))

        return ("noop", 0)
