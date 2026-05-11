"""
decision_engine.py — Pure scaling logic; no I/O.
"""
from config import Config


class DecisionEngine:
    def __init__(self, cfg: Config):
        self._cfg = cfg

    def decide(self, queue_depth: int, active_nodes: int) -> str:
        """
        Returns 'scale_up', 'scale_down', or 'noop'.
        Cooldown enforcement is handled by the caller.
        """
        if queue_depth >= self._cfg.scale_up_threshold and active_nodes < self._cfg.total_nodes:
            return "scale_up"
        if queue_depth <= self._cfg.scale_down_threshold and active_nodes > self._cfg.min_active_nodes:
            return "scale_down"
        return "noop"

