"""
Unit tests for decision_engine.py, state_store.py, config.py
No external dependencies — runs without Kubernetes or RabbitMQ.

Run from the project root:
  pip install pytest
  pytest autoscaler/tests/ -v
"""
import sys
import os
import time

# Make autoscaler/ importable without installing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from decision_engine import DecisionEngine
from state_store import StateStore
import config as cfg_module


# ── Helpers ──────────────────────────────────────────────────────────────────

def make_cfg(**overrides):
    """Create a Config with test-friendly defaults, overridable by kwargs."""
    env = {
        "TOTAL_NODES": "12",
        "MIN_ACTIVE_NODES": "2",
        "REPLICAS_PER_NODE": "6",
        "SCALE_UP_THRESHOLD": "20",
        "SCALE_DOWN_THRESHOLD": "5",
        "POLL_INTERVAL_SECONDS": "15",
        "SCALE_COOLDOWN_SECONDS": "60",
        "NODE_LABEL_SELECTOR": "workload-role=app",
        "RABBITMQ_HOST": "localhost",
        "RABBITMQ_USER": "guest",
        "RABBITMQ_PASS": "guest",
        "RABBITMQ_EXCHANGE": "upload.events",
    }
    env.update({k.upper(): str(v) for k, v in overrides.items()})
    old = {}
    for k, v in env.items():
        old[k] = os.environ.get(k)
        os.environ[k] = v
    cfg = cfg_module.load()
    for k, v in old.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    return cfg


# ── DecisionEngine tests ─────────────────────────────────────────────────────

class TestDecisionEngine:
    def setup_method(self):
        self.cfg = make_cfg()
        self.engine = DecisionEngine(self.cfg)

    def test_noop_when_idle(self):
        # queue empty, minimum nodes active → do nothing
        assert self.engine.decide(0, 2) == "noop"

    def test_noop_when_middle_range(self):
        # queue between thresholds
        assert self.engine.decide(10, 4) == "noop"

    def test_scale_up_at_threshold(self):
        assert self.engine.decide(20, 4) == "scale_up"

    def test_scale_up_above_threshold(self):
        assert self.engine.decide(50, 4) == "scale_up"

    def test_no_scale_up_when_all_nodes_active(self):
        # all 12 nodes already active
        assert self.engine.decide(100, 12) == "noop"

    def test_scale_down_at_threshold(self):
        assert self.engine.decide(5, 4) == "scale_down"

    def test_scale_down_below_threshold(self):
        assert self.engine.decide(0, 6) == "scale_down"

    def test_no_scale_down_at_minimum_nodes(self):
        # already at MIN_ACTIVE_NODES=2, don't go lower
        assert self.engine.decide(0, 2) == "noop"

    def test_scale_up_takes_priority_over_down(self):
        # queue at 20 and active_nodes < total → scale up even if > min
        result = self.engine.decide(20, 6)
        assert result == "scale_up"

    def test_boundary_just_below_scale_up(self):
        assert self.engine.decide(19, 4) == "noop"

    def test_boundary_just_above_scale_down(self):
        assert self.engine.decide(6, 4) == "noop"


# ── StateStore tests ──────────────────────────────────────────────────────────

class TestStateStore:
    def setup_method(self):
        self.store = StateStore()

    def test_initially_no_cooldown(self):
        assert not self.store.is_cooldown_active(60)

    def test_cooldown_active_immediately_after_scale(self):
        self.store.record_scale_event()
        assert self.store.is_cooldown_active(60)

    def test_cooldown_expires(self):
        self.store.record_scale_event()
        # Manually move last_scale_time back far enough
        self.store._last_scale_time = time.monotonic() - 61
        assert not self.store.is_cooldown_active(60)

    def test_node_states_changed_when_empty(self):
        states = {"node-1": {"cordoned": False}}
        assert self.store.node_states_changed(states)

    def test_node_states_not_changed_after_record(self):
        states = {"node-1": {"cordoned": False}}
        self.store.record_publish(states)
        assert not self.store.node_states_changed(states)

    def test_node_states_changed_after_cordon(self):
        states = {"node-1": {"cordoned": False}}
        self.store.record_publish(states)
        new_states = {"node-1": {"cordoned": True}}
        assert self.store.node_states_changed(new_states)

    def test_bootstrap_flag(self):
        assert not self.store.has_bootstrapped()
        self.store.mark_bootstrapped()
        assert self.store.has_bootstrapped()
        self.store.reset_bootstrap()
        assert not self.store.has_bootstrapped()

    def test_reset_clears_published_states(self):
        states = {"node-1": {"cordoned": False}}
        self.store.record_publish(states)
        self.store.reset_bootstrap()
        # After reset, any state should appear as changed
        assert self.store.node_states_changed(states)


# ── Config validation tests ───────────────────────────────────────────────────

class TestConfig:
    def test_loads_defaults(self):
        cfg = make_cfg()
        assert cfg.total_nodes == 12
        assert cfg.min_active_nodes == 2
        assert cfg.scale_up_threshold == 20
        assert cfg.scale_down_threshold == 5

    def test_custom_values(self):
        cfg = make_cfg(total_nodes=6, min_active_nodes=1, replicas_per_node=3)
        assert cfg.total_nodes == 6
        assert cfg.min_active_nodes == 1
        assert cfg.replicas_per_node == 3

    def test_invalid_min_active_zero_raises(self):
        with pytest.raises(ValueError, match="MIN_ACTIVE_NODES"):
            make_cfg(min_active_nodes=0)

    def test_invalid_min_active_exceeds_total(self):
        with pytest.raises(ValueError, match="MIN_ACTIVE_NODES"):
            make_cfg(total_nodes=5, min_active_nodes=6)

    def test_invalid_threshold_order(self):
        with pytest.raises(ValueError, match="SCALE_DOWN_THRESHOLD"):
            make_cfg(scale_up_threshold=10, scale_down_threshold=15)

    def test_equal_thresholds_raises(self):
        with pytest.raises(ValueError, match="SCALE_DOWN_THRESHOLD"):
            make_cfg(scale_up_threshold=10, scale_down_threshold=10)

