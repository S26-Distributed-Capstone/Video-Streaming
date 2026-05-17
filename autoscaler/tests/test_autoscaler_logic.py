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
import threading

# Make autoscaler/ importable without installing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import autoscaler as autoscaler_module
import autoscaling_state as autoscaling_state_module
from decision_engine import DecisionEngine
import scaling_executor as scaling_executor_module
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
        action, steps = self.engine.decide(0, 2)
        assert action == "noop"
        assert steps == 0

    def test_noop_when_middle_range(self):
        # queue between thresholds
        action, steps = self.engine.decide(10, 4)
        assert action == "noop"
        assert steps == 0

    def test_scale_up_at_threshold(self):
        # queue == threshold → ceil(20/20) = 1 node
        action, steps = self.engine.decide(20, 4)
        assert action == "scale_up"
        assert steps == 1

    def test_scale_up_above_threshold(self):
        # queue 2× threshold → ceil(50/20) = 3, capped at max_scale_up_step=3
        action, steps = self.engine.decide(50, 4)
        assert action == "scale_up"
        assert steps == 3

    def test_scale_up_proportional_two_steps(self):
        # queue exactly 2× threshold → ceil(40/20) = 2 nodes
        action, steps = self.engine.decide(40, 4)
        assert action == "scale_up"
        assert steps == 2

    def test_scale_up_capped_at_max_step(self):
        # queue 5× threshold → ceil(100/20)=5, but capped at max_scale_up_step=3
        action, steps = self.engine.decide(100, 4)
        assert action == "scale_up"
        assert steps == 3

    def test_scale_up_capped_by_available_nodes(self):
        # Only 1 cordoned node left (total=12, active=11)
        action, steps = self.engine.decide(100, 11)
        assert action == "scale_up"
        assert steps == 1

    def test_no_scale_up_when_all_nodes_active(self):
        # all 12 nodes already active
        action, steps = self.engine.decide(100, 12)
        assert action == "noop"
        assert steps == 0

    def test_scale_down_at_threshold(self):
        action, steps = self.engine.decide(5, 4)
        assert action == "scale_down"
        assert steps == 1

    def test_scale_down_below_threshold(self):
        action, steps = self.engine.decide(0, 6)
        assert action == "scale_down"
        assert steps == 1

    def test_no_scale_down_at_minimum_nodes(self):
        # already at MIN_ACTIVE_NODES=2, don't go lower
        action, steps = self.engine.decide(0, 2)
        assert action == "noop"
        assert steps == 0

    def test_scale_up_takes_priority_over_down(self):
        # queue at 20 and active_nodes < total → scale up even if > min
        action, steps = self.engine.decide(20, 6)
        assert action == "scale_up"
        assert steps == 1

    def test_boundary_just_below_scale_up(self):
        action, steps = self.engine.decide(19, 4)
        assert action == "noop"
        assert steps == 0

    def test_boundary_just_above_scale_down(self):
        action, steps = self.engine.decide(6, 4)
        assert action == "noop"
        assert steps == 0

    def test_custom_max_scale_up_step(self):
        # With max_scale_up_step=5 and huge queue, should allow up to 5 nodes
        cfg = make_cfg(max_scale_up_step=5)
        engine = DecisionEngine(cfg)
        action, steps = engine.decide(200, 2)  # ceil(200/20)=10, capped at 5, available=10
        assert action == "scale_up"
        assert steps == 5

    def test_custom_max_scale_down_step(self):
        cfg = make_cfg(max_scale_down_step=2)
        engine = DecisionEngine(cfg)
        action, steps = engine.decide(0, 6)
        assert action == "scale_down"
        assert steps == 2


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
        assert cfg.max_scale_up_step == 3
        assert cfg.max_scale_down_step == 1

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


class _FakeLeaseMetadata:
    def __init__(self, annotations=None):
        self.annotations = annotations or {}


class _FakeLease:
    def __init__(self, annotations=None):
        self.metadata = _FakeLeaseMetadata(annotations)


class _FakeCoordinationApi:
    def __init__(self):
        self.lease = None
        self.patch_calls = []

    def read_namespaced_lease(self, name, namespace):
        if self.lease is None:
            raise autoscaling_state_module.ApiException(status=404)
        return self.lease

    def create_namespaced_lease(self, namespace, body):
        if self.lease is not None:
            raise autoscaling_state_module.ApiException(status=409)
        self.lease = _FakeLease(dict(body.metadata.annotations or {}))

    def patch_namespaced_lease(self, name, namespace, body):
        self.patch_calls.append((name, namespace, body))
        if self.lease is None:
            raise autoscaling_state_module.ApiException(status=404)
        self.lease.metadata.annotations.update(body["metadata"]["annotations"])


class TestAutoscalingStateStore:
    def test_defaults_to_off_and_persists_on_state_in_shared_lease(self, monkeypatch):
        fake_coord = _FakeCoordinationApi()
        monkeypatch.setattr(autoscaling_state_module.client, "CoordinationV1Api", lambda: fake_coord)

        store = autoscaling_state_module.AutoscalingStateStore("default", "video-autoscaler")

        assert store.get_autoscaling_on() is False

        store.set_autoscaling_on(True, 4)

        assert store.get_autoscaling_on() is True
        assert store.get_restore_active_nodes() == 4
        assert fake_coord.patch_calls[-1][0] == "video-autoscaler-state"


class _FakeAppsApi:
    def __init__(self, replicas: int):
        self.replicas = replicas
        self.patch_calls = []

    def read_namespaced_stateful_set(self, name, namespace):
        return type("StatefulSet", (), {
            "spec": type("Spec", (), {"replicas": self.replicas})()
        })()

    def patch_namespaced_stateful_set(self, name, namespace, body):
        self.patch_calls.append((name, namespace, body))
        if "replicas" in body.get("spec", {}):
            self.replicas = body["spec"]["replicas"]


class _FakeTopology:
    def __init__(self, target_replicas: int):
        self.target_replicas = target_replicas

    def total_replicas_for_active_nodes(self):
        return self.target_replicas


class _FakeTopologyForActiveCount:
    def __init__(self, cfg):
        self._cfg = cfg
        self.states = {
            "node-a": {"cordoned": True, "cpu_count": 8},
            "node-b": {"cordoned": True, "cpu_count": 4},
            "node-c": {"cordoned": False, "cpu_count": 2},
            "node-d": {"cordoned": False, "cpu_count": 2},
        }
        self.cordoned = []
        self.uncordoned = []
        self.evicted = []

    def get_node_states(self):
        return {name: dict(state) for name, state in self.states.items()}

    def replicas_for_node(self, cpu_count):
        return max(self._cfg.replicas_per_node, int(cpu_count * self._cfg.replicas_per_cpu))

    def cordon_node(self, name):
        self.cordoned.append(name)
        self.states[name]["cordoned"] = True

    def uncordon_node(self, name):
        self.uncordoned.append(name)
        self.states[name]["cordoned"] = False

    def evict_pods_on_node(self, node_name, namespace, app_label_value):
        self.evicted.append((node_name, namespace, app_label_value))


class TestScalingExecutor:
    def test_reconcile_replicas_to_active_topology_patches_when_out_of_sync(self, monkeypatch):
        fake_apps = _FakeAppsApi(replicas=8)
        monkeypatch.setattr(scaling_executor_module.client, "AppsV1Api", lambda: fake_apps)

        cfg = make_cfg()
        executor = scaling_executor_module.ScalingExecutor(cfg)

        changed = executor.reconcile_replicas_to_active_topology(_FakeTopology(target_replicas=12))

        assert changed is True
        assert fake_apps.replicas == 12
        assert fake_apps.patch_calls == [
            (cfg.statefulset_name, cfg.kube_namespace, {"spec": {"replicas": 12}})
        ]

    def test_reconcile_replicas_to_active_topology_noops_when_aligned(self, monkeypatch):
        fake_apps = _FakeAppsApi(replicas=12)
        monkeypatch.setattr(scaling_executor_module.client, "AppsV1Api", lambda: fake_apps)

        cfg = make_cfg()
        executor = scaling_executor_module.ScalingExecutor(cfg)

        changed = executor.reconcile_replicas_to_active_topology(_FakeTopology(target_replicas=12))

        assert changed is False
        assert fake_apps.replicas == 12
        assert fake_apps.patch_calls == []

    def test_enforce_active_node_count_maxes_out_two_processing_nodes(self, monkeypatch):
        fake_apps = _FakeAppsApi(replicas=2)
        monkeypatch.setattr(scaling_executor_module.client, "AppsV1Api", lambda: fake_apps)

        cfg = make_cfg(replicas_per_node=1, replicas_per_cpu=1.0)
        executor = scaling_executor_module.ScalingExecutor(cfg)
        topology = _FakeTopologyForActiveCount(cfg)

        executor.enforce_active_node_count(topology, 2)

        assert topology.uncordoned == ["node-a", "node-b"]
        assert topology.cordoned == ["node-c", "node-d"]
        assert fake_apps.replicas == 12
        assert fake_apps.patch_calls == [
            (cfg.statefulset_name, cfg.kube_namespace, {"spec": {"replicas": 12}})
        ]

    def test_enforce_processing_baseline_without_cordoning_pins_pods_only(self, monkeypatch):
        fake_apps = _FakeAppsApi(replicas=2)
        monkeypatch.setattr(scaling_executor_module.client, "AppsV1Api", lambda: fake_apps)

        cfg = make_cfg(replicas_per_node=1, replicas_per_cpu=1.0)
        executor = scaling_executor_module.ScalingExecutor(cfg)
        topology = _FakeTopologyForActiveCount(cfg)

        executor.enforce_processing_baseline_without_cordoning(topology, 2)

        assert topology.uncordoned == []
        assert topology.cordoned == []
        assert topology.evicted == []
        assert fake_apps.replicas == 12
        affinity_patch = fake_apps.patch_calls[0][2]
        values = affinity_patch["spec"]["template"]["spec"]["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"]
        assert values == ["node-a", "node-b"]
        assert fake_apps.patch_calls[-1] == (
            cfg.statefulset_name,
            cfg.kube_namespace,
            {"spec": {"replicas": 12}},
        )


class _FakeMetrics:
    class ProcessingBacklog:
        def __init__(self, open_upload_tasks=0, active_claims=0):
            self.open_upload_tasks = open_upload_tasks
            self.active_claims = active_claims

        def has_active_work(self):
            return self.open_upload_tasks > 0 or self.active_claims > 0

    def __init__(self, queue_depth=0, open_upload_tasks=0, active_claims=0):
        self.queue_depth = queue_depth
        self.processing_backlog = self.ProcessingBacklog(open_upload_tasks, active_claims)

    def get_queue_depth(self):
        return self.queue_depth

    def get_processing_backlog(self):
        return self.processing_backlog


class _FakeTopologyForPoll:
    def get_node_states(self):
        return {
            "node-1": {"cordoned": False, "cpu_count": 4},
            "node-2": {"cordoned": False, "cpu_count": 4},
        }


class _FakeExecutorForPoll:
    def __init__(self):
        self.bootstrap_calls = 0
        self.active_count_calls = []
        self.baseline_calls = []
        self.reconcile_calls = 0
        self.clear_constraint_calls = 0
        self.scale_up_calls = []
        self.processing_pool_nodes = {"node-1"}

    def enforce_initial_state(self, topology):
        self.bootstrap_calls += 1

    def enforce_active_node_count(self, topology, target_active):
        self.active_count_calls.append(target_active)

    def enforce_processing_baseline_without_cordoning(self, topology, target_active):
        self.baseline_calls.append(target_active)

    def reconcile_replicas_to_active_topology(self, topology):
        self.reconcile_calls += 1

    def clear_processing_node_constraint(self):
        self.clear_constraint_calls += 1

    def scale_up_nodes(self, topology, steps):
        self.scale_up_calls.append(steps)

    def get_processing_pool_nodes(self, topology):
        return set(self.processing_pool_nodes)


class _FakePublisher:
    def __init__(self):
        self.published = []

    def publish_node_status(self, node_states, processing_pool_nodes, queue_depth):
        self.published.append((node_states, processing_pool_nodes, queue_depth))
        autoscaler_module._shutdown.set()


class _FakeElection:
    is_leader = True


class _FakeAutoscalingState:
    def __init__(self, autoscaling_on=False, restore_active_nodes=None):
        self.autoscaling_on = autoscaling_on
        self.restore_active_nodes = restore_active_nodes

    def get_autoscaling_on(self):
        return self.autoscaling_on

    def get_restore_active_nodes(self):
        return self.restore_active_nodes


class TestAutoscalerOffMode:
    def test_apply_startup_pause_sets_pause_flag(self, monkeypatch):
        monkeypatch.setattr(autoscaler_module, "_paused", threading.Event())

        autoscaler_module._apply_startup_pause()

        assert autoscaler_module._paused.is_set()

    def test_run_poll_loop_autoscaling_off_enforces_two_node_baseline_without_reconcile(self, monkeypatch):
        monkeypatch.setattr(autoscaler_module, "_paused", threading.Event())
        monkeypatch.setattr(autoscaler_module, "_shutdown", threading.Event())
        autoscaler_module._paused.set()

        cfg = make_cfg()
        executor = _FakeExecutorForPoll()
        publisher = _FakePublisher()
        store = StateStore()

        autoscaler_module.run_poll_loop(
            cfg,
            _FakeTopologyForPoll(),
            executor,
            _FakeMetrics(),
            DecisionEngine(cfg),
            store,
            publisher,
            _FakeElection(),
            _FakeAutoscalingState(False),
        )

        assert executor.bootstrap_calls == 0
        assert executor.active_count_calls == []
        assert executor.baseline_calls == [cfg.min_active_nodes]
        assert executor.reconcile_calls == 0
        assert store.has_bootstrapped() is True
        assert len(publisher.published) == 1

    def test_run_poll_loop_autoscaling_on_runs_normal_bootstrap(self, monkeypatch):
        monkeypatch.setattr(autoscaler_module, "_paused", threading.Event())
        monkeypatch.setattr(autoscaler_module, "_shutdown", threading.Event())

        cfg = make_cfg()
        executor = _FakeExecutorForPoll()
        publisher = _FakePublisher()
        store = StateStore()
        autoscaler_module._paused.clear()

        autoscaler_module.run_poll_loop(
            cfg,
            _FakeTopologyForPoll(),
            executor,
            _FakeMetrics(),
            DecisionEngine(cfg),
            store,
            publisher,
            _FakeElection(),
            _FakeAutoscalingState(True),
        )

        assert executor.bootstrap_calls == 1
        assert executor.active_count_calls == []
        assert executor.baseline_calls == []
        assert executor.reconcile_calls == 1

    def test_resume_restore_count_uses_shared_state_before_local_replica_state(self):
        cfg = make_cfg(total_nodes=12, min_active_nodes=2)
        store = StateStore()
        store.record_autoscaling_off_active_nodes(3)

        restore_count = autoscaler_module._resolve_resume_active_nodes(
            cfg,
            store,
            _FakeAutoscalingState(False, restore_active_nodes=6),
        )

        assert restore_count == 6

    def test_resume_restore_count_falls_back_to_minimum_and_clamps_to_cluster_size(self):
        cfg = make_cfg(total_nodes=12, min_active_nodes=2)
        store = StateStore()

        assert autoscaler_module._resolve_resume_active_nodes(
            cfg,
            store,
            _FakeAutoscalingState(False, restore_active_nodes=None),
        ) == 2
        assert autoscaler_module._resolve_resume_active_nodes(
            cfg,
            store,
            _FakeAutoscalingState(False, restore_active_nodes=99),
        ) == 12

    def test_shared_autoscaling_on_enables_leader_even_when_local_flag_is_off(self, monkeypatch):
        monkeypatch.setattr(autoscaler_module, "_paused", threading.Event())
        monkeypatch.setattr(autoscaler_module, "_shutdown", threading.Event())
        autoscaler_module._paused.set()

        cfg = make_cfg()
        executor = _FakeExecutorForPoll()
        publisher = _FakePublisher()
        store = StateStore()

        autoscaler_module.run_poll_loop(
            cfg,
            _FakeTopologyForPoll(),
            executor,
            _FakeMetrics(queue_depth=100),
            DecisionEngine(cfg),
            store,
            publisher,
            _FakeElection(),
            _FakeAutoscalingState(True, restore_active_nodes=2),
        )

        assert executor.clear_constraint_calls == 1
        assert executor.active_count_calls == [2]
        assert executor.reconcile_calls == 1
        assert executor.scale_up_calls == [3]
        assert not autoscaler_module._paused.is_set()

    def test_scale_down_is_deferred_while_processing_backlog_is_active(self, monkeypatch):
        monkeypatch.setattr(autoscaler_module, "_paused", threading.Event())
        monkeypatch.setattr(autoscaler_module, "_shutdown", threading.Event())
        autoscaler_module._paused.clear()

        cfg = make_cfg(min_active_nodes=1)
        executor = _FakeExecutorForPoll()
        publisher = _FakePublisher()
        store = StateStore()

        autoscaler_module.run_poll_loop(
            cfg,
            _FakeTopologyForPoll(),
            executor,
            _FakeMetrics(queue_depth=0, open_upload_tasks=3, active_claims=1),
            DecisionEngine(cfg),
            store,
            publisher,
            _FakeElection(),
            _FakeAutoscalingState(True),
        )

        assert executor.scale_up_calls == []

