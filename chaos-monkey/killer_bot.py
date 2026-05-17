#!/usr/bin/env python3
"""
Chaos Killer Bot — Kill Kubernetes pods randomly to test durability.

This service runs as a Kubernetes Deployment and uses in-cluster authentication.

Configuration via environment variables (set by Kubernetes):
  CHAOS_ENABLED:           Enable/disable chaos (default: false)
  CHAOS_INTERVAL_MIN:      Min seconds between kills (default: 30)
  CHAOS_INTERVAL_MAX:      Max seconds between kills (default: 60)
  CHAOS_PROBABILITY:       Kill probability per interval 0.0-1.0 (default: 0.7)
  CHAOS_PRESET:            Service preset: core, infra, all, aggressive
  CHAOS_STATEFUL_SAFE_MODE: Graceful restart for stateful services (default: true)
  CHAOS_NAMESPACE:         Kubernetes namespace (injected from pod metadata)
  CHAOS_LOG_TO_RABBITMQ:   Log kills to RabbitMQ (default: true)
  RABBITMQ_HOST:           RabbitMQ hostname
  RABBITMQ_PORT:           RabbitMQ port
  RABBITMQ_USER:           RabbitMQ user (from secret)
  RABBITMQ_PASS:           RabbitMQ password (from secret)

Presets:
  core:       Only core services (upload-service, processing-service, streaming-service)
  infra:      Only infrastructure (rabbitmq, postgres, minio)
  all:        Everything (core + infra + utilities)
  aggressive: All services, higher probability
"""

import os
import sys
import time
import signal
import random
import logging
from datetime import datetime
from typing import Optional, List

from kubernetes import client, config as k8s_config, watch
from kubernetes.client.rest import ApiException

# Try to import RabbitMQ client
try:
    import pika
    PIKA_AVAILABLE = True
except ImportError:
    PIKA_AVAILABLE = False

# ────────────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────────────

class ChaosConfig:
    """Chaos bot configuration."""

    # Service groups
    CORE_SERVICES = ["vs-upload", "vs-processing-small", "vs-processing-medium", "vs-processing-large", "vs-streaming"]
    INFRA_SERVICES = ["vs-rabbitmq", "vs-postgres", "vs-minio"]
    UTILITY_SERVICES = ["vs-node-watcher"]
    STATEFUL_SERVICES = ["vs-rabbitmq", "vs-postgres", "vs-minio"]

    # Presets
    PRESETS = {
        "core": CORE_SERVICES,
        "infra": INFRA_SERVICES,
        "all": CORE_SERVICES + INFRA_SERVICES + UTILITY_SERVICES,
        "aggressive": CORE_SERVICES + INFRA_SERVICES,
    }

    def __init__(self):
        self.enabled = os.getenv("CHAOS_ENABLED", "false").lower() == "true"
        self.interval_min = int(os.getenv("CHAOS_INTERVAL_MIN", "30"))
        self.interval_max = int(os.getenv("CHAOS_INTERVAL_MAX", "60"))
        self.probability = float(os.getenv("CHAOS_PROBABILITY", "0.7"))
        self.stateful_safe_mode = os.getenv("CHAOS_STATEFUL_SAFE_MODE", "true").lower() == "true"
        self.namespace = os.getenv("CHAOS_NAMESPACE", "default")
        self.log_to_rabbitmq = os.getenv("CHAOS_LOG_TO_RABBITMQ", "true").lower() == "true"
        
        # Load target services from preset or explicit config
        preset = os.getenv("CHAOS_PRESET", "core").lower()
        if preset in self.PRESETS:
            self.target_services = self.PRESETS[preset]
            logger.info(f"Using preset: {preset}")
        else:
            services_str = os.getenv("CHAOS_TARGET_SERVICES", ",".join(self.CORE_SERVICES))
            self.target_services = [s.strip() for s in services_str.split(",") if s.strip()]
        
        # RabbitMQ config
        self.rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        self.rabbitmq_port = int(os.getenv("RABBITMQ_PORT", "5672"))
        self.rabbitmq_user = os.getenv("RABBITMQ_USER", "guest")
        self.rabbitmq_pass = os.getenv("RABBITMQ_PASS", "guest")

    def __str__(self):
        return f"""
Chaos Configuration:
  Enabled:              {self.enabled}
  Interval:             {self.interval_min}s - {self.interval_max}s
  Kill Probability:     {self.probability * 100:.0f}%
  Target Services:      {", ".join(self.target_services)}
  Stateful Safe Mode:   {self.stateful_safe_mode}
  Namespace:            {self.namespace}
  Log to RabbitMQ:      {self.log_to_rabbitmq and PIKA_AVAILABLE}
"""
    
    def is_stateful(self, service_name: str) -> bool:
        """Check if service is stateful."""
        return service_name in self.STATEFUL_SERVICES


# ────────────────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────
# RabbitMQ Dev Log Publisher
# ────────────────────────────────────────────────────────────────────────

class DevLogPublisher:
    """Publish kill events to RabbitMQ dev log exchange."""

    def __init__(self, config: ChaosConfig):
        self.config = config
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self):
        """Connect to RabbitMQ."""
        try:
            credentials = pika.PlainCredentials(
                self.config.rabbitmq_user,
                self.config.rabbitmq_pass
            )
            parameters = pika.ConnectionParameters(
                host=self.config.rabbitmq_host,
                port=self.config.rabbitmq_port,
                credentials=credentials,
                connection_attempts=3,
                retry_delay=2
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            logger.info(f"Connected to RabbitMQ: {self.config.rabbitmq_host}:{self.config.rabbitmq_port}")
        except Exception as e:
            logger.warning(f"Failed to connect to RabbitMQ: {e}")
            self.connection = None
            self.channel = None

    def publish(self, service_name: str, pod_name: str, action: str = "killed"):
        """Publish a kill event."""
        if not self.connection or not self.channel or self.channel.is_closed:
            return

        try:
            message = f"[CHAOS] {action.capitalize()} {service_name} pod: {pod_name}"
            self.channel.basic_publish(
                exchange="dev-logs",
                routing_key="chaos-monkey",
                body=message,
                properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
            )
            logger.info(f"Published to RabbitMQ: {message}")
        except Exception as e:
            logger.warning(f"Failed to publish to RabbitMQ: {e}")

    def close(self):
        """Close connection."""
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                logger.warning(f"Error closing RabbitMQ connection: {e}")


# ────────────────────────────────────────────────────────────────────────
# Kubernetes Pod Management
# ────────────────────────────────────────────────────────────────────────

class KubernetesPodManager:
    """Manage Kubernetes pods via kubectl."""

    def __init__(self, namespace: str = "default"):
        self.namespace = namespace
        self._load_config()
        self.v1 = client.CoreV1Api()

    @staticmethod
    def _load_config():
        """Load Kubernetes config from within cluster or kubeconfig."""
        try:
            k8s_config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes config")
        except k8s_config.ConfigException:
            try:
                k8s_config.load_kube_config()
                logger.info("Loaded kubeconfig for Kubernetes access")
            except Exception as e:
                logger.error(f"Failed to load Kubernetes config: {e}")
                raise

    def delete_pod(self, pod_name: str, grace_period_seconds: int = 5) -> bool:
        """Delete a pod with grace period."""
        try:
            self.v1.delete_namespaced_pod(
                name=pod_name,
                namespace=self.namespace,
                grace_period_seconds=grace_period_seconds
            )
            logger.info(f"Deleted pod: {pod_name} (grace: {grace_period_seconds}s)")
            return True
        except ApiException as e:
            logger.warning(f"Failed to delete pod {pod_name}: {e}")
            return False

    def restart_pod(self, pod_name: str) -> bool:
        """Gracefully restart a pod (safe restart)."""
        try:
            # Delete with 30 second grace period for graceful shutdown
            self.v1.delete_namespaced_pod(
                name=pod_name,
                namespace=self.namespace,
                grace_period_seconds=30
            )
            logger.info(f"Gracefully restarted pod: {pod_name} (allows 30s shutdown)")
            return True
        except ApiException as e:
            logger.warning(f"Failed to restart pod {pod_name}: {e}")
            return False

    def get_pods_for_service(self, service_name: str) -> List[str]:
        """Get list of pod names for a service."""
        try:
            label_selector = f"app={service_name}"
            pods = self.v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=label_selector
            )
            pod_names = [pod.metadata.name for pod in pods.items]
            return pod_names
        except ApiException as e:
            logger.warning(f"Failed to list pods for {service_name}: {e}")
            return []


# ────────────────────────────────────────────────────────────────────────
# Chaos Killer Bot
# ────────────────────────────────────────────────────────────────────────

class ChaosKillerBot:
    """Main chaos killer bot."""

    def __init__(self, config: ChaosConfig):
        self.config = config
        self.pod_manager = KubernetesPodManager(config.namespace)
        self.dev_logger: Optional[DevLogPublisher] = None
        self.shutdown_event = False

        if config.log_to_rabbitmq and PIKA_AVAILABLE:
            self.dev_logger = DevLogPublisher(config)
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals from Kubernetes."""
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.shutdown_event = True

    def get_random_pod_to_kill(self) -> Optional[tuple]:
        """
        Select a random pod from target services.
        Returns: (service_name, pod_name) or None if no pods found.
        """
        all_pods = {}

        # Collect all pods from all target services
        for service in self.config.target_services:
            service = service.strip()
            pods = self.pod_manager.get_pods_for_service(service)
            if pods:
                all_pods[service] = pods

        if not all_pods:
            logger.info("No pods found for any target service")
            return None

        # Randomly select a service then a pod
        service = random.choice(list(all_pods.keys()))
        pod = random.choice(all_pods[service])
        return (service, pod)

    def run(self):
        """Run the chaos killer bot main loop."""
        logger.info("Chaos Killer Bot starting...")
        logger.info(str(self.config))

        if not self.config.enabled:
            logger.info("Chaos is disabled. Set CHAOS_ENABLED=true to activate.")
            return

        try:
            while not self.shutdown_event:
                # Random interval between min and max
                interval = random.uniform(self.config.interval_min, self.config.interval_max)
                logger.info(f"Sleeping for {interval:.1f}s before next chaos decision...")
                time.sleep(interval)

                if self.shutdown_event:
                    break

                # Decide whether to kill this round
                if random.random() > self.config.probability:
                    logger.info("Chaos probability check: spared this round")
                    continue

                # Pick a pod to kill
                target = self.get_random_pod_to_kill()
                if not target:
                    logger.warning("No pods available to kill")
                    continue

                service_name, pod_name = target
                
                # Determine kill strategy based on service type
                is_stateful = self.config.is_stateful(service_name)
                if is_stateful and self.config.stateful_safe_mode:
                    logger.warning(f"CHAOS: Gracefully restarting stateful {service_name} pod: {pod_name}")
                    self.pod_manager.restart_pod(pod_name)
                    action = "gracefully restarted"
                else:
                    logger.warning(f"CHAOS: Killing {service_name} pod: {pod_name}")
                    self.pod_manager.delete_pod(pod_name)
                    action = "killed"

                # Log to RabbitMQ
                if self.dev_logger:
                    self.dev_logger.publish(service_name, pod_name, action)

        except KeyboardInterrupt:
            logger.info("Chaos Killer Bot interrupted by user...")
        except Exception as e:
            logger.error(f"Unexpected error in chaos bot: {e}", exc_info=True)
        finally:
            logger.info("Chaos Killer Bot shutting down...")
            if self.dev_logger:
                self.dev_logger.close()


# ────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    config = ChaosConfig()
    bot = ChaosKillerBot(config)
    bot.run()
