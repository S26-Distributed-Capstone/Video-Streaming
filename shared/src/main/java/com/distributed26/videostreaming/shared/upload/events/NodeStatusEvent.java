package com.distributed26.videostreaming.shared.upload.events;

import java.util.List;
import java.util.Objects;

/**
 * Cluster-wide node status event published by the autoscaler.
 * jobId is always "__cluster__" — the status-service broadcasts this to all WebSocket clients.
 */
public class NodeStatusEvent extends JobEvent {
    private final String type = "node_status";
    private final List<NodeInfo> nodes;
    private final int queueDepth;
    private final int activeCount;
    private final int totalCount;

    public NodeStatusEvent(List<NodeInfo> nodes, int queueDepth, int activeCount, int totalCount) {
        super("__cluster__", "node_status");
        this.nodes = Objects.requireNonNull(nodes, "nodes is null");
        this.queueDepth = queueDepth;
        this.activeCount = activeCount;
        this.totalCount = totalCount;
    }

    public String getType() { return type; }
    public List<NodeInfo> getNodes() { return nodes; }
    public int getQueueDepth() { return queueDepth; }
    public int getActiveCount() { return activeCount; }
    public int getTotalCount() { return totalCount; }

    /** Represents a single k8s worker node and its scheduling state. */
    public static class NodeInfo {
        private final String name;
        private final String state; // "active" or "inactive"
        private final boolean ready;

        public NodeInfo(String name, String state, boolean ready) {
            this.name = Objects.requireNonNull(name, "name is null");
            this.state = Objects.requireNonNull(state, "state is null");
            this.ready = ready;
        }

        public String getName() { return name; }
        public String getState() { return state; }
        public boolean isReady() { return ready; }
    }
}
