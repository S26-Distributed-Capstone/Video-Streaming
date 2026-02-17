package com.distributed26.videostreaming.shared.upload;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class InMemoryJobTaskBus implements JobTaskBus {
    private final Map<String, List<JobTaskListener>> listenersByJobId = new ConcurrentHashMap<>();

    @Override
    public void publish(JobTaskEvent event) {
        Objects.requireNonNull(event, "event is null");
        List<JobTaskListener> listeners = listenersByJobId.get(event.getJobId());
        if (listeners == null) {
            return;
        }
        for (JobTaskListener listener : listeners) {
            listener.onTask(event);
        }
    }

    @Override
    public void subscribe(String jobId, JobTaskListener listener) {
        Objects.requireNonNull(jobId, "jobId is null");
        Objects.requireNonNull(listener, "listener is null");
        listenersByJobId
                .computeIfAbsent(jobId, key -> new CopyOnWriteArrayList<>())
                .add(listener);
    }

    @Override
    public void unsubscribe(String jobId, JobTaskListener listener) {
        Objects.requireNonNull(jobId, "jobId is null");
        Objects.requireNonNull(listener, "listener is null");
        List<JobTaskListener> listeners = listenersByJobId.get(jobId);
        if (listeners == null) {
            return;
        }
        listeners.remove(listener);
        if (listeners.isEmpty()) {
            listenersByJobId.remove(jobId, listeners);
        }
    }
}
