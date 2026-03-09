package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.upload.JobEventListener;
import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.events.JobEvent;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

class TestStatusEventBus implements StatusEventBus {
    private final Map<String, List<JobEventListener>> listenersByJobId = new ConcurrentHashMap<>();

    @Override
    public void publish(JobEvent event) {
        Objects.requireNonNull(event, "event is null");
        List<JobEventListener> listeners = listenersByJobId.get(event.getJobId());
        if (listeners == null) {
            return;
        }
        for (JobEventListener listener : listeners) {
            listener.onEvent(event);
        }
    }

    @Override
    public void subscribe(String jobId, JobEventListener listener) {
        Objects.requireNonNull(jobId, "jobId is null");
        Objects.requireNonNull(listener, "listener is null");
        listenersByJobId
            .computeIfAbsent(jobId, key -> new CopyOnWriteArrayList<>())
            .add(listener);
    }

    @Override
    public void unsubscribe(String jobId, JobEventListener listener) {
        Objects.requireNonNull(jobId, "jobId is null");
        Objects.requireNonNull(listener, "listener is null");
        List<JobEventListener> listeners = listenersByJobId.get(jobId);
        if (listeners == null) {
            return;
        }
        listeners.remove(listener);
        if (listeners.isEmpty()) {
            listenersByJobId.remove(jobId, listeners);
        }
    }

    @Override
    public void close() {
    }
}
