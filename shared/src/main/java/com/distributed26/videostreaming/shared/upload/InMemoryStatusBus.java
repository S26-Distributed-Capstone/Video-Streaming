package com.distributed26.videostreaming.shared.upload;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class InMemoryStatusBus implements StatusBus {
    private final Map<String, List<UploadStatusListener>> listenersByUploadId = new ConcurrentHashMap<>();

    @Override
    public void publish(UploadStatusEvent event) {
        Objects.requireNonNull(event, "event is null");
        List<UploadStatusListener> listeners = listenersByUploadId.get(event.getUploadId());
        if (listeners == null) {
            return;
        }
        for (UploadStatusListener listener : listeners) {
            listener.onStatus(event);
        }
    }

    @Override
    public void subscribe(String uploadId, UploadStatusListener listener) {
        Objects.requireNonNull(uploadId, "uploadId is null");
        Objects.requireNonNull(listener, "listener is null");
        listenersByUploadId
                .computeIfAbsent(uploadId, key -> new CopyOnWriteArrayList<>())
                .add(listener);
    }

    @Override
    public void unsubscribe(String uploadId, UploadStatusListener listener) {
        Objects.requireNonNull(uploadId, "uploadId is null");
        Objects.requireNonNull(listener, "listener is null");
        List<UploadStatusListener> listeners = listenersByUploadId.get(uploadId);
        if (listeners == null) {
            return;
        }
        listeners.remove(listener);
        if (listeners.isEmpty()) {
            listenersByUploadId.remove(uploadId, listeners);
        }
    }
}
