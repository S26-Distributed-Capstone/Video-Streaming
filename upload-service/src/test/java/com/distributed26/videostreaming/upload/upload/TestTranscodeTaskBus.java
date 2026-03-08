package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.upload.TranscodeTaskBus;
import com.distributed26.videostreaming.shared.upload.TranscodeTaskListener;
import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

class TestTranscodeTaskBus implements TranscodeTaskBus {
    private final List<TranscodeTaskListener> listeners = new CopyOnWriteArrayList<>();

    @Override
    public void publish(TranscodeTaskEvent event) {
        Objects.requireNonNull(event, "event is null");
        for (TranscodeTaskListener listener : listeners) {
            listener.onEvent(event);
        }
    }

    @Override
    public void subscribe(TranscodeTaskListener listener) {
        Objects.requireNonNull(listener, "listener is null");
        listeners.add(listener);
    }

    @Override
    public void close() {
    }
}
