package com.distributed26.videostreaming.shared.upload;

import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;

/**
 * Publishes and consumes distributed transcode work items. Each message
 * represents one ({@code chunk x profile}) task.
 */
public interface TranscodeTaskBus extends AutoCloseable {
    void publish(TranscodeTaskEvent event);

    void subscribe(TranscodeTaskListener listener);

    @Override
    default void close() throws Exception {}
}
