package com.distributed26.videostreaming.shared.upload;

import com.distributed26.videostreaming.shared.upload.events.JobEvent;

/**
 * Publishes and consumes status/progress events that are fanned out to the UI
 * and other status observers.
 */
public interface StatusEventBus extends AutoCloseable {
    void publish(JobEvent event);

    void subscribe(String jobId, JobEventListener listener);

    void unsubscribe(String jobId, JobEventListener listener);

    /**
     * Registers a listener that receives every incoming status event regardless
     * of jobId. Processing uses this to react to metadata events.
     */
    default void subscribeAll(JobEventListener listener) {}

    @Override
    default void close() throws Exception {}
}
