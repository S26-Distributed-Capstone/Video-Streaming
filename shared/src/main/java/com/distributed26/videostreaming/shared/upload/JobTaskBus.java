package com.distributed26.videostreaming.shared.upload;

import com.distributed26.videostreaming.shared.upload.events.JobTaskEvent;

public interface JobTaskBus {
    void publish(JobTaskEvent event);

    void subscribe(String jobId, JobTaskListener listener);

    void unsubscribe(String jobId, JobTaskListener listener);

    /**
     * Registers a listener that receives every incoming event regardless of jobId.
     * Useful for services (e.g. processing) that must handle events for videos
     * they have not yet seen.
     */
    default void subscribeAll(JobTaskListener listener) {}
}
