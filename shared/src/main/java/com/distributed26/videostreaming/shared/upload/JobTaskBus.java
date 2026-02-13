package com.distributed26.videostreaming.shared.upload;

public interface JobTaskBus {
    void publish(JobTaskEvent event);

    void subscribe(String jobId, JobTaskListener listener);

    void unsubscribe(String jobId, JobTaskListener listener);
}
