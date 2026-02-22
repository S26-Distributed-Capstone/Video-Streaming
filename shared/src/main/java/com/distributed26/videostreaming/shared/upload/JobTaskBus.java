package com.distributed26.videostreaming.shared.upload;

import com.distributed26.videostreaming.shared.upload.events.JobTaskEvent;

public interface JobTaskBus {
    void publish(JobTaskEvent event);

    void subscribe(String jobId, JobTaskListener listener);

    void unsubscribe(String jobId, JobTaskListener listener);
}
