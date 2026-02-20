package com.distributed26.videostreaming.shared.upload;

import com.distributed26.videostreaming.shared.upload.events.JobTaskEvent;

@FunctionalInterface
public interface JobTaskListener {
    void onTask(JobTaskEvent event);
}
