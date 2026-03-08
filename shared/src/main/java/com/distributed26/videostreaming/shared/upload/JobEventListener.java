package com.distributed26.videostreaming.shared.upload;

import com.distributed26.videostreaming.shared.upload.events.JobEvent;

@FunctionalInterface
public interface JobEventListener {
    void onEvent(JobEvent event);
}
