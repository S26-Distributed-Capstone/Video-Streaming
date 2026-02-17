package com.distributed26.videostreaming.shared.upload;

@FunctionalInterface
public interface JobTaskListener {
    void onTask(JobTaskEvent event);
}
