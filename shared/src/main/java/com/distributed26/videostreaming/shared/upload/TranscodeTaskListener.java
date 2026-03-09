package com.distributed26.videostreaming.shared.upload;

import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;

@FunctionalInterface
public interface TranscodeTaskListener {
    void onEvent(TranscodeTaskEvent event);
}
