package com.distributed26.videostreaming.shared.upload;

import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;
import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface TranscodeTaskListener {
    /**
     * @return {@code true} when the event was handled successfully and can be acknowledged,
     *         {@code false} when the message should be retried.
     */
    CompletionStage<Boolean> onEvent(TranscodeTaskEvent event);
}
