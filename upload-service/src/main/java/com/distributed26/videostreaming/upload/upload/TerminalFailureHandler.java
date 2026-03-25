package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.events.UploadFailedEvent;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository.FailedTransitionResult;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import io.javalin.http.Context;
import java.util.UUID;

public final class TerminalFailureHandler {
    private final VideoUploadRepository videoUploadRepository;
    private final StatusEventBus statusEventBus;
    private final String machineId;
    private final String containerId;

    public TerminalFailureHandler(
            VideoUploadRepository videoUploadRepository,
            StatusEventBus statusEventBus,
            String machineId,
            String containerId
    ) {
        this.videoUploadRepository = videoUploadRepository;
        this.statusEventBus = statusEventBus;
        this.machineId = machineId;
        this.containerId = containerId;
    }

    public void markFailed(Context ctx) {
        String videoId = ctx.pathParam("videoId");
        try {
            UUID.fromString(videoId);
        } catch (IllegalArgumentException e) {
            ctx.status(400).result("Invalid videoId");
            return;
        }

        if (videoUploadRepository == null) {
            ctx.status(500).result("Upload info store not configured");
            return;
        }

        FailedTransitionResult result = videoUploadRepository.markFailedIfProcessing(videoId);
        if (result == FailedTransitionResult.NOT_FOUND) {
            ctx.status(404).result("Video not found");
            return;
        }
        if (result == FailedTransitionResult.NOT_PROCESSING) {
            ctx.status(409).result("Video is not in PROCESSING state");
            return;
        }

        if (statusEventBus != null) {
            statusEventBus.publish(new UploadFailedEvent(
                    videoId,
                    "client_retry_exhausted",
                    machineId,
                    containerId
            ));
        }
        ctx.status(204);
    }
}
