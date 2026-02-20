package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.upload.db.VideoUploadRecord;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import io.javalin.http.Context;
import java.util.Optional;

public class UploadInfoHandler {
    private final VideoUploadRepository videoUploadRepository;

    public UploadInfoHandler(VideoUploadRepository videoUploadRepository) {
        this.videoUploadRepository = videoUploadRepository;
    }

    public void getInfo(Context ctx) {
        String videoId = ctx.pathParam("videoId");
        if (videoUploadRepository == null) {
            ctx.status(500).result("Upload info store not configured");
            return;
        }

        Optional<VideoUploadRecord> record = videoUploadRepository.findByVideoId(videoId);
        if (record.isEmpty()) {
            ctx.status(404).result("Video not found");
            return;
        }

        VideoUploadRecord r = record.get();
        ctx.json(new UploadInfoResponse(
                r.getVideoId(),
                r.getStatus(),
                r.getTotalSegments(),
                r.getMachineId(),
                r.getContainerId()
        ));
    }

    private record UploadInfoResponse(
            String videoId,
            String status,
            int totalSegments,
            String machineId,
            String containerId
    ) {
    }
}
