package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.upload.db.VideoUploadRecord;
import com.distributed26.videostreaming.upload.db.VideoUploadRepository;
import com.distributed26.videostreaming.upload.db.SegmentUploadRepository;
import com.distributed26.videostreaming.upload.db.TranscodedSegmentStatusRepository;
import io.javalin.http.Context;
import java.util.Optional;

public class UploadInfoHandler {
    private final VideoUploadRepository videoUploadRepository;
    private final SegmentUploadRepository segmentUploadRepository;
    private final TranscodedSegmentStatusRepository transcodedSegmentStatusRepository;

    public UploadInfoHandler(
            VideoUploadRepository videoUploadRepository,
            SegmentUploadRepository segmentUploadRepository,
            TranscodedSegmentStatusRepository transcodedSegmentStatusRepository
    ) {
        this.videoUploadRepository = videoUploadRepository;
        this.segmentUploadRepository = segmentUploadRepository;
        this.transcodedSegmentStatusRepository = transcodedSegmentStatusRepository;
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
        int uploadedSegments = segmentUploadRepository == null ? 0 : segmentUploadRepository.countByVideoId(videoId);
        int lowDone = transcodedSegmentStatusRepository == null ? 0 : transcodedSegmentStatusRepository.countByState(videoId, "low", "DONE");
        int mediumDone = transcodedSegmentStatusRepository == null ? 0 : transcodedSegmentStatusRepository.countByState(videoId, "medium", "DONE");
        int highDone = transcodedSegmentStatusRepository == null ? 0 : transcodedSegmentStatusRepository.countByState(videoId, "high", "DONE");
        ctx.json(new UploadInfoResponse(
                r.getVideoId(),
                r.getVideoName(),
                r.getStatus(),
                UploadStatusPresenter.isRetryingMinioConnection(r.getStatus()),
                UploadStatusPresenter.statusMessage(r.getStatus()),
                r.getTotalSegments(),
                r.getMachineId(),
                r.getContainerId(),
                uploadedSegments,
                new TranscodeProgressSnapshot(lowDone, mediumDone, highDone)
        ));
    }

    private record TranscodeProgressSnapshot(
            int lowDone,
            int mediumDone,
            int highDone
    ) {
    }

    private record UploadInfoResponse(
            String videoId,
            String videoName,
            String status,
            boolean retryingMinioConnection,
            String statusMessage,
            int totalSegments,
            String machineId,
            String containerId,
            int uploadedSegments,
            TranscodeProgressSnapshot transcode
    ) {
    }
}
