package com.distributed26.videostreaming.shared.upload;

import java.time.Instant;
import java.util.Objects;

import com.distributed26.videostreaming.shared.jobs.Status;

public class UploadStatusEvent {
    private final String uploadId;
    private final Status status;
    private final Instant timestamp;
    private final Double progress;
    private final Long bytesUploaded;
    private final Long totalBytes;
    private final String message;

    /*
    
        UploadStatusEvent gets emitted at each stage of the upload.
    
    */

    public UploadStatusEvent(String uploadId, Status status, Instant timestamp) {
        this(uploadId, status, timestamp, null, null, null, null);
    }

    public UploadStatusEvent(
            String uploadId,
            Status status,
            Instant timestamp,
            Double progress,
            Long bytesUploaded,
            Long totalBytes,
            String message
    ) {
        this.uploadId = Objects.requireNonNull(uploadId, "uploadId is null");
        this.status = Objects.requireNonNull(status, "status is null");
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp is null");
        this.progress = progress;
        this.bytesUploaded = bytesUploaded;
        this.totalBytes = totalBytes;
        this.message = message;
    }

    public String getUploadId() {
        return uploadId;
    }

    public Status getStatus() {
        return status;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Double getProgress() {
        return progress;
    }

    public Long getBytesUploaded() {
        return bytesUploaded;
    }

    public Long getTotalBytes() {
        return totalBytes;
    }

    public String getMessage() {
        return message;
    }
}
