package com.distributed26.videostreaming.shared.upload;

import com.distributed26.videostreaming.shared.upload.events.JobEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeProgressEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import com.distributed26.videostreaming.shared.upload.events.UploadFailedEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadMetaEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadStorageStatusEvent;
import com.fasterxml.jackson.databind.JsonNode;

final class RabbitMQStatusEventCodec {
    private RabbitMQStatusEventCodec() {
    }

    static JobEvent toEvent(JsonNode node) {
        String jobId = node.path("jobId").asText();
        String type = node.path("type").asText();
        if ("failed".equals(type)) {
            String reason = node.path("reason").asText(null);
            String machineId = node.path("machineId").asText(null);
            String containerId = node.path("containerId").asText(null);
            return new UploadFailedEvent(jobId, reason, machineId, containerId);
        }
        if ("meta".equals(type) && node.has("totalSegments")) {
            return new UploadMetaEvent(jobId, node.path("totalSegments").asInt());
        }
        if ("storage_status".equals(type)) {
            String state = node.path("state").asText("");
            String reason = node.path("reason").asText(null);
            return new UploadStorageStatusEvent(jobId, state, reason);
        }
        if ("transcode_progress".equals(type)) {
            String profile = node.path("profile").asText("");
            int segmentNumber = node.path("segmentNumber").asInt(-1);
            int doneSegments = node.path("doneSegments").asInt(0);
            int totalSegments = node.path("totalSegments").asInt(0);
            String stateRaw = node.path("state").asText("FAILED");
            TranscodeSegmentState state;
            try {
                state = TranscodeSegmentState.valueOf(stateRaw.toUpperCase());
            } catch (IllegalArgumentException e) {
                state = TranscodeSegmentState.FAILED;
            }
            return new TranscodeProgressEvent(jobId, profile, segmentNumber, state, doneSegments, totalSegments);
        }
        String taskId = node.path("taskId").asText("task");
        return new JobEvent(jobId, taskId);
    }

    static String describeEventType(JobEvent event) {
        if (event instanceof UploadFailedEvent failed) {
            return failed.getType();
        }
        if (event instanceof UploadMetaEvent) {
            return "meta";
        }
        if (event instanceof UploadStorageStatusEvent) {
            return "storage_status";
        }
        if (event instanceof TranscodeProgressEvent) {
            return "transcode_progress";
        }
        return "task";
    }
}
