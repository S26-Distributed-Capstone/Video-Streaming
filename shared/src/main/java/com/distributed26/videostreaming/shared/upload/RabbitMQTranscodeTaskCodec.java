package com.distributed26.videostreaming.shared.upload;

import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;
import com.fasterxml.jackson.databind.JsonNode;

final class RabbitMQTranscodeTaskCodec {
    private RabbitMQTranscodeTaskCodec() {
    }

    static TranscodeTaskEvent toEvent(JsonNode node) {
        String jobId = node.path("jobId").asText();
        String chunkKey = node.path("chunkKey").asText("");
        String profile = node.path("profile").asText("");
        int segmentNumber = node.path("segmentNumber").asInt(-1);
        return new TranscodeTaskEvent(jobId, chunkKey, profile, segmentNumber);
    }
}
