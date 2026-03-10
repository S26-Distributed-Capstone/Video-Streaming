package com.distributed26.videostreaming.upload.processing;

import io.javalin.http.Context;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class UploadRequestParser {
    private static final Logger logger = LogManager.getLogger(UploadRequestParser.class);

    private final int maxVideoNameLength;

    public UploadRequestParser(int maxVideoNameLength) {
        this.maxVideoNameLength = maxVideoNameLength;
    }

    public UploadRequest parse(Context ctx) {
        String videoName = resolveVideoName(ctx);
        if (videoName == null) {
            return null;
        }
        String videoId = resolveVideoId(ctx);
        return new UploadRequest(videoId, videoName, buildUploadStatusUrl(ctx, videoId));
    }

    private String buildUploadStatusUrl(Context ctx, String videoId) {
        String scheme = ctx.scheme();
        String wsScheme = "https".equalsIgnoreCase(scheme) ? "wss" : "ws";
        String statusHost = resolveStatusHost(ctx);
        return wsScheme + "://" + statusHost + "/upload-status?jobId=" + videoId;
    }

    private String resolveStatusHost(Context ctx) {
        String statusHost = System.getenv("STATUS_HOST");
        if (statusHost != null && !statusHost.isBlank()) {
            return statusHost.trim();
        }
        String statusPort = System.getenv("STATUS_PORT");
        if (statusPort != null && !statusPort.isBlank()) {
            return ctx.host().replaceAll(":\\d+$", ":" + statusPort.trim());
        }
        return ctx.host();
    }

    private String resolveVideoId(Context ctx) {
        String requestedVideoId = ctx.formParam("videoId");
        if (requestedVideoId == null || requestedVideoId.isBlank()) {
            requestedVideoId = ctx.queryParam("videoId");
        }
        if (requestedVideoId != null && !requestedVideoId.isBlank()) {
            try {
                String videoId = UUID.fromString(requestedVideoId.trim()).toString();
                logger.info("Using provided video ID: {}", videoId);
                return videoId;
            } catch (IllegalArgumentException e) {
                String videoId = UUID.randomUUID().toString();
                logger.warn("Invalid provided videoId '{}'; generated new video ID: {}", requestedVideoId, videoId);
                return videoId;
            }
        }
        String videoId = UUID.randomUUID().toString();
        logger.info("Assigned video ID: {}", videoId);
        return videoId;
    }

    private String resolveVideoName(Context ctx) {
        String name = ctx.formParam("name");
        if (name == null || name.isBlank()) {
            name = ctx.formParam("videoName");
        }
        if (name == null || name.isBlank()) {
            name = ctx.queryParam("name");
        }
        if (name == null) {
            return null;
        }
        String trimmed = name.trim();
        if (trimmed.isEmpty() || trimmed.length() > maxVideoNameLength) {
            return null;
        }
        return trimmed;
    }
}
