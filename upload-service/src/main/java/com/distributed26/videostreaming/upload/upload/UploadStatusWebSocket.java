package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.JobEventListener;
import com.distributed26.videostreaming.shared.upload.events.JobEvent;
import com.distributed26.videostreaming.shared.upload.events.SourceChunkRepairEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeProgressEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import com.distributed26.videostreaming.shared.upload.events.UploadFailedEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadMetaEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadProgressEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadStorageStatusEvent;
import com.distributed26.videostreaming.upload.db.SegmentUploadRepository;
import com.distributed26.videostreaming.upload.db.TranscodedSegmentStatusRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.javalin.websocket.WsConfig;
import io.javalin.websocket.WsConnectContext;
import io.javalin.websocket.WsContext;
import io.javalin.websocket.WsErrorContext;
import io.javalin.websocket.WsCloseContext;
import io.javalin.websocket.WsMessageContext;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UploadStatusWebSocket {
    private static final Logger logger = LogManager.getLogger(UploadStatusWebSocket.class);
    private static final String instanceId = resolveInstanceId();
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private final StatusEventBus statusEventBus;
    private final SegmentUploadRepository segmentUploadRepository;
    private final TranscodedSegmentStatusRepository transcodedSegmentStatusRepository;
    private final Map<WsContext, JobEventListener> jobListenersByContext = new ConcurrentHashMap<>();
    private final Map<WsContext, String> jobIdByContext = new ConcurrentHashMap<>();

    public UploadStatusWebSocket(
            StatusEventBus statusEventBus,
            SegmentUploadRepository segmentUploadRepository,
            TranscodedSegmentStatusRepository transcodedSegmentStatusRepository
    ) {
        this.statusEventBus = Objects.requireNonNull(statusEventBus, "statusEventBus is null");
        this.segmentUploadRepository = segmentUploadRepository;
        this.transcodedSegmentStatusRepository = transcodedSegmentStatusRepository;
    }

    public void configure(WsConfig ws) {
        ws.onConnect(this::bindIfJobIdProvided);
        ws.onMessage(this::handleMessage);
        ws.onClose(this::cleanup);
        ws.onError(this::cleanup);
    }

    private void bindIfJobIdProvided(WsConnectContext ctx) {
        String jobId = ctx.queryParam("jobId");
        if (jobId != null && !jobId.isBlank()) {
            bindJob(ctx, jobId.trim());
        }
    }

    private void handleMessage(WsMessageContext ctx) {
        if (jobIdByContext.containsKey(ctx)) {
            return;
        }
        String message = ctx.message();
        if (message == null || message.isBlank()) {
            ctx.send("missing jobId");
            return;
        }
        String trimmed = message.trim();
        if (trimmed.startsWith("job:")) {
            bindJob(ctx, trimmed.substring("job:".length()).trim());
        } else {
            bindJob(ctx, trimmed);
        }
    }

    private void bindJob(WsContext ctx, String jobId) {
        if (jobId == null || jobId.isBlank()) {
            ctx.send("{\"error\":\"missing_jobId\"}");
            ctx.closeSession();
            return;
        }
        logger.info("Status-service instance={} handling jobId={}", instanceId, jobId);
        JobEventListener listener = event -> {
            try {
                logger.debug("WS send event jobId={} type={}", event.getJobId(), describeEventType(event));
                ctx.send(objectMapper.writeValueAsString(event));
            } catch (JsonProcessingException e) {
                ctx.send("{\"error\":\"serialization_failed\"}");
            }
        };
        jobIdByContext.put(ctx, jobId);
        jobListenersByContext.put(ctx, listener);
        statusEventBus.subscribe(jobId, listener);

        if (segmentUploadRepository != null) {
            try {
                sendProgressSnapshot(ctx, jobId);
            } catch (Exception e) {
                logger.warn("Failed to fetch progress for jobId={}", jobId, e);
                ctx.send("{\"error\":\"progress_lookup_failed\"}");
            }
        } else {
            logger.warn("SegmentUploadRepository is null; progress snapshot disabled");
        }
        if (transcodedSegmentStatusRepository != null) {
            try {
                sendTranscodeSnapshot(ctx, jobId);
            } catch (Exception e) {
                logger.warn("Failed to fetch transcode progress for jobId={}", jobId, e);
                ctx.send("{\"error\":\"transcode_progress_lookup_failed\"}");
            }
        } else {
            logger.warn("TranscodedSegmentStatusRepository is null; transcode snapshot disabled");
        }
    }

    private void cleanup(WsCloseContext ctx) {
        cleanupContext(ctx);
    }

    private void cleanup(WsErrorContext ctx) {
        cleanupContext(ctx);
    }

    private void cleanupContext(WsContext ctx) {
        String jobId = jobIdByContext.remove(ctx);
        JobEventListener jobListener = jobListenersByContext.remove(ctx);
        if (jobId != null && jobListener != null) {
            logger.info("Status-service instance={} disconnected jobId={}", instanceId, jobId);
            statusEventBus.unsubscribe(jobId, jobListener);
        }
    }


    private void sendProgressSnapshot(WsContext ctx, String jobId) throws JsonProcessingException {
        int completedSegments = segmentUploadRepository.countByVideoId(jobId);
        logger.info("WS progress snapshot for jobId={} completedSegments={}", jobId, completedSegments);
        ctx.send(objectMapper.writeValueAsString(new UploadProgressEvent(jobId, completedSegments)));
    }

    private void sendTranscodeSnapshot(WsContext ctx, String jobId) throws JsonProcessingException {
        String[] profiles = {"low", "medium", "high"};
        for (String profile : profiles) {
            int done = transcodedSegmentStatusRepository.countByState(jobId, profile, TranscodeSegmentState.DONE.name());
            ctx.send(objectMapper.writeValueAsString(
                new TranscodeProgressEvent(jobId, profile, -1, TranscodeSegmentState.DONE, done, 0)
            ));
        }
    }

    private String describeEventType(JobEvent event) {
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
        if (event instanceof SourceChunkRepairEvent) {
            return "source_chunk_repair";
        }
        return "task";
    }

    private static String resolveInstanceId() {
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isBlank()) {
            return hostname;
        }
        return "status-service-unknown";
    }
}
