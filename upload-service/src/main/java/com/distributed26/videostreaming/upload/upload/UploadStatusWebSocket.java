package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.upload.JobTaskBus;
import com.distributed26.videostreaming.shared.upload.JobTaskListener;
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

public class UploadStatusWebSocket {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private final JobTaskBus jobTaskBus;
    private final Map<WsContext, JobTaskListener> jobListenersByContext = new ConcurrentHashMap<>();
    private final Map<WsContext, String> jobIdByContext = new ConcurrentHashMap<>();

    public UploadStatusWebSocket(JobTaskBus jobTaskBus) {
        this.jobTaskBus = Objects.requireNonNull(jobTaskBus, "jobTaskBus is null");
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
        JobTaskListener listener = event -> {
            try {
                ctx.send(objectMapper.writeValueAsString(event));
            } catch (JsonProcessingException e) {
                ctx.send("{\"error\":\"serialization_failed\"}");
            }
        };
        jobIdByContext.put(ctx, jobId);
        jobListenersByContext.put(ctx, listener);
        jobTaskBus.subscribe(jobId, listener);
    }

    private void cleanup(WsCloseContext ctx) {
        cleanupContext(ctx);
    }

    private void cleanup(WsErrorContext ctx) {
        cleanupContext(ctx);
    }

    private void cleanupContext(WsContext ctx) {
        String jobId = jobIdByContext.remove(ctx);
        JobTaskListener jobListener = jobListenersByContext.remove(ctx);
        if (jobId != null && jobListener != null) {
            jobTaskBus.unsubscribe(jobId, jobListener);
        }
    }
}
