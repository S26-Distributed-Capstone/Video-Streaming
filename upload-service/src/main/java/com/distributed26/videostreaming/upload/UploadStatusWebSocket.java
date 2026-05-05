package com.distributed26.videostreaming.upload;

import com.distributed26.videostreaming.shared.upload.StatusBus;
import com.distributed26.videostreaming.shared.upload.UploadStatusListener;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final StatusBus statusBus;
    private final Map<WsContext, UploadStatusListener> listenersByContext = new ConcurrentHashMap<>();
    private final Map<WsContext, String> uploadIdByContext = new ConcurrentHashMap<>();

    public UploadStatusWebSocket(StatusBus statusBus) {
        this.statusBus = Objects.requireNonNull(statusBus, "statusBus is null");
    }

    public void configure(WsConfig ws) {
        ws.onConnect(this::bindIfUploadIdProvided);
        ws.onMessage(this::handleMessage);
        ws.onClose(this::cleanup);
        ws.onError(this::cleanup);
    }

    private void bindIfUploadIdProvided(WsConnectContext ctx) {
        String uploadId = ctx.queryParam("uploadId");
        if (uploadId != null && !uploadId.isBlank()) {
            bind(ctx, uploadId.trim());
        }
    }

    private void handleMessage(WsMessageContext ctx) {
        if (uploadIdByContext.containsKey(ctx)) {
            return;
        }
        String uploadId = ctx.message();
        if (uploadId == null || uploadId.isBlank()) {
            ctx.send("missing uploadId");
            return;
        }
        bind(ctx, uploadId.trim());
    }

    private void bind(WsContext ctx, String uploadId) {
        UploadStatusListener listener = event -> {
            try {
                ctx.send(objectMapper.writeValueAsString(event));
            } catch (JsonProcessingException e) {
                ctx.send("{\"error\":\"serialization_failed\"}");
            }
        };
        uploadIdByContext.put(ctx, uploadId);
        listenersByContext.put(ctx, listener);
        statusBus.subscribe(uploadId, listener);
    }

    private void cleanup(WsCloseContext ctx) {
        cleanupContext(ctx);
    }

    private void cleanup(WsErrorContext ctx) {
        cleanupContext(ctx);
    }

    private void cleanupContext(WsContext ctx) {
        String uploadId = uploadIdByContext.remove(ctx);
        UploadStatusListener listener = listenersByContext.remove(ctx);
        if (uploadId != null && listener != null) {
            statusBus.unsubscribe(uploadId, listener);
        }
    }
}
