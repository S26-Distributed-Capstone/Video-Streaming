package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.upload.StatusEventBus;
import com.distributed26.videostreaming.shared.upload.JobEventListener;
import com.distributed26.videostreaming.shared.upload.events.JobEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeProgressEvent;
import com.distributed26.videostreaming.shared.upload.events.TranscodeSegmentState;
import com.distributed26.videostreaming.shared.upload.events.UploadFailedEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadMetaEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadProgressEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadStorageStatusEvent;
import com.distributed26.videostreaming.shared.upload.events.NodeStatusEvent;
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
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UploadStatusWebSocket {
    private static final Logger logger = LogManager.getLogger(UploadStatusWebSocket.class);
    private static final String instanceId = resolveInstanceId();
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    // Cache the last NodeStatusEvent so newly connected browsers see the current
    // cluster state immediately rather than waiting for the next autoscaler poll.
    private static volatile String lastNodeStatusJson = null;
    private static volatile String lastQueueDepthJson = null;
    private static volatile java.util.List<?> lastNodeStatusNodes = java.util.List.of();
    private static volatile int lastActiveCount = 0;
    private static volatile int lastTotalCount = 0;
    private static volatile int lastQueueDepth = 0;

    private final StatusEventBus statusEventBus;
    private final SegmentUploadRepository segmentUploadRepository;
    private final TranscodedSegmentStatusRepository transcodedSegmentStatusRepository;
    private final Map<WsContext, JobEventListener> jobListenersByContext = new ConcurrentHashMap<>();
    private final Map<WsContext, String> jobIdByContext = new ConcurrentHashMap<>();
    private final Map<WsContext, JobEventListener> clusterListenersByContext = new ConcurrentHashMap<>();
    private final QueueDepthPoller queueDepthPoller;

    public UploadStatusWebSocket(
            StatusEventBus statusEventBus,
            SegmentUploadRepository segmentUploadRepository,
            TranscodedSegmentStatusRepository transcodedSegmentStatusRepository
    ) {
        this.statusEventBus = Objects.requireNonNull(statusEventBus, "statusEventBus is null");
        this.segmentUploadRepository = segmentUploadRepository;
        this.transcodedSegmentStatusRepository = transcodedSegmentStatusRepository;
        this.queueDepthPoller = QueueDepthPoller.fromEnv(this::broadcastQueueDepth);
        this.queueDepthPoller.start();
    }

    public void configure(WsConfig ws) {
        ws.onConnect(this::bindIfJobIdProvided);
        ws.onMessage(this::handleMessage);
        ws.onClose(this::cleanup);
        ws.onError(this::cleanup);
    }

    private void bindIfJobIdProvided(WsConnectContext ctx) {
        // Always subscribe all connections to cluster-wide node status events
        subscribeToCluster(ctx);
        String jobId = ctx.queryParam("jobId");
        if (jobId != null && !jobId.isBlank()) {
            bindJob(ctx, jobId.trim());
        }
    }

    private void subscribeToCluster(WsContext ctx) {
        // Replay the last known state immediately — no waiting for next poll
        String cached = lastNodeStatusJson;
        if (cached != null) {
            try {
                ctx.send(cached);
            } catch (Exception e) {
                logger.warn("Failed to send cached node status on connect", e);
            }
        }
        String cachedQueueDepth = lastQueueDepthJson;
        if (cachedQueueDepth != null) {
            try {
                ctx.send(cachedQueueDepth);
            } catch (Exception e) {
                logger.warn("Failed to send cached queue depth on connect", e);
            }
        }

        JobEventListener clusterListener = event -> {
            try {
                String json = objectMapper.writeValueAsString(event);
                if (event instanceof NodeStatusEvent) {
                    lastNodeStatusJson = json;  // keep cache fresh
                    lastNodeStatusNodes = List.copyOf(((NodeStatusEvent) event).getNodes());
                    lastActiveCount = ((NodeStatusEvent) event).getActiveCount();
                    lastTotalCount = ((NodeStatusEvent) event).getTotalCount();
                    lastQueueDepth = ((NodeStatusEvent) event).getQueueDepth();
                }
                logger.debug("WS cluster send type={}", describeEventType(event));
                ctx.send(json);
            } catch (JsonProcessingException e) {
                logger.warn("Failed to serialize cluster event", e);
            }
        };
        clusterListenersByContext.put(ctx, clusterListener);
        statusEventBus.subscribe("__cluster__", clusterListener);
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
        JobEventListener clusterListener = clusterListenersByContext.remove(ctx);
        if (clusterListener != null) {
            statusEventBus.unsubscribe("__cluster__", clusterListener);
        }
    }

    public void close() {
        queueDepthPoller.close();
    }

    public Map<String, Object> currentClusterSnapshot() {
        return Map.of(
                "jobId", "__cluster__",
                "taskId", "node_status",
                "type", "node_status",
                "nodes", lastNodeStatusNodes,
                "queueDepth", lastQueueDepth,
                "activeCount", lastActiveCount,
                "totalCount", lastTotalCount
        );
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
        if (event instanceof NodeStatusEvent) {
            return "node_status";
        }
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

    private static String resolveInstanceId() {
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isBlank()) {
            return hostname;
        }
        return "status-service-unknown";
    }

    private void broadcastQueueDepth(int queueDepth) {
        try {
            String json = objectMapper.writeValueAsString(Map.of(
                    "jobId", "__cluster__",
                    "taskId", "queue_depth",
                    "type", "queue_depth",
                    "queueDepth", queueDepth
            ));
            lastQueueDepth = queueDepth;
            lastQueueDepthJson = json;
            for (WsContext ctx : clusterListenersByContext.keySet()) {
                try {
                    ctx.send(json);
                } catch (Exception e) {
                    logger.debug("Failed to send queue depth update to websocket client", e);
                }
            }
        } catch (JsonProcessingException e) {
            logger.warn("Failed to serialize queue depth update", e);
        }
    }

    private static final class QueueDepthPoller implements AutoCloseable {
        private static final long POLL_INTERVAL_MILLIS = 1000L;

        private final Logger logger = LogManager.getLogger(QueueDepthPoller.class);
        private final ScheduledExecutorService executor;
        private final String queueUrl;
        private final String basicAuthHeader;
        private final java.util.function.IntConsumer consumer;
        private volatile Integer lastPublishedDepth = null;

        private QueueDepthPoller(String queueUrl, String basicAuthHeader, java.util.function.IntConsumer consumer) {
            this.queueUrl = queueUrl;
            this.basicAuthHeader = basicAuthHeader;
            this.consumer = consumer;
            this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "queue-depth-poller");
                    t.setDaemon(true);
                    return t;
                }
            });
        }

        static QueueDepthPoller fromEnv(java.util.function.IntConsumer consumer) {
            String rabbitmqHost = readEnv("RABBITMQ_HOST", "localhost");
            String rabbitmqUser = readEnv("RABBITMQ_USER", "guest");
            String rabbitmqPass = readEnv("RABBITMQ_PASS", "guest");
            String rabbitmqVhost = readEnv("RABBITMQ_VHOST", "/");
            String rabbitmqTaskQueue = readEnv("RABBITMQ_TASK_QUEUE", "processing.tasks.queue");
            String managementPort = readEnv("RABBITMQ_MANAGEMENT_PORT", "15672");
            String vhost = urlEncode(rabbitmqVhost);
            String queue = urlEncode(rabbitmqTaskQueue);
            String queueUrl = "http://" + rabbitmqHost + ":" + managementPort + "/api/queues/" + vhost + "/" + queue;
            String auth = Base64.getEncoder().encodeToString(
                    (rabbitmqUser + ":" + rabbitmqPass).getBytes(StandardCharsets.UTF_8)
            );
            return new QueueDepthPoller(queueUrl, "Basic " + auth, consumer);
        }

        void start() {
            executor.scheduleWithFixedDelay(this::pollOnce, 0L, POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        }

        private void pollOnce() {
            try {
                int depth = fetchQueueDepth();
                if (lastPublishedDepth == null || depth != lastPublishedDepth.intValue()) {
                    lastPublishedDepth = depth;
                    consumer.accept(depth);
                }
            } catch (Exception e) {
                logger.debug("Queue depth poll failed", e);
            }
        }

        private int fetchQueueDepth() throws Exception {
            HttpURLConnection connection = (HttpURLConnection) URI.create(queueUrl).toURL().openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Authorization", basicAuthHeader);
            connection.setConnectTimeout((int) Duration.ofSeconds(2).toMillis());
            connection.setReadTimeout((int) Duration.ofSeconds(2).toMillis());
            connection.setRequestProperty("Accept", "application/json");
            try {
                int status = connection.getResponseCode();
                if (status < 200 || status >= 300) {
                    throw new IllegalStateException("RabbitMQ management API returned status " + status);
                }
                try (InputStream input = connection.getInputStream()) {
                    com.fasterxml.jackson.databind.JsonNode data = objectMapper.readTree(input);
                    int ready = data.path("messages_ready").asInt(0);
                    int unacked = data.path("messages_unacknowledged").asInt(0);
                    return ready + unacked;
                }
            } finally {
                connection.disconnect();
            }
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }

        private static String readEnv(String key, String fallback) {
            String value = System.getenv(key);
            return value == null || value.isBlank() ? fallback : value;
        }

        private static String urlEncode(String value) {
            return URLEncoder.encode(value, StandardCharsets.UTF_8);
        }
    }
}
