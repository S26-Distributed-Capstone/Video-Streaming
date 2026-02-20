package com.distributed26.videostreaming.shared.upload;

import com.distributed26.videostreaming.shared.upload.events.JobTaskEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadFailedEvent;
import com.distributed26.videostreaming.shared.upload.events.UploadMetaEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import io.github.cdimascio.dotenv.Dotenv;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RabbitMQJobTaskBus implements JobTaskBus, AutoCloseable {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LogManager.getLogger(RabbitMQJobTaskBus.class);

    private final Connection connection;
    private final Channel channel;
    private final String exchange;
    private final String statusQueue;
    private final String statusBinding;
    private final boolean consumeStatus;
    private final Map<String, List<JobTaskListener>> listenersByJobId = new ConcurrentHashMap<>();

    public static RabbitMQJobTaskBus fromEnv() {
        Dotenv dotenv = Dotenv.configure().directory("./").ignoreIfMissing().load();
        String host = getEnvOrDotenv(dotenv, "RABBITMQ_HOST", "localhost");
        int port = Integer.parseInt(getEnvOrDotenv(dotenv, "RABBITMQ_PORT", "5672"));
        String user = getEnvOrDotenv(dotenv, "RABBITMQ_USER", "guest");
        String pass = getEnvOrDotenv(dotenv, "RABBITMQ_PASS", "guest");
        String vhost = getEnvOrDotenv(dotenv, "RABBITMQ_VHOST", "/");
        String exchange = getEnvOrDotenv(dotenv, "RABBITMQ_EXCHANGE", "upload.events");
        String statusQueue = getEnvOrDotenv(dotenv, "RABBITMQ_STATUS_QUEUE", "upload.status.queue");
        String statusBinding = getEnvOrDotenv(dotenv, "RABBITMQ_STATUS_BINDING", "upload.status.*");
        String serviceMode = getEnvOrDotenv(dotenv, "SERVICE_MODE", "");
        boolean consumeStatus = !"upload".equalsIgnoreCase(serviceMode);
        return new RabbitMQJobTaskBus(host, port, user, pass, vhost, exchange, statusQueue, statusBinding, consumeStatus);
    }

    public RabbitMQJobTaskBus(
            String host,
            int port,
            String username,
            String password,
            String vhost,
            String exchange,
            String statusQueue,
            String statusBinding,
            boolean consumeStatus
    ) {
        this.exchange = Objects.requireNonNull(exchange, "exchange is null");
        this.statusQueue = Objects.requireNonNull(statusQueue, "statusQueue is null");
        this.statusBinding = Objects.requireNonNull(statusBinding, "statusBinding is null");
        this.consumeStatus = consumeStatus;

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setVirtualHost(vhost);
            this.connection = factory.newConnection("upload-service-jobtaskbus");
            this.channel = connection.createChannel();

            channel.exchangeDeclare(this.exchange, BuiltinExchangeType.TOPIC, true);

            channel.queueDeclare(this.statusQueue, true, false, false, null);
            channel.queueBind(this.statusQueue, this.exchange, this.statusBinding);

            if (consumeStatus) {
                startStatusConsumer();
            } else {
                logger.info("Status consumer disabled for {}", this.exchange);
            }
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException("Failed to initialize RabbitMQJobTaskBus", e);
        }
    }

    @Override
    public void publish(JobTaskEvent event) {
        Objects.requireNonNull(event, "event is null");
        try {
            byte[] body = objectMapper.writeValueAsBytes(event);
            String jobId = event.getJobId();
            String statusKey = "upload.status." + jobId;

            channel.basicPublish(exchange, statusKey, null, body);
        } catch (IOException e) {
            throw new RuntimeException("Failed to publish job task event", e);
        }
    }

    @Override
    public void subscribe(String jobId, JobTaskListener listener) {
        Objects.requireNonNull(jobId, "jobId is null");
        Objects.requireNonNull(listener, "listener is null");
        listenersByJobId
            .computeIfAbsent(jobId, key -> new CopyOnWriteArrayList<>())
            .add(listener);
    }

    @Override
    public void unsubscribe(String jobId, JobTaskListener listener) {
        Objects.requireNonNull(jobId, "jobId is null");
        Objects.requireNonNull(listener, "listener is null");
        List<JobTaskListener> listeners = listenersByJobId.get(jobId);
        if (listeners == null) {
            return;
        }
        listeners.remove(listener);
        if (listeners.isEmpty()) {
            listenersByJobId.remove(jobId, listeners);
        }
    }

    private void startStatusConsumer() throws IOException {
        DeliverCallback callback = (consumerTag, delivery) -> {
            String json = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
                JsonNode node = objectMapper.readTree(json);
                String jobId = node.path("jobId").asText(null);
                if (jobId == null || jobId.isBlank()) {
                    return;
                }
                String type = node.path("type").asText("");
                String taskId = node.path("taskId").asText("");
                int totalSegments = node.path("totalSegments").asInt(-1);
                int completedSegments = node.path("completedSegments").asInt(-1);
                if (totalSegments >= 0) {
                    logger.info("Status event jobId={} type={} totalSegments={}", jobId, type, totalSegments);
                } else if (completedSegments >= 0) {
                    logger.info("Status event jobId={} type={} completedSegments={}", jobId, type, completedSegments);
                } else if (!taskId.isBlank()) {
                    logger.debug("Status event jobId={} type={} taskId={}", jobId, type, taskId);
                } else {
                    logger.debug("Status event jobId={} type={}", jobId, type);
                }
                JobTaskEvent event = toEvent(node);
                List<JobTaskListener> listeners = listenersByJobId.get(jobId);
                if (listeners == null) {
                    return;
                }
                for (JobTaskListener listener : listeners) {
                    listener.onTask(event);
                }
            } catch (Exception ignored) {
                // Swallow malformed messages for now.
            }
        };
        channel.basicConsume(statusQueue, true, callback, consumerTag -> {});
    }

    private JobTaskEvent toEvent(JsonNode node) {
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
        String taskId = node.path("taskId").asText("task");
        return new JobTaskEvent(jobId, taskId);
    }

    private static String getEnvOrDotenv(Dotenv dotenv, String key, String defaultValue) {
        String envVal = System.getenv(key);
        if (envVal != null && !envVal.isBlank()) {
            return envVal;
        }
        String dotenvVal = dotenv.get(key);
        if (dotenvVal == null || dotenvVal.isBlank()) {
            return defaultValue;
        }
        return dotenvVal;
    }

    @Override
    public void close() throws Exception {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }
}
