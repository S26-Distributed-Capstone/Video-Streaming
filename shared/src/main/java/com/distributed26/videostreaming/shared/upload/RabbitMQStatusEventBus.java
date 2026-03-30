package com.distributed26.videostreaming.shared.upload;

import com.distributed26.videostreaming.shared.upload.events.JobEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.distributed26.videostreaming.shared.upload.events.UploadFailedEvent;

/**
 * RabbitMQ-backed bus for status/progress events. This is the queue path used
 * by the UI, status service, and processing manifest coordination.
 */
public class RabbitMQStatusEventBus implements StatusEventBus {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LogManager.getLogger(RabbitMQStatusEventBus.class);

    private final Connection connection;
    private final Channel channel;
    private final String exchange;
    private final String consumerQueueName;
    private final String failureQueueName;
    private final Map<String, List<JobEventListener>> listenersByJobId = new ConcurrentHashMap<>();
    private final List<JobEventListener> globalListeners = new CopyOnWriteArrayList<>();

    public static RabbitMQStatusEventBus fromEnv() {
        return new RabbitMQStatusEventBus(RabbitMQBusConfig.fromEnv(), shouldConsumeStatusEvents());
    }

    public RabbitMQStatusEventBus(RabbitMQBusConfig config, boolean consumeStatus) {
        this.exchange = Objects.requireNonNull(config.exchange(), "exchange is null");
        RabbitMQResources resources = RabbitMQRetrySupport.retry(
                "initialize RabbitMQ status event bus",
                () -> {
                    ConnectionFactory factory = config.createConnectionFactory();
                    Connection connection = factory.newConnection("upload-status-event-bus");
                    try {
                        Channel channel = connection.createChannel();
                        channel.exchangeDeclare(this.exchange, BuiltinExchangeType.TOPIC, true);
                        return new RabbitMQResources(connection, channel);
                    } catch (IOException | RuntimeException e) {
                        try {
                            connection.close();
                        } catch (Exception closeError) {
                            LOGGER.warn("Failed to close RabbitMQ connection after status bus init error", closeError);
                        }
                        throw e;
                    }
                }
        );
        this.connection = resources.connection();
        this.channel = resources.channel();

        try {
            if (consumeStatus) {
                this.consumerQueueName = declareConsumerQueue(config);
                channel.queueBind(this.consumerQueueName, this.exchange, config.statusBinding());
                startConsumer(this.consumerQueueName);
                this.failureQueueName = declareFailureQueue(config);
                if (this.failureQueueName != null) {
                    channel.queueBind(this.failureQueueName, this.exchange, config.failureBinding());
                    startConsumer(this.failureQueueName);
                }
            } else {
                this.consumerQueueName = null;
                this.failureQueueName = null;
                LOGGER.info("Status event consumer disabled for {}", this.exchange);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize RabbitMQStatusEventBus", e);
        }
    }

    @Override
    public void publish(JobEvent event) {
        Objects.requireNonNull(event, "event is null");
        try {
            byte[] body = OBJECT_MAPPER.writeValueAsBytes(event);
            String routingKey = "upload.status." + event.getJobId();
            LOGGER.debug("Publishing status event jobId={} type={}",
                    event.getJobId(), RabbitMQStatusEventCodec.describeEventType(event));
            channel.basicPublish(exchange, routingKey, null, body);
            if (event instanceof UploadFailedEvent) {
                channel.basicPublish(exchange, "upload.failure", null, body);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to publish status event", e);
        }
    }

    @Override
    public void subscribe(String jobId, JobEventListener listener) {
        Objects.requireNonNull(jobId, "jobId is null");
        Objects.requireNonNull(listener, "listener is null");
        listenersByJobId.computeIfAbsent(jobId, key -> new CopyOnWriteArrayList<>()).add(listener);
    }

    @Override
    public void unsubscribe(String jobId, JobEventListener listener) {
        Objects.requireNonNull(jobId, "jobId is null");
        Objects.requireNonNull(listener, "listener is null");
        List<JobEventListener> listeners = listenersByJobId.get(jobId);
        if (listeners == null) {
            return;
        }
        listeners.remove(listener);
        if (listeners.isEmpty()) {
            listenersByJobId.remove(jobId, listeners);
        }
    }

    @Override
    public void subscribeAll(JobEventListener listener) {
        Objects.requireNonNull(listener, "listener is null");
        globalListeners.add(listener);
    }

    private void startConsumer(String queueName) throws IOException {
        DeliverCallback callback = (consumerTag, delivery) -> {
            String json = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
                JsonNode node = OBJECT_MAPPER.readTree(json);
                String jobId = node.path("jobId").asText(null);
                if (jobId == null || jobId.isBlank()) {
                    return;
                }
                JobEvent event = RabbitMQStatusEventCodec.toEvent(node);
                LOGGER.debug("Dispatching status event jobId={} type={}",
                        jobId, RabbitMQStatusEventCodec.describeEventType(event));
                for (JobEventListener global : globalListeners) {
                    global.onEvent(event);
                }
                List<JobEventListener> listeners = listenersByJobId.get(jobId);
                if (listeners == null) {
                    return;
                }
                for (JobEventListener listener : listeners) {
                    listener.onEvent(event);
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to consume status event payload={}", json, e);
            }
        };
        channel.basicConsume(queueName, true, callback, consumerTag -> {});
    }

    private String declareConsumerQueue(RabbitMQBusConfig config) throws IOException {
        if (shouldUseReplicaStatusQueue()) {
            String queueName = channel.queueDeclare("", false, true, true, null).getQueue();
            LOGGER.info("Declared replica-local status queue={} exchange={}", queueName, exchange);
            return queueName;
        }
        channel.queueDeclare(config.statusQueue(), true, false, false,
                Map.of("x-queue-type", "quorum"));
        return config.statusQueue();
    }

    private String declareFailureQueue(RabbitMQBusConfig config) throws IOException {
        if (!shouldUseReplicaFailureQueue()) {
            return null;
        }
        String queueName = channel.queueDeclare("", false, true, true, null).getQueue();
        LOGGER.info("Declared replica-local failure queue={} exchange={}", queueName, exchange);
        return queueName;
    }

    private static boolean shouldConsumeStatusEvents() {
        String mode = System.getenv("SERVICE_MODE");
        return "status".equalsIgnoreCase(mode)
                || "processing".equalsIgnoreCase(mode)
                || "upload".equalsIgnoreCase(mode);
    }

    private static boolean shouldUseReplicaStatusQueue() {
        String mode = System.getenv("SERVICE_MODE");
        return "status".equalsIgnoreCase(mode)
                || "upload".equalsIgnoreCase(mode);
    }

    private static boolean shouldUseReplicaFailureQueue() {
        String mode = System.getenv("SERVICE_MODE");
        return "processing".equalsIgnoreCase(mode);
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
