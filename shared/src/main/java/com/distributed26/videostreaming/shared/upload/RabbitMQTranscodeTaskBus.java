package com.distributed26.videostreaming.shared.upload;

import com.distributed26.videostreaming.shared.upload.events.TranscodeTaskEvent;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * RabbitMQ-backed bus for distributed transcoding work. This queue path is the
 * shared work scheduler across processing containers.
 */
public class RabbitMQTranscodeTaskBus implements TranscodeTaskBus {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LogManager.getLogger(RabbitMQTranscodeTaskBus.class);

    private final Connection connection;
    private final Channel channel;
    private final String exchange;
    private final String taskBinding;
    private final int taskPrefetch;
    private final List<TranscodeTaskListener> listeners = new CopyOnWriteArrayList<>();

    public static RabbitMQTranscodeTaskBus fromEnv() {
        return new RabbitMQTranscodeTaskBus(RabbitMQBusConfig.fromEnv(), shouldConsumeTasks());
    }

    public RabbitMQTranscodeTaskBus(RabbitMQBusConfig config, boolean consumeTasks) {
        this.exchange = Objects.requireNonNull(config.exchange(), "exchange is null");
        this.taskBinding = Objects.requireNonNull(config.taskBinding(), "taskBinding is null");
        this.taskPrefetch = resolveTaskPrefetch();
        RabbitMQResources resources = RabbitMQRetrySupport.retry(
                "initialize RabbitMQ transcode task bus",
                () -> {
                    ConnectionFactory factory = config.createConnectionFactory();
                    Connection connection = factory.newConnection("upload-transcode-task-bus");
                    try {
                        Channel channel = connection.createChannel();
                        channel.exchangeDeclare(this.exchange, BuiltinExchangeType.TOPIC, true);
                        channel.queueDeclare(config.taskQueue(), true, false, false,
                                Map.of("x-queue-type", "quorum"));
                        channel.queueBind(config.taskQueue(), this.exchange, config.taskBinding());
                        return new RabbitMQResources(connection, channel);
                    } catch (IOException | RuntimeException e) {
                        try {
                            connection.close();
                        } catch (Exception closeError) {
                            LOGGER.warn("Failed to close RabbitMQ connection after task bus init error", closeError);
                        }
                        throw e;
                    }
                }
        );
        this.connection = resources.connection();
        this.channel = resources.channel();

        try {
            if (consumeTasks) {
                channel.basicQos(taskPrefetch);
                startConsumer(config.taskQueue());
            } else {
                LOGGER.info("Transcode task consumer disabled for {}", this.exchange);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize RabbitMQTranscodeTaskBus", e);
        }
    }

    @Override
    public void publish(TranscodeTaskEvent event) {
        Objects.requireNonNull(event, "event is null");
        try {
            byte[] body = OBJECT_MAPPER.writeValueAsBytes(event);
            synchronized (channel) {
                channel.basicPublish(exchange, taskBinding, null, body);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to publish transcode task", e);
        }
    }

    @Override
    public void subscribe(TranscodeTaskListener listener) {
        Objects.requireNonNull(listener, "listener is null");
        listeners.add(listener);
    }

    private void startConsumer(String queueName) throws IOException {
        DeliverCallback callback = (consumerTag, delivery) -> {
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();
            String json = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
                JsonNode node = OBJECT_MAPPER.readTree(json);
                if (!"transcode_task".equals(node.path("type").asText())) {
                    acknowledge(deliveryTag);
                    return;
                }
                TranscodeTaskEvent taskEvent = RabbitMQTranscodeTaskCodec.toEvent(node);
                invokeListeners(taskEvent).whenComplete((handled, error) -> {
                    if (error != null) {
                        LOGGER.warn("Failed to process transcode task payload={}", json, error);
                        rejectAndRequeue(deliveryTag);
                        return;
                    }
                    if (Boolean.TRUE.equals(handled)) {
                        acknowledge(deliveryTag);
                    } else {
                        rejectAndRequeue(deliveryTag);
                    }
                });
            } catch (Exception e) {
                LOGGER.warn("Failed to consume transcode task payload={}", json, e);
                rejectAndRequeue(deliveryTag);
            }
        };
        channel.basicConsume(queueName, false, callback, consumerTag -> {});
    }

    private CompletionStage<Boolean> invokeListeners(TranscodeTaskEvent taskEvent) {
        CompletionStage<Boolean> stage = CompletableFuture.completedFuture(true);
        for (TranscodeTaskListener listener : listeners) {
            stage = stage.thenCompose(handled -> {
                if (!handled) {
                    return CompletableFuture.completedFuture(false);
                }
                try {
                    return listener.onEvent(taskEvent);
                } catch (Exception e) {
                    return CompletableFuture.failedStage(e);
                }
            });
        }
        return stage;
    }

    private void acknowledge(long deliveryTag) {
        synchronized (channel) {
            try {
                channel.basicAck(deliveryTag, false);
            } catch (IOException e) {
                throw new RuntimeException("Failed to ack transcode task deliveryTag=" + deliveryTag, e);
            }
        }
    }

    private void rejectAndRequeue(long deliveryTag) {
        synchronized (channel) {
            try {
                channel.basicNack(deliveryTag, false, true);
            } catch (IOException e) {
                throw new RuntimeException("Failed to nack transcode task deliveryTag=" + deliveryTag, e);
            }
        }
    }

    private static boolean shouldConsumeTasks() {
        String mode = System.getenv("SERVICE_MODE");
        return "processing".equalsIgnoreCase(mode);
    }

    private static int resolveTaskPrefetch() {
        String prefetch = System.getenv("RABBITMQ_TASK_PREFETCH");
        if (prefetch != null && !prefetch.isBlank()) {
            return Math.max(1, Integer.parseInt(prefetch));
        }
        String poolSize = System.getenv("WORKER_POOL_SIZE");
        if (poolSize != null && !poolSize.isBlank()) {
            return Math.max(1, Integer.parseInt(poolSize));
        }
        // Match the processing-service default: 3/4 of available CPUs
        return Math.max(1, (Runtime.getRuntime().availableProcessors() * 3) / 4);
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
