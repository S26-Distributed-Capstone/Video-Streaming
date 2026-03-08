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
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
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
    private final List<TranscodeTaskListener> listeners = new CopyOnWriteArrayList<>();

    public static RabbitMQTranscodeTaskBus fromEnv() {
        return new RabbitMQTranscodeTaskBus(RabbitMQBusConfig.fromEnv(), shouldConsumeTasks());
    }

    public RabbitMQTranscodeTaskBus(RabbitMQBusConfig config, boolean consumeTasks) {
        this.exchange = Objects.requireNonNull(config.exchange(), "exchange is null");
        this.taskBinding = Objects.requireNonNull(config.taskBinding(), "taskBinding is null");
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(config.host());
            factory.setPort(config.port());
            factory.setUsername(config.username());
            factory.setPassword(config.password());
            factory.setVirtualHost(config.vhost());
            this.connection = factory.newConnection("upload-transcode-task-bus");
            this.channel = connection.createChannel();

            channel.exchangeDeclare(this.exchange, BuiltinExchangeType.TOPIC, true);
            channel.queueDeclare(config.taskQueue(), true, false, false, null);
            channel.queueBind(config.taskQueue(), this.exchange, config.taskBinding());

            if (consumeTasks) {
                startConsumer(config.taskQueue());
            } else {
                LOGGER.info("Transcode task consumer disabled for {}", this.exchange);
            }
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException("Failed to initialize RabbitMQTranscodeTaskBus", e);
        }
    }

    @Override
    public void publish(TranscodeTaskEvent event) {
        Objects.requireNonNull(event, "event is null");
        try {
            byte[] body = OBJECT_MAPPER.writeValueAsBytes(event);
            channel.basicPublish(exchange, taskBinding, null, body);
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
            String json = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
                JsonNode node = OBJECT_MAPPER.readTree(json);
                if (!"transcode_task".equals(node.path("type").asText())) {
                    return;
                }
                TranscodeTaskEvent taskEvent = RabbitMQTranscodeTaskCodec.toEvent(node);
                for (TranscodeTaskListener listener : listeners) {
                    listener.onEvent(taskEvent);
                }
            } catch (Exception ignored) {
                // Swallow malformed messages for now.
            }
        };
        channel.basicConsume(queueName, true, callback, consumerTag -> {});
    }

    private static boolean shouldConsumeTasks() {
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
