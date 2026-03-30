package com.distributed26.videostreaming.shared.upload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * High-signal RabbitMQ publisher for developer-facing operational logs.
 *
 * <p>Intended for infrequent, readable messages such as service degradation,
 * recovery, and major state transitions. Default channel name: {@code dev.log}.
 */
public final class RabbitMQDevLogPublisher implements AutoCloseable {
    public static final String DEFAULT_QUEUE = "dev.logs.queue";
    public static final String DEFAULT_BINDING = "dev.log";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LogManager.getLogger(RabbitMQDevLogPublisher.class);

    private final Connection connection;
    private final Channel channel;
    private final String exchange;
    private final String routingKey;

    public static RabbitMQDevLogPublisher fromEnv() {
        return new RabbitMQDevLogPublisher(RabbitMQBusConfig.fromEnv());
    }

    public RabbitMQDevLogPublisher(RabbitMQBusConfig config) {
        this.exchange = Objects.requireNonNull(config.exchange(), "exchange is null");
        this.routingKey = Objects.requireNonNull(config.devLogBinding(), "devLogBinding is null");
        RabbitMQResources resources = RabbitMQRetrySupport.retry(
                "initialize RabbitMQ dev log publisher",
                () -> {
                    ConnectionFactory factory = createConnectionFactory(config);
                    Connection connection = factory.newConnection("dev-log-publisher");
                    try {
                        Channel channel = connection.createChannel();
                        channel.exchangeDeclare(this.exchange, BuiltinExchangeType.TOPIC, true);
                        channel.queueDeclare(config.devLogQueue(), true, false, false, null);
                        channel.queueBind(config.devLogQueue(), this.exchange, this.routingKey);
                        return new RabbitMQResources(connection, channel);
                    } catch (IOException | RuntimeException e) {
                        try {
                            connection.close();
                        } catch (Exception closeError) {
                            LOGGER.warn("Failed to close RabbitMQ connection after dev log publisher init error", closeError);
                        }
                        throw e;
                    }
                }
        );
        this.connection = resources.connection();
        this.channel = resources.channel();
    }

    private static ConnectionFactory createConnectionFactory(RabbitMQBusConfig config) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setUsername(config.username());
        factory.setPassword(config.password());
        factory.setVirtualHost(config.vhost());
        return factory;
    }

    public void publishInfo(String serviceName, String message) {
        publish("INFO", serviceName, message);
    }

    public void publishWarn(String serviceName, String message) {
        publish("WARN", serviceName, message);
    }

    public void publishError(String serviceName, String message) {
        publish("ERROR", serviceName, message);
    }

    public void publish(String level, String serviceName, String message) {
        String normalizedLevel = normalizeLevel(level);
        String normalizedService = normalizeServiceName(serviceName);
        String normalizedMessage = normalizeMessage(message);
        DevLogMessage payload = new DevLogMessage(
                "dev_log",
                normalizedLevel,
                normalizedService,
                normalizedMessage,
                formatMessage(normalizedService, normalizedMessage),
                Instant.now().toString()
        );
        try {
            byte[] body = OBJECT_MAPPER.writeValueAsBytes(payload);
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .type("dev_log")
                    .timestamp(java.util.Date.from(Instant.now()))
                    .build();
            synchronized (channel) {
                channel.basicPublish(exchange, routingKey, props, body);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to publish dev log", e);
        }
    }

    public String formatMessage(String serviceName, String message) {
        return "[" + normalizeServiceName(serviceName) + "]: " + normalizeMessage(message);
    }

    private static String normalizeLevel(String level) {
        String normalized = level == null ? "" : level.trim().toUpperCase();
        if (normalized.isEmpty()) {
            return "INFO";
        }
        return normalized;
    }

    private static String normalizeServiceName(String serviceName) {
        String normalized = serviceName == null ? "" : serviceName.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("serviceName is blank");
        }
        return normalized;
    }

    private static String normalizeMessage(String message) {
        String normalized = message == null ? "" : message.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("message is blank");
        }
        return normalized;
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

    public record DevLogMessage(
            String type,
            String level,
            String service,
            String message,
            String formattedMessage,
            String emittedAt
    ) {
    }
}
