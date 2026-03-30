package com.distributed26.videostreaming.shared.upload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Small debug reader for dev-log messages stored in RabbitMQ.
 *
 * <p>This reader uses basic-get and requeues messages after reading so the
 * endpoint can inspect recent queue contents without permanently draining them.
 */
public final class RabbitMQDevLogReader implements AutoCloseable {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LogManager.getLogger(RabbitMQDevLogReader.class);

    /** Hard default when no env/config override is provided. */
    static final int DEFAULT_MAX_PEEK_LIMIT = 1000;

    private final Connection connection;
    private final Channel channel;
    private final String queueName;
    private final String bindingKey;
    private final int maxPeekLimit;

    public static RabbitMQDevLogReader fromEnv() {
        return new RabbitMQDevLogReader(RabbitMQBusConfig.fromEnv());
    }

    public RabbitMQDevLogReader(RabbitMQBusConfig config) {
        this(config, resolveMaxPeekLimit());
    }

    public RabbitMQDevLogReader(RabbitMQBusConfig config, int maxPeekLimit) {
        Objects.requireNonNull(config, "config is null");
        this.queueName = Objects.requireNonNull(config.devLogQueue(), "devLogQueue is null");
        this.bindingKey = Objects.requireNonNull(config.devLogBinding(), "devLogBinding is null");
        this.maxPeekLimit = Math.max(1, maxPeekLimit);
        RabbitMQResources resources = RabbitMQRetrySupport.retry(
                "initialize RabbitMQ dev log reader",
                () -> {
                    ConnectionFactory factory = config.createConnectionFactory();
                    Connection connection = factory.newConnection("dev-log-reader");
                    try {
                        Channel channel = connection.createChannel();
                        channel.exchangeDeclare(config.exchange(), BuiltinExchangeType.TOPIC, true);
                        channel.queueDeclare(this.queueName, true, false, false, null);
                        channel.queueBind(this.queueName, config.exchange(), this.bindingKey);
                        return new RabbitMQResources(connection, channel);
                    } catch (IOException | RuntimeException e) {
                        try {
                            connection.close();
                        } catch (Exception closeError) {
                            LOGGER.warn("Failed to close RabbitMQ connection after dev log reader init error", closeError);
                        }
                        throw e;
                    }
                }
        );
        this.connection = resources.connection();
        this.channel = resources.channel();
    }

    public int maxPeekLimit() {
        return maxPeekLimit;
    }

    private static int resolveMaxPeekLimit() {
        String value = System.getenv("DEV_LOG_MAX_PEEK");
        if (value == null || value.isBlank()) {
            try {
                io.github.cdimascio.dotenv.Dotenv dotenv =
                        io.github.cdimascio.dotenv.Dotenv.configure().directory("./").ignoreIfMissing().load();
                value = dotenv.get("DEV_LOG_MAX_PEEK");
            } catch (RuntimeException ignored) { }
        }
        if (value != null && !value.isBlank()) {
            try {
                return Math.max(1, Integer.parseInt(value.trim()));
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid DEV_LOG_MAX_PEEK value '{}', using default {}", value, DEFAULT_MAX_PEEK_LIMIT);
            }
        }
        return DEFAULT_MAX_PEEK_LIMIT;
    }

    public String queueName() {
        return queueName;
    }

    public String bindingKey() {
        return bindingKey;
    }

    public synchronized List<RabbitMQDevLogPublisher.DevLogMessage> peek(int limit) {
        int normalizedLimit = Math.max(1, Math.min(limit, maxPeekLimit));
        List<Long> deliveryTags = new ArrayList<>(normalizedLimit);
        List<RabbitMQDevLogPublisher.DevLogMessage> messages = new ArrayList<>(normalizedLimit);
        try {
            for (int i = 0; i < normalizedLimit; i++) {
                GetResponse response = channel.basicGet(queueName, false);
                if (response == null) {
                    break;
                }
                deliveryTags.add(response.getEnvelope().getDeliveryTag());
                messages.add(OBJECT_MAPPER.readValue(
                        response.getBody(),
                        RabbitMQDevLogPublisher.DevLogMessage.class
                ));
            }
            return messages;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read dev logs", e);
        } finally {
            requeue(deliveryTags);
        }
    }

    /**
     * Reads up to {@link #maxPeekLimit()} messages currently in the queue, then requeues them.
     */
    public synchronized List<RabbitMQDevLogPublisher.DevLogMessage> peekAll() {
        int count = messageCount();
        if (count <= 0) {
            return List.of();
        }
        return peek(Math.min(count, maxPeekLimit));
    }

    /**
     * Returns the number of messages currently sitting in the queue without
     * consuming any of them.
     */
    public synchronized int messageCount() {
        try {
            return channel.queueDeclarePassive(queueName).getMessageCount();
        } catch (IOException e) {
            LOGGER.warn("Failed to query queue depth for {}", queueName, e);
            return 0;
        }
    }

    private void requeue(List<Long> deliveryTags) {
        for (int i = deliveryTags.size() - 1; i >= 0; i--) {
            try {
                channel.basicNack(deliveryTags.get(i), false, true);
            } catch (IOException e) {
                LOGGER.warn("Failed to requeue dev log deliveryTag={}", deliveryTags.get(i), e);
            }
        }
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
