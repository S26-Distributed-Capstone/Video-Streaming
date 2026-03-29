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
import java.util.concurrent.TimeoutException;
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

    private final Connection connection;
    private final Channel channel;
    private final String queueName;
    private final String bindingKey;

    public static RabbitMQDevLogReader fromEnv() {
        return new RabbitMQDevLogReader(RabbitMQBusConfig.fromEnv());
    }

    public RabbitMQDevLogReader(RabbitMQBusConfig config) {
        Objects.requireNonNull(config, "config is null");
        this.queueName = Objects.requireNonNull(config.devLogQueue(), "devLogQueue is null");
        this.bindingKey = Objects.requireNonNull(config.devLogBinding(), "devLogBinding is null");
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(config.host());
            factory.setPort(config.port());
            factory.setUsername(config.username());
            factory.setPassword(config.password());
            factory.setVirtualHost(config.vhost());
            this.connection = factory.newConnection("dev-log-reader");
            this.channel = connection.createChannel();

            channel.exchangeDeclare(config.exchange(), BuiltinExchangeType.TOPIC, true);
            channel.queueDeclare(this.queueName, true, false, false, null);
            channel.queueBind(this.queueName, config.exchange(), this.bindingKey);
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException("Failed to initialize RabbitMQDevLogReader", e);
        }
    }

    public String queueName() {
        return queueName;
    }

    public String bindingKey() {
        return bindingKey;
    }

    public synchronized List<RabbitMQDevLogPublisher.DevLogMessage> peek(int limit) {
        int normalizedLimit = Math.max(1, Math.min(limit, 100));
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
