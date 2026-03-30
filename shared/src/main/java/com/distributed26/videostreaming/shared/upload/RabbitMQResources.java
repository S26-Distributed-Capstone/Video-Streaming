package com.distributed26.videostreaming.shared.upload;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

record RabbitMQResources(Connection connection, Channel channel) {
}
