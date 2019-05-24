package io.eventuate.messaging.rabbitmq.consumer;

import java.util.function.Consumer;

public interface RabbitMQMessageHandler extends Consumer<RabbitMQMessage> {
}
