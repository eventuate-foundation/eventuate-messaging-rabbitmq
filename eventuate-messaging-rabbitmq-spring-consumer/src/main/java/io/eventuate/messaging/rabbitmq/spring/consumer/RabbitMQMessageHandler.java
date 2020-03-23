package io.eventuate.messaging.rabbitmq.spring.consumer;

import java.util.function.Consumer;

public interface RabbitMQMessageHandler extends Consumer<RabbitMQMessage> {
}
