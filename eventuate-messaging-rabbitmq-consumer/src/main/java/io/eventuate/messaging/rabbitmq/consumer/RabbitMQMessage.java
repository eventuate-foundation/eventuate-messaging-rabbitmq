package io.eventuate.messaging.rabbitmq.consumer;

public class RabbitMQMessage {
  private String payload;

  public RabbitMQMessage(String payload) {
    this.payload = payload;
  }

  public String getPayload() {
    return payload;
  }
}
