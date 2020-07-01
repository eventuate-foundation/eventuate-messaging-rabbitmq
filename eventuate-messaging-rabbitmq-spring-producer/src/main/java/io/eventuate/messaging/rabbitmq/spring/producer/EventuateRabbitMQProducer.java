package io.eventuate.messaging.rabbitmq.spring.producer;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

public class EventuateRabbitMQProducer {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private Connection connection;
  private Channel channel;

  public EventuateRabbitMQProducer(Address[] brokerAddresses) {

    ConnectionFactory factory = new ConnectionFactory();
    try {
      logger.info("Creating channel/connection");
      connection = factory.newConnection(brokerAddresses);
      channel = connection.createChannel();
      logger.info("Created channel/connection");
    } catch (IOException | TimeoutException e) {
      logger.error("Creating channel/connection failed", e);
      throw new RuntimeException(e);
    }
  }

  public CompletableFuture<?> send(String topic, String key, String body) {
    try {
      AMQP.BasicProperties bp = new AMQP.BasicProperties.Builder().headers(Collections.singletonMap("key", key)).build();

      channel.exchangeDeclare(topic, "fanout");
      channel.basicPublish(topic, key, bp, body.getBytes("UTF-8"));

    } catch (IOException e) {
      logger.error("sending message failed", e);
      throw new RuntimeException(e);
    }

    return CompletableFuture.completedFuture(null);
  }

  public void close() {
    try {
      logger.info("Closing channel/connection");
      channel.close();
      connection.close();
      logger.info("Closed channel/connection");
    } catch (IOException | TimeoutException e) {
      logger.error("Closing channel/connection failed", e);
    }
  }
}
