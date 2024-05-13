package io.eventuate.messaging.rabbitmq.spring.consumer;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.eventuate.messaging.partitionmanagement.CommonMessageConsumer;
import io.eventuate.messaging.partitionmanagement.CoordinatorFactory;
import io.eventuate.messaging.partitionmanagement.SubscriptionLeaderHook;
import io.eventuate.messaging.partitionmanagement.SubscriptionLifecycleHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class MessageConsumerRabbitMQImpl implements CommonMessageConsumer {

  private Logger logger = LoggerFactory.getLogger(getClass());

  public final String id = UUID.randomUUID().toString();

  public final String consumerId;
  private Supplier<String> subscriptionIdSupplier;

  private CoordinatorFactory coordinatorFactory;
  private Connection connection;
  private int partitionCount;
  private Address[] brokerAddresses;

  private ConcurrentLinkedQueue<Subscription> subscriptions = new ConcurrentLinkedQueue<>();

  public MessageConsumerRabbitMQImpl(CoordinatorFactory coordinatorFactory,ConnectionFactory connectionFactory,
                                     Address[] brokerAddresses,
                                     int partitionCount) {
    this(() -> UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            coordinatorFactory,
            connectionFactory,
            brokerAddresses,
            partitionCount);
  }

  public MessageConsumerRabbitMQImpl(Supplier<String> subscriptionIdSupplier,
                                     String consumerId,
                                     CoordinatorFactory coordinatorFactory,
                                     ConnectionFactory connectionFactory,
                                     Address[] brokerAddresses,
                                     int partitionCount) {

    this.subscriptionIdSupplier = subscriptionIdSupplier;
    this.consumerId = consumerId;
    this.coordinatorFactory = coordinatorFactory;
    this.partitionCount = partitionCount;
    this.brokerAddresses = brokerAddresses;

    prepareRabbitMQConnection(connectionFactory);

    logger.info("consumer {} created and ready to subscribe", id);
  }

  public void setSubscriptionLifecycleHook(SubscriptionLifecycleHook subscriptionLifecycleHook) {
    subscriptions.forEach(subscription -> subscription.setSubscriptionLifecycleHook(subscriptionLifecycleHook));
  }

  public void setLeaderHook(SubscriptionLeaderHook leaderHook) {
    subscriptions.forEach(subscription -> subscription.setLeaderHook(leaderHook));
  }

  private void prepareRabbitMQConnection(ConnectionFactory factory) {
    try {
      logger.info("Creating connection");
      connection = factory.newConnection(brokerAddresses);
      logger.info("Created connection");
    } catch (IOException | TimeoutException e) {
      logger.error("Creating connection failed", e);
      throw new RuntimeException(e);
    }
  }

  public Subscription subscribe(String subscriberId, Set<String> channels, RabbitMQMessageHandler handler) {
    logger.info("consumer {} with subscriberId {} is subscribing to channels {}", id, subscriberId, channels);

    Subscription subscription = new Subscription(coordinatorFactory,
            consumerId,
            subscriptionIdSupplier.get(),
            connection,
            subscriberId,
            channels,
            partitionCount,
            (message, acknowledgeCallback) -> handleMessage(subscriberId, handler, message, acknowledgeCallback));


    subscriptions.add(subscription);

    subscription.setClosingCallback(() -> subscriptions.remove(subscription));

    logger.info("consumer {} with subscriberId {} subscribed to channels {}", id, subscriberId, channels);
    return subscription;
  }

  private void handleMessage(String subscriberId, RabbitMQMessageHandler handler, RabbitMQMessage message, Runnable acknowledgeCallback) {
    try {
      handler.accept(message);
      logger.info("consumer {} with subscriberId {} handled message {}", id, subscriberId, message);
    } catch (Throwable t) {
      logger.info("consumer {} with subscriberId {} got exception when tried to handle message {}", id, subscriberId, message);
      logger.info("Got exception ", t);
      throw new RuntimeException(t);
    }

    acknowledgeCallback.run();
  }

  @Override
  public void close() {
    logger.info("consumer {} is closing", id);

    subscriptions.forEach(Subscription::close);

    try {
      logger.info("Closing connection");
      connection.close();
      logger.info("Closed connection");
    } catch (IOException e) {
      logger.error("Closing connection failed", e);
    }

    logger.info("consumer {} is closed", id);
  }

  public String getId() {
    return consumerId;
  }
}