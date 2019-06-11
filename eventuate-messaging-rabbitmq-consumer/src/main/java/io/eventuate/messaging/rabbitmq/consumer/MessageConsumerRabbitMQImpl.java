package io.eventuate.messaging.rabbitmq.consumer;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.eventuate.messaging.partitionmanagement.CommonMessageConsumer;
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

  private ConcurrentLinkedQueue<Subscription> subscriptions = new ConcurrentLinkedQueue<>();

  public MessageConsumerRabbitMQImpl(CoordinatorFactory coordinatorFactory,
                                     String rabbitMQUrl,
                                     int partitionCount) {
    this(() -> UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            coordinatorFactory,
            rabbitMQUrl,
            partitionCount);
  }

  public MessageConsumerRabbitMQImpl(Supplier<String> subscriptionIdSupplier,
                                     String consumerId,
                                     CoordinatorFactory coordinatorFactory,
                                     String rabbitMQUrl,
                                     int partitionCount) {

    this.subscriptionIdSupplier = subscriptionIdSupplier;
    this.consumerId = consumerId;
    this.coordinatorFactory = coordinatorFactory;
    this.partitionCount = partitionCount;
    prepareRabbitMQConnection(rabbitMQUrl);

    logger.info("consumer {} created and ready to subscribe", id);
  }

  public void setSubscriptionLifecycleHook(SubscriptionLifecycleHook subscriptionLifecycleHook) {
    subscriptions.forEach(subscription -> subscription.setSubscriptionLifecycleHook(subscriptionLifecycleHook));
  }

  public void setLeaderHook(SubscriptionLeaderHook leaderHook) {
    subscriptions.forEach(subscription -> subscription.setLeaderHook(leaderHook));
  }

  private void prepareRabbitMQConnection(String rabbitMQUrl) {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(rabbitMQUrl);
    try {
      connection = factory.newConnection();
    } catch (IOException | TimeoutException e) {
      logger.error(e.getMessage(), e);
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
      connection.close();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }

    logger.info("consumer {} is closed", id);
  }

  public String getId() {
    return consumerId;
  }
}