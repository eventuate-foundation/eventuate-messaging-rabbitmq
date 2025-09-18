package io.eventuate.messaging.rabbitmq.spring.consumer;

import com.rabbitmq.client.*;
import io.eventuate.coordination.leadership.LeaderSelectedCallback;
import io.eventuate.coordination.leadership.LeadershipController;
import io.eventuate.messaging.partitionmanagement.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class Subscription {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private final String subscriptionId;
  private final String consumerId;

  private CoordinatorFactory coordinatorFactory;
  private Connection connection;
  private String subscriberId;
  private Set<String> channels;
  private int partitionCount;
  private BiConsumer<RabbitMQMessage, Runnable> handleMessageCallback;

  private Channel consumerChannel;
  private Map<String, String> consumerTagByQueue = new HashMap<>();
  private Coordinator coordinator;
  private Map<String, Set<Integer>> currentPartitionsByChannel = new HashMap<>();
  private Optional<SubscriptionLifecycleHook> subscriptionLifecycleHook = Optional.empty();
  private Optional<SubscriptionLeaderHook> leaderHook = Optional.empty();
  private Optional<Runnable> closingCallback = Optional.empty();

  public Subscription(CoordinatorFactory coordinatorFactory,
                      String consumerId,
                      String subscriptionId,
                      Connection connection,
                      String subscriberId,
                      Set<String> channels,
                      int partitionCount,
                      BiConsumer<RabbitMQMessage, Runnable> handleMessageCallback) {

    logger.info("Creating subscription for channels {} and partition count {}. {}",
            channels, partitionCount, identificationInformation());

    this.coordinatorFactory = coordinatorFactory;
    this.consumerId = consumerId;
    this.subscriptionId = subscriptionId;
    this.connection = connection;
    this.subscriberId = subscriberId;
    this.channels = channels;
    this.partitionCount = partitionCount;
    this.handleMessageCallback = handleMessageCallback;

    channels.forEach(channelName -> currentPartitionsByChannel.put(channelName, new HashSet<>()));

    logger.info("Creating channel");

    consumerChannel = createRabbitMQChannel();

    logger.info("Created channel");

    coordinator = createCoordinator(
            subscriptionId,
            subscriberId,
            channels,
            this::leaderSelected,
            this::leaderRemoved,
            this::assignmentUpdated);

    logger.info("Created subscription for channels {} and partition count {}. {}",
            channels, partitionCount, identificationInformation());
  }

  public void setSubscriptionLifecycleHook(SubscriptionLifecycleHook subscriptionLifecycleHook) {
    this.subscriptionLifecycleHook = Optional.ofNullable(subscriptionLifecycleHook);
  }

  public void setLeaderHook(SubscriptionLeaderHook leaderHook) {
    this.leaderHook = Optional.ofNullable(leaderHook);
  }

  public void setClosingCallback(Runnable closingCallback) {
    this.closingCallback = Optional.of(closingCallback);
  }

  protected Coordinator createCoordinator(String groupMemberId,
                                          String subscriberId,
                                          Set<String> channels,
                                          LeaderSelectedCallback leaderSelectedCallback,
                                          Runnable leaderRemovedCallback,
                                          java.util.function.Consumer<Assignment> assignmentUpdatedCallback) {

    return coordinatorFactory.makeCoordinator(subscriberId,
            channels,
            subscriptionId,
            assignmentUpdatedCallback,
            "/eventuate-tram/rabbitmq/consumer-leaders/%s".formatted(subscriberId),
            leaderSelectedCallback,
            leaderRemovedCallback);
  }


  public synchronized void close() {
    if (!consumerChannel.isOpen()) {
      return;
    }

    logger.info("Closing coordinator: subscription = {}", identificationInformation());
    coordinator.close();

    try {
      logger.info("Closing consumer channel: subscription = {}", identificationInformation());
      consumerChannel.close();
    } catch (IOException | TimeoutException e) {
      logger.error("Closing consumer failed: subscription = {}", identificationInformation());
      logger.error("Closing consumer failed", e);
    }

    closingCallback.ifPresent(Runnable::run);

    logger.info("Subscription is closed. {}", identificationInformation());
  }

  private Channel createRabbitMQChannel() {
    try {
      logger.info("Creating channel");
      Channel channel = connection.createChannel();
      logger.info("Created channel");
      return channel;
    } catch (IOException e) {
      logger.error("Creating channel failed", e);
      throw new RuntimeException(e);
    }
  }

  private void leaderSelected(LeadershipController leadershipController) {
    leaderHook.ifPresent(hook -> hook.leaderUpdated(true, subscriptionId));
    logger.info("Subscription selected as leader. {}", identificationInformation());

    Channel subscriberGroupChannel = createRabbitMQChannel();

    for (String channelName : channels) {
      try {
        logger.info("Leading subscription is creating exchanges and queues for channel {}. {}",
                channelName, identificationInformation());

        subscriberGroupChannel.exchangeDeclare(makeConsistentHashExchangeName(channelName, subscriberId), "x-consistent-hash");

        for (int i = 0; i < partitionCount; i++) {
          subscriberGroupChannel.queueDeclare(makeConsistentHashQueueName(channelName, subscriberId, i), true, false, false, null);
          subscriberGroupChannel.queueBind(makeConsistentHashQueueName(channelName, subscriberId, i), makeConsistentHashExchangeName(channelName, subscriberId), "10");
        }

        subscriberGroupChannel.exchangeDeclare(channelName, "fanout");
        subscriberGroupChannel.exchangeBind(makeConsistentHashExchangeName(channelName, subscriberId), channelName, "");

        logger.info("Leading subscription created exchanges and queues for channel {}. {}",
                channelName, identificationInformation());
      } catch (IOException e) {
        logger.error("Failed creating exchanges and queues for channel {}. {}",
                channelName, identificationInformation());
        logger.error("Failed creating exchanges and queues", e);
        throw new RuntimeException(e);
      }
    }

    try {
      logger.info("Closing subscriber group channel. {}", identificationInformation());
      subscriberGroupChannel.close();
      logger.info("Closed subscriber group channel. {}", identificationInformation());
    } catch (Exception e) {
      logger.error("Closing subscriber group channel failed. {}", identificationInformation());
      logger.error(e.getMessage(), e);
    }
  }

  private void leaderRemoved() {
    logger.info("Revoking leadership from subscription. {}", identificationInformation());
    leaderHook.ifPresent(hook -> hook.leaderUpdated(false, subscriptionId));
    logger.info("Leadership is revoked from subscription. {}", identificationInformation());
  }

  private void assignmentUpdated(Assignment assignment) {
    logger.info("Updating assignment {}. {} ", assignment, identificationInformation());

    for (String channelName : currentPartitionsByChannel.keySet()) {
      Set<Integer> currentPartitions = currentPartitionsByChannel.get(channelName);

      logger.info("Current partitions {} for channel {}. {}", currentPartitions, channelName, identificationInformation());

      Set<Integer> expectedPartitions = assignment.getPartitionAssignmentsByChannel().get(channelName);

      logger.info("Expected partitions {} for channel {}. {}", expectedPartitions, channelName, identificationInformation());

      Set<Integer> resignedPartitions = currentPartitions
              .stream()
              .filter(currentPartition -> !expectedPartitions.contains(currentPartition))
              .collect(Collectors.toSet());

      logger.info("Resigned partitions {} for channel {}. {}", resignedPartitions, channelName, identificationInformation());

      Set<Integer> assignedPartitions = expectedPartitions
              .stream()
              .filter(expectedPartition -> !currentPartitions.contains(expectedPartition))
              .collect(Collectors.toSet());

      logger.info("Assigned partitions {} for channel {}. {}", assignedPartitions, channelName, identificationInformation());

      resignedPartitions.forEach(resignedPartition -> {
        try {
          logger.info("Removing partition {} for channel {}. {}", resignedPartition, channelName, identificationInformation());

          String queue = makeConsistentHashQueueName(channelName, subscriberId, resignedPartition);
          consumerChannel.basicCancel(consumerTagByQueue.remove(queue));

          logger.info("Partition {} is removed for channel {}. {}", resignedPartition, channelName, identificationInformation());
        } catch (Exception e) {
          logger.error("Removing partition {} for channel {} failed. {}", resignedPartition, channelName, identificationInformation());
          logger.error("Removing partition failed", e);
          throw new RuntimeException(e);
        }
      });

      assignedPartitions.forEach(assignedPartition -> {
        try {
          logger.info("Assigning partition {} for channel {}. {}", assignedPartition, channelName, identificationInformation());


          String queue = makeConsistentHashQueueName(channelName, subscriberId, assignedPartition);
          String exchange = makeConsistentHashExchangeName(channelName, subscriberId);

          consumerChannel.exchangeDeclare(exchange, "x-consistent-hash");
          consumerChannel.queueDeclare(queue, true, false, false, null);
          consumerChannel.queueBind(queue, exchange, "10");

          String tag = consumerChannel.basicConsume(queue, false, createConsumer());

          consumerTagByQueue.put(queue, tag);

          logger.info("Partition {} is assigned for channel {}. {}", assignedPartition, channelName, identificationInformation());
        } catch (IOException e) {
          logger.error("Partition assignment failed. {}", identificationInformation());
          logger.error("Partition assignment failed", e);
          throw new RuntimeException(e);
        }
      });

      currentPartitionsByChannel.put(channelName, expectedPartitions);

      subscriptionLifecycleHook.ifPresent(sh -> sh.partitionsUpdated(channelName, subscriptionId, expectedPartitions));
    }

    logger.info("assignment {} is updated. {}", assignment, identificationInformation());
  }

  private Consumer createConsumer() {
    return new DefaultConsumer(consumerChannel) {
      @Override
      public void handleDelivery(String consumerTag,
                                 Envelope envelope,
                                 AMQP.BasicProperties properties,
                                 byte[] body) throws IOException {
        RabbitMQMessage message = new RabbitMQMessage(new String(body, "UTF-8"));

        try {
          if (consumerChannel.isOpen()) {
            handleMessageCallback.accept(message, () -> acknowledge(envelope, consumerChannel));
          }
        } catch (Exception e) {
          close();
        }
      }
    };
  }

  private void acknowledge(Envelope envelope, Channel channel) {
    try {
      channel.basicAck(envelope.getDeliveryTag(), false);
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
  }

  private String makeConsistentHashExchangeName(String channelName, String subscriberId) {
    return "%s-%s".formatted(channelName, subscriberId);
  }

  private String makeConsistentHashQueueName(String channelName, String subscriberId, int partition) {
    return "%s-%s-%s".formatted(channelName, subscriberId, partition);
  }

  private String identificationInformation() {
    return "(consumerId = [%s], subscriptionId = [%s], subscriberId = [%s])".formatted(consumerId, subscriptionId, subscriberId);
  }
}
