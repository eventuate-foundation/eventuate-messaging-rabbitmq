package io.eventuate.messaging.rabbitmq.spring.integrationtests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.rabbitmq.client.ConnectionFactory;
import io.eventuate.coordination.leadership.zookeeper.ZkLeaderSelector;
import io.eventuate.messaging.partitionmanagement.CoordinatorFactory;
import io.eventuate.messaging.partitionmanagement.CoordinatorFactoryImpl;
import io.eventuate.messaging.partitionmanagement.tests.AbstractMessagingTest;
import io.eventuate.messaging.rabbitmq.spring.consumer.*;
import io.eventuate.messaging.rabbitmq.spring.producer.EventuateRabbitMQProducer;
import io.eventuate.util.test.async.Eventually;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {RabbitMQProducerTestConfiguration.class, EventuateRabbitMQConsumerConfigurationPropertiesConfiguration.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class MessagingTest extends AbstractMessagingTest {

  @Autowired
  private EventuateRabbitMQConsumerConfigurationProperties eventuateRabbitMQConsumerConfigurationProperties;

  @Autowired
  private EventuateRabbitMQProducer eventuateRabbitMQProducer;

  @Test
  public void testThatMessageConsumingStoppedAfterFirstException() throws Exception {
    MessageConsumerRabbitMQImpl consumer = createConsumer(2);

    AtomicInteger exceptions = new AtomicInteger(0);

    consumer.subscribe(subscriberId, ImmutableSet.of(destination), message -> {
      try {
        throw new RuntimeException("Test!");
      } finally {
        exceptions.incrementAndGet();
      }
    });

    TestSubscription testSubscription = new TestSubscription(consumer, null);
    consumer.setSubscriptionLifecycleHook((channel, subscriptionId, currentPartitions) -> testSubscription.setCurrentPartitions(currentPartitions));
    assertSubscriptionPartitionsBalanced(ImmutableList.of(testSubscription), 2);

    sendMessages(10, 2);

    Eventually.eventually(() -> Assert.assertEquals(1, exceptions.get()));
    Thread.sleep(3000);
    Eventually.eventually(() -> Assert.assertEquals(1, exceptions.get()));
  }

  @Override
  protected TestSubscription subscribe(int partitionCount) {
    ConcurrentLinkedQueue<Integer> messageQueue = new ConcurrentLinkedQueue<>();

    MessageConsumerRabbitMQImpl consumer = createConsumer(partitionCount);

    consumer.subscribe(subscriberId, ImmutableSet.of(destination), message ->
            messageQueue.add(Integer.parseInt(message.getPayload())));

    TestSubscription testSubscription = new TestSubscription(consumer, messageQueue);

    consumer.setSubscriptionLifecycleHook((channel, subscriptionId, currentPartitions) -> testSubscription.setCurrentPartitions(currentPartitions));
    consumer.setLeaderHook((leader, subscriptionId) -> testSubscription.setLeader(leader));

    return testSubscription;
  }

  private MessageConsumerRabbitMQImpl createConsumer(int partitionCount) {
    CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(eventuateRabbitMQConsumerConfigurationProperties.getZkUrl(),
            new ExponentialBackoffRetry(1000, 5));
    curatorFramework.start();

    CoordinatorFactory coordinatorFactory = new CoordinatorFactoryImpl(new ZkAssignmentManager(curatorFramework),
            (groupId, memberId, assignmentUpdatedCallback) -> new ZkAssignmentListener(curatorFramework, groupId, memberId, assignmentUpdatedCallback),
            (groupId, memberId, groupMembersUpdatedCallback) -> new ZkMemberGroupManager(curatorFramework, groupId, memberId, groupMembersUpdatedCallback),
            (lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback) -> new ZkLeaderSelector(curatorFramework, lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback),
            (groupId, memberId) -> new ZkGroupMember(curatorFramework, groupId, memberId),
            partitionCount);
    ConnectionFactory connectionFactory=new ConnectionFactory();
    connectionFactory.setUsername(eventuateRabbitMQConsumerConfigurationProperties.getUsername());
    connectionFactory.setUsername(eventuateRabbitMQConsumerConfigurationProperties.getPassword());
    MessageConsumerRabbitMQImpl messageConsumerRabbitMQ = new MessageConsumerRabbitMQImpl(subscriptionIdSupplier,
            consumerIdSupplier.get(),
            coordinatorFactory,
            connectionFactory,
            eventuateRabbitMQConsumerConfigurationProperties.getBrokerAddresses(),
            partitionCount);

    return messageConsumerRabbitMQ;
  }

  @Override
  protected void sendMessages(int messageCount, int partitions) {
    for (int i = 0; i < messageCount; i++) {
      eventuateRabbitMQProducer.send(destination,
              String.valueOf(i),
              String.valueOf(i));
    }
  }
}
