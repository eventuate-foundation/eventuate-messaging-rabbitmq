package io.eventuate.messaging.rabbitmq.spring.consumer;

import com.rabbitmq.client.ConnectionFactory;
import io.eventuate.coordination.leadership.LeaderSelectorFactory;
import io.eventuate.coordination.leadership.zookeeper.ZkLeaderSelector;
import io.eventuate.messaging.partitionmanagement.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(EventuateRabbitMQConsumerConfigurationPropertiesConfiguration.class)
public class MessageConsumerRabbitMQConfiguration {

    @Bean
    public MessageConsumerRabbitMQImpl messageConsumerRabbitMQ(EventuateRabbitMQConsumerConfigurationProperties properties,
                                                               CoordinatorFactory coordinatorFactory,ConnectionFactory connectionFactory) {
        return new MessageConsumerRabbitMQImpl(coordinatorFactory,connectionFactory,
                properties.getBrokerAddresses(),
                properties.getPartitionCount());
    }

    @Bean
    public CoordinatorFactory coordinatorFactory(EventuateRabbitMQConsumerConfigurationProperties properties,
                                                 AssignmentManager assignmentManager,
                                                 AssignmentListenerFactory assignmentListenerFactory,
                                                 MemberGroupManagerFactory memberGroupManagerFactory,
                                                 LeaderSelectorFactory leaderSelectorFactory,
                                                 GroupMemberFactory groupMemberFactory) {
        return new CoordinatorFactoryImpl(assignmentManager,
                assignmentListenerFactory,
                memberGroupManagerFactory,
                leaderSelectorFactory,
                groupMemberFactory,
                properties.getPartitionCount());
    }

    @Bean
    public ConnectionFactory connectionFactory(EventuateRabbitMQConsumerConfigurationProperties properties) {
        ConnectionFactory factory = new ConnectionFactory();
        if (properties.getUsername() != null && !properties.getUsername().trim().isEmpty()) {
            factory.setUsername(properties.getUsername());
        }
        if (properties.getPassword() != null && !properties.getPassword().trim().isEmpty()) {
            factory.setPassword(properties.getPassword());
        }
        return factory;
    }

    @Bean
    public GroupMemberFactory groupMemberFactory(CuratorFramework curatorFramework) {
        return (groupId, memberId) -> new ZkGroupMember(curatorFramework, groupId, memberId);
    }

    @Bean
    public LeaderSelectorFactory leaderSelectorFactory(CuratorFramework curatorFramework) {
        return (lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback) ->
                new ZkLeaderSelector(curatorFramework, lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback);
    }

    @Bean
    public MemberGroupManagerFactory memberGroupManagerFactory(CuratorFramework curatorFramework) {
        return (groupId, memberId, groupMembersUpdatedCallback) ->
                new ZkMemberGroupManager(curatorFramework, groupId, memberId, groupMembersUpdatedCallback);
    }

    @Bean
    public AssignmentListenerFactory assignmentListenerFactory(CuratorFramework curatorFramework) {
        return (groupId, memberId, assignmentUpdatedCallback) ->
                new ZkAssignmentListener(curatorFramework, groupId, memberId, assignmentUpdatedCallback);
    }

    @Bean
    public AssignmentManager assignmentManager(CuratorFramework curatorFramework) {
        return new ZkAssignmentManager(curatorFramework);
    }

    @Bean
    public CuratorFramework curatorFramework(EventuateRabbitMQConsumerConfigurationProperties properties) {
        CuratorFramework framework = CuratorFrameworkFactory.newClient(properties.getZkUrl(),
                new ExponentialBackoffRetry(1000, 5));
        framework.start();
        return framework;
    }
}
