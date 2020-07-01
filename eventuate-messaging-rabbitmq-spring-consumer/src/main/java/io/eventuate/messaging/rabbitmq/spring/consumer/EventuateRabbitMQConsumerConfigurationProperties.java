package io.eventuate.messaging.rabbitmq.spring.consumer;

import io.eventuate.messaging.rabbitmq.spring.common.EventuateRabbitMQCommonConfigurationProperties;
import org.springframework.beans.factory.annotation.Value;

public class EventuateRabbitMQConsumerConfigurationProperties extends EventuateRabbitMQCommonConfigurationProperties {

  @Value("${eventuatelocal.zookeeper.connection.string}")
  private String zkUrl;

  @Value("${eventuate.rabbitmq.partition.count:#{2}}")
  private int partitionCount;

  public String getZkUrl() {
    return zkUrl;
  }

  public int getPartitionCount() {
    return partitionCount;
  }
}
