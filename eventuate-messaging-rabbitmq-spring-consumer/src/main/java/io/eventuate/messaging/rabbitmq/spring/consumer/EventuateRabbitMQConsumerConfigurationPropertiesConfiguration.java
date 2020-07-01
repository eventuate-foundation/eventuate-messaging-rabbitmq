package io.eventuate.messaging.rabbitmq.spring.consumer;

import io.eventuate.messaging.rabbitmq.spring.common.EventuateRabbitMQCommonConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventuateRabbitMQConsumerConfigurationPropertiesConfiguration extends EventuateRabbitMQCommonConfigurationProperties {
  @Bean
  public EventuateRabbitMQConsumerConfigurationProperties eventuateRabbitMQConsumerConfigurationProperties() {
    return new EventuateRabbitMQConsumerConfigurationProperties();
  }
}
