package io.eventuate.messaging.rabbitmq.spring.producer;

import io.eventuate.messaging.rabbitmq.spring.common.EventuateRabbitMQCommonConfigurationProperties;
import org.springframework.context.annotation.Bean;

public class EventuateRabbitMQProducerConfigurationPropertiesConfiguration extends EventuateRabbitMQCommonConfigurationProperties {
  @Bean
  public EventuateRabbitMQProducerConfigurationProperties eventuateRabbitMQProducerConfigurationProperties() {
    return new EventuateRabbitMQProducerConfigurationProperties();
  }
}
