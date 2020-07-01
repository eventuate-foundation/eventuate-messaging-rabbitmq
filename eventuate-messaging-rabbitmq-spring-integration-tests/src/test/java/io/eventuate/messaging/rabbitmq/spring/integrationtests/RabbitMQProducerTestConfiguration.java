package io.eventuate.messaging.rabbitmq.spring.integrationtests;

import io.eventuate.messaging.rabbitmq.spring.producer.EventuateRabbitMQProducer;
import io.eventuate.messaging.rabbitmq.spring.producer.EventuateRabbitMQProducerConfigurationProperties;
import io.eventuate.messaging.rabbitmq.spring.producer.EventuateRabbitMQProducerConfigurationPropertiesConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(EventuateRabbitMQProducerConfigurationPropertiesConfiguration.class)
public class RabbitMQProducerTestConfiguration {
  @Bean
  public EventuateRabbitMQProducer eventuateRabbitMQProducer(EventuateRabbitMQProducerConfigurationProperties properties) {
    return new EventuateRabbitMQProducer(properties.getParsedBrokerAddresses());
  }
}
