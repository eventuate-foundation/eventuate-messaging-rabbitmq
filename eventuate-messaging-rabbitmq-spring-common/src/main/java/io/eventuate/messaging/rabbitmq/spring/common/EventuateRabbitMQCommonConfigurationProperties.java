package io.eventuate.messaging.rabbitmq.spring.common;

import com.rabbitmq.client.Address;
import org.springframework.beans.factory.annotation.Value;

public class EventuateRabbitMQCommonConfigurationProperties {
  @Value("${rabbitmq.broker.addresses}")
  private String brokerAddresses;

  public String getBrokerAddresses() {
    return brokerAddresses;
  }

  public Address[] getParsedBrokerAddresses() {
    return Address.parseAddresses(brokerAddresses);
  }
}
