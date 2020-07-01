package io.eventuate.messaging.rabbitmq.spring.common;

import com.rabbitmq.client.Address;
import org.springframework.beans.factory.annotation.Value;

public class EventuateRabbitMQCommonConfigurationProperties {
  @Value("${rabbitmq.broker.addresses:#{null}}")
  private String brokerAddresses;

  @Value("${rabbitmq.url:#{null}}")
  private String url;

  public Address[] getBrokerAddresses() {
    if (url == null && brokerAddresses == null) {
      throw new IllegalArgumentException("One of rabbitmq.broker.addresses or rabbitmq.url should be specified");
    }

    if (url != null && brokerAddresses != null) {
      throw new IllegalArgumentException("Only one of rabbitmq.broker.addresses or rabbitmq.url should be specified");
    }

    if (url != null) return Address.parseAddresses(url);

    return Address.parseAddresses(brokerAddresses);
  }
}
