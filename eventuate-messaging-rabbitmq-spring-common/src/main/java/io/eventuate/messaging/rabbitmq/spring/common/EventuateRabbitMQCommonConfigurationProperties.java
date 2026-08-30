package io.eventuate.messaging.rabbitmq.spring.common;

import com.rabbitmq.client.Address;
import org.springframework.beans.factory.annotation.Value;

public class EventuateRabbitMQCommonConfigurationProperties {
  @Value("${rabbitmq.broker.addresses:#{null}}")
  private String brokerAddresses;
  @Value("${rabbitmq.username:#{null}}")
  private String username;
  @Value("${rabbitmq.password:#{null}}")
  private String password;


  @Value("${rabbitmq.url:#{null}}")
  private String url;

  public Address[] getBrokerAddresses() {
    if ((url == null && brokerAddresses == null) || (url != null && brokerAddresses != null)) {
      throw new IllegalArgumentException("One of rabbitmq.broker.addresses or rabbitmq.url should be specified");
    }

    return Address.parseAddresses(url == null ? brokerAddresses : url);
  }
  public String getUsername(){
    return username;
  }
  public String getPassword(){
    return password;
  }
}
