package com.example.kafka.nonblockingretries;

import kafka.server.KafkaConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

@PropertySource("application-kafka.properties")
public class EmbeddedKafkaBrokerConfig {

  @Value("${kafka.embedded.port}")
  private int embeddedKafkaBrokerPort;

  @Bean
  public EmbeddedKafkaBroker embeddedKafkaBroker() {
    return new EmbeddedKafkaBroker(
            1,
            false,
            3,
            "orders",
            "orders-retry-0",
            "orders-retry-1",
            "orders-retry-2",
            "orders-dlt")
        .kafkaPorts(embeddedKafkaBrokerPort)
        .brokerProperty(KafkaConfig.AutoCreateTopicsEnableProp(), "false");
  }
}
