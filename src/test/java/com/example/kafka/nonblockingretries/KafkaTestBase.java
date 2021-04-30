package com.example.kafka.nonblockingretries;

import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles("kafka")
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=localhost:${kafka.embedded.port}")
public class KafkaTestBase {

  public static final EmbeddedKafkaBroker KAFKA_BROKER = createEmbeddedKafkaBroker();

  private static EmbeddedKafkaBroker createEmbeddedKafkaBroker() {
    AnnotationConfigApplicationContext context =
        new AnnotationConfigApplicationContext(EmbeddedKafkaBrokerConfig.class);
    EmbeddedKafkaBroker kafkaBroker = context.getBean(EmbeddedKafkaBroker.class);
    Runtime.getRuntime().addShutdownHook(new Thread(context::close));
    return kafkaBroker;
  }

  protected Consumer<String, String> createConsumer() {
    Map<String, Object> consumerProps =
        KafkaTestUtils.consumerProps(
            this.getClass().getSimpleName() + "-consumer", "true", KAFKA_BROKER);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    DefaultKafkaConsumerFactory<String, String> cf =
        new DefaultKafkaConsumerFactory<>(
            consumerProps, new StringDeserializer(), new StringDeserializer());
    return cf.createConsumer();
  }
}
