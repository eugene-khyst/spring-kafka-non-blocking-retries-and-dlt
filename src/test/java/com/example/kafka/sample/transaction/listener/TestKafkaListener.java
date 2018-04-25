package com.example.kafka.sample.transaction.listener;

import com.example.kafka.sample.transaction.model.TestEntity;
import com.example.kafka.sample.transaction.repository.TestEntityRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TestKafkaListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestKafkaListener.class);

  public static final String INPUT_TEST_TOPIC = "transaction-sample-topic-in";
  public static final String OUTPUT_TEST_TOPIC = "transaction-sample-topic-out";

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private TestEntityRepository testEntityRepository;

  @Transactional("chainedKafkaTransactionManager")
  @KafkaListener(topics = INPUT_TEST_TOPIC)
  public void listen(ConsumerRecord<String, String> record) {
    LOGGER.info("Received Kafka record from {}: {}", INPUT_TEST_TOPIC, record);

    kafkaTemplate.send(OUTPUT_TEST_TOPIC, record.key(), record.value());
    LOGGER.info("Forwarded Kafka record to {}: {}, {}",
        OUTPUT_TEST_TOPIC, record.key(), record.value());

    TestEntity testEntity = new TestEntity(record.key(), record.value());
    testEntityRepository.save(testEntity);
    LOGGER.info("Persisted TestEntity: {}", testEntity);

    if ("test_exception".equals(record.key())) {
      throw new RuntimeException("Simulating runtime exception in listener");
    }
  }
}
