package com.example.kafka.sample.transaction.listener;

import com.example.kafka.sample.transaction.model.Record;
import com.example.kafka.sample.transaction.repository.RecordRepository;
import java.util.concurrent.atomic.LongAdder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TestKafkaListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestKafkaListener.class);

  public static final String INPUT_TEST_TOPIC = "transaction-sample-topic-in";
  public static final String OUTPUT_TEST_TOPIC = "transaction-sample-topic-out";

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private RecordRepository recordRepository;

  private final LongAdder counter = new LongAdder();

  @KafkaListener(topics = INPUT_TEST_TOPIC)
  public void listen(ConsumerRecord<String, String> record) {
    LOGGER.info("Received Kafka record from {}: {}", INPUT_TEST_TOPIC, record);

    kafkaTemplate.send(OUTPUT_TEST_TOPIC, record.key(), record.value());
    LOGGER.info("Forwarded Kafka record to {}: {}, {}",
        OUTPUT_TEST_TOPIC, record.key(), record.value());

    Record testEntity = new Record(record.key(), record.value());
    recordRepository.save(testEntity);
    LOGGER.info("Persisted Record: {}", testEntity);

    counter.increment();

    if ("test_exception".equals(record.key())) {
      throw new RuntimeException("Simulating runtime exception in listener");
    }
  }

  public int getCounter() {
    return counter.intValue();
  }

  public void resetCounter() {
    counter.reset();
  }
}
