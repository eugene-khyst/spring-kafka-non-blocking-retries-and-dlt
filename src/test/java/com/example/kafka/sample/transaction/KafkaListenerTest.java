package com.example.kafka.sample.transaction;

import static com.example.kafka.sample.transaction.listener.TestKafkaListener.INPUT_TEST_TOPIC;
import static com.example.kafka.sample.transaction.listener.TestKafkaListener.OUTPUT_TEST_TOPIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.example.kafka.sample.transaction.model.TestEntity;
import com.example.kafka.sample.transaction.repository.TestEntityRepository;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@TestPropertySource("classpath:test.properties")
@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaListenerTest {

  @Autowired
  private KafkaEmbedded kafkaEmbedded;

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private TestEntityRepository testEntityRepository;

  @Test
  public void commitTransaction() throws Exception {
    String testKey = "test_key";
    String testData = "test_data";

    kafkaTemplate.executeInTransaction(kt ->
        kt.send(INPUT_TEST_TOPIC, testKey, testData));

    try (Consumer<String, String> consumer = createConsumer()) {
      kafkaEmbedded.consumeFromAnEmbeddedTopic(consumer, OUTPUT_TEST_TOPIC);
      ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer);
      Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
      ConsumerRecord<String, String> record = iterator.next();

      assertEquals(testKey, record.key());
      assertEquals(testData, record.value());
      assertFalse(iterator.hasNext());
    }

    Optional<TestEntity> testEntity = testEntityRepository.findById(testKey);
    assertTrue(testEntity.isPresent());
    assertEquals(testData, testEntity.get().getDescription());
  }

  @Test
  public void rollbackTransaction() throws Exception {
    String testKey = "test_exception";
    String testData = "test_exception_data";

    kafkaTemplate.executeInTransaction(kt ->
        kt.send(INPUT_TEST_TOPIC, testKey, testData));

    try (Consumer<String, String> consumer = createConsumer()) {
      kafkaEmbedded.consumeFromAnEmbeddedTopic(consumer, OUTPUT_TEST_TOPIC);
      ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, 2_000);
      assertTrue(records.isEmpty());
    }

    Optional<TestEntity> testEntity = testEntityRepository.findById(testKey);
    assertFalse(testEntity.isPresent());
  }

  private Consumer<String, String> createConsumer() {
    Map<String, Object> consumerProps =
        KafkaTestUtils.consumerProps("test-consumer", "true", kafkaEmbedded);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
        consumerProps, new StringDeserializer(), new StringDeserializer());
    return cf.createConsumer();
  }
}