package com.example.kafka.sample.transaction;

import static com.example.kafka.sample.transaction.listener.TestKafkaListener.INPUT_TEST_TOPIC;
import static com.example.kafka.sample.transaction.listener.TestKafkaListener.OUTPUT_TEST_TOPIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Iterator;
import java.util.Map;
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

  @Test
  public void shouldProcessEvent() throws Exception {
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
  }

  private Consumer<String, String> createConsumer() {
    Map<String, Object> consumerProps =
        KafkaTestUtils.consumerProps("test-consumer", "true", kafkaEmbedded);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
        consumerProps, new StringDeserializer(), new StringDeserializer());
    return cf.createConsumer();
  }
}