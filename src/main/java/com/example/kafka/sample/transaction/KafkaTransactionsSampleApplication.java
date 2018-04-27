package com.example.kafka.sample.transaction;

import javax.persistence.EntityManagerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.transaction.ChainedKafkaTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;

@SpringBootApplication
public class KafkaTransactionsSampleApplication {

  @Bean
  public JpaTransactionManager transactionManager(EntityManagerFactory emf) {
    return new JpaTransactionManager(emf);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
      ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
      ConsumerFactory<Object, Object> kafkaConsumerFactory,
      ChainedKafkaTransactionManager<Object, Object> chainedKafkaTransactionManager) {
    ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
    configurer.configure(factory, kafkaConsumerFactory);
    factory.getContainerProperties().setTransactionManager(chainedKafkaTransactionManager);
    return factory;
  }

  @Bean
  public ChainedKafkaTransactionManager<Object, Object> chainedKafkaTransactionManager(
      KafkaTransactionManager<Object, Object> kafkaTransactionManager,
      JpaTransactionManager jpaTransactionManager) {
    return new ChainedKafkaTransactionManager<>(kafkaTransactionManager, jpaTransactionManager);
  }

  public static void main(String[] args) {
    SpringApplication.run(KafkaTransactionsSampleApplication.class, args);
  }
}
