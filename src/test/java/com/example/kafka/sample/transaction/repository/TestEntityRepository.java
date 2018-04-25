package com.example.kafka.sample.transaction.repository;

import com.example.kafka.sample.transaction.model.TestEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TestEntityRepository extends JpaRepository<TestEntity, String> {

}
