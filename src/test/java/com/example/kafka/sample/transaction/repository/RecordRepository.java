package com.example.kafka.sample.transaction.repository;

import com.example.kafka.sample.transaction.model.Record;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RecordRepository extends JpaRepository<Record, String> {

}
