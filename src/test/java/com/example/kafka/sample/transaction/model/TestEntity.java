package com.example.kafka.sample.transaction.model;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class TestEntity {

  @Id
  private String id;

  private String description;

  public TestEntity() {
  }

  public TestEntity(String id, String description) {
    this.id = id;
    this.description = description;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return "TestEntity{" +
        "id='" + id + '\'' +
        ", description='" + description + '\'' +
        '}';
  }
}
