package com.example.kafka.sample.transaction.model;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Record {

  @Id
  private String key;

  private String value;

  public Record() {
  }

  public Record(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "Record{" +
        "key='" + key + '\'' +
        ", value='" + value + '\'' +
        '}';
  }
}
