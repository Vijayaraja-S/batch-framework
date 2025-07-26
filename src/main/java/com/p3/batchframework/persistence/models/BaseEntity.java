package com.p3.batchframework.persistence.models;

import jakarta.persistence.*;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.hibernate.annotations.UuidGenerator;

@Data
@AllArgsConstructor
@MappedSuperclass
public class BaseEntity implements Serializable {
  public BaseEntity() {
    updatedAt = createdAt = System.currentTimeMillis() / 1000;
  }

  @Id
  @GeneratedValue
  @UuidGenerator 
  @Column(name = "id", unique = true, columnDefinition = "character varying")
  private String id;


  private long createdAt;
  private long updatedAt;
}
