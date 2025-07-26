package com.p3.batchframework.persistence.models;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.io.Serializable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder
@Table(name = "sequences")
@Entity
public class SequencesEntity implements Serializable {

  @Id private String name;
  private Long value;

  public SequencesEntity() {
    super();
  }
}
