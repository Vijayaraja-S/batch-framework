package com.p3.batchframework.persistence.models;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldNameConstants;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Data
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder
@Table(name = "job_instance")
@Entity
@FieldNameConstants
public class JobInstanceEntity {

  @Id private String id;
  private String jobName;
  private String jobKey;
  private int version;

  @Column(name = "context_map")
  @JdbcTypeCode(SqlTypes.BINARY)
  @Lob
  private byte[] jobParameters;

  public JobInstanceEntity() {
    super();
  }
}
