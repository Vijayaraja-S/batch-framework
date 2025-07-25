package com.p3.batchframework.custom_models;

import jakarta.persistence.*;
import java.io.Serializable;
import lombok.Data;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Data
@Entity
@Table(name = "job_execution_context")
public class JobExecutionContextEntity implements Serializable {

  @Id private String id;

  @JdbcTypeCode(SqlTypes.BINARY)
  @Lob
  @Column(name = "context_map")
  private byte[] contextMap;

  public JobExecutionContextEntity() {
    super();
  }
}
