package com.p3.batchframework.persistence.models;

import jakarta.persistence.*;
import java.io.Serializable;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Getter
@Setter
@Entity
@Builder
@Table(name = "job_execution_context")
@AllArgsConstructor
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
