package com.p3.batchframework.persistence.models;

import jakarta.persistence.*;
import java.io.Serializable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Data
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder
@Table(name = "step_execution_context")
@Entity
public class StepExecutionContextEntity implements Serializable {

  @Id private String id;
  private Long jobExecutionId;

  @Column(name = "contextMap")
  @JdbcTypeCode(SqlTypes.BINARY)
  @Lob
  private byte[] contextMap;

  @OneToOne(cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
  @Fetch(FetchMode.SELECT)
  private ProgressDetailsEntity progress;

  public StepExecutionContextEntity() {
    super();
  }
}
