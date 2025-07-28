package com.p3.batchframework.persistence.models;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldNameConstants;

@Data
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder
@Table(name = "step_execution")
@Entity
@FieldNameConstants
public class StepExecutionEntity {
  @Id private String id;

  private String stepName;
  private Long jobExecutionId;
  private LocalDateTime startTime;
  private LocalDateTime endTime;
  private String status;
  private long commitCount;
  private long readCount;
  private long filterCount;
  private long writeCount;
  private String exitCode;

  @Column(columnDefinition = "TEXT")
  private String exitMessage;

  private long readSkipCount;
  private long writeSkipCount;
  private long processSkipCount;
  private long rollbackCount;
  private LocalDateTime lastUpdated;
  private long version;

  public StepExecutionEntity() {
    super();
  }
}
