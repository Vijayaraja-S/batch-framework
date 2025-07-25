package com.p3.batchframework.custom_models;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.Date;
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
  private Date startTime;
  private Date endTime;
  private String status;
  private int commitCount;
  private int readCount;
  private int filterCount;
  private int writeCount;
  private String exitCode;

  @Column(columnDefinition = "TEXT")
  private String exitMessage;

  private int readSkipCount;
  private int writeSkipCount;
  private int processSkipCount;
  private int rollbackCount;
  private Date lastUpdated;
  private int version;

  public StepExecutionEntity() {
    super();
  }
}
