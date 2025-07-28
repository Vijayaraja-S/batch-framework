package com.p3.batchframework.persistence.models;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.*;
import lombok.experimental.FieldNameConstants;

@Getter
@Setter
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder
@Entity
@FieldNameConstants
@Table(name = "job_execution")
public class JobExecutionEntity {

  @Id private String id;
  private Long jobInstanceId;
  private LocalDateTime startTime;
  private LocalDateTime endTime;
  private String status;
  private String exitCode;

  @Column(columnDefinition = "TEXT")
  private String exitMessage;

  private LocalDateTime createTime;
  private LocalDateTime lastUpdated;
  private int version;

  public JobExecutionEntity() {
    super();
  }
}
