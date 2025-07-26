package com.p3.batchframework.persistence.models;

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
@Entity
@FieldNameConstants
@Table(name = "job_execution")
public class JobExecutionEntity {

  @Id private String id;
  private Long jobInstanceId;
  private Date startTime;
  private Date endTime;
  private String status;
  private String exitCode;

  @Column(columnDefinition = "TEXT")
  private String exitMessage;

  private Date createTime;
  private Date lastUpdated;
  private int version;

  public JobExecutionEntity() {
    super();
  }
}
