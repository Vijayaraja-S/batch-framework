package com.p3.batchframework.listners;

import com.p3.batchframework.persistence.models.BGStatus;
import com.p3.batchframework.persistence.repository.BackgroundJobEntityRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class JobListener implements JobExecutionListener {

  private final BackgroundJobEntityRepository backgroundJobEntityRepository;

  public JobListener(BackgroundJobEntityRepository backgroundJobEntityRepository) {
    this.backgroundJobEntityRepository = backgroundJobEntityRepository;
  }

  @Override
  public void afterJob(JobExecution jobExecution) {
    ExecutionContext ctx = jobExecution.getExecutionContext();
    int totalTablesProcessed = 0;
    int totalRecordsProcessed = 0;

    for (String key : ctx.toMap().keySet()) {
      if (key.startsWith("recordsProcessedFor_")) {
        totalTablesProcessed++;
        totalRecordsProcessed += ctx.getInt(key);
      }
    }

    log.info(
        "Job completed - Total Tables: {}, Total Records: {}",
        totalTablesProcessed,
        totalRecordsProcessed);
    String backgroundJobId = ctx.getString("backgroundJobId");
    backgroundJobEntityRepository
        .findById(backgroundJobId)
        .ifPresent(backgroundJobEntity -> backgroundJobEntity.setStatus(BGStatus.COMPLETED));
  }

  @Override
  public void beforeJob(JobExecution jobExecution) {
    log.info("Job started: {}", jobExecution.getJobInstance().getJobName());
  }
}
