package com.p3.batchframework.scheduler;

import com.p3.batchframework.job_execution_service.JobExecutionService;
import com.p3.batchframework.persistence.models.BGStatus;
import com.p3.batchframework.persistence.models.BackgroundJobEntity;
import com.p3.batchframework.persistence.repository.BackgroundJobEntityRepository;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component("jobExecutionScheduler")
@Slf4j
@RequiredArgsConstructor
public class jobExecutionScheduler {

  private static final int MAX_JOBS = 4;
  private final BackgroundJobEntityRepository backgroundJobEntityRepository;

  private final JobExecutionService jobExecutionService;

  @Scheduled(cron = "*/30 * * * * *")
  public void scheduleJob() {
    List<BackgroundJobEntity> readyStateJobs = jobExecutionService.findReadyStateJobs();
    if (readyStateJobs.isEmpty()) {
      return;
    }
    if (!checkForMaxAllowedJobs()) {
      return;
    }
    readyStateJobs.stream()
        .filter(backgroundJobEntity -> checkForMaxAllowedJobs())
        .forEach(
            backgroundJobEntity -> {
              backgroundJobEntity.setStatus(BGStatus.IN_PROGRESS);
              backgroundJobEntity.setStartTime(LocalDateTime.now());
              backgroundJobEntityRepository.save(backgroundJobEntity);

              Long jobExecutionId;
              try {
                jobExecutionId = jobExecutionService.initJob(backgroundJobEntity);
              } catch (JobInstanceAlreadyExistsException
                  | JobParametersInvalidException
                  | NoSuchJobException
                  | IOException e) {
                backgroundJobEntity.setStatus(BGStatus.FAILED);
                backgroundJobEntity.setMessage(e.getMessage().getBytes(StandardCharsets.UTF_8));
                backgroundJobEntityRepository.save(backgroundJobEntity);
                throw new RuntimeException(e);
              }
            });
  }

  private boolean checkForMaxAllowedJobs() {
    List<BackgroundJobEntity> inpProgressJobs =
        backgroundJobEntityRepository.findByStatus(BGStatus.IN_PROGRESS);
    return inpProgressJobs.size() < MAX_JOBS;
  }
}
