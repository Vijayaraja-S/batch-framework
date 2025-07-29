package com.p3.batchframework.configuration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.SimpleJobExplorer;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class TaskConfigurer {
  private final ExecutionContextDao executionContextDao;
  private final JobExecutionDao jobExecutionDao;
  private final JobInstanceDao jobInstanceDao;
  private final StepExecutionDao stepExecutionDao;

  @Bean
  public JobRepository getJobRepository() {
    return new SimpleJobRepository(
        jobInstanceDao, jobExecutionDao, stepExecutionDao, executionContextDao);
  }

  @Bean
  public TaskExecutorJobLauncher getJobLauncher() throws Exception {
    TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
    jobLauncher.setJobRepository(getJobRepository());
    jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
    jobLauncher.afterPropertiesSet();
    return jobLauncher;
  }

  @Bean
  public JobExplorer getJobExplorer() {
    return new SimpleJobExplorer(
        jobInstanceDao, jobExecutionDao, stepExecutionDao, executionContextDao);
  }
  @Bean
  public JobRegistry jobRegistry() {
    return new MapJobRegistry();
  }

}
