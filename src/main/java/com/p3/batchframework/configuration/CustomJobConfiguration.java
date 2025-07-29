package com.p3.batchframework.configuration;

import com.p3.batchframework.operations.CustomJobOperator;
import com.p3.batchframework.persistence.repository.StepExecutionRepository;
import com.p3.batchframework.utils.MapperUtilsClusterTask;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class CustomJobConfiguration {

  private final TaskConfigurer taskConfigurer;
  private final Job job;
  private final MapperUtilsClusterTask mapperUtils;
  private final StepExecutionRepository stepExecutionRepository;

  @Bean
  public CustomJobOperator customJobOperator() throws Exception {
    return new CustomJobOperator(
        taskConfigurer.getJobLauncher(),
        taskConfigurer.getJobExplorer(),
        taskConfigurer.getJobRepository(),
        job,
        mapperUtils,
        stepExecutionRepository);
  }


}
