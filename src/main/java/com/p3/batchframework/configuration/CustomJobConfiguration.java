package com.p3.batchframework.configuration;

import com.p3.batchframework.job_operator.CustomJobOperator;
import com.p3.batchframework.persistence.repository.StepExecutionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class CustomJobConfiguration {

  private final TaskConfigurer taskConfigurer;
  private final StepExecutionRepository stepExecutionRepository;

  @Bean
  public CustomJobOperator customJobOperator() throws Exception {
    return new CustomJobOperator(
        taskConfigurer.getJobLauncher(),
        taskConfigurer.getJobExplorer(),
        taskConfigurer.getJobRepository(),
        taskConfigurer.jobRegistry(),
        stepExecutionRepository);
  }
}
