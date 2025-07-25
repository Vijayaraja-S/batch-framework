package com.p3.batchframework.configuration;

import com.p3.batchframework.operations.CustomJobOperator;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.converter.JobParametersConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class CustomJobConfiguration {

  private final TaskConfigurer taskConfigurer;
  private final Job job;

  @Bean
  public CustomJobOperator customJobOperator() throws Exception {
    return new CustomJobOperator(
        taskConfigurer.getJobLauncher(),
        taskConfigurer.getJobExplorer(),
        taskConfigurer.getJobRepository(),
        getJobParametersConverter(),
        job);
  }

  private JobParametersConverter getJobParametersConverter() {
    return new DefaultJobParametersConverter();
  }
}
