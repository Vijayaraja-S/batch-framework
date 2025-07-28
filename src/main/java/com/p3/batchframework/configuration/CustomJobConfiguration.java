package com.p3.batchframework.configuration;

import com.p3.batchframework.operations.CustomJobOperator;
import com.p3.batchframework.persistence.repository.StepExecutionRepository;
import com.p3.batchframework.utils.CommonUtility;
import com.p3.batchframework.utils.MapperUtilsClusterTask;
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
  private final MapperUtilsClusterTask mapperUtils;
  private final CommonUtility commonUtility;
  private final StepExecutionRepository stepExecutionRepository;

  @Bean
  public CustomJobOperator customJobOperator() throws Exception {
    return new CustomJobOperator(
        taskConfigurer.getJobLauncher(),
        taskConfigurer.getJobExplorer(),
        taskConfigurer.getJobRepository(),
        getJobParametersConverter(),
        job,
        mapperUtils,
        commonUtility,
        stepExecutionRepository);
  }

  private JobParametersConverter getJobParametersConverter() {
    return new DefaultJobParametersConverter();
  }
}
