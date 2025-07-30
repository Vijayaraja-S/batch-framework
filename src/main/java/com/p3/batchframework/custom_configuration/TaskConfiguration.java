package com.p3.batchframework.custom_configuration;

import com.p3.batchframework.custom_configuration.partitioner.PartitionHandler;
import com.p3.batchframework.custom_configuration.processor.ItemProcessorHandler;
import com.p3.batchframework.custom_configuration.reader.ItemReaderHandler;
import com.p3.batchframework.custom_configuration.writer.ItemWriterHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class TaskConfiguration {

  @StepScope
  @Bean
  public ItemReaderHandler<?> customItemReader(
      @Value("#{jobParameters['backgroundJobId']}") String backgroundJobId,
      @Value("#{jobParameters['inputBean']}") String inpuBeanString) {

    return new ItemReaderHandler<>(inpuBeanString, backgroundJobId);
  }

  @StepScope
  @Bean
  public PartitionHandler customPartitioner(
      @Value("#{jobParameters['inputBean']}") String inpuBeanString) {
    return new PartitionHandler(inpuBeanString);
  }

  @StepScope
  @Bean
  public ItemProcessorHandler<?, ?> customItemProcessor(
      @Value("#{jobParameters['backgroundJobId']}") String backgroundJobId,
      @Value("#{jobParameters['inputBean']}") String inpuBeanString) {

    return new ItemProcessorHandler<>(backgroundJobId, inpuBeanString);
  }

  @StepScope
  @Bean
  public ItemWriterHandler<?> customItemWriter(
      @Value("#{jobParameters['backgroundJobId']}") String backgroundJobId,
      @Value("#{jobParameters['inputBean']}") String inpuBeanString) {

    return new ItemWriterHandler<>(backgroundJobId, inpuBeanString);
  }
}
