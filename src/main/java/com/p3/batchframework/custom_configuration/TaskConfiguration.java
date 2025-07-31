package com.p3.batchframework.custom_configuration;

import com.p3.batchframework.custom_configuration.partitioner.PartitionHandler;
import com.p3.batchframework.custom_configuration.processor.ItemProcessorHandler;
import com.p3.batchframework.custom_configuration.reader.ItemReaderHandler;
import com.p3.batchframework.custom_configuration.writer.ItemWriterHandler;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
    String inputString =
        new String(Base64.getDecoder().decode(inpuBeanString), StandardCharsets.UTF_8);

    return new ItemReaderHandler<>(inputString, backgroundJobId);
  }

  @StepScope
  @Bean
  public PartitionHandler customPartitioner(
      @Value("#{jobParameters['backgroundJobId']}") String backgroundJobId,
      @Value("#{jobParameters['inputBean']}") String inpuBeanString) {
    String inputString =
        new String(Base64.getDecoder().decode(inpuBeanString), StandardCharsets.UTF_8);
    return new PartitionHandler(inputString, backgroundJobId);
  }

  @StepScope
  @Bean
  public ItemProcessorHandler<?, ?> customItemProcessor(
      @Value("#{jobParameters['backgroundJobId']}") String backgroundJobId,
      @Value("#{jobParameters['inputBean']}") String inpuBeanString) {
    String inputString =
        new String(Base64.getDecoder().decode(inpuBeanString), StandardCharsets.UTF_8);
    return new ItemProcessorHandler<>(backgroundJobId, inputString);
  }

  @StepScope
  @Bean
  public ItemWriterHandler<?> customItemWriter(
      @Value("#{jobParameters['backgroundJobId']}") String backgroundJobId,
      @Value("#{jobParameters['inputBean']}") String inpuBeanString) {
    String inputString =
        new String(Base64.getDecoder().decode(inpuBeanString), StandardCharsets.UTF_8);
    return new ItemWriterHandler<>(backgroundJobId, inputString);
  }
}
