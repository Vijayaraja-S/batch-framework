package com.p3.batchframework.configuration;

import com.p3.batchframework.custom_configuration.partitioner.CustomPartitioner;
import com.p3.batchframework.custom_configuration.processor.CustomItemProcessor;
import com.p3.batchframework.custom_configuration.reader.CustomItemReader;
import com.p3.batchframework.custom_configuration.writer.CustomItemWriter;
import com.p3.batchframework.listners.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class MasterConfiguration {
  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;
  private final CustomPartitioner customPartitioner;
  private final CustomItemReader<Object> customItemReader;
  private final CustomItemProcessor<Object, Object> customItemProcessor;
  private final CustomItemWriter<Object> customItemWriter;
  private final JobListener jobListener;
  private final StepListener stepListener;
  private final ChunkProcessListener chunkProcessListener;
  private final ItemReaderListener<Object> itemReaderListener;
  private final ItemProcessorListener<Object, Object> itemProcessorListener;
  private final ItemWriterListener<Object> itemWriterListener;
  private final TaskConfigurer taskConfigurer;

  @Bean
  public TaskExecutor taskExecutor() {
    ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
    exec.setCorePoolSize(4);
    exec.setMaxPoolSize(4);
    exec.setThreadNamePrefix("partition-");
    exec.initialize();
    return exec;
  }

  @Bean
  public Step workerStep() {
    return new StepBuilder("workerStep", taskConfigurer.getJobRepository())
        .chunk(100, transactionManager)
        .reader(customItemReader)
        .processor(customItemProcessor)
        .writer(customItemWriter)
        .listener(itemReaderListener)
        .listener(itemProcessorListener)
        .listener(itemWriterListener)
        .listener(chunkProcessListener)
        .listener(stepListener)
        .build();
  }

  @Bean
  public Step partitionedMasterStep(Step workerStep, TaskExecutor taskExecutor) {
    return new StepBuilder("partitionedMasterStep", taskConfigurer.getJobRepository())
        .partitioner(workerStep.getName(), customPartitioner)
        .step(workerStep)
        .gridSize(4)
        .taskExecutor(taskExecutor)
        .listener(stepListener)
        .build();
  }

  @Bean
  public Job partitionJob(Step partitionedMasterStep) {
    JobBuilder partitionJob = new JobBuilder("partitionJob", taskConfigurer.getJobRepository());
    return partitionJob
        .listener(jobListener)
        .start(partitionedMasterStep)
        .build();
  }
}
