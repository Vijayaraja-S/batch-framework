package com.p3.batchframework;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableBatchProcessing
@EnableScheduling
public class BatchFrameworkApplication {

  public static void main(String[] args) {
    SpringApplication.run(BatchFrameworkApplication.class, args);
  }
}
