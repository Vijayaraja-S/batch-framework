package com.p3.batchframework.custom_configuration.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.p3.batchframework.job_execution_service.bean.ConnectionInputBean;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
public class ItemProcessorHandler<I, O> extends AbstractItemProcessorHandler<I, O> {

  private final ConnectionInputBean inputBean;
  private final String backgroundJobId;
  private int processedCount = 0;
  private static final String PROCESSED_COUNT_KEY = "processedCount";

  public ItemProcessorHandler(
          @Value("#{jobParameters['backgroundJobId']}") String backgroundJobId,
          @Value("#{jobParameters['inputBean']}") String inputBeanString) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      this.inputBean = objectMapper.readValue(inputBeanString, ConnectionInputBean.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize input bean", e);
    }
    this.backgroundJobId = backgroundJobId;
  }

  @Override
  @SuppressWarnings("unchecked")
  public O process(@NonNull I item) {
    if (item instanceof Map) {
      Map<String, Object> row = new HashMap<>((Map<String, Object>) item);
      for (Map.Entry<String, Object> entry : row.entrySet()) {
        Object value = entry.getValue();
        if (value instanceof String) {
          entry.setValue(((String) value).toUpperCase());
        }
      }
      processedCount++;
      return (O) row;
    }
    return null;
  }

  @Override
  public void open(@NonNull ExecutionContext executionContext) throws ItemStreamException {
    if (executionContext.containsKey(PROCESSED_COUNT_KEY)) {
      processedCount = executionContext.getInt(PROCESSED_COUNT_KEY);
      log.info("Resuming from previously processed count: {}", processedCount);
    } else {
      processedCount = 0;
      log.info("Starting fresh with processed count: 0");
    }
  }

  @Override
  public void update(@NonNull ExecutionContext executionContext) throws ItemStreamException {
    executionContext.putInt(PROCESSED_COUNT_KEY, processedCount);
    log.info("Checkpoint - processed count saved: {}", processedCount);
  }

  @Override
  public void close() throws ItemStreamException {
    log.info("Processor closed. Total records processed: {}", processedCount);
  }
}
