package com.p3.batchframework.daos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.p3.batchframework.persistence.models.StepExecutionContextEntity;
import com.p3.batchframework.persistence.models.StepExecutionEntity;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;

/** MongoExecutionContextDao */
@Slf4j
@Component
public class ExecutionContextDao extends AbstractDao
    implements org.springframework.batch.core.repository.dao.ExecutionContextDao {
  private final StepExecutionContextRepository stepExecutionContextRepository;
  private final JobExecutionContextRepository jobExecutionContextRepository;
  private final CommonUtility commonUtility;

  private final ReentrantLock stepExecutionLock = new ReentrantLock();

  @Override
  public @NonNull ExecutionContext getExecutionContext(JobExecution jobExecution) {
    return getExecutionContextFromJobExecution(jobExecution.getId());
  }

  @Override
  public @NonNull ExecutionContext getExecutionContext(StepExecution stepExecution) {
    return getExecutionContextFromStepExecution(stepExecution.getId());
  }

  @Override
  public void saveExecutionContext(JobExecution jobExecution) {
    saveOrUpdateJobExecutionContext(jobExecution.getId(), jobExecution.getExecutionContext());
  }

  @Override
  public void saveExecutionContext(StepExecution stepExecution) {
    saveOrUpdateStepExecutionContext(
        stepExecution.getId(),
        stepExecution.getExecutionContext(),
        stepExecution.getJobExecutionId());
  }

  @Override
  public void saveExecutionContexts(Collection<StepExecution> stepExecutions) {
    Assert.notNull(stepExecutions, "Attempt to save a null collection of step executions");
    for (StepExecution stepExecution : stepExecutions) {
      saveExecutionContext(stepExecution);
      saveExecutionContext(stepExecution.getJobExecution());
    }
  }

  @Override
  public void updateExecutionContext(JobExecution jobExecution) {
    saveOrUpdateJobExecutionContext(jobExecution.getId(), jobExecution.getExecutionContext());
  }

  @Override
  public void updateExecutionContext(StepExecution stepExecution) {
    stepExecutionLock.lock();
    try {
      saveOrUpdateStepExecutionContext(
          stepExecution.getId(),
          stepExecution.getExecutionContext(),
          stepExecution.getJobExecutionId());
    } finally {
      stepExecutionLock.unlock();
    }
  }

  private void saveOrUpdateStepExecutionContext(
      Long executionId, ExecutionContext executionContext, Long jobExecutionId) {
    Assert.notNull(executionId, "ExecutionId must not be null.");
    Assert.notNull(executionContext, "The ExecutionContext must not be null.");
    Map<String, Object> contextMap = new HashMap<>();
    ObjectMapper oMapper = new ObjectMapper();

    for (Map.Entry<String, Object> entry : executionContext.entrySet()) {
      Object value = entry.getValue();
      String key = entry.getKey();
      contextMap.put(key.replaceAll(DOT_STRING, DOT_ESCAPE_STRING), value);

      if (key.contains("ReportData") || key.contains("TableExtractionPerformanceReport")) {
        ConcurrentHashMap<String, Object> subMap =
            oMapper.convertValue(value, ConcurrentHashMap.class);
        for (Map.Entry<String, Object> mapEntity : subMap.entrySet()) {
          if (mapEntity.getKey().contains(".")) {
            subMap.put(
                mapEntity.getKey().replaceAll(DOT_STRING, DOT_ESCAPE_STRING), mapEntity.getValue());
            subMap.remove(mapEntity.getKey());
            contextMap.put(entry.getKey(), subMap);
          }
        }
      }
       contextMap.put("jobExecutionId", jobExecutionId);
      if (value instanceof BigDecimal || value instanceof BigInteger) {
        contextMap.put(key + TYPE_SUFFIX, value.getClass().getName());
      }
    }

    Optional<StepExecutionEntity> mongoStepExecutionContextOptional =
        stepExecutionContextRepository.findById(String.valueOf(executionId));
    StepExecutionEntity mongoStepExecutionContext;
    if (mongoStepExecutionContextOptional.isPresent()) {
      mongoStepExecutionContext = mongoStepExecutionContextOptional.orElse(null);
      mongoStepExecutionContext.setContextMap(
          commonUtility.convertMapToByteArray(contextMap, false));
      mongoStepExecutionContext.setJobExecutionId(jobExecutionId);
      stepExecutionContextRepository.save(mongoStepExecutionContext);
    } else {
      try {
        mongoStepExecutionContext =
            StepExecutionContextEntity.builder()
                .id(String.valueOf(executionId))
                .contextMap(commonUtility.convertMapToByteArray(contextMap, false))
                .jobExecutionId(jobExecutionId)
                .build();
        stepExecutionContextRepository.save(mongoStepExecutionContext);
      } catch (Exception e) {
        log.error("Exception while mongo step save ***** " + e.getMessage());
      }
    }
  }

  private void saveOrUpdateJobExecutionContext(
      Long executionId, ExecutionContext executionContext) {
    Assert.notNull(executionId, "ExecutionId must not be null.");
    Assert.notNull(executionContext, "The ExecutionContext must not be null.");

    Map<String, Object> contextMap = new HashMap<>();
    for (Map.Entry<String, Object> entry : executionContext.entrySet()) {
      Object value = entry.getValue();
      String key = entry.getKey();
      contextMap.put(key.replaceAll(DOT_STRING, DOT_ESCAPE_STRING), value);
      if (value instanceof BigDecimal || value instanceof BigInteger) {
        contextMap.put(key + TYPE_SUFFIX, value.getClass().getName());
      }
    }

    Optional<StepExecutionContextEntity> mongoJobExecutionContextOptional =
        jobExecutionContextRepository.findById(String.valueOf(executionId));
    StepExecutionContextEntity mongoJobExecutionContext;
    if (mongoJobExecutionContextOptional.isPresent()) {
      mongoJobExecutionContext = mongoJobExecutionContextOptional.get();
      mongoJobExecutionContext.setContextMap(
          commonUtility.convertMapToByteArray(contextMap, false));
      jobExecutionContextRepository.save(mongoJobExecutionContext);
    } else {
      mongoJobExecutionContext =
              StepExecutionContextEntity.builder()
              .id(String.valueOf(executionId))
              .contextMap(commonUtility.convertMapToByteArray(contextMap, false))
              .build();
      try {
        jobExecutionContextRepository.save(mongoJobExecutionContext);
      } catch (Exception e) {
        log.error("Exception while mongo step save ***** " + e.getMessage());
      }
    }
  }

  // observation: after step execution method being called based on partition we get this called
  // only once for step
  // execution method(only once)
  private ExecutionContext getExecutionContextFromStepExecution(Long stepExecutionId) {
    Assert.notNull(stepExecutionId, "ExecutionId must not be null.");
    Optional<StepExecutionContextEntity> postgresStepExecutionContextOptional =
        stepExecutionContextRepository.findById(commonUtility.convertLongToString(stepExecutionId));

    ExecutionContext executionContext = new ExecutionContext();
    if (postgresStepExecutionContextOptional.isPresent()) {
      StepExecutionContextEntity postgresStepExecutionContextModel =
          postgresStepExecutionContextOptional.get();

      Map<String, ?> contextMap =
          commonUtility.convertByteArrayToMap(postgresStepExecutionContextModel.getContextMap());
      executionContext.put("_id", postgresStepExecutionContextOptional.get().getId());
      for (Map.Entry<String, ?> entry : contextMap.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();
        String type = (String) contextMap.get(key + TYPE_SUFFIX);
        if (type != null && Number.class.isAssignableFrom(value.getClass())) {
          try {
            value =
                NumberUtils.convertNumberToTargetClass(
                    (Number) value, (Class<? extends Number>) Class.forName(type));
          } catch (Exception e) {
            log.warn("Failed to convert {} to {}", key, type);
          }
        }
        // Mongo db does not allow key name with "." character.
        executionContext.put(key.replaceAll(DOT_ESCAPE_STRING, DOT_STRING), value);
      }
    }
    return executionContext;
  }

  private ExecutionContext getExecutionContextFromJobExecution(Long jobExecutionId) {
    Assert.notNull(jobExecutionId, "ExecutionId must not be null.");
    Optional<StepExecutionContextEntity> postgresJobExecutionContextOptional =
        jobExecutionContextRepository.findById(commonUtility.convertLongToString(jobExecutionId));
    ExecutionContext executionContext = new ExecutionContext();
    if (postgresJobExecutionContextOptional.isPresent()) {
      StepExecutionContextEntity postgresJobExecutionContext =
          postgresJobExecutionContextOptional.get();

      Map<String, ?> contextMap =
          commonUtility.convertByteArrayToMap(postgresJobExecutionContext.getContextMap());
      executionContext.put("_id", postgresJobExecutionContextOptional.orElse(null).getId());
      for (Map.Entry<String, ?> entry : contextMap.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();
        String type = (String) contextMap.get(key + TYPE_SUFFIX);
        if (type != null && Number.class.isAssignableFrom(value.getClass())) {
          try {
            value =
                NumberUtils.convertNumberToTargetClass(
                    (Number) value, (Class<? extends Number>) Class.forName(type));
          } catch (Exception e) {
            log.warn("Failed to convert {} to {}", key, type);
          }
        }
        executionContext.put(key.replaceAll(DOT_ESCAPE_STRING, DOT_STRING), value);
      }
    }
    return executionContext;
  }
}
