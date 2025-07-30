package com.p3.batchframework.daos;

import static com.p3.batchframework.utils.Constants.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.p3.batchframework.persistence.models.JobExecutionContextEntity;
import com.p3.batchframework.persistence.models.StepExecutionContextEntity;
import com.p3.batchframework.persistence.repository.JobExecutionContextRepository;
import com.p3.batchframework.persistence.repository.JobInstanceRepository;
import com.p3.batchframework.persistence.repository.SequenceRepository;
import com.p3.batchframework.persistence.repository.StepExecutionContextRepository;
import com.p3.batchframework.utils.CommonUtility;
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

@Slf4j
@Component
public class ExecutionContextDao extends AbstractDao
    implements org.springframework.batch.core.repository.dao.ExecutionContextDao {
  private final StepExecutionContextRepository stepExecutionContextRepository;
  private final JobExecutionContextRepository jobExecutionContextRepository;
  private final CommonUtility commonUtility;

  private final ReentrantLock stepExecutionLock = new ReentrantLock();

  protected ExecutionContextDao(
      SequenceRepository sequencesRepository,
      JobInstanceRepository jobInstanceRepository,
      CommonUtility commonUtility,
      StepExecutionContextRepository stepExecutionContextRepository,
      JobExecutionContextRepository jobExecutionContextRepository) {
    super(sequencesRepository, jobInstanceRepository, commonUtility);
    this.stepExecutionContextRepository = stepExecutionContextRepository;
    this.jobExecutionContextRepository = jobExecutionContextRepository;
    this.commonUtility = commonUtility;
  }

  @Override
  public @NonNull ExecutionContext getExecutionContext(JobExecution jobExecution) {
    return getExecutionContext(jobExecution.getId(), true);
  }

  @Override
  public @NonNull ExecutionContext getExecutionContext(StepExecution stepExecution) {
    return getExecutionContext(stepExecution.getId(), false);
  }

  @Override
  public void saveExecutionContext(JobExecution jobExecution) {
    saveOrUpdateExecutionContext(
        null, jobExecution.getExecutionContext(), jobExecution.getId(), false);
  }

  @Override
  public void saveExecutionContext(StepExecution stepExecution) {
    saveOrUpdateExecutionContext(
        stepExecution.getId(),
        stepExecution.getExecutionContext(),
        stepExecution.getJobExecutionId(),
        true);
  }

  @Override
  public void saveExecutionContexts(@NonNull Collection<StepExecution> stepExecutions) {
    Assert.notNull(stepExecutions, "Attempt to save a null collection of step executions");
    for (StepExecution stepExecution : stepExecutions) {
      saveExecutionContext(stepExecution);
      saveExecutionContext(stepExecution.getJobExecution());
    }
  }

  @Override
  public void updateExecutionContext(JobExecution jobExecution) {
    saveOrUpdateExecutionContext(
        null, jobExecution.getExecutionContext(), jobExecution.getId(), false);
  }

  @Override
  public void updateExecutionContext(StepExecution stepExecution) {
    stepExecutionLock.lock();
    try {
      saveOrUpdateExecutionContext(
          stepExecution.getId(),
          stepExecution.getExecutionContext(),
          stepExecution.getJobExecutionId(),
          true);
    } finally {
      stepExecutionLock.unlock();
    }
  }

  private void saveOrUpdateExecutionContext(
      Long stepExecutionId,
      ExecutionContext executionContext,
      Long jobExecutionId,
      boolean isStepContext) {

    if (isStepContext) {
      Assert.notNull(stepExecutionId, "ExecutionId must not be null.");
    }

    Assert.notNull(executionContext, "The ExecutionContext must not be null.");

    Map<String, Object> contextMap = buildContextMap(executionContext, jobExecutionId);

    byte[] contextBytes = commonUtility.convertMapToByteArray(contextMap, false);
    String stringId = String.valueOf(stepExecutionId);

    if (isStepContext) {
      saveOrUpdateStepContextEntity(stringId, contextBytes, jobExecutionId);
    } else {
      saveOrUpdateJobContextEntity(stringId, contextBytes);
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> buildContextMap(
      ExecutionContext executionContext, Long jobExecutionId) {
    Map<String, Object> contextMap = new HashMap<>();
    ObjectMapper objectMapper = new ObjectMapper();

    for (Map.Entry<String, Object> entry : executionContext.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();

      String escapedKey = key.replaceAll(DOT_STRING, DOT_ESCAPE_STRING);
      contextMap.put(escapedKey, value);

      // Handle nested ReportData keys (only applies to step context)
      if (key.contains("ReportData") || key.contains("TableExtractionPerformanceReport")) {
        ConcurrentHashMap<String, Object> subMap =
            objectMapper.convertValue(value, ConcurrentHashMap.class);
        escapeNestedKeys(subMap);
        contextMap.put(key, subMap); // Re-add after escaping inner keys
      }

      if (value instanceof BigDecimal || value instanceof BigInteger) {
        contextMap.put(escapedKey + TYPE_SUFFIX, value.getClass().getName());
      }
    }

    // Only step context needs jobExecutionId stored
    if (jobExecutionId != null) {
      contextMap.put("jobExecutionId", jobExecutionId);
    }

    return contextMap;
  }

  private void escapeNestedKeys(ConcurrentHashMap<String, Object> subMap) {
    List<String> keysToReplace = new ArrayList<>();
    for (String key : subMap.keySet()) {
      if (key.contains(".")) {
        keysToReplace.add(key);
      }
    }

    for (String oldKey : keysToReplace) {
      Object value = subMap.remove(oldKey);
      String newKey = oldKey.replaceAll(DOT_STRING, DOT_ESCAPE_STRING);
      subMap.put(newKey, value);
    }
  }

  private void saveOrUpdateStepContextEntity(String id, byte[] contextBytes, Long jobExecutionId) {
    Optional<StepExecutionContextEntity> existing = stepExecutionContextRepository.findById(id);
    StepExecutionContextEntity entity;

    if (existing.isPresent()) {
      entity = existing.get();
      entity.setContextMap(contextBytes);
      entity.setJobExecutionId(jobExecutionId);
    } else {
      entity =
          StepExecutionContextEntity.builder()
              .id(id)
              .contextMap(contextBytes)
              .jobExecutionId(jobExecutionId)
              .build();
    }

    try {
      stepExecutionContextRepository.save(entity);
    } catch (Exception e) {
      error(e);
    }
  }

  private void saveOrUpdateJobContextEntity(String id, byte[] contextBytes) {
    Optional<JobExecutionContextEntity> existing = jobExecutionContextRepository.findById(id);
    JobExecutionContextEntity entity;

    if (existing.isPresent()) {
      entity = existing.get();
      entity.setContextMap(contextBytes);
    } else {
      entity = JobExecutionContextEntity.builder().id(id).contextMap(contextBytes).build();
    }

    try {
      jobExecutionContextRepository.save(entity);
    } catch (Exception e) {
      error(e);
    }
  }

  private ExecutionContext getExecutionContext(Long id, boolean isJobExecutionId) {
    validateJobExecutionId(id);
    if (isJobExecutionId) {
      return getExecutionContextFromJobExecution(id);
    } else {
      return getExecutionContextFromStepExecution(id);
    }
  }

  private ExecutionContext getExecutionContextFromStepExecution(Long id) {
    Optional<StepExecutionContextEntity> stepExecutionContextEntity =
        stepExecutionContextRepository.findById(commonUtility.convertLongToString(id));
    if (stepExecutionContextEntity.isEmpty()) {
      return new ExecutionContext();
    }
    StepExecutionContextEntity contextEntity = stepExecutionContextEntity.get();
    return buildExecutionContext(contextEntity.getId(), contextEntity.getContextMap());
  }

  private ExecutionContext getExecutionContextFromJobExecution(Long id) {
    Optional<JobExecutionContextEntity> optionalEntity =
        jobExecutionContextRepository.findById(commonUtility.convertLongToString(id));
    if (optionalEntity.isEmpty()) {
      return new ExecutionContext();
    }
    JobExecutionContextEntity contextEntity = optionalEntity.get();
    return buildExecutionContext(contextEntity.getId(), contextEntity.getContextMap());
  }

  private ExecutionContext buildExecutionContext(String id, byte[] contextMapByte) {
    ExecutionContext executionContext = new ExecutionContext();

    executionContext.put("_id", id);

    Map<String, ?> contextMap = commonUtility.convertByteArrayToMap(contextMapByte);

    for (Map.Entry<String, ?> entry : contextMap.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();

      if (key.endsWith(TYPE_SUFFIX)) {
        continue; // Skip metadata keys
      }

      String typeKey = key + TYPE_SUFFIX;
      String type = (String) contextMap.get(typeKey);

      // Convert number types back to target class
      if (type != null && value instanceof Number) {
        value = convertNumber(value, type, key);
      }

      // Put cleaned-up key and value into ExecutionContext
      executionContext.put(cleanKey(key), value);
    }

    return executionContext;
  }

  @SuppressWarnings("unchecked")
  private Object convertNumber(Object value, String type, String key) {
    try {
      return NumberUtils.convertNumberToTargetClass(
          (Number) value, (Class<? extends Number>) Class.forName(type));
    } catch (Exception e) {
      log.warn("Failed to convert key '{}' to type '{}': {}", key, type, e.getMessage());
      return value;
    }
  }

  private String cleanKey(String key) {
    return key.replaceAll(DOT_STRING, DOT_ESCAPE_STRING);
  }

  private void validateJobExecutionId(Long jobExecutionId) {
    if (jobExecutionId == null) {
      throw new IllegalArgumentException("ExecutionId must not be null.");
    }
  }

  private static void error(Exception e) {
    log.error("Exception while mongo step save ***** {}", e.getMessage());
  }
}
