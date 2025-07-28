package com.p3.batchframework.daos;

import com.p3.batchframework.persistence.models.JobExecutionEntity;
import com.p3.batchframework.persistence.models.StepExecutionEntity;
import com.p3.batchframework.persistence.repository.JobExecutionRepository;
import com.p3.batchframework.persistence.repository.JobInstanceRepository;
import com.p3.batchframework.persistence.repository.SequenceRepository;
import com.p3.batchframework.persistence.repository.StepExecutionRepository;
import com.p3.batchframework.utils.CommonUtility;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import java.util.Collection;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

@Slf4j
@Component
public class StepExecutionDao extends AbstractDao
    implements org.springframework.batch.core.repository.dao.StepExecutionDao, InitializingBean {
  private final StepExecutionRepository stepExecutionRepository;
  private final JobExecutionRepository jobExecutionRepository;
  private final CommonUtility commonUtility;
  @PersistenceContext EntityManager entityManager;

  protected StepExecutionDao(
      SequenceRepository sequencesRepository,
      JobInstanceRepository jobInstanceRepository,
      CommonUtility commonUtility,
      StepExecutionRepository stepExecutionRepository,
      JobExecutionRepository jobExecutionRepository,
      CommonUtility commonUtility1) {
    super(sequencesRepository, jobInstanceRepository, commonUtility);
    this.stepExecutionRepository = stepExecutionRepository;
    this.jobExecutionRepository = jobExecutionRepository;
    this.commonUtility = commonUtility1;
  }

  @Override
  public long countStepExecutions(JobInstance jobInstance, @NonNull String stepName) {
    CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
    CriteriaQuery<Long> criteriaQuery = criteriaBuilder.createQuery(Long.class);
    Root<StepExecutionEntity> root = criteriaQuery.from(StepExecutionEntity.class);
    criteriaQuery.select(criteriaBuilder.count(root));
    criteriaQuery.where(
        criteriaBuilder.equal(root.get("jobExecutionId"), jobInstance.getInstanceId()),
        criteriaBuilder.equal(root.get("stepName"), stepName));

    Long count = entityManager.createQuery(criteriaQuery).getSingleResult();
    return count != null ? count.intValue() : 0;
  }

  @Override
  public void saveStepExecution(StepExecution stepExecution) {
    Assert.isNull(
        stepExecution.getId(),
        "to-be-saved (not updated) StepExecution can't already have an id assigned");
    Assert.isNull(
        stepExecution.getVersion(),
        "to-be-saved (not updated) StepExecution can't already have a version assigned");

    validateStepExecution(stepExecution);

    stepExecution.setId(getNextId("step_execution"));
    stepExecution.incrementVersion();

    StepExecutionEntity stepExecutionEntity = toMongoStepExecution(stepExecution);
    stepExecutionRepository.save(stepExecutionEntity);
  }

  private StepExecutionEntity toMongoStepExecution(StepExecution stepExecution) {
    return StepExecutionEntity.builder()
        .id(stepExecution.getId().toString())
        .stepName(stepExecution.getStepName())
        .jobExecutionId(stepExecution.getJobExecutionId())
        .startTime(stepExecution.getStartTime())
        .status(stepExecution.getStatus().toString())
        .commitCount(stepExecution.getCommitCount())
        .readCount(stepExecution.getReadCount())
        .filterCount(stepExecution.getFilterCount())
        .writeCount(stepExecution.getWriteCount())
        .exitCode(stepExecution.getExitStatus().getExitCode())
        .exitMessage(stepExecution.getExitStatus().getExitDescription())
        .readSkipCount(stepExecution.getReadSkipCount())
        .writeSkipCount(stepExecution.getWriteSkipCount())
        .processSkipCount(stepExecution.getProcessSkipCount())
        .rollbackCount(stepExecution.getRollbackCount())
        .lastUpdated(stepExecution.getLastUpdated())
        .version(stepExecution.getVersion())
        .build();
  }

  @Override
  public void saveStepExecutions(@NonNull Collection<StepExecution> stepExecutions) {
    Assert.notNull(stepExecutions, "Attempt to save an null collect of step executions");
    for (StepExecution stepExecution : stepExecutions) {
      saveStepExecution(stepExecution);
    }
  }

  @Override
  public void updateStepExecution(@NonNull StepExecution stepExecution) {
    StepExecutionEntity stepExecutionEntity = toMongoStepExecution(stepExecution);
    stepExecutionRepository.save(stepExecutionEntity);
    stepExecution.incrementVersion();
  }

  @Override
  @Nullable
  public StepExecution getStepExecution(JobExecution jobExecution, @NonNull Long stepExecutionId) {
    StepExecutionEntity stepExecutionEntity =
        stepExecutionRepository
            .findByIdAndJobExecutionId(
                commonUtility.convertLongToString(stepExecutionId),
                commonUtility.convertLongToString(jobExecution.getId()))
            .orElse(null);

    return mapStepExecution(stepExecutionEntity, jobExecution);
  }

  @Override
  public StepExecution getLastStepExecution(JobInstance jobInstance, @NonNull String stepName) {
    StepExecution latest = null;
    List<StepExecutionEntity> stepExecutionEntities = null;
    List<JobExecutionEntity> postgresJobExecutionModelOptional =
        jobExecutionRepository.findByJobInstanceIdOrderById((jobInstance.getInstanceId()));
    if (!postgresJobExecutionModelOptional.isEmpty()) {
      int position = 0;
      int size = postgresJobExecutionModelOptional.size();
      if (postgresJobExecutionModelOptional.size() >= 2) position = size - 2;
      while (position >= 0) {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<StepExecutionEntity> criteriaQuery =
            criteriaBuilder.createQuery(StepExecutionEntity.class);
        Root<StepExecutionEntity> postgresStepExecutionModelRoot =
            criteriaQuery.from(StepExecutionEntity.class);
        criteriaQuery.select(postgresStepExecutionModelRoot);
        Predicate c1;
        Predicate c2;
        Predicate finalPredicate;
        c1 =
            criteriaBuilder.equal(
                postgresStepExecutionModelRoot.get(
                        StepExecutionEntity.Fields.jobExecutionId),
                Long.parseLong(postgresJobExecutionModelOptional.get(position).getId()));
        c2 =
            criteriaBuilder.equal(
                postgresStepExecutionModelRoot.get(StepExecutionEntity.Fields.stepName),
                stepName);
        finalPredicate = criteriaBuilder.and(c1, c2);
        criteriaQuery.where(finalPredicate);
        criteriaQuery.orderBy(
            criteriaBuilder.desc(
                postgresStepExecutionModelRoot.get(StepExecutionEntity.Fields.id)));
        TypedQuery<StepExecutionEntity> query = entityManager.createQuery(criteriaQuery);
        stepExecutionEntities = query.getResultList();

        if (stepExecutionEntities != null && !stepExecutionEntities.isEmpty())
          break;
        else position--;
      }
      if (CollectionUtils.isEmpty(stepExecutionEntities)) {
        return null;
      } else {
        StepExecutionEntity postgresStepExecutionModel =
            stepExecutionEntities.get(stepExecutionEntities.size() - 1);
        if (stepExecutionEntities.size() > 1) {
          throw new IllegalStateException("There must be at most one latest job execution");
        }

        JobExecution jobExecution = mapJobExecution(jobInstance, postgresStepExecutionModel);
        return mapStepExecution(postgresStepExecutionModel, jobExecution);
      }

    } else {
      return latest;
    }
  }

  private JobExecution mapJobExecution(
      JobInstance jobInstance, StepExecutionEntity postgresStepExecutionMapperBean) {
    if (postgresStepExecutionMapperBean == null) {
      return null;
    }
    Long id = postgresStepExecutionMapperBean.getJobExecutionId();
    JobExecution jobExecution;
    if (jobInstance == null) {
      jobExecution = new JobExecution(id);
    } else {
      JobParameters jobParameters = getJobParameters(jobInstance.getId());
      jobExecution = new JobExecution(jobInstance, id, jobParameters);
    }
    jobExecution.setStartTime(postgresStepExecutionMapperBean.getStartTime());
    jobExecution.setEndTime(postgresStepExecutionMapperBean.getEndTime());
    jobExecution.setStatus(BatchStatus.valueOf(postgresStepExecutionMapperBean.getStatus()));
    jobExecution.setExitStatus(
        new ExitStatus(
            (postgresStepExecutionMapperBean.getExitCode()),
            postgresStepExecutionMapperBean.getExitMessage()));
    jobExecution.setLastUpdated(postgresStepExecutionMapperBean.getLastUpdated());
    jobExecution.setVersion((int) postgresStepExecutionMapperBean.getVersion());
    return jobExecution;
  }

  private StepExecution mapStepExecution(
      StepExecutionEntity postgresStepExecutionMapperBean, JobExecution jobExecution) {
    if (postgresStepExecutionMapperBean == null) {
      return null;
    }
    StepExecution stepExecution =
        new StepExecution(
            (postgresStepExecutionMapperBean.getStepName()),
            jobExecution,
            commonUtility.convertStringToLong(postgresStepExecutionMapperBean.getId()));
    stepExecution.setStartTime(postgresStepExecutionMapperBean.getStartTime());
    stepExecution.setEndTime(postgresStepExecutionMapperBean.getEndTime());
    stepExecution.setStatus(BatchStatus.valueOf(postgresStepExecutionMapperBean.getStatus()));
    stepExecution.setCommitCount(postgresStepExecutionMapperBean.getCommitCount());
    stepExecution.setReadCount(postgresStepExecutionMapperBean.getReadCount());
    stepExecution.setFilterCount(postgresStepExecutionMapperBean.getFilterCount());
    stepExecution.setWriteCount(postgresStepExecutionMapperBean.getWriteCount());
    stepExecution.setExitStatus(
        new ExitStatus(
            postgresStepExecutionMapperBean.getExitCode(),
            postgresStepExecutionMapperBean.getExitMessage()));
    stepExecution.setReadSkipCount(postgresStepExecutionMapperBean.getReadSkipCount());
    stepExecution.setWriteSkipCount(postgresStepExecutionMapperBean.getWriteSkipCount());
    stepExecution.setProcessSkipCount(postgresStepExecutionMapperBean.getProcessSkipCount());
    stepExecution.setRollbackCount(postgresStepExecutionMapperBean.getRollbackCount());
    stepExecution.setLastUpdated(postgresStepExecutionMapperBean.getLastUpdated());
    stepExecution.setVersion((int) postgresStepExecutionMapperBean.getVersion());
    return stepExecution;
  }

  @Override
  public void addStepExecutions(JobExecution jobExecution) {
    List<StepExecutionEntity> postgresStepExecutionModelList =
        stepExecutionRepository.findByJobExecutionId(jobExecution.getId());
    postgresStepExecutionModelList.forEach(i -> mapStepExecution(i, jobExecution));
  }

  private void validateStepExecution(StepExecution stepExecution) {
    Assert.notNull(stepExecution, "StepExecution cannot be null.");
    Assert.notNull(stepExecution.getStepName(), "StepExecution step name cannot be null.");
    Assert.notNull(stepExecution.getStartTime(), "StepExecution start time cannot be null.");
    Assert.notNull(stepExecution.getStatus(), "StepExecution status cannot be null.");
  }

  @Override
  public void afterPropertiesSet() {}
}
