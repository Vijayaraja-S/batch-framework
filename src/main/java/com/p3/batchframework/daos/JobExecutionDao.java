package com.p3.batchframework.daos;

import com.p3.batchframework.persistence.models.JobExecutionEntity;
import com.p3.batchframework.persistence.models.JobInstanceEntity;
import com.p3.batchframework.persistence.repository.JobExecutionRepository;
import com.p3.batchframework.persistence.repository.JobInstanceRepository;
import com.p3.batchframework.persistence.repository.SequenceRepository;
import com.p3.batchframework.utils.CommonUtility;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import java.util.*;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

@Slf4j
@Component
public class JobExecutionDao extends AbstractDao
    implements org.springframework.batch.core.repository.dao.JobExecutionDao {
  private final JobExecutionRepository jobExecutionRepository;
  private final JobInstanceRepository jobInstanceRepository;
  private final CommonUtility commonUtility;
  @PersistenceContext EntityManager entityManager;

  protected JobExecutionDao(
      SequenceRepository sequencesRepository,
      JobInstanceRepository jobInstanceRepository,
      CommonUtility commonUtility,
      JobExecutionRepository jobExecutionRepository) {
    super(sequencesRepository, jobInstanceRepository, commonUtility);
    this.jobExecutionRepository = jobExecutionRepository;
    this.jobInstanceRepository = jobInstanceRepository;
    this.commonUtility = commonUtility;
  }

  @Override
  public void saveJobExecution(@NonNull JobExecution jobExecution) {
    validateJobExecution(jobExecution);
    jobExecution.incrementVersion();
    Long id = getNextId("job_execution");
    save(jobExecution, id);
  }

  @Override
  public void updateJobExecution(@NonNull JobExecution jobExecution) {
    validateJobExecution(jobExecution);
    Long jobExecutionId = jobExecution.getId();
    Assert.notNull(
        jobExecutionId,
        "JobExecution ID cannot be null. JobExecution must be saved before it can be updated");
    Assert.notNull(
        jobExecution.getVersion(),
        "JobExecution version cannot be null. JobExecution must be saved before it can be updated");

    jobExecutionRepository
        .findById(commonUtility.convertLongToString(jobExecutionId))
        .orElseThrow(
            () ->
                new NoSuchObjectException(
                    String.format("Invalid JobExecution, ID %s not found.", jobExecutionId)));

    JobExecutionEntity jobExecutionEntity = getJobExecutionEntity(jobExecution);

    jobExecutionRepository.save(jobExecutionEntity);

    jobExecution.incrementVersion();
  }

  private JobExecutionEntity getJobExecutionEntity(JobExecution jobExecution) {
    return JobExecutionEntity.builder()
        .id(commonUtility.convertLongToString(jobExecution.getId()))
        .jobInstanceId((jobExecution.getJobId()))
        .startTime(jobExecution.getStartTime())
        .endTime(jobExecution.getEndTime())
        .status(jobExecution.getStatus().toString())
        .exitCode(jobExecution.getExitStatus().getExitCode())
        .exitMessage(jobExecution.getExitStatus().getExitDescription())
        .createTime(jobExecution.getCreateTime())
        .lastUpdated(jobExecution.getLastUpdated())
        .version(jobExecution.getVersion())
        .build();
  }

  @Override
  public @NonNull List<JobExecution> findJobExecutions(JobInstance jobInstance) {
    Optional<JobExecutionEntity> jobExecutionModels =
        jobExecutionRepository.findFirstByJobInstanceIdOrderByIdDesc(jobInstance.getInstanceId());
    List<JobExecution> result = new ArrayList<>();
    jobExecutionModels.ifPresent(
        jobExecutionEntity -> result.add(mapJobExecution(jobExecutionEntity)));
    return result;
  }

  @Nullable
  @Override
  public JobExecution getLastJobExecution(@Nullable JobInstance jobInstance) {
    CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
    CriteriaQuery<JobExecutionEntity> criteriaQuery =
        criteriaBuilder.createQuery(JobExecutionEntity.class);
    Root<JobExecutionEntity> postgresJobExecutionModelRoot =
        criteriaQuery.from(JobExecutionEntity.class);
    criteriaQuery.select(postgresJobExecutionModelRoot);
    assert jobInstance != null;
    Predicate c1 =
        criteriaBuilder.equal(
            postgresJobExecutionModelRoot.get(JobExecutionEntity.Fields.jobInstanceId),
            jobInstance.getInstanceId());
    criteriaQuery.where(c1);
    criteriaQuery.orderBy(
        criteriaBuilder.desc(
            postgresJobExecutionModelRoot.get(JobExecutionEntity.Fields.createTime)));
    TypedQuery<JobExecutionEntity> query = entityManager.createQuery(criteriaQuery);
    query.setMaxResults(1);
    List<JobExecutionEntity> postgresJobExecutionModelList = query.getResultList();
    if (postgresJobExecutionModelList.isEmpty()) {
      return null;
    } else {
      JobExecutionEntity postgresJobExecutionModel = postgresJobExecutionModelList.get(0);

      if (postgresJobExecutionModelList.size() > 1) {
        throw new IllegalStateException("There must be at most one latest job execution");
      }
      return mapJobExecution(postgresJobExecutionModel, jobInstance);
    }
  }

  @Override
  public @NonNull Set<JobExecution> findRunningJobExecutions(@NonNull String jobName) {

    List<JobInstanceEntity> postgresJobInstanceModelList =
        jobInstanceRepository.findByJobName(jobName);
    List<String> ids = new ArrayList<>();

    postgresJobInstanceModelList.forEach(i -> ids.add(i.getId()));

    String queryString =
        "SELECT * FROM job_execution WHERE id IS NOT NULL "
            + " AND id IN (?1) )"
            + " AND (end_time IS NULL)"
            + " order by id desc";
    Query query = entityManager.createNativeQuery(queryString);
    query.setParameter(1, ids);
    List<JobExecutionEntity> postgresJobExecutionModelList = query.getResultList();
    Set<JobExecution> result = new HashSet<>();
    postgresJobExecutionModelList.forEach(i -> result.add(mapJobExecution(i)));
    return result;
  }

  @Override
  @Nullable
  public JobExecution getJobExecution(@NonNull Long executionId) {
    return mapJobExecution(
        jobExecutionRepository
            .findById(commonUtility.convertLongToString(executionId))
            .orElse(null));
  }

  @Override
  public void synchronizeStatus(JobExecution jobExecution) {
    Long id = jobExecution.getId();
    Optional<JobExecutionEntity> optionalJobExecutionEntity =
        jobExecutionRepository.findById(commonUtility.convertLongToString(id));
    JobExecutionEntity jobExecutionEntity;
    if (optionalJobExecutionEntity.isPresent()) {
      jobExecutionEntity = optionalJobExecutionEntity.get();

      int currentVersion = jobExecutionEntity.getVersion();

      if (currentVersion != jobExecution.getVersion()) {

        String status = jobExecutionEntity.getStatus();
        jobExecution.upgradeStatus(BatchStatus.valueOf(status));
        jobExecution.setVersion(currentVersion);
      }
    }
  }

  private JobExecutionEntity save(JobExecution jobExecution, Long id) {
    jobExecution.setId(id);
    JobExecutionEntity postgresJobExecutionModel1 = null;
    try {
      JobExecutionEntity postgresJobExecutionModel = getJobExecutionEntity(jobExecution);

      postgresJobExecutionModel1 = jobExecutionRepository.save(postgresJobExecutionModel);
    } catch (Exception e) {
      log.error("Unexpected error Occurred: {}", e.getMessage());
    }
    return postgresJobExecutionModel1;
  }

  private void validateJobExecution(JobExecution jobExecution) {
    Assert.notNull(jobExecution, "JobExecution cannot be null.");
    Assert.notNull(jobExecution.getJobId(), "JobExecution Job-Id cannot be null.");
    Assert.notNull(jobExecution.getStatus(), "JobExecution status cannot be null.");
    Assert.notNull(jobExecution.getCreateTime(), "JobExecution create time cannot be null");
  }

  private JobExecution mapJobExecution(
      JobExecutionEntity postgresJobExecutionModel, JobInstance jobInstance) {

    if (postgresJobExecutionModel == null) {
      return null;
    }

    Long id = commonUtility.convertStringToLong(postgresJobExecutionModel.getId());
    Long jobInstanceId = postgresJobExecutionModel.getJobInstanceId();
    JobParameters jobParameters;
    JobExecution jobExecution;
    if (jobInstance == null) {
      jobParameters = getJobParameters(jobInstanceId);
      jobExecution = new JobExecution(id, jobParameters);
    } else {
      jobParameters = getJobParameters(jobInstance.getId());
      jobExecution = new JobExecution(jobInstance, id, jobParameters);
    }
    jobExecution.setStartTime(postgresJobExecutionModel.getStartTime());
    jobExecution.setEndTime(postgresJobExecutionModel.getEndTime());
    jobExecution.setStatus(BatchStatus.valueOf((postgresJobExecutionModel.getStatus())));
    jobExecution.setExitStatus(
        new ExitStatus(
            postgresJobExecutionModel.getExitCode(), postgresJobExecutionModel.getExitMessage()));
    jobExecution.setCreateTime(postgresJobExecutionModel.getCreateTime());
    jobExecution.setLastUpdated(postgresJobExecutionModel.getLastUpdated());
    jobExecution.setVersion(postgresJobExecutionModel.getVersion());
    return jobExecution;
  }

  private JobExecution mapJobExecution(JobExecutionEntity jobExecutionEntity) {
    return mapJobExecution(jobExecutionEntity, null);
  }
}
