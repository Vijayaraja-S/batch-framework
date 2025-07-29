package com.p3.batchframework.daos;

import static com.p3.batchframework.utils.Constants.*;

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
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

@Component
@Slf4j
public class JobInstanceDao extends AbstractDao
    implements org.springframework.batch.core.repository.dao.JobInstanceDao {

  private final JobInstanceRepository jobInstanceRepository;
  private final JobExecutionRepository jobExecutionRepository;

  private final CommonUtility commonUtility;
  private static final String SHA512HASH = "SHA-512";
  @PersistenceContext EntityManager entityManager;

  public JobInstanceDao(
      JobInstanceRepository jobInstanceRepository,
      JobExecutionRepository jobExecutionRepository,
      SequenceRepository sequenceRepository,
      CommonUtility commonUtility) {
    super(sequenceRepository, jobInstanceRepository, commonUtility);
    this.jobInstanceRepository = jobInstanceRepository;
    this.jobExecutionRepository = jobExecutionRepository;
    this.commonUtility = commonUtility;
  }

  @Override
  public @NonNull JobInstance createJobInstance(
      @NonNull String jobName, @NonNull JobParameters jobParameters) {
    Assert.notNull(jobName, "Job name must not be null.");
    Assert.notNull(jobParameters, "JobParameters must not be null.");
    getJobInstance(jobName, jobParameters);
    Assert.state(false, "JobInstance must not already exist");

    Long jobId = getNextId("job-instance");
    JobInstance jobInstance = new JobInstance(jobId, jobName);
    jobInstance.incrementVersion();
    Map<String, JobParameter<?>> jobParams = jobParameters.getParameters();
    Map<String, Object> paramMap = new HashMap<>(jobParams.size());
    for (Map.Entry<String, JobParameter<?>> entry : jobParams.entrySet()) {
      paramMap.put(
          entry.getKey().replaceAll(DOT_STRING, DOT_ESCAPE_STRING), entry.getValue().getValue());
    }

    JobInstanceEntity postgresJobInstanceModel =
        JobInstanceEntity.builder()
            .id(commonUtility.convertLongToString(jobId))
            .jobName(jobName)
            .jobKey(createJobKey(jobParameters))
            .jobParameters(commonUtility.convertMapToByteArray(paramMap, false))
            .version(jobInstance.getVersion())
            .build();

    jobInstanceRepository.save(postgresJobInstanceModel);

    return jobInstance;
  }

  @Override
  public @NonNull JobInstance getJobInstance(
      @NonNull String jobName, @NonNull JobParameters jobParameters) {
    Assert.notNull(jobName, "Job name must not be null.");
    Assert.notNull(jobParameters, "JobParameters must not be null.");
    String jobKey = createJobKey(jobParameters);
    return mapJobInstance(
        jobInstanceRepository.findByJobNameAndJobKey(jobName, jobKey).orElse(null));
  }

  @Override
  @Nullable
  public JobInstance getJobInstance(@Nullable Long instanceId) {
    return mapJobInstance(
        jobInstanceRepository.findById(commonUtility.convertLongToString(instanceId)).orElse(null));
  }

  @Override
  @Nullable
  public JobInstance getJobInstance(JobExecution jobExecution) {
    JobExecutionEntity jobExecutionEntity =
        jobExecutionRepository
            .findById(commonUtility.convertLongToString(jobExecution.getId()))
            .orElse(null);

    assert jobExecutionEntity != null;
    return mapJobInstance(
        jobInstanceRepository
            .findById((String.valueOf(jobExecutionEntity.getJobInstanceId())))
            .orElse(null));
  }

  @Override
  @Nullable
  public JobInstance getLastJobInstance(@NonNull String jobName) {
    List<JobInstance> jobInstances = getJobInstances(jobName, 0, 1);
    return jobInstances.isEmpty() ? null : jobInstances.get(0);
  }

  @Override
  public @NonNull List<JobInstance> getJobInstances(@NonNull String jobName, int start, int count) {
    CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
    CriteriaQuery<JobInstanceEntity> criteriaQuery =
        criteriaBuilder.createQuery(JobInstanceEntity.class);
    Root<JobInstanceEntity> postgresJobInstanceModelRoot =
        criteriaQuery.from(JobInstanceEntity.class);
    criteriaQuery.select(postgresJobInstanceModelRoot);
    Predicate c1;
    Predicate finalPredicate;
    c1 =
        criteriaBuilder.equal(
            postgresJobInstanceModelRoot.get(JobInstanceEntity.Fields.jobName), jobName);
    finalPredicate = criteriaBuilder.and(nullChecker(c1, criteriaBuilder));
    criteriaQuery.where(finalPredicate);
    criteriaQuery.orderBy(
        criteriaBuilder.desc(postgresJobInstanceModelRoot.get(JobInstanceEntity.Fields.id)));
    TypedQuery<JobInstanceEntity> query = entityManager.createQuery(criteriaQuery);
    query.setFirstResult(start * count);
    query.setMaxResults(count);
    List<JobInstanceEntity> resultList = query.getResultList();
    return mapJobInstances(resultList);
  }

  private Predicate nullChecker(Predicate criteria, CriteriaBuilder criteriaBuilder) {
    if (criteria != null) {
      return criteria;
    }
    return criteriaBuilder.conjunction();
  }

  @Override
  public @NonNull List<String> getJobNames() {
    List<JobInstanceEntity> jobInstances =
        jobInstanceRepository.findDistinctByJobName(JOB_NAME_KEY);
    List<String> jobNamesList = new ArrayList<>();
    for (JobInstanceEntity model : jobInstances) {

      jobNamesList.add(model.getJobName());
    }
    return jobNamesList;
  }

  @Override
  public @NonNull List<JobInstance> findJobInstancesByName(
      @NonNull String jobName, int start, int count) {
    String queryString =
        "SELECT * FROM postgres_job_instance WHERE id IS NOT NULL AND job_name = ? ORDER BY id DESC";

    Query query = entityManager.createNativeQuery(queryString, JobInstanceEntity.class);
    query.setParameter(1, jobName);
    query.setFirstResult(start);
    query.setMaxResults(count);

    List<JobInstanceEntity> postgresJobInstancesListModel = query.getResultList();

    List<JobInstance> jobInstances = mapJobInstances(postgresJobInstancesListModel);
    List<JobInstance> result = new ArrayList<>();

    for (JobInstance instanceEntry : jobInstances) {
      String key = instanceEntry.getJobName();
      String curJobName = key.substring(0, key.lastIndexOf("|"));
      if (curJobName.equals(jobName)) {
        result.add(instanceEntry);
      }
    }

    return result;
  }

  @Override
  public long getJobInstanceCount(@Nullable String jobName) {
    String sql = "SELECT count(*) FROM postgres_job_instance WHERE id IS NOT NULL";

    if (jobName != null) {
      sql += " AND job_name = :jobName";
    }

    Query query = entityManager.createNativeQuery(sql);

    if (jobName != null) {
      query.setParameter("jobName", jobName);
    }

    Object result = query.getSingleResult();
    return ((Number) result).longValue();
  }

  private List<JobInstance> mapJobInstances(List<JobInstanceEntity> postgresJobInstancesListModel) {
    List<JobInstance> results = new ArrayList<>();
    postgresJobInstancesListModel.forEach(i -> results.add(mapJobInstance(i)));
    return results;
  }

  private JobInstance mapJobInstance(JobInstanceEntity jobInstanceEntity) {
    JobInstance jobInstance = null;
    if (jobInstanceEntity != null) {

      Long id = Long.parseLong(jobInstanceEntity.getId());
      jobInstance = new JobInstance(id, jobInstanceEntity.getJobName());
      jobInstance.incrementVersion();
    }
    return jobInstance;
  }

  private String createJobKey(JobParameters jobParameters) {

    Map<String, JobParameter<?>> props = jobParameters.getParameters();
    StringBuilder stringBuilder = new StringBuilder();
    List<String> keys = new ArrayList<>(props.keySet());
    Collections.sort(keys);
    for (String key : keys) {
      stringBuilder.append(key).append("=").append(props.get(key).toString()).append(";");
    }
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance(SHA512HASH);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(
          "SHA-512 algorithm not available.  Fatal (should be in the JDK).");
    }

    byte[] bytes = digest.digest(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
    return String.format("%032x", new BigInteger(1, bytes));
  }
}
