package com.p3.batchframework.daos;


import com.p3solutions.taskhandler.daos.models.PostgresJobExecutionModel;
import com.p3solutions.taskhandler.daos.models.PostgresJobInstanceModel;
import com.p3solutions.taskhandler.daos.repositories.JobExecutionRepository;
import com.p3solutions.taskhandler.daos.repositories.JobInstanceRepository;
import com.p3solutions.taskhandler.utility.CommonUtility;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

/**
 * PostgresJobInstanceDao
 */
@Component
@Slf4j
public class JobInstanceDao extends AbstractDao implements org.springframework.batch.core.repository.dao.JobInstanceDao {

    private JobInstanceRepository jobInstanceRepository;

    private JobExecutionRepository jobExecutionRepository;

    @Autowired
    private CommonUtility commonUtility;
    private static final String SHA512HASH = "SHA-512";
    @PersistenceContext
    EntityManager entityManager;

    @Autowired
    public JobInstanceDao(JobInstanceRepository jobInstanceRepository, JobExecutionRepository jobExecutionRepository) {
        this.jobInstanceRepository = jobInstanceRepository;
        this.jobExecutionRepository = jobExecutionRepository;
    }

    @Override
    public JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");
        Assert.state(getJobInstance(jobName, jobParameters) == null, "JobInstance must not already exist");

        Long jobId = getNextId("job-instance");
        JobInstance jobInstance = new JobInstance(jobId, jobName);
        jobInstance.incrementVersion();
        Map<String, JobParameter> jobParams = jobParameters.getParameters();
        Map<String, Object> paramMap = new HashMap<>(jobParams.size());
        for (Map.Entry<String, JobParameter> entry : jobParams.entrySet()) {
            paramMap.put(entry.getKey().replaceAll(DOT_STRING, DOT_ESCAPE_STRING), entry.getValue().getValue());
        }

        PostgresJobInstanceModel postgresJobInstanceModel = PostgresJobInstanceModel.builder().id(commonUtility.convertLongToString(jobId))
                .jobName(jobName)
                .jobKey(createJobKey(jobParameters))
                .jobParameters(commonUtility.convertMapToByteArray(paramMap,false))
                .version(jobInstance.getVersion())
                .build();

        jobInstanceRepository.save(postgresJobInstanceModel);


        return jobInstance;
    }

    @Override
    @Nullable
    public JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");
        String jobKey = createJobKey(jobParameters);
        return mapJobInstance(jobInstanceRepository.findByJobNameAndJobKey(jobName, jobKey).orElse(null));
    }

    @Override
    @Nullable
    public JobInstance getJobInstance(@Nullable Long instanceId) {
        return mapJobInstance(jobInstanceRepository.findById(commonUtility.convertLongToString(instanceId)).orElse(null));
    }

    @Override
    @Nullable
    public JobInstance getJobInstance(JobExecution jobExecution) {
        PostgresJobExecutionModel postgresJobExecutionModel =
                jobExecutionRepository.findById(commonUtility.convertLongToString(jobExecution.getId())).orElse(null);


        return mapJobInstance(jobInstanceRepository.findById(
                (String.valueOf(postgresJobExecutionModel.getJobInstanceId()))).orElse(null));
    }

    @Override
    @Nullable
    public JobInstance getLastJobInstance(String jobName) {
        List<JobInstance> jobInstances = getJobInstances(jobName, 0, 1);
        return jobInstances.isEmpty() ? null : jobInstances.get(0);
    }


    @Override
    public List<JobInstance> getJobInstances(String jobName, int start, int count) {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<PostgresJobInstanceModel> criteriaQuery = criteriaBuilder.createQuery(PostgresJobInstanceModel.class);
        Root<PostgresJobInstanceModel> postgresJobInstanceModelRoot = criteriaQuery.from(PostgresJobInstanceModel.class);
        criteriaQuery.select(postgresJobInstanceModelRoot);
        Predicate c1;
        Predicate finalPredicate;
        c1 = criteriaBuilder.equal(postgresJobInstanceModelRoot.get(PostgresJobInstanceModel.Fields.jobName), jobName);
        finalPredicate = criteriaBuilder.and(nullChecker(c1, criteriaBuilder));
        criteriaQuery.where(finalPredicate);
        criteriaQuery.orderBy(criteriaBuilder.desc(postgresJobInstanceModelRoot.get(PostgresJobInstanceModel.Fields.id)));
        TypedQuery<PostgresJobInstanceModel> query = entityManager.createQuery(criteriaQuery);
        query.setFirstResult(start * count);
        query.setMaxResults(count);
        List<PostgresJobInstanceModel> resultList = query.getResultList();
        return mapJobInstances(resultList);

    }

    private Predicate nullChecker(Predicate criteria, CriteriaBuilder criteriaBuilder) { //TESTED
        if (criteria != null) {
            return criteria;
        }
        return criteriaBuilder.conjunction();
    }

    @Override
    public List<String> getJobNames() {
        List<PostgresJobInstanceModel> jobInstances = jobInstanceRepository.findDistinctByJobName(JOB_NAME_KEY);
        List<String> jobNamesList = new ArrayList<>();
        for (PostgresJobInstanceModel model : jobInstances) {

            jobNamesList.add(model.getJobName());
        }
        return jobNamesList;
    }

    @Override
    public List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
        StringBuilder queryString = new StringBuilder("SELECT * FROM postgres_job_instance WHERE id IS NOT NULL ");
        queryString.append(" AND (job_name =:jobName )");
        queryString.append(" order by id desc");
        Query query = entityManager.createNativeQuery(queryString.toString());
        query.setParameter("jobName", jobName);

        List<PostgresJobInstanceModel> postgresJobInstancesListModel = query.getResultList();
        List<JobInstance> result = new ArrayList<>();
        List<JobInstance> jobInstances = mapJobInstances(postgresJobInstancesListModel);
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
    public int getJobInstanceCount(@Nullable String jobName) {
        StringBuilder queryString = new StringBuilder("SELECT count(*) FROM postgres_job_instance WHERE id IS NOT NULL ");
        queryString.append(" AND (job_name =:jobName)");
        Query query = entityManager.createNativeQuery(queryString.toString());
        query.setParameter("jobName", jobName);
        List<PostgresJobInstanceModel> taskList = query.getResultList();
        return taskList.size();

    }

    private List<JobInstance> mapJobInstances(List<PostgresJobInstanceModel> postgresJobInstancesListModel) {
        List<JobInstance> results = new ArrayList<>();
        postgresJobInstancesListModel.forEach(i -> results.add(mapJobInstance(i)));
        return results;
    }

    private JobInstance mapJobInstance(PostgresJobInstanceModel postgresJobInstanceModel) {
        JobInstance jobInstance = null;
        if (postgresJobInstanceModel != null) {

            Long id = Long.parseLong(postgresJobInstanceModel.getId());
            jobInstance = new JobInstance(id, postgresJobInstanceModel.getJobName());
            jobInstance.incrementVersion();
        }
        return jobInstance;
    }

    private String createJobKey(JobParameters jobParameters) {

        Map<String, JobParameter> props = jobParameters.getParameters();
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
            throw new IllegalStateException("SHA-512 algorithm not available.  Fatal (should be in the JDK).");
        }

        byte[] bytes = digest.digest(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
        return String.format("%032x", new BigInteger(1, bytes));

    }

}
