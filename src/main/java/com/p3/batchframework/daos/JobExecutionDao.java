package com.p3.batchframework.daos;



import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

/**
 * MongoJobExecutionDao
 */
@Slf4j
@Component
public class JobExecutionDao extends AbstractDao implements org.springframework.batch.core.repository.dao.JobExecutionDao {
    private JobExecutionRepository jobExecutionRepository;
    private JobInstanceRepository jobInstanceRepository;
    @Autowired
    private CommonUtility commonUtility;
    @PersistenceContext
    EntityManager entityManager;


    @Autowired
    public JobExecutionDao(JobExecutionRepository jobExecutionRepository, JobInstanceRepository jobInstanceRepository) {
        this.jobExecutionRepository = jobExecutionRepository;
        this.jobInstanceRepository = jobInstanceRepository;
    }

    @Override
    public void saveJobExecution(JobExecution jobExecution) {
        validateJobExecution(jobExecution);
        jobExecution.incrementVersion();
        //reference of doc name
        Long id = getNextId("job_execution");
        save(jobExecution, id);
    }

    @Override
    public void updateJobExecution(JobExecution jobExecution) {
        validateJobExecution(jobExecution);
        Long jobExecutionId = jobExecution.getId();
        Assert.notNull(jobExecutionId,
                "JobExecution ID cannot be null. JobExecution must be saved before it can be updated");
        Assert.notNull(jobExecution.getVersion(),
                "JobExecution version cannot be null. JobExecution must be saved before it can be updated");


        jobExecutionRepository.findById(commonUtility.convertLongToString(jobExecutionId)).orElseThrow(() ->
                new NoSuchObjectException(String.format("Invalid JobExecution, ID %s not found.", jobExecutionId)));

        PostgresJobExecutionModel postgresJobExecutionModel = toMongoJobExecution(jobExecution);

        jobExecutionRepository.save(postgresJobExecutionModel);

        jobExecution.incrementVersion();
    }

    private PostgresJobExecutionModel toMongoJobExecution(JobExecution jobExecution) {
        return PostgresJobExecutionModel.builder()
                .id(commonUtility.convertLongToString(jobExecution.getId()))
                .jobInstanceId((jobExecution.getJobId()))
                .startTime(jobExecution.getStartTime()).endTime(jobExecution.getEndTime())
                .status(jobExecution.getStatus().toString()).exitCode(jobExecution.getExitStatus().getExitCode())
                .exitMessage(jobExecution.getExitStatus().getExitDescription()).createTime(jobExecution.getCreateTime())
                .lastUpdated(jobExecution.getLastUpdated()).version(jobExecution.getVersion()).build();

    }


    @Override
    public List<JobExecution> findJobExecutions(JobInstance jobInstance) {
        List<PostgresJobExecutionModel> jobExecutionModels = jobExecutionRepository.findByJobInstanceIdWithLIMIT(jobInstance.getInstanceId());
        List<JobExecution> result = new ArrayList<>();
        jobExecutionModels.forEach(i -> result.add(mapJobExecution(i)));
        return result;
    }

    @Nullable
    @Override
    public JobExecution getLastJobExecution(@Nullable JobInstance jobInstance) {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<PostgresJobExecutionModel> criteriaQuery = criteriaBuilder.createQuery(PostgresJobExecutionModel.class);
        Root<PostgresJobExecutionModel> postgresJobExecutionModelRoot = criteriaQuery.from(PostgresJobExecutionModel.class);
        criteriaQuery.select(postgresJobExecutionModelRoot);
        Predicate c1 = criteriaBuilder.equal(postgresJobExecutionModelRoot.get(PostgresJobExecutionModel.Fields.jobInstanceId), jobInstance.getInstanceId());
        criteriaQuery.where(c1);
        criteriaQuery.orderBy(criteriaBuilder.desc(postgresJobExecutionModelRoot.get(PostgresJobExecutionModel.Fields.createTime)));
        TypedQuery<PostgresJobExecutionModel> query = entityManager.createQuery(criteriaQuery);
        query.setMaxResults(1);
        List<PostgresJobExecutionModel> postgresJobExecutionModelList = query.getResultList();
        if (postgresJobExecutionModelList.isEmpty()) {
            return null;
        } else {
            PostgresJobExecutionModel postgresJobExecutionModel = postgresJobExecutionModelList.get(0);

            if (postgresJobExecutionModelList.size() > 1) {
                throw new IllegalStateException("There must be at most one latest job execution");
            }
            return mapJobExecution(postgresJobExecutionModel, jobInstance);
        }
    }

    @Override
    public Set<JobExecution> findRunningJobExecutions(String jobName) {

        List<PostgresJobInstanceModel> postgresJobInstanceModelList = jobInstanceRepository.findByJobName(jobName);
        List<String> ids = new ArrayList<>();

        postgresJobInstanceModelList.forEach(i -> ids.add(i.getId()));


        StringBuilder queryString = new StringBuilder("SELECT * FROM job_execution WHERE id IS NOT NULL ");
        queryString.append(" AND id IN (?1) )");
        queryString.append(" AND (end_time IS NULL)");
        queryString.append(" order by id desc");
        Query query = entityManager.createNativeQuery(queryString.toString());
        query.setParameter(1, ids);
        List<PostgresJobExecutionModel> postgresJobExecutionModelList = query.getResultList();
        Set<JobExecution> result = new HashSet<>();
        postgresJobExecutionModelList.forEach(i -> result.add(mapJobExecution(i)));
        return result;
    }

    @Override
    @Nullable
    public JobExecution getJobExecution(Long executionId) {
        return mapJobExecution(jobExecutionRepository.findById(commonUtility.convertLongToString(executionId)).orElse(null));
    }

    @Override
    public void synchronizeStatus(JobExecution jobExecution) {
        Long id = jobExecution.getId();
        Optional<PostgresJobExecutionModel> mongoJobExecutionOptional =
                jobExecutionRepository.findById(commonUtility.convertLongToString(id));


        PostgresJobExecutionModel postgresJobExecutionModel = mongoJobExecutionOptional.get();


        int currentVersion = postgresJobExecutionModel != null ? postgresJobExecutionModel.getVersion() : 0;
        if (currentVersion != jobExecution.getVersion()) {
            if (postgresJobExecutionModel == null) {
                postgresJobExecutionModel = save(jobExecution, id);
            }

            String status = postgresJobExecutionModel.getStatus();
            jobExecution.upgradeStatus(BatchStatus.valueOf(status));
            jobExecution.setVersion(currentVersion);
        }
    }

    private PostgresJobExecutionModel save(JobExecution jobExecution, Long id) {
        jobExecution.setId(id);
        PostgresJobExecutionModel postgresJobExecutionModel1 = null;
        try {
            PostgresJobExecutionModel postgresJobExecutionModel = toMongoJobExecution(jobExecution);


            postgresJobExecutionModel1 = jobExecutionRepository.save(postgresJobExecutionModel);
        } catch (Exception e) {
            log.error("Unexpected error Occurred: {}",  e.getMessage());         }
        return postgresJobExecutionModel1;
    }

    private void validateJobExecution(JobExecution jobExecution) {
        Assert.notNull(jobExecution, "JobExecution cannot be null.");
        Assert.notNull(jobExecution.getJobId(), "JobExecution Job-Id cannot be null.");
        Assert.notNull(jobExecution.getStatus(), "JobExecution status cannot be null.");
        Assert.notNull(jobExecution.getCreateTime(), "JobExecution create time cannot be null");
    }

    private JobExecution mapJobExecution(PostgresJobExecutionModel postgresJobExecutionModel, JobInstance jobInstance) {

        if (postgresJobExecutionModel == null) {
            return null;
        }


        Long id = commonUtility.convertStringToLong(postgresJobExecutionModel.getId());
        Long jobInstanceId = postgresJobExecutionModel.getJobInstanceId();
        JobParameters jobParameters;
        JobExecution jobExecution;
        if (jobInstance == null) {
            jobParameters = getJobParameters(jobInstanceId);
            jobExecution = new JobExecution(id, jobParameters, null);
        } else {
            jobParameters = getJobParameters(jobInstance.getId());
            jobExecution = new JobExecution(jobInstance, id, jobParameters, null);
        }
        jobExecution.setStartTime(postgresJobExecutionModel.getStartTime());
        jobExecution.setEndTime(postgresJobExecutionModel.getEndTime());
        jobExecution.setStatus(BatchStatus.valueOf((postgresJobExecutionModel.getStatus())));
        jobExecution.setExitStatus(
                new ExitStatus(postgresJobExecutionModel.getExitCode(), postgresJobExecutionModel.getExitMessage()));
        jobExecution.setCreateTime(postgresJobExecutionModel.getCreateTime());
        jobExecution.setLastUpdated(postgresJobExecutionModel.getLastUpdated());
        jobExecution.setVersion(postgresJobExecutionModel.getVersion());
        return jobExecution;
    }

    private JobExecution mapJobExecution(PostgresJobExecutionModel postgresJobExecutionModel) {
        return mapJobExecution(postgresJobExecutionModel, null);
    }

}
