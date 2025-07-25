package com.p3.batchframework.daos;


import com.p3solutions.taskhandler.daos.models.PostgresJobExecutionModel;
import com.p3solutions.taskhandler.daos.models.PostgresStepExecutionModel;
import com.p3solutions.taskhandler.daos.repositories.JobExecutionRepository;
import com.p3solutions.taskhandler.daos.repositories.StepExecutionRepository;
import com.p3solutions.taskhandler.utility.CommonUtility;
import java.util.Collection;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * MongoStepExecutionRepository
 */
@Slf4j
@Component
public class StepExecutionDao extends AbstractDao implements org.springframework.batch.core.repository.dao.StepExecutionDao, InitializingBean {

    private StepExecutionRepository stepExecutionRepository;
    @Autowired
    private JobExecutionRepository jobExecutionRepository;
    @Autowired
    private CommonUtility commonUtility;
    @PersistenceContext
    EntityManager entityManager;

    @Autowired
    public StepExecutionDao(StepExecutionRepository stepExecutionRepository) {
        this.stepExecutionRepository = stepExecutionRepository;
    }
    @Override
    public int countStepExecutions(JobInstance jobInstance, String stepName) {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> criteriaQuery = criteriaBuilder.createQuery(Long.class);
        Root<PostgresStepExecutionModel> root = criteriaQuery.from(PostgresStepExecutionModel.class);
        criteriaQuery.select(criteriaBuilder.count(root));
        criteriaQuery.where(
                criteriaBuilder.equal(root.get("jobExecutionId"), jobInstance.getInstanceId()),
                criteriaBuilder.equal(root.get("stepName"), stepName)
        );

        Long count = entityManager.createQuery(criteriaQuery).getSingleResult();
        return count != null ? count.intValue() : 0;
    }
    @Override
    public void saveStepExecution(StepExecution stepExecution) {
        Assert.isNull(stepExecution.getId(),
                "to-be-saved (not updated) StepExecution can't already have an id assigned");
        Assert.isNull(stepExecution.getVersion(),
                "to-be-saved (not updated) StepExecution can't already have a version assigned");

        validateStepExecution(stepExecution);

        stepExecution.setId(getNextId("step_execution"));
        stepExecution.incrementVersion();

        PostgresStepExecutionModel postgresStepExecutionModel = toMongoStepExecution(stepExecution);
        stepExecutionRepository.save(postgresStepExecutionModel);

    }

    private PostgresStepExecutionModel toMongoStepExecution(StepExecution stepExecution) {
        return PostgresStepExecutionModel.builder()
                .id(stepExecution.getId().toString()).stepName(stepExecution.getStepName()).jobExecutionId(stepExecution.getJobExecutionId())
                .startTime(stepExecution.getStartTime()).status(stepExecution.getStatus().toString())
                .commitCount(stepExecution.getCommitCount()).readCount(stepExecution.getReadCount())
                .filterCount(stepExecution.getFilterCount()).writeCount(stepExecution.getWriteCount())
                .exitCode(stepExecution.getExitStatus().getExitCode()).exitMessage(stepExecution.getExitStatus().getExitDescription())
                .readSkipCount(stepExecution.getReadSkipCount()).writeSkipCount(stepExecution.getWriteSkipCount())
                .processSkipCount(stepExecution.getProcessSkipCount()).rollbackCount(stepExecution.getRollbackCount())
                .lastUpdated(stepExecution.getLastUpdated()).version(stepExecution.getVersion()).build();
    }

    @Override
    public void saveStepExecutions(Collection<StepExecution> stepExecutions) {
        Assert.notNull(stepExecutions, "Attempt to save an null collect of step executions");
        for (StepExecution stepExecution : stepExecutions) {
            saveStepExecution(stepExecution);
        }
    }

    @Override
    public void updateStepExecution(StepExecution stepExecution) {
        PostgresStepExecutionModel postgresStepExecutionMapperBean = toMongoStepExecution(stepExecution);
        stepExecutionRepository.save(postgresStepExecutionMapperBean);
        stepExecution.incrementVersion();
    }

    @Override
    @Nullable
    public StepExecution getStepExecution(JobExecution jobExecution, Long stepExecutionId) {
        PostgresStepExecutionModel postgresStepExecutionModel =
                stepExecutionRepository.findByIdAndJobExecutionId(commonUtility.convertLongToString(stepExecutionId),
                        commonUtility.convertLongToString(jobExecution.getId())).orElse(null);

        return mapStepExecution(postgresStepExecutionModel, jobExecution);

    }


    @Override
    public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
        StepExecution latest = null;
        List<PostgresStepExecutionModel> postgresStepExecutionModelList = null;
        List<PostgresJobExecutionModel> postgresJobExecutionModelOptional =
                jobExecutionRepository.findByJobInstanceIdOrderById((jobInstance.getInstanceId()));
        if (!postgresJobExecutionModelOptional.isEmpty()) {
            int position = 0;
            int size = postgresJobExecutionModelOptional.size();
            if (postgresJobExecutionModelOptional.size() >= 2)
                position = size - 2;
            while (position >= 0) {
                CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
                CriteriaQuery<PostgresStepExecutionModel> criteriaQuery = criteriaBuilder.createQuery(PostgresStepExecutionModel.class);
                Root<PostgresStepExecutionModel> postgresStepExecutionModelRoot = criteriaQuery.from(PostgresStepExecutionModel.class);
                criteriaQuery.select(postgresStepExecutionModelRoot);
                Predicate c1;
                Predicate c2;
                Predicate finalPredicate;
                c1 = criteriaBuilder.equal(postgresStepExecutionModelRoot.get(PostgresStepExecutionModel.Fields.jobExecutionId), Long.parseLong(postgresJobExecutionModelOptional.get(position).getId()));
                c2 = criteriaBuilder.equal(postgresStepExecutionModelRoot.get(PostgresStepExecutionModel.Fields.stepName), stepName);
                finalPredicate = criteriaBuilder.and(c1, c2);
                criteriaQuery.where(finalPredicate);
                criteriaQuery.orderBy(criteriaBuilder.desc(postgresStepExecutionModelRoot.get(PostgresStepExecutionModel.Fields.id)));
                TypedQuery<PostgresStepExecutionModel> query = entityManager.createQuery(criteriaQuery);
                postgresStepExecutionModelList = query.getResultList();

                if (postgresStepExecutionModelList != null && !postgresStepExecutionModelList.isEmpty())
                    break;
                else
                    position--;
            }
            if (CollectionUtils.isEmpty(postgresStepExecutionModelList)) {
                return null;
            } else {
                PostgresStepExecutionModel postgresStepExecutionModel = postgresStepExecutionModelList.get(postgresStepExecutionModelList.size() - 1);
                if (postgresStepExecutionModelList.size() > 1) {
                    throw new IllegalStateException("There must be at most one latest job execution");
                }

                JobExecution jobExecution = mapJobExecution(jobInstance, postgresStepExecutionModel);
                return mapStepExecution(postgresStepExecutionModel, jobExecution);
            }

        } else {
            return latest;
        }
    }

    private JobExecution mapJobExecution(JobInstance jobInstance, PostgresStepExecutionModel postgresStepExecutionMapperBean) {
        //step execution coming once
        if (postgresStepExecutionMapperBean == null) {
            return null;
        }
        Long id = postgresStepExecutionMapperBean.getJobExecutionId();
        JobExecution jobExecution;
        if (jobInstance == null) {
            jobExecution = new JobExecution(id);
        } else {
            JobParameters jobParameters = getJobParameters(jobInstance.getId());
            jobExecution = new JobExecution(jobInstance, id, jobParameters, null);
        }
        jobExecution.setStartTime(postgresStepExecutionMapperBean.getStartTime());
        jobExecution.setEndTime(postgresStepExecutionMapperBean.getEndTime());
        jobExecution.setStatus(BatchStatus.valueOf(postgresStepExecutionMapperBean.getStatus()));
        jobExecution.setExitStatus(
                new ExitStatus((postgresStepExecutionMapperBean.getExitCode()), postgresStepExecutionMapperBean.getExitMessage()));
        jobExecution.setLastUpdated(postgresStepExecutionMapperBean.getLastUpdated());
        jobExecution.setVersion(postgresStepExecutionMapperBean.getVersion());
        return jobExecution;
    }

    private StepExecution mapStepExecution(PostgresStepExecutionModel postgresStepExecutionMapperBean, JobExecution jobExecution) {
        if (postgresStepExecutionMapperBean == null) {
            return null;
        }
        StepExecution stepExecution = new StepExecution((postgresStepExecutionMapperBean.getStepName()), jobExecution,
                commonUtility.convertStringToLong(postgresStepExecutionMapperBean.getId()));
        stepExecution.setStartTime(postgresStepExecutionMapperBean.getStartTime());
        stepExecution.setEndTime(postgresStepExecutionMapperBean.getEndTime());
        stepExecution.setStatus(BatchStatus.valueOf(postgresStepExecutionMapperBean.getStatus()));
        stepExecution.setCommitCount(postgresStepExecutionMapperBean.getCommitCount());
        stepExecution.setReadCount(postgresStepExecutionMapperBean.getReadCount());
        stepExecution.setFilterCount(postgresStepExecutionMapperBean.getFilterCount());
        stepExecution.setWriteCount(postgresStepExecutionMapperBean.getWriteCount());
        stepExecution.setExitStatus(
                new ExitStatus(postgresStepExecutionMapperBean.getExitCode(), postgresStepExecutionMapperBean.getExitMessage()));
        stepExecution.setReadSkipCount(postgresStepExecutionMapperBean.getReadSkipCount());
        stepExecution.setWriteSkipCount(postgresStepExecutionMapperBean.getWriteSkipCount());
        stepExecution.setProcessSkipCount(postgresStepExecutionMapperBean.getProcessSkipCount());
        stepExecution.setRollbackCount(postgresStepExecutionMapperBean.getRollbackCount());
        stepExecution.setLastUpdated(postgresStepExecutionMapperBean.getLastUpdated());
        stepExecution.setVersion(postgresStepExecutionMapperBean.getVersion());
        return stepExecution;
    }

    @Override
    public void addStepExecutions(JobExecution jobExecution) {

        List<PostgresStepExecutionModel> postgresStepExecutionModelList = stepExecutionRepository.findByJobExecutionId(jobExecution.getId());
        postgresStepExecutionModelList.forEach(i -> mapStepExecution(i, jobExecution));
    }

    private void validateStepExecution(StepExecution stepExecution) {
        Assert.notNull(stepExecution, "StepExecution cannot be null.");
        Assert.notNull(stepExecution.getStepName(), "StepExecution step name cannot be null.");
        Assert.notNull(stepExecution.getStartTime(), "StepExecution start time cannot be null.");
        Assert.notNull(stepExecution.getStatus(), "StepExecution status cannot be null.");
    }

    @Override
    public void afterPropertiesSet() throws Exception {

    }
}
