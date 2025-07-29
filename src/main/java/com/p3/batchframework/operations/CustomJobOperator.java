package com.p3.batchframework.operations;

import static com.p3.batchframework.utils.Constants.*;

import com.p3.batchframework.persistence.models.StepExecutionEntity;
import com.p3.batchframework.persistence.repository.StepExecutionRepository;
import com.p3.batchframework.utils.MapperUtilsClusterTask;
import java.time.LocalDateTime;
import java.util.*;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.converter.JobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.*;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.NoSuchStepException;
import org.springframework.batch.core.step.StepLocator;
import org.springframework.batch.core.step.tasklet.StoppableTasklet;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.support.PropertiesConverter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

@Slf4j
public class CustomJobOperator implements JobOperator, InitializingBean {

  private final JobLauncher jobLauncher;
  private final JobExplorer jobExplorer;
  private final JobRepository jobRepository;
  private final JobParametersConverter jobParametersConverter = new DefaultJobParametersConverter();
  private final Job job;
  private final MapperUtilsClusterTask mapperUtils;
  private final StepExecutionRepository stepExecutionRepository;

  public CustomJobOperator(
      JobLauncher jobLauncher,
      JobExplorer jobExplorer,
      JobRepository jobRepository,
      Job job,
      MapperUtilsClusterTask mapperUtils,
      StepExecutionRepository stepExecutionRepository) {
    this.jobLauncher = jobLauncher;
    this.jobExplorer = jobExplorer;
    this.jobRepository = jobRepository;
    this.job = job;
    this.mapperUtils = mapperUtils;
    this.stepExecutionRepository = stepExecutionRepository;
  }

  @Override
  public @NonNull List<Long> getExecutions(long instanceId) throws NoSuchJobInstanceException {
    JobInstance jobInstance = jobExplorer.getJobInstance(instanceId);
    if (jobInstance == null) {
      throw new NoSuchJobInstanceException(String.format("No job instance with id=%d", instanceId));
    }
    List<Long> list = new ArrayList<>();
    jobExplorer
        .getJobExecutions(jobInstance)
        .forEach(jobExecution -> list.add(jobExecution.getId()));
    return list;
  }

  @Override
  public @NonNull List<Long> getJobInstances(@NonNull String jobName, int start, int count)
      throws NoSuchJobException {
    List<Long> list = new ArrayList<>();
    List<JobInstance> jobInstances = jobExplorer.getJobInstances(jobName, start, count);
    jobInstances.forEach(jobInstance -> list.add(jobInstance.getId()));
    if (list.isEmpty() && !getTaskIds().contains(jobName)) {
      throw new NoSuchJobException(
          (String.format("No such job (either in registry or in historical data): %s", jobName)));
    }
    return list;
  }

  @Override
  public @NonNull Set<Long> getRunningExecutions(@NonNull String jobName)
      throws NoSuchJobException {
    Set<Long> set = new LinkedHashSet<>();
    jobExplorer
        .findRunningJobExecutions(jobName)
        .forEach(jobExecution -> set.add(jobExecution.getId()));
    if (set.isEmpty() && !getTaskIds().contains(jobName)) {
      throw new NoSuchJobException(
          String.format("No such job (either in registry or in historical data): %s", jobName));
    }
    return set;
  }

  @Override
  public @NonNull String getParameters(long executionId) throws NoSuchJobExecutionException {
    JobExecution jobExecution = findExecutionById(executionId);
    Properties properties = jobParametersConverter.getProperties(jobExecution.getJobParameters());
    return PropertiesConverter.propertiesToString(properties);
  }

  @Override
  public @NonNull Long start(@NonNull String jobName, @NonNull Properties properties)
      throws JobInstanceAlreadyExistsException, JobParametersInvalidException {
    log.info("Checking status of job with name={}", jobName);

    JobParameters jobParameters = jobParametersConverter.getJobParameters(properties);

    if (jobRepository.isJobInstanceExists(jobName, jobParameters)) {
      throw new JobInstanceAlreadyExistsException(
          String.format(
              "Cannot start a job instance that already exists with name=%s and parameters={%s}",
              jobName, jobParameters));
    }

    try {
      // this job interface directly injected it will work only one job multiple jobs means use job
      // register
      return jobLauncher.run(job, jobParameters).getId();

    } catch (JobExecutionAlreadyRunningException e) {
      throw new UnexpectedJobExecutionException(
          String.format(ILLEGAL_STATE_MSG, JOB_EXECUTION_ALREADY_RUNNING, jobName, jobParameters),
          e);
    } catch (JobRestartException e) {
      throw new UnexpectedJobExecutionException(
          String.format(ILLEGAL_STATE_MSG, JOB_NOT_RESTARTABLE, jobName, jobParameters), e);
    } catch (JobInstanceAlreadyCompleteException e) {
      throw new UnexpectedJobExecutionException(
          String.format(ILLEGAL_STATE_MSG, JOB_ALREADY_COMPLETE, jobName, jobParameters), e);
    }
  }

  @Override
  public @NonNull Long restart(long executionId)
      throws JobInstanceAlreadyCompleteException,
          NoSuchJobExecutionException,
          JobRestartException,
          JobParametersInvalidException {
    log.info("Checking status of job execution with id={}", executionId);
    JobExecution jobExecution = findExecutionById(executionId);
    String jobName = jobExecution.getJobInstance().getJobName();
    JobParameters parameters = jobExecution.getJobParameters();

    log.info("Attempting to resume job with name={} and parameters={}", jobName, parameters);
    try {
      String task = parameters.getString("TASK");
      if (task != null) {
        //        Task taskObj =
        //            new
        // ObjectMapper().readerFor(Task.class).readValue(parameters.getString("TASK"));
        //        if (taskObj.getTaskDestination().equals("MOBIUS")) {
        //          JobParametersBuilder builder = new JobParametersBuilder();
        //          builder.addJobParameters(parameters);
        //          builder.addDate("date", new Date());
        //          return jobLauncher.run(job, builder.toJobParameters()).getId();
        //        }
      }
      if (jobExecution.getStatus() == BatchStatus.STARTED
          || jobExecution.getStatus() == BatchStatus.STARTING) {
        log.info("Job execution is already running, attempting to stop and restart.");
        stopJobExecution(jobExecution, parameters);
      }
      return jobLauncher.run(job, parameters).getId();
    } catch (JobExecutionAlreadyRunningException e) {
      throw new UnexpectedJobExecutionException(
          String.format(ILLEGAL_STATE_MSG, "job execution already running", jobName, parameters),
          e);
    }
    //    catch (JsonProcessingException e) {
    //      throw new RuntimeException(e);
    //    }
  }

  private void stopJobExecution(JobExecution jobExecution, JobParameters parameters) {
    JobExecution lastExecution = jobRepository.getLastJobExecution(job.getName(), parameters);
    if (lastExecution != null) {
      BatchStatus status = lastExecution.getStatus();
      if (lastExecution.getStatus() == BatchStatus.STOPPING || status.isRunning()) {
        lastExecution.setStatus(BatchStatus.STOPPED);
        lastExecution.setEndTime(LocalDateTime.now());
        jobRepository.update(lastExecution);
      }
    }
    List<StepExecutionEntity> jobExecutionId =
        stepExecutionRepository.findAllJobExecutionId(jobExecution.getId());
    if (jobExecutionId != null) {
      for (StepExecutionEntity postgresStepExecutionModel : jobExecutionId) {
        if (postgresStepExecutionModel.getStatus().equals(BatchStatus.STARTED.name())
            || postgresStepExecutionModel.getStatus().equals(BatchStatus.STARTING.name())) {
          postgresStepExecutionModel.setStatus(BatchStatus.STOPPED.name());
          stepExecutionRepository.save(postgresStepExecutionModel);
        }
      }
    }
  }

  @Override
  public Long startNextInstance(String jobName)
      throws NoSuchJobException,
          JobParametersNotFoundException,
          JobExecutionAlreadyRunningException,
          JobInstanceAlreadyCompleteException,
          UnexpectedJobExecutionException,
          JobParametersInvalidException {
    log.info("Locating parameters for next instance of job with name=" + jobName);

    JobParameters parameters =
        new JobParametersBuilder(jobExplorer).getNextJobParameters(job).toJobParameters();

    try {
      return jobLauncher.run(job, parameters).getId();
    } catch (JobExecutionAlreadyRunningException e) {
      throw new UnexpectedJobExecutionException(
          String.format(ILLEGAL_STATE_MSG, "job already running", jobName, parameters), e);
    } catch (JobRestartException e) {
      throw new UnexpectedJobExecutionException(
          String.format(ILLEGAL_STATE_MSG, "job not restartable", jobName, parameters), e);
    } catch (JobInstanceAlreadyCompleteException e) {
      throw new UnexpectedJobExecutionException(
          String.format(ILLEGAL_STATE_MSG, "job instance already complete", jobName, parameters),
          e);
    }
  }

  @Override
  public boolean stop(long executionId)
      throws NoSuchJobExecutionException, JobExecutionNotRunningException {
    JobExecution jobExecution = findExecutionById(executionId);
    // Indicate the execution should be stopped by setting it's status to
    // 'STOPPING'. It is assumed that
    // the step implementation will check this status at chunk boundaries.
    BatchStatus status = jobExecution.getStatus();
    if (!(status == BatchStatus.STARTED || status == BatchStatus.STARTING)) {
      throw new JobExecutionNotRunningException(
          "JobExecution must be running so that it can be stopped: " + jobExecution);
    }
    jobExecution.setStatus(BatchStatus.STOPPING);
    jobRepository.update(jobExecution);

    if (job
        instanceof
        StepLocator) { // can only process as StepLocator is the only way to get the step object
      // get the current stepExecution
      for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
        if (stepExecution.getStatus().isRunning()) {
          try {
            // have the step execution that's running -> need to 'stop' it
            Step step = ((StepLocator) job).getStep(stepExecution.getStepName());
            if (step instanceof TaskletStep) {
              Tasklet tasklet = ((TaskletStep) step).getTasklet();
              StepSynchronizationManager.register(stepExecution);
              ((StoppableTasklet) tasklet).stop();

              StepSynchronizationManager.release();
            }
          } catch (NoSuchStepException e) {
            log.warn("Step not found", e);
          }
        }
      }
    }
    return true;
  }

  @SneakyThrows
  @Override
  public String getSummary(long executionId) throws NoSuchJobExecutionException {
    JobExecution jobExecution = findExecutionById(executionId);
    JobParameters parameters = jobExecution.getJobParameters();
    String taskId = parameters.getString(TaskConstants.TASKID);
    Optional<Task> taskOptional = taskRepository.findById(taskId);
    if (taskOptional.isEmpty()) {
      throw new NoSuchJobExecutionException("Task not found");
    }
    Map<String, TaskObject> taskObjectMap = taskOptional.get().getTaskObjectList();
    TaskObject taskObjectInfo = taskObjectMap.get("Process");
    if (taskObjectInfo.getTaskObjectDetails().equals("EXTRACTION")
        || taskObjectInfo.getTaskObjectDetails().equals("CHARACTER_ANALYSIS")
        || taskObjectInfo.getTaskObjectDetails().equals("EDB_INGESTION")
        || taskObjectInfo.getTaskObjectDetails().equals("ARCHON_ARCHIVAL_INGESTION")) {
      AdditionalInfo additionalInfo = taskOptional.get().getAdditionalInfo();
      List<Long> executionIds = new ArrayList<>();
      if (additionalInfo.getExecutionId() != null)
        executionIds.addAll(additionalInfo.getExecutionId());
      executionIds.add(executionId);
      TaskMapperBean taskMapperBean = mapperUtils.map(taskOptional.get(), TaskMapperBean.class);
      try {
        reportSummary.generateReportSummary(taskMapperBean, executionIds, jobExecution);
      } catch (Exception ex) {
        log.error("Unexpected error Occurred: {}", ex.getMessage());
        throw new Exception(ex);
      }
    } else if (taskObjectInfo.getTaskObjectDetails().equals("PRE_ANALYSIS")
        || taskObjectInfo.getTaskObjectDetails().equals("RE_ANALYSIS")) {
      AdditionalInfo additionalInfo = taskOptional.get().getAdditionalInfo();
      List<Long> executionIds = new ArrayList<>();
      if (additionalInfo.getExecutionId() != null)
        executionIds.addAll(additionalInfo.getExecutionId());
      executionIds.add(executionId);
      TaskMapperBean taskMapperBean = mapperUtils.map(taskOptional.get(), TaskMapperBean.class);
      try {
        preAnalysisReportSummary.generatePreAnalysisReportSummary(
            taskMapperBean, executionIds, jobExecution);
      } catch (Exception ex) {
        log.error("Unexpected error Occurred: {}", ex.getMessage());
        throw new Exception(ex);
      }
    }
    return jobExecution.toString();
  }

  @Override
  public Map<Long, String> getStepExecutionSummaries(long executionId)
      throws NoSuchJobExecutionException {
    JobExecution jobExecution = findExecutionById(executionId);

    Map<Long, String> map = new LinkedHashMap<>();
    jobExecution
        .getStepExecutions()
        .forEach(stepExecution -> map.put(stepExecution.getId(), stepExecution.toString()));
    return map;
  }

  @Override
  public Set<String> getJobNames() {
    return Set.of();
  }

  @Override
  public JobExecution abandon(long jobExecutionId)
      throws NoSuchJobExecutionException, JobExecutionAlreadyRunningException {
    JobExecution jobExecution = findExecutionById(jobExecutionId);

    if (jobExecution.getStatus().isLessThan(BatchStatus.STOPPING)) {
      throw new JobExecutionAlreadyRunningException(
          "JobExecution is running or complete and therefore cannot be aborted");
    }

    log.info("Aborting job execution: " + jobExecution);
    jobExecution.upgradeStatus(BatchStatus.ABANDONED);
    jobExecution.setEndTime(LocalDateTime.now());
    jobRepository.update(jobExecution);

    return jobExecution;
  }

  public JobExecution findExecutionById(long executionId) throws NoSuchJobExecutionException {
    JobExecution jobExecution = jobExplorer.getJobExecution(executionId);
    if (jobExecution == null) {
      throw new NoSuchJobExecutionException("No JobExecution found for id: [" + executionId + "]");
    }
    return jobExecution;
  }

  @Override
  public void afterPropertiesSet() {
    Assert.notNull(jobLauncher, "JobLauncher must be provided");
    Assert.notNull(jobExplorer, "JobExplorer must be provided");
    Assert.notNull(jobRepository, "JobRepository must be provided");
  }

  public List<String> getTaskIds() {
    return null;
  }
}
