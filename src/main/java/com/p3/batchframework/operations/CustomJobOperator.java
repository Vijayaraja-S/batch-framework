package com.p3.batchframework.operations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.p3solutions.common_beans_dto.job.enums.JobPriority;
import com.p3solutions.taskhandler.TaskConstants;
import com.p3solutions.taskhandler.beans.AdditionalInfo;
import com.p3solutions.taskhandler.beans.TaskObject;
import com.p3solutions.taskhandler.configuration.ArchonCustomThrottleSupport;
import com.p3solutions.taskhandler.daos.models.PostgresStepExecutionModel;
import com.p3solutions.taskhandler.daos.repositories.StepExecutionRepository;
import com.p3solutions.taskhandler.job.JobExecutorRegistry;
import com.p3solutions.taskhandler.models.Task;
import com.p3solutions.taskhandler.repositories.TaskRepository;
import com.p3solutions.taskhandler.services.PreAnalysisReportSummary;
import com.p3solutions.taskhandler.services.ReportSummary;
import com.p3solutions.taskhandler.task.TaskMapperBean;
import com.p3solutions.taskhandler.utility.CommonUtility;
import com.p3solutions.taskhandler.utility.MapperUtilsClusterTask;
import java.util.*;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.Assert;


@Slf4j
public class CustomJobOperator implements JobOperator, InitializingBean {

  private static final String ILLEGAL_STATE_MSG =
      "Illegal state (only happens on a race condition): " + "%s with name=%s and parameters=%s";

  private JobLauncher jobLauncher;
  private JobExplorer jobExplorer;
  private JobRepository jobRepository;
  private JobParametersConverter jobParametersConverter;
  private Job job;

  private TaskRepository taskRepository;

  @Autowired(required = false)
  private PreAnalysisReportSummary preAnalysisReportSummary;

  @Autowired(required = false)
  private ReportSummary reportSummary;

  @Autowired private MapperUtilsClusterTask mapperUtils;
  @Autowired private CommonUtility commonUtility;
  @Autowired private StepExecutionRepository stepExecutionRepository;
  @Autowired private JobExecutorRegistry registry;

  @Value(value = "${maxCoreSize:2}")
  private String maxCoreSize;

  public CustomJobOperator(
      JobLauncher jobLauncher,
      JobExplorer jobExplorer,
      JobRepository jobRepository,
      JobParametersConverter jobParametersConverter,
      Job job) {
    this.jobLauncher = jobLauncher;
    this.jobExplorer = jobExplorer;
    this.jobRepository = jobRepository;
    this.jobParametersConverter = jobParametersConverter;
    this.job = job;
  }

  @Override
  public List<Long> getExecutions(long instanceId) throws NoSuchJobInstanceException {
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
  public List<Long> getJobInstances(String jobName, int start, int count)
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
  public Set<Long> getRunningExecutions(String jobName) throws NoSuchJobException {
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
  public String getParameters(long executionId) throws NoSuchJobExecutionException {
    JobExecution jobExecution = findExecutionById(executionId);
    return PropertiesConverter.propertiesToString(
        jobParametersConverter.getProperties(jobExecution.getJobParameters()));
  }

  public JobParameters getJobParameters(String parameters) {
    List<String> params = Arrays.asList(parameters.split("\\|\\|~#-\\$Archon-ETL#-\\$~\\|\\|"));
    Map<String, JobParameter> map = new HashMap<>();
    params.forEach(
        i ->
            map.put(
                i.substring(0, i.indexOf("=")), new JobParameter(i.substring(i.indexOf("=") + 1))));
    return new JobParameters(map);
  }

  @SneakyThrows
  @Override
  public Long start(String jobName, String parameters)
      throws JobInstanceAlreadyExistsException, JobParametersInvalidException {
    log.info("Checking status of job with name=" + jobName);
    JobParameters jobParameters = getJobParameters(parameters);
    if (jobRepository.isJobInstanceExists(jobName, jobParameters)) {
      throw new JobInstanceAlreadyExistsException(
          String.format(
              "Cannot start a job instance that already exists with name=%s and parameters=%s",
              jobName, parameters));
    }
    JobPriority jobPriority = getJobPriority(jobParameters);
    try {
      ArchonCustomThrottleSupport executor = createExecutor(2, jobPriority);
      JobExecution execution = jobLauncher.run(job, jobParameters);
      registry.register(execution.getId(), executor, jobPriority);
      return execution.getId();
    } catch (JobExecutionAlreadyRunningException e) {
      throw new UnexpectedJobExecutionException(
          String.format(ILLEGAL_STATE_MSG, "job execution already running", jobName, parameters),
          e);
    } catch (JobRestartException e) {
      throw new UnexpectedJobExecutionException(
          String.format(ILLEGAL_STATE_MSG, "job not restartable", jobName, parameters), e);
    } catch (JobInstanceAlreadyCompleteException e) {
      throw new UnexpectedJobExecutionException(
          String.format(ILLEGAL_STATE_MSG, "job already complete", jobName, parameters), e);
    }
  }

  private JobPriority getJobPriority(JobParameters jobParameters) {
    try {
      JSONObject obj = new JSONObject(jobParameters.getParameters().get("TASK").toString());
      String priority = obj.getString("jobPriority");
      return JobPriority.valueOf(priority);
    } catch (Exception e) {
      return JobPriority.LOW;
    }
  }

  public ArchonCustomThrottleSupport createExecutor(int threads, JobPriority jobPriority) {
    if (JobPriority.HIGH.equals(jobPriority)) {
      ArchonCustomThrottleSupport executor = new ArchonCustomThrottleSupport();
      Integer lowPriorityJobsConcurrencyCount = registry.getLowPriorityJobsConcurrencyCount();
      if (lowPriorityJobsConcurrencyCount != null && lowPriorityJobsConcurrencyCount != 0) {
        int concurrencyLimit = Integer.parseInt(maxCoreSize) - lowPriorityJobsConcurrencyCount;
        if (concurrencyLimit <= 0) {
          executor.setConcurrencyLimit(1);
        } else {
          executor.setConcurrencyLimit(concurrencyLimit);
        }
      } else {
        executor.setConcurrencyLimit(Integer.parseInt(maxCoreSize));
      }
      return executor;
    } else {
      ArchonCustomThrottleSupport executor = new ArchonCustomThrottleSupport();
      executor.setConcurrencyLimit(threads);
      return executor;
    }
  }

  @Override
  public Long restart(long executionId)
      throws JobInstanceAlreadyCompleteException,
          NoSuchJobExecutionException,
          NoSuchJobException,
          JobRestartException,
          JobParametersInvalidException {
    log.info("Checking status of job execution with id=" + executionId);
    JobExecution jobExecution = findExecutionById(executionId);
    String jobName = jobExecution.getJobInstance().getJobName();
    JobParameters parameters = jobExecution.getJobParameters();
    JobPriority jobPriority = getJobPriority(parameters);
    ArchonCustomThrottleSupport executor = createExecutor(2, jobPriority);
    registry.register(executionId + 1, executor, jobPriority);
    log.info(
        String.format(
            "Attempting to resume job with name=%s and parameters=%s", jobName, parameters));
    try {
      String task = parameters.getString("TASK");
      if (task != null) {
        Task taskObj =
            new ObjectMapper().readerFor(Task.class).readValue(parameters.getString("TASK"));
        if (taskObj.getTaskDestination().equals("MOBIUS")) {
          JobParametersBuilder builder = new JobParametersBuilder();
          builder.addJobParameters(parameters);
          builder.addDate("date", new Date());
          return jobLauncher.run(job, builder.toJobParameters()).getId();
        }
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
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private void stopJobExecution(JobExecution jobExecution, JobParameters parameters) {
    JobExecution lastExecution = jobRepository.getLastJobExecution(job.getName(), parameters);
    if (lastExecution != null) {
      BatchStatus status = lastExecution.getStatus();
      if (lastExecution.getStatus() == BatchStatus.STOPPING || status.isRunning()) {
        lastExecution.setStatus(BatchStatus.STOPPED);
        lastExecution.setEndTime(new Date());
        jobRepository.update(lastExecution);
      }
    }
    List<PostgresStepExecutionModel> jobExecutionId =
        stepExecutionRepository.findAllJobExecutionId(jobExecution.getId());
    if (jobExecutionId != null) {
      for (PostgresStepExecutionModel postgresStepExecutionModel : jobExecutionId) {
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
    return new TreeSet<>( // jobRegistry.getJobNames()
        getTaskIds());
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
    jobExecution.setEndTime(new Date());
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
  public void afterPropertiesSet() throws Exception {
    Assert.notNull(jobLauncher, "JobLauncher must be provided");
    Assert.notNull(jobExplorer, "JobExplorer must be provided");
    Assert.notNull(jobRepository, "JobRepository must be provided");
  }

  public List<String> getTaskIds() {
    return taskRepository.findAll().stream().map(Task::getId).collect(Collectors.toList());
  }
}
