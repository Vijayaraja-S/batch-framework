package com.p3.batchframework.listners;

import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class StepListener implements StepExecutionListener {
  @Override
  public void beforeStep(@NonNull StepExecution stepExecution) {
    log.info("In stepResultListener beforeStep");
  }

  @Override
  public ExitStatus afterStep(StepExecution stepExecution) {
    ExecutionContext jobContext = stepExecution.getJobExecution().getExecutionContext();
    ExecutionContext stepContext = stepExecution.getExecutionContext();

    if (!stepExecution.getStepName().equalsIgnoreCase("partitionedMasterStep")) {
      String tableName = stepContext.getString("currentTable");
      int recordCount = stepContext.getInt("recordsProcessedFor_" + tableName, 0);
      String backgroundJobId = stepContext.getString("backgroundJobId");
      jobContext.putInt("recordsProcessedFor_" + tableName, recordCount);
      jobContext.putString("backgroundJobId", backgroundJobId);
    }

    log.info("In stepResultListener afterStep");
    List<Throwable> exceptions = stepExecution.getFailureExceptions();
    log.info((String) stepExecution.getExecutionContext().get("message"));
    if (exceptions.isEmpty()) {
      return ExitStatus.COMPLETED;
    } else {
      exceptions.forEach(th -> log.info("Exception has occurred in job"));
      return ExitStatus.FAILED;
    }
  }
}
