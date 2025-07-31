package com.p3.batchframework.custom_configuration.partitioner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.p3.batchframework.job_execution_service.bean.ConnectionInputBean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;

@Slf4j
public class PartitionHandler extends AbstractPartitionHandler {
  private final ConnectionInputBean inputBean;
  private final String backgroundJobId;

  public PartitionHandler(
          @Value("#{jobParameters['inputBean']}") String inputBeanString,
          @Value("#{jobParameters['backgroundJobId']}") String backgroundJobId) {

    ObjectMapper objectMapper = new ObjectMapper();
    try {
      this.inputBean = objectMapper.readValue(inputBeanString, ConnectionInputBean.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to parse inputBean job parameter", e);
    }
    this.backgroundJobId = backgroundJobId;
  }

  @Override
  public @NonNull Map<String, ExecutionContext> partition(int gridSize) {
    Map<String, ExecutionContext> result = new HashMap<>();

    List<String> tables = inputBean.getTableslist();
    int totalTables = tables.size();
    int partitionSize = (int) Math.ceil((double) totalTables / gridSize);

    for (int i = 0; i < gridSize; i++) {
      int start = i * partitionSize;
      int end = Math.min(start + partitionSize, totalTables);

      if (start >= end) break;

      List<String> partitionTables = new ArrayList<>(tables.subList(start, end));

      ExecutionContext context = new ExecutionContext();
      context.put("partitionTables", partitionTables);
      context.put("backgroundJobId", backgroundJobId);
      context.put("partitionSize", partitionSize);
      result.put("partition" + i, context);
    }

    return result;
  }

}
