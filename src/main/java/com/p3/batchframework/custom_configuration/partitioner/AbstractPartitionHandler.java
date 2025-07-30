package com.p3.batchframework.custom_configuration.partitioner;

import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;

@Slf4j
public abstract class AbstractPartitionHandler implements CustomPartitioner {
  /** sample partition with based on grid size */
  @Override
  public @NonNull Map<String, ExecutionContext> partition(int gridSize) {
    Map<String, ExecutionContext> result = new HashMap<>();
    int range = 10000 / gridSize;
    int start = 0;
    int end = range - 1;

    for (int i = 0; i < gridSize; i++) {
      ExecutionContext context = new ExecutionContext();
      context.putInt("startIndex", start);
      context.putInt("endIndex", end);
      result.put("partition" + i, context);

      start = end + 1;
      end = start + range - 1;
    }
    return result;
  }
}
