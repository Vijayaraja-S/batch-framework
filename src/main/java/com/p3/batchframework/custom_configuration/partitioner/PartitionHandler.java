package com.p3.batchframework.custom_configuration.partitioner;

import java.util.Map;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

@Component("PartitionHandler")
@Slf4j
public class PartitionHandler extends AbstractPartitionHandler {


  @Override
  public @NonNull Map<String, ExecutionContext> partition(int gridSize) {
    return super.partition(gridSize);
  }
}
