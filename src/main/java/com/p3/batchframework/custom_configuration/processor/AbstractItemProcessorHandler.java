package com.p3.batchframework.custom_configuration.processor;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;

@Slf4j
public abstract  class AbstractItemProcessorHandler<I, O> implements CustomItemProcessor<I, O> {

  @Override
  public O process(@NonNull I item) {
    return null;
  }

  @Override
  public void open(@NonNull ExecutionContext executionContext) throws ItemStreamException {
    CustomItemProcessor.super.open(executionContext);
  }

  @Override
  public void update(@NonNull ExecutionContext executionContext) throws ItemStreamException {
    CustomItemProcessor.super.update(executionContext);
  }

  @Override
  public void close() throws ItemStreamException {
    CustomItemProcessor.super.close();
  }
}
