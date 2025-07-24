package com.p3.batchframework.custom_configuration.reader;

import lombok.NonNull;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;

public abstract class AbstractItemReaderHandler<T> implements CustomItemReader<T> {
  @Override
  public T read() {
    return null;
  }

  @Override
  public void open(@NonNull ExecutionContext executionContext) throws ItemStreamException {
    CustomItemReader.super.open(executionContext);
  }

  @Override
  public void update(@NonNull ExecutionContext executionContext) throws ItemStreamException {
    CustomItemReader.super.update(executionContext);
  }

  @Override
  public void close() throws ItemStreamException {
    CustomItemReader.super.close();
  }
}
