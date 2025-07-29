package com.p3.batchframework.custom_configuration.writer;

import lombok.NonNull;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;

public abstract class AbstractItemWriterHandler<T> implements CustomItemWriter<T> {
  @Override
  public void write(@NonNull Chunk<? extends T> chunk) {}

  @Override
  public void open(@NonNull ExecutionContext executionContext) throws ItemStreamException {
    CustomItemWriter.super.open(executionContext);
  }

  @Override
  public void update(@NonNull ExecutionContext executionContext) throws ItemStreamException {
    CustomItemWriter.super.update(executionContext);
  }

  @Override
  public void close() throws ItemStreamException {
    CustomItemWriter.super.close();
  }
}
