package com.p3.batchframework.listners;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ItemWriterListener<T> implements ItemWriteListener<T> {

  @Override
  public void beforeWrite(Chunk<? extends T> items) {
    log.info("About to write {} items", items.size());
  }

  @Override
  public void afterWrite(Chunk<? extends T> items) {
    log.info("Successfully wrote {} items", items.size());
  }

  @Override
  public void onWriteError(@NonNull Exception exception, Chunk<? extends T> items) {
    log.error(
        "Error while writing items: {}. Exception: {}",
        items.getItems(),
        exception.getMessage(),
        exception);
  }
}
