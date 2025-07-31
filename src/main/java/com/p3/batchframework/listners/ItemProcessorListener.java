package com.p3.batchframework.listners;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ItemProcessorListener<I, O> implements ItemProcessListener<I, O> {
  @Override
  public void beforeProcess(@NonNull I input) {
    log.info("In itemProcessorResultListener beforeProcess");
  }

  @Override
  public void afterProcess(@NonNull I input, O output) {
    log.info("In itemProcessorResultListener afterProcess");
  }

  @Override
  public void onProcessError(@NonNull I input, @NonNull Exception e) {
    log.error("In itemProcessorResultListener onProcessError");
  }
}
