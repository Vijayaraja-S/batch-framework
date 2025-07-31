package com.p3.batchframework.listners;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ItemReaderListener<I> implements ItemReadListener<I> {
  @Override
  public void beforeRead() {
    log.info("In itemReaderResultListener beforeRead");
  }

  @Override
  public void afterRead(@NonNull I input) {
    log.info("In itemReaderResultListener afterRead");
    String formName = (String) input;
    log.info(formName);
  }

  @Override
  public void onReadError(@NonNull Exception e) {
    log.error("In itemReaderResultListener onReadError");
  }
}
