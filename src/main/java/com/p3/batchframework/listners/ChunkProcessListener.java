package com.p3.batchframework.listners;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ChunkProcessListener implements ChunkListener {
  @Override
  public void beforeChunk(@NonNull ChunkContext chunkContext) {
    log.info("In ChunkResultListener beforeChunk");
  }

  @Override
  public void afterChunk(@NonNull ChunkContext chunkContext) {
    log.info("In itemWriterResultListener beforeWrite");
  }

  @Override
  public void afterChunkError(@NonNull ChunkContext chunkContext) {
    log.error("In chunkResultListener afterChunkError");
  }
}
