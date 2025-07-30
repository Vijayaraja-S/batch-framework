package com.p3.batchframework.custom_configuration.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.p3.batchframework.job_execution_service.bean.ConnectionInputBean;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component("ItemWriterHandler")
public class ItemWriterHandler<T> extends AbstractItemWriterHandler<T> {

  private final ConnectionInputBean inputBean;
  private final String backgroundJobId;
  private BufferedWriter writer;
  private boolean headerWritten = false;

  public ItemWriterHandler(
          @Value("#{jobParameters['backgroundJobId']}") String backgroundJobId,
          @Value("#{jobParameters['inputBean']}") String inputBeanString) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      this.inputBean = objectMapper.readValue(inputBeanString, ConnectionInputBean.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize input bean", e);
    }
    this.backgroundJobId = backgroundJobId;
  }

  @Override
  public void open(@NonNull ExecutionContext executionContext) throws ItemStreamException {
    try {
      String outputFilePath = inputBean.getOutputPath() + "/" + backgroundJobId + "_output.csv";
      writer = Files.newBufferedWriter(Paths.get(outputFilePath));
    } catch (IOException e) {
      throw new ItemStreamException("Failed to open output file", e);
    }
  }

  @Override
  public void write(@NonNull Chunk<? extends T> chunk) {
    try {
      for (T item : chunk) {
        if (item instanceof Map<?, ?> mapItem) {
          if (!headerWritten) {
            writer.write(String.join(",", (CharSequence) mapItem.keySet()));
            writer.newLine();
            headerWritten = true;
          }
          String row = mapItem.values().stream()
                  .map(val -> val != null ? val.toString() : "")
                  .collect(Collectors.joining(","));
          writer.write(row);
          writer.newLine();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to write chunk to CSV", e);
    }
  }

  @Override
  public void update(@NonNull ExecutionContext executionContext) throws ItemStreamException {
    // You can persist checkpoint info here if needed
  }

  @Override
  public void close() throws ItemStreamException {
    try {
      if (writer != null) {
        writer.close();
      }
    } catch (IOException e) {
      throw new ItemStreamException("Failed to close writer", e);
    }
  }
}

