package com.p3.batchframework.custom_configuration.reader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.p3.batchframework.job_execution_service.bean.ConnectionInputBean;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component("ItemReaderHandler")
public class ItemReaderHandler<T> extends AbstractItemReaderHandler<T> {

  private final ConnectionInputBean inputBean;
  private final String backgroundJobId;
  private JdbcTemplate jdbcTemplate;
  private Iterator<Map<String, Object>> resultIterator;
  private List<String> partitionTables;
  private int currentTableIndex = 0;

  public ItemReaderHandler(
      @Value("#{jobParameters['inputBean']}") String inputBeanString,
      @Value("#{jobParameters['backgroundJobId']}") String backgroundJobId) {

    ObjectMapper objectMapper = new ObjectMapper();
    try {
      this.inputBean = objectMapper.readValue(inputBeanString, ConnectionInputBean.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to parse inputBean job parameter", e);
    }
    this.backgroundJobId = backgroundJobId;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void open(@NonNull ExecutionContext executionContext) throws ItemStreamException {
    this.partitionTables = (List<String>) executionContext.get("partitionTables");
    if (partitionTables == null || partitionTables.isEmpty()) {
      log.warn("No tables found for this partition");
      return;
    }

    DataSource dataSource =
        DataSourceBuilder.create()
            .url(
                "jdbc:postgresql://"
                    + inputBean.getHost()
                    + ":"
                    + inputBean.getPort()
                    + "/"
                    + inputBean.getDatabase())
            .username(inputBean.getUsername())
            .password(inputBean.getPassword())
            .driverClassName("org.postgresql.Driver")
            .build();

    this.jdbcTemplate = new JdbcTemplate(dataSource);

    loadNextTableData();
  }

  private void loadNextTableData() {
    while (currentTableIndex < partitionTables.size()) {
      String table = partitionTables.get(currentTableIndex);
      log.info("Reading table: {}", table);
      List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM " + table);

      if (!results.isEmpty()) {
        this.resultIterator = results.iterator();
        currentTableIndex++;
        return;
      }

      currentTableIndex++;
    }

    this.resultIterator = null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T read() {
    if (resultIterator == null) return null;

    if (resultIterator.hasNext()) {
      return (T) resultIterator.next();
    } else {
      loadNextTableData();
      return read();
    }
  }

  @Override
  public void update(@NonNull ExecutionContext executionContext) throws ItemStreamException {
    log.info("Updating execution context...");
    executionContext.put("lastProcessedTableIndex", currentTableIndex);
  }

  @Override
  public void close() throws ItemStreamException {
    log.info("Closing reader...");
    this.resultIterator = null;
  }
}
