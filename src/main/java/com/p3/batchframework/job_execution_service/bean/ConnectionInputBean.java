package com.p3.batchframework.job_execution_service.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class ConnectionInputBean implements Serializable {
  private String username;
  private String password;
  private String port;
  private String host;
  private String database;
  private String outputPath;
  @Builder.Default private List<String> tableslist = new ArrayList<>();
}
