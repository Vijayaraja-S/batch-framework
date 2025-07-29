package com.p3.batchframework.service.bean;

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
public class ConnectionInputBean {
  private String username;
  private String password;
  private String port;
  private String host;
  private String database;
  @Builder.Default private List<String> tableslist = new ArrayList<>();
}
