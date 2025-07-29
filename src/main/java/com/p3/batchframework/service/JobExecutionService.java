package com.p3.batchframework.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.p3.batchframework.operations.CustomJobOperator;
import com.p3.batchframework.persistence.models.BGStatus;
import com.p3.batchframework.persistence.models.BackgroundJobEntity;
import com.p3.batchframework.persistence.repository.BackgroundJobEntityRepository;
import com.p3.batchframework.service.bean.ConnectionInputBean;
import java.util.List;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class JobExecutionService {
  private final BackgroundJobEntityRepository backgroundJobEntityRepository;
  private final CustomJobOperator customJobOperator;

  private final ObjectMapper objectMapper = new ObjectMapper();

  public List<BackgroundJobEntity> findReadyStateJobs() {
    return backgroundJobEntityRepository.findByStatus(BGStatus.DRAFT);
  }

  public Long initJob(BackgroundJobEntity backgroundJobEntity)
      throws JobInstanceAlreadyExistsException, JobParametersInvalidException {
    ConnectionInputBean inputBean = getInputBean(backgroundJobEntity);
    Properties properties = prepareJobParameter(inputBean, backgroundJobEntity.getId());
    return customJobOperator.start("partitionJob", properties);
  }

  private Properties prepareJobParameter(ConnectionInputBean inputBean, String id) {
    Properties properties = new Properties();
    properties.put("username", inputBean.getUsername());
    properties.put("password", inputBean.getPassword());
    properties.put("port", inputBean.getPort());
    properties.put("host", inputBean.getHost());
    properties.put("database", inputBean.getDatabase());
    properties.put("backgroundJobId", id);
    return properties;
  }

  private ConnectionInputBean getInputBean(BackgroundJobEntity backgroundJobEntity) {
    return objectMapper.convertValue(backgroundJobEntity.getJobInput(), ConnectionInputBean.class);
  }
}
