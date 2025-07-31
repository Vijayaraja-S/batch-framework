package com.p3.batchframework.job_execution_service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.p3.batchframework.job_execution_service.bean.ConnectionInputBean;
import com.p3.batchframework.job_operator.CustomJobOperator;
import com.p3.batchframework.persistence.models.BGStatus;
import com.p3.batchframework.persistence.models.BackgroundJobEntity;
import com.p3.batchframework.persistence.repository.BackgroundJobEntityRepository;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.NoSuchJobException;
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
      throws JobInstanceAlreadyExistsException,
          JobParametersInvalidException,
          NoSuchJobException,
          IOException {
    ConnectionInputBean inputBean = getInputBean(backgroundJobEntity);
    Properties properties = prepareJobParameter(inputBean, backgroundJobEntity.getId());
    return customJobOperator.start("partitionJob", properties);
  }

  private Properties prepareJobParameter(ConnectionInputBean inputBean, String id) {
    Properties properties = new Properties();
    try {
      String inputBeanString = objectMapper.writeValueAsString(inputBean);
      String base64InputBean =
          Base64.getEncoder().encodeToString(inputBeanString.getBytes(StandardCharsets.UTF_8));
      properties.put("inputBean", base64InputBean);
      properties.put("backgroundJobId", id);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error serializing input bean", e);
    }
    return properties;
  }

  private ConnectionInputBean getInputBean(BackgroundJobEntity backgroundJobEntity)
      throws IOException {
    return objectMapper.readValue(backgroundJobEntity.getJobInput(), ConnectionInputBean.class);
  }
}
