package com.p3.batchframework.process_flow.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.p3.batchframework.job_execution_service.bean.ConnectionInputBean;
import com.p3.batchframework.persistence.models.BGStatus;
import com.p3.batchframework.persistence.models.BackgroundJobEntity;
import com.p3.batchframework.persistence.repository.BackgroundJobEntityRepository;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class BackGroundJobService {
  private final BackgroundJobEntityRepository backgroundJobEntityRepository;

  private final ObjectMapper objectMapper = new ObjectMapper();

  public String initiateBackgroundJob(ConnectionInputBean connectionInputBean)
      throws JsonProcessingException {
    BackgroundJobEntity build =
        BackgroundJobEntity.builder()
            .name("BG_BATCH_JOB" + new Date().getTime())
            .status(BGStatus.DRAFT)
            .type("DATA_EXPORT")
            .jobInput(
                objectMapper
                    .writeValueAsString(connectionInputBean)
                    .getBytes(StandardCharsets.UTF_8))
            .build();
    backgroundJobEntityRepository.save(build);
    return "Background job triggered successfully";
  }

  public BGStatus getStatus(String id) {
    return backgroundJobEntityRepository
        .findById(id)
        .map(BackgroundJobEntity::getStatus)
        .orElseThrow(() -> new RuntimeException("unable to find background job with id " + id));
  }
}
