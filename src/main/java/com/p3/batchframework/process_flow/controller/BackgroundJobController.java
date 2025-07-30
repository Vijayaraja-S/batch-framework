package com.p3.batchframework.process_flow.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.p3.batchframework.job_execution_service.bean.ConnectionInputBean;
import com.p3.batchframework.persistence.models.BGStatus;
import com.p3.batchframework.process_flow.Service.BackGroundJobService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController("/app/background-job")
@RequiredArgsConstructor
public class BackgroundJobController {
  private final BackGroundJobService backGroundJobService;

  @PostMapping("/init")
  public ResponseEntity<String> InItBackGroundJob(
      @RequestBody ConnectionInputBean connectionInputBean) throws JsonProcessingException {
    return ResponseEntity.ok(backGroundJobService.initiateBackgroundJob(connectionInputBean));
  }


  @GetMapping("/status/{id}")
  public ResponseEntity<BGStatus> getStatus(@PathVariable String id) {
    return ResponseEntity.ok(backGroundJobService.getStatus(id));
  }
}
