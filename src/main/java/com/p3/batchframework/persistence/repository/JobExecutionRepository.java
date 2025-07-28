package com.p3.batchframework.persistence.repository;

import com.p3.batchframework.persistence.models.JobExecutionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface JobExecutionRepository extends JpaRepository<JobExecutionEntity, String> {
  Optional<JobExecutionEntity> findFirstByJobInstanceIdOrderByIdDesc(Long jobInstanceId);

  List<JobExecutionEntity> findByJobInstanceIdOrderById(long instanceId);
}
