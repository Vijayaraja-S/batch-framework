package com.p3.batchframework.persistence.repository;

import com.p3.batchframework.persistence.models.JobExecutionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface JobExecutionRepository extends JpaRepository<JobExecutionEntity, String> {}
