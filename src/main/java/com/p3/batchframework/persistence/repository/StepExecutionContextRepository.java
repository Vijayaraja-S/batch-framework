package com.p3.batchframework.persistence.repository;

import com.p3.batchframework.persistence.models.StepExecutionContextEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface StepExecutionContextRepository extends JpaRepository<StepExecutionContextEntity, String> {}
