package com.p3.batchframework.persistence.repository;

import com.p3.batchframework.persistence.models.SequencesEntity;
import com.p3.batchframework.persistence.models.StepExecutionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface StepExecutionRepository extends JpaRepository<StepExecutionEntity  , String> {}
