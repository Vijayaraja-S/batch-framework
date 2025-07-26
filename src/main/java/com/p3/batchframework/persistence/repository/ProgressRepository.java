package com.p3.batchframework.persistence.repository;

import com.p3.batchframework.persistence.models.JobInstanceEntity;
import com.p3.batchframework.persistence.models.ProgressDetailsEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProgressRepository extends JpaRepository<ProgressDetailsEntity, String> {}
