package com.p3.batchframework.persistence.repository;

import com.p3.batchframework.persistence.models.ProgressDetailsEntity;
import com.p3.batchframework.persistence.models.SequencesEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SequenceRepository extends JpaRepository<SequencesEntity, String> {}
