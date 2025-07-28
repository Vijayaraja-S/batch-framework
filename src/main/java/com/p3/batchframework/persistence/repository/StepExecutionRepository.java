package com.p3.batchframework.persistence.repository;

import com.p3.batchframework.persistence.models.StepExecutionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface StepExecutionRepository extends JpaRepository<StepExecutionEntity  , String> {
    @Query("select s from StepExecutionEntity s where s.id = ?1 and s.jobExecutionId = ?2")
    Optional<StepExecutionEntity> findByIdAndJobExecutionId(String s, String s1);

    @Query("select s from StepExecutionEntity s where s.jobExecutionId = ?1")
    List<StepExecutionEntity> findByJobExecutionId(Long id);

    @Query("select s from StepExecutionEntity s")
    List<StepExecutionEntity> findAllJobExecutionId(Long id);
}
