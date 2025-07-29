package com.p3.batchframework.persistence.repository;

import com.p3.batchframework.persistence.models.BGStatus;
import com.p3.batchframework.persistence.models.BackgroundJobEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface BackgroundJobEntityRepository extends JpaRepository<BackgroundJobEntity, String> {
    @Query("select b from BackgroundJobEntity b where b.status = ?1")
    List<BackgroundJobEntity> findByStatus(BGStatus status);
}
