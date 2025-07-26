package com.p3.batchframework.persistence.repository;

import com.p3.batchframework.persistence.models.BackgroundJobEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BackgroundJobRepository extends JpaRepository<BackgroundJobEntity, String> {}
