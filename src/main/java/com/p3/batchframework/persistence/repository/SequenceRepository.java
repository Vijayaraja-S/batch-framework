package com.p3.batchframework.persistence.repository;

import com.p3.batchframework.persistence.models.SequencesEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface SequenceRepository extends JpaRepository<SequencesEntity, String> {
    @Query("select s from SequencesEntity s where s.name = ?1")
    Optional<SequencesEntity> findByName(String name);
}
