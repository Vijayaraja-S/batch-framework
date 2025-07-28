package com.p3.batchframework.persistence.repository;

import com.p3.batchframework.persistence.models.JobExecutionEntity;
import com.p3.batchframework.persistence.models.JobInstanceEntity;
import lombok.NonNull;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface JobInstanceRepository extends JpaRepository<JobInstanceEntity, String> {

    @Query("select j from JobInstanceEntity j where j.jobName = ?1")
    List<JobInstanceEntity> findByJobName(@NonNull String jobName);

    List<JobInstanceEntity> findDistinctByJobName(String jobNameKey);

    Optional<JobInstanceEntity> findByJobNameAndJobKey(@NonNull String jobName, String jobKey);
}
