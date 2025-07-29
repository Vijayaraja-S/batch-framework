package com.p3.batchframework.daos;

import static com.p3.batchframework.utils.Constants.DOT_ESCAPE_STRING;
import static com.p3.batchframework.utils.Constants.DOT_STRING;

import com.p3.batchframework.persistence.models.JobInstanceEntity;
import com.p3.batchframework.persistence.models.SequencesEntity;
import com.p3.batchframework.persistence.repository.JobInstanceRepository;
import com.p3.batchframework.persistence.repository.SequenceRepository;
import com.p3.batchframework.utils.CommonUtility;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;

@Slf4j
public abstract class AbstractDao {
  private final SequenceRepository sequencesRepository;
  private final JobInstanceRepository jobInstanceRepository;
  private final CommonUtility commonUtility;

  protected AbstractDao(
      SequenceRepository sequencesRepository,
      JobInstanceRepository jobInstanceRepository,
      CommonUtility commonUtility) {
    this.sequencesRepository = sequencesRepository;
    this.jobInstanceRepository = jobInstanceRepository;
    this.commonUtility = commonUtility;
  }

  public Long getNextId(String name) {
    Optional<SequencesEntity> sequencesOptional = sequencesRepository.findByName(name);
    SequencesEntity sequences;
    if (sequencesOptional.isPresent()) {
      sequences = sequencesOptional.get();
      sequences.setValue(sequences.getValue() + 1);
    } else {
      sequences = SequencesEntity.builder().name(name).value(1L).build();
    }
    return sequencesRepository.save(sequences).getValue();
  }

  public JobParameters getJobParameters(Long jobInstanceId) {
    Optional<JobInstanceEntity> postgresJobInstanceOptional =
        jobInstanceRepository.findById(commonUtility.convertLongToString(jobInstanceId));
    JobInstanceEntity postgresJobInstanceModel;
    if (postgresJobInstanceOptional.isPresent()) {
      postgresJobInstanceModel = postgresJobInstanceOptional.get();
    } else {
      return null;
    }

    Map<String, ?> jobParamMap =
        commonUtility.convertByteArrayToMap(postgresJobInstanceModel.getJobParameters());
    if (jobParamMap != null) {
      Map<String, JobParameter<?>> map = new HashMap<>(jobParamMap.size());
      for (Map.Entry<String, ?> entry : jobParamMap.entrySet()) {
        Object param = entry.getValue();
        String key = entry.getKey().replace(DOT_ESCAPE_STRING, DOT_STRING);
        if (param instanceof String) {
          map.put(key, new JobParameter<>((String) param, String.class, true));
        } else if (param instanceof Long) {
          map.put(key, new JobParameter<>((Long) param, Long.class, true));
        } else if (param instanceof Double) {
          map.put(key, new JobParameter<>((Double) param, Double.class, true));
        } else if (param instanceof LocalDateTime) {
          map.put(key, new JobParameter<>((LocalDateTime) param, LocalDateTime.class, true));
        } else {
          map.put(key, null);
        }
      }
      return new JobParameters(map);
    }
    return null;
  }
}
