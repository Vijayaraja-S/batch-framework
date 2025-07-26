package com.p3.batchframework.daos;


import com.p3.batchframework.persistence.repository.JobInstanceRepository;
import com.p3.batchframework.persistence.repository.SequenceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractDao {
    private final SequenceRepository sequencesRepository;
    private final JobInstanceRepository jobInstanceRepository;
    private final CommonUtility commonUtility;

    Long getNextId(String name) {

        Optional<Sequences> sequencesOptional = sequencesRepository.findByName(name);
        Sequences sequences;
        if (sequencesOptional.isPresent()) {
            sequences = sequencesOptional.get();
            sequences.setValue(sequences.getValue() + 1);
        } else {
            sequences = Sequences.builder().name(name).value(1L).build();
        }
        return sequencesRepository.save(sequences).getValue();
    }

    JobParameters getJobParameters(Long jobInstanceId) {
        Optional<PostgresJobInstanceModel> postgresJobInstanceOptional =
                jobInstanceRepository.findById(commonUtility.convertLongToString(jobInstanceId));
        PostgresJobInstanceModel postgresJobInstanceModel;
        if (postgresJobInstanceOptional.isPresent()) {
            postgresJobInstanceModel = postgresJobInstanceOptional.get();
        } else {
            return null;
        }


        Map<String, ?> jobParamMap = commonUtility.convertByteArrayToMap(postgresJobInstanceModel.getJobParameters());
        if (jobParamMap != null) {
            Map<String, JobParameter> map = new HashMap<>(jobParamMap.size());
            for (Map.Entry<String, ?> entry : jobParamMap.entrySet()) {
                Object param = entry.getValue();
                String key = entry.getKey().replace(DOT_ESCAPE_STRING, DOT_STRING);
                if (param instanceof String) {
                    map.put(key, new JobParameter((String) param));
                } else if (param instanceof Long) {
                    map.put(key, new JobParameter((Long) param));
                } else if (param instanceof Double) {
                    map.put(key, new JobParameter((Double) param));
                } else if (param instanceof Date) {
                    map.put(key, new JobParameter((Date) param));
                } else {
                    map.put(key, null);
                }
            }
            return new JobParameters(map);
        }
        return null;
    }

}
