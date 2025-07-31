package com.p3.batchframework.persistence.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@Table(name = "background_job")
public class BackgroundJobEntity extends BaseEntity implements Serializable {
  private String name;

  private String description;

  @Enumerated(EnumType.STRING)
  private BGStatus status;

  private String type;

  private String downloadStatus;

  @Temporal(TemporalType.TIMESTAMP)
  private LocalDateTime startTime;

  @JdbcTypeCode(SqlTypes.BINARY)
  @Lob
  private byte[] jobInput;

  @JdbcTypeCode(SqlTypes.BINARY)
  @Lob
  private byte[] message;

  @Column
  @JdbcTypeCode(SqlTypes.BINARY)
  @Lob
  private byte[] downloadMessage;

  @JsonIgnore
  @JdbcTypeCode(SqlTypes.BINARY)
  @Lob
  private byte[] outputFile;

  @ElementCollection
  @CollectionTable(
      name = "job_execution_ids",
      joinColumns = @JoinColumn(name = "background_job_id"))
  @Column(name = "execution_id")
  @Builder.Default
  private List<Long> executionIds = new ArrayList<>();
}
